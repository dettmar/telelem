defmodule Telelem.Admission.Authority do
  @moduledoc """
  Local grant owner for one instance (TEL-002/TEL-006, M1).

  A GenServer around the pure `Telelem.Admission.Ledger`. Each call or
  message applies at most one ledger transition, commits the ledger to the
  instance directory, and only then replies. That gives one serialisation
  point for:

    * all-or-none `reserve/4` (slot + units, idempotent by op id)
    * `authorize/2` dispatch, `cancel/2`, `settle/3`, `reconcile/3`
    * reservation TTL timers armed through the injected clock
    * **monitored owner death**: a live reserved grant is released, and a
      dispatched grant becomes `:unresolved` (slot freed, spend still encumbered)

  ## Incarnations and fencing

  On `init/1` the authority loads the last committed ledger of **this
  instance life** from the directory and calls `Ledger.next_incarnation/1`.
  That retires every never-dispatched grant at once (bounded recovery: by
  the time `:authority_up` is observed, a fenced reservation holds nothing,
  even if its owner is alive). It then re-monitors the owners of held
  `:dispatched` grants. Those keep their slot and encumbrance until the
  owner's `:DOWN` (→ `:unresolved`), then until `reconcile/3`. A live direct
  owner of fenced dispatched work therefore holds its slot for as long as it
  lives. Instance workers are already gone (`:rest_for_one`), so their
  `:DOWN` arrives immediately.

  Tokens are `{instance_id, incarnation, seq}`. A token from an earlier
  incarnation gets `{:error, :stale_incarnation}`, and one from another
  instance, or from an earlier life of this one, gets
  `{:error, :foreign_instance}`. Neither changes anything, including late
  timers that reach the new pid.

  ## Coverage (M1: deliberately volatile, local and soft)

  The ledger is in memory, owned by the instance supervisor. An instance
  restart, for example by its parent after restart intensity is exceeded,
  starts a **new** instance id with an empty ledger. Exposure from the
  previous life is *unknown*, not zero. There is no WAL, no durable billing
  and no global hard limit in M1 (durable authority is TEL-040).
  `snapshot/1` reports this as `coverage`:

      %{instance_id: hex, ledger: :volatile, covers: :this_instance_life,
        prior_instances: n,          # other instance ids seen in this spool dir
        prior_exposure: :unknown | :no_prior_instance_in_spool}

  `prior_exposure` is never `:none`. A spool with no earlier instance only
  shows that this spool directory saw none.

  ## Observations

  Every process in the directory's subscriber list receives
  `{:telelem_admission, tag, event}`, where `event` is one of:

    * `{:fenced, incarnation, %{released: [grant_id], held: [grant_id]}}`
      (sent just before `:authority_up`)
    * `{:authority_up, pid, incarnation}`
    * `{:owner_down, owner_pid, [{grant_id, :ok | :duplicate | {:error, term}}]}`
    * `{:timer, grant_id, :ok | :duplicate | {:error, term}}`
  """
  use GenServer

  alias Telelem.Admission.Ledger
  alias Telelem.Instance.Directory

  @type grant_id :: Ledger.grant_id()
  @type result :: {:ok, grant_id()} | {:duplicate, grant_id()} | {:error, term()}

  # -- API -------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts),
    do: %{id: Keyword.get(opts, :id, __MODULE__), start: {__MODULE__, :start_link, [opts]}}

  @spec reserve(GenServer.server(), term(), pid(), pos_integer()) :: result()
  def reserve(auth, op_id, owner, units) when is_pid(owner),
    do: GenServer.call(auth, {:reserve, op_id, owner, units})

  @spec authorize(GenServer.server(), grant_id()) :: result()
  def authorize(auth, gid), do: GenServer.call(auth, {:authorize, gid})

  @spec cancel(GenServer.server(), grant_id()) :: result()
  def cancel(auth, gid), do: GenServer.call(auth, {:cancel, gid})

  @spec settle(GenServer.server(), grant_id(), non_neg_integer()) :: result()
  def settle(auth, gid, actual), do: GenServer.call(auth, {:settle, gid, actual})

  @spec reconcile(GenServer.server(), grant_id(), non_neg_integer()) :: result()
  def reconcile(auth, gid, actual), do: GenServer.call(auth, {:reconcile, gid, actual})

  @doc "Ledger observations plus `stale_rejections` and the authority pid."
  @spec snapshot(GenServer.server()) :: map()
  def snapshot(auth), do: GenServer.call(auth, :snapshot)

  # -- server ----------------------------------------------------------------

  @impl true
  def init(opts) do
    table = Keyword.fetch!(opts, :table)
    meta = Directory.meta(table)

    {ledger, fence} =
      (Directory.ledger(table) || Ledger.new(meta.limits, meta.instance_id))
      |> Ledger.next_incarnation()

    # Commit the new incarnation (and the fence) before anything can observe it.
    Directory.put_ledger(table, ledger)

    state = %{
      table: table,
      tag: meta.tag,
      clock: meta.clock,
      ttl: meta.reserve_ttl,
      spool_dir: meta.spool_dir,
      ledger: ledger,
      monitors: %{},
      stale_rejections: 0,
      coverage: nil
    }

    # Re-monitor fenced live owners. A dead owner's :DOWN is queued now, so it
    # is handled before any request that can reach this new pid.
    state =
      Enum.reduce(Ledger.live_owners(ledger), state, fn {owner, gid}, s ->
        track(s, owner, gid)
      end)

    File.mkdir_p!(state.spool_dir)
    iid = Base.encode16(ledger.instance_id, case: :lower)
    state = %{state | coverage: coverage(state.spool_dir, iid)}

    spool(
      state,
      "authority.log",
      "instance=#{iid} incarnation=#{ledger.incarnation} pid=#{inspect(self())}\n"
    )

    Directory.put_authority(table, self(), ledger.incarnation)
    notify(state, {:fenced, ledger.incarnation, fence})
    notify(state, {:authority_up, self(), ledger.incarnation})
    {:ok, state}
  end

  @impl true
  def handle_call({:reserve, op_id, owner, units}, _from, s) do
    case Ledger.reserve(s.ledger, op_id, owner, units) do
      {:ok, gid, l} ->
        s = s |> commit(l) |> track(owner, gid)
        arm_ttl(s, gid)
        {:reply, {:ok, gid}, s}

      other ->
        reply(other, s)
    end
  end

  def handle_call({:authorize, gid}, _from, s),
    do: apply_transition(&Ledger.authorize/2, [gid], s)

  def handle_call({:cancel, gid}, _from, s), do: apply_transition(&Ledger.cancel/2, [gid], s)

  def handle_call({:settle, gid, actual}, _from, s),
    do: apply_transition(&Ledger.settle/3, [gid, actual], s)

  def handle_call({:reconcile, gid, actual}, _from, s),
    do: apply_transition(&Ledger.reconcile/3, [gid, actual], s)

  def handle_call(:snapshot, _from, s) do
    snap =
      s.ledger
      |> Ledger.snapshot()
      |> Map.merge(%{
        stale_rejections: s.stale_rejections,
        authority: self(),
        coverage: s.coverage
      })

    {:reply, snap, s}
  end

  def handle_call(:table, _from, s), do: {:reply, s.table, s}

  @impl true
  def handle_info({:telelem_reservation_ttl, gid}, s) do
    {result, s} = run(&Ledger.expire/2, [gid], s)
    notify(s, {:timer, gid, result})
    {:noreply, s}
  end

  def handle_info({:DOWN, ref, :process, owner, _reason}, s) do
    case Map.pop(s.monitors, owner) do
      {{^ref, gids}, monitors} ->
        s = %{s | monitors: monitors}

        {results, s} =
          gids
          |> Enum.sort()
          |> Enum.map_reduce(s, fn gid, s ->
            {r, s} = run(&Ledger.owner_down/2, [gid], s)
            {{gid, r}, s}
          end)

        notify(s, {:owner_down, owner, results})
        {:noreply, s}

      _ ->
        {:noreply, s}
    end
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # -- internals -------------------------------------------------------------

  defp apply_transition(fun, args, s) do
    {result, s} = run(fun, args, s)
    {:reply, result, s}
  end

  defp run(fun, args, s) do
    case apply(fun, [s.ledger | args]) do
      {:ok, gid, l} ->
        s = commit(s, l)
        g = Ledger.grant(l, gid)
        s = if Ledger.live?(g), do: s, else: untrack(s, g.owner, gid)
        if g.state == :settled, do: receipt(s, g)
        {{:ok, gid}, s}

      {:duplicate, gid, _l} ->
        {{:duplicate, gid}, s}

      {:error, :stale_incarnation, _l} ->
        {{:error, :stale_incarnation}, %{s | stale_rejections: s.stale_rejections + 1}}

      {:error, reason, _l} ->
        {{:error, reason}, s}
    end
  end

  defp reply({:duplicate, gid, _}, s), do: {:reply, {:duplicate, gid}, s}
  defp reply({:error, reason, _}, s), do: {:reply, {:error, reason}, s}

  defp commit(s, l) do
    Directory.put_ledger(s.table, l)
    %{s | ledger: l}
  end

  defp track(s, owner, gid) do
    monitors =
      case Map.fetch(s.monitors, owner) do
        {:ok, {ref, gids}} -> Map.put(s.monitors, owner, {ref, MapSet.put(gids, gid)})
        :error -> Map.put(s.monitors, owner, {Process.monitor(owner), MapSet.new([gid])})
      end

    %{s | monitors: monitors}
  end

  defp untrack(s, owner, gid) do
    case Map.fetch(s.monitors, owner) do
      {:ok, {ref, gids}} ->
        gids = MapSet.delete(gids, gid)

        if MapSet.size(gids) == 0 do
          Process.demonitor(ref, [:flush])
          %{s | monitors: Map.delete(s.monitors, owner)}
        else
          %{s | monitors: Map.put(s.monitors, owner, {ref, gids})}
        end

      :error ->
        s
    end
  end

  defp arm_ttl(%{ttl: nil}, _gid), do: :ok

  defp arm_ttl(%{clock: {mod, arg}, ttl: ttl, table: table}, gid),
    do:
      mod.send_after(
        arg,
        {:telelem_instance_authority, table},
        {:telelem_reservation_ttl, gid},
        ttl
      )

  defp receipt(s, g) do
    spool(
      s,
      "receipts.log",
      "grant=#{inspect(g.id)} op=#{inspect(g.op_id)} units=#{g.units} actual=#{g.actual}\n"
    )
  end

  # What this ledger can and cannot vouch for (see moduledoc).
  defp coverage(spool_dir, iid) do
    prior =
      case File.read(Path.join(spool_dir, "authority.log")) do
        {:ok, log} ->
          Regex.scan(~r/^instance=([0-9a-f]+) /m, log, capture: :all_but_first)
          |> List.flatten()
          |> Enum.uniq()
          |> Enum.reject(&(&1 == iid))

        {:error, _} ->
          []
      end

    %{
      instance_id: iid,
      ledger: :volatile,
      covers: :this_instance_life,
      prior_instances: length(prior),
      prior_exposure: if(prior == [], do: :no_prior_instance_in_spool, else: :unknown)
    }
  end

  defp spool(s, file, line), do: File.write!(Path.join(s.spool_dir, file), line, [:append])

  defp notify(s, event) do
    for pid <- Directory.subscribers(s.table), do: send(pid, {:telelem_admission, s.tag, event})
    :ok
  end
end
