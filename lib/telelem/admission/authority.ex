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

  On `init/1` the authority loads the last committed ledger from the
  directory, increments the incarnation, and re-monitors every owner that
  still holds a slot. Tokens (`{incarnation, seq}`) from earlier incarnations
  get `{:error, :stale_incarnation}` and change nothing, including late timers
  that reach the new pid. A fenced grant leaves the live set only when its
  owner's monitor fires; fenced unresolved spend is resolved only by
  `reconcile/3`.

  Known M1 limit: the directory is in memory, owned by the instance supervisor.
  Stopping the instance discards the ledger (durable authority is TEL-040).

  ## Observations

  Every process in the directory's subscriber list receives
  `{:telelem_admission, tag, event}`, where `event` is one of:

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

    ledger =
      (Directory.ledger(table) || Ledger.new(meta.limits))
      |> Ledger.next_incarnation()

    # Commit the new incarnation before anything can observe it.
    Directory.put_ledger(table, ledger)

    state = %{
      table: table,
      tag: meta.tag,
      clock: meta.clock,
      ttl: meta.reserve_ttl,
      spool_dir: meta.spool_dir,
      ledger: ledger,
      monitors: %{},
      stale_rejections: 0
    }

    # Re-monitor fenced live owners. A dead owner's :DOWN is queued now, so it
    # is handled before any request that can reach this new pid.
    state =
      Enum.reduce(Ledger.live_owners(ledger), state, fn {owner, gid}, s ->
        track(s, owner, gid)
      end)

    File.mkdir_p!(state.spool_dir)

    spool(state, "authority.log", "incarnation=#{ledger.incarnation} pid=#{inspect(self())}\n")

    Directory.put_authority(table, self(), ledger.incarnation)
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
      |> Map.merge(%{stale_rejections: s.stale_rejections, authority: self()})

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

  defp spool(s, file, line), do: File.write!(Path.join(s.spool_dir, file), line, [:append])

  defp notify(s, event) do
    for pid <- Directory.subscribers(s.table), do: send(pid, {:telelem_admission, s.tag, event})
    :ok
  end
end
