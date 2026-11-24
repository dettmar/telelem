defmodule Telelem.AdmissionModelTest do
  @moduledoc """
  TEL-006: the real authority (running inside a `Telelem.Instance`) is
  compared with the independent `Telelem.Test.Admission.Model` after
  **every** command. The comparison covers the command result and the
  observed incarnation, occupancy, held/encumbered/spent/available units,
  and per-op grant states.

  Owners are real processes that the authority monitors. Crashes kill the
  real authority pid. TTL timers run on a manual clock. Each wait is on a
  monitor, an admission observation or a clock return value; timeouts only
  detect failure.

  Rerun:   scripts/mix test test/telelem/admission_model_test.exs --seed <seed>
  A failure prints the seed and the shrunk `{config, commands}`; paste it
  into `@regressions` to make a named case.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Telelem.Instance
  alias Telelem.Instance.ManualClock
  alias Telelem.Test.Admission.Model

  @moduletag :tmp_dir
  @wait 1_000

  # Named regressions: {name, config, commands}.
  @regressions [
    {"owner death after dispatch keeps exposure; late settle resolves it once",
     %{slots: 2, units: 10, ttl: 5},
     [
       {:reserve, 0, 0, 4},
       {:authorize, 0},
       {:kill_owner, 0},
       {:authorize, 0},
       {:settle, 0, 3},
       {:settle, 0, 3},
       {:settle, 0, 2},
       {:kill_owner, 0}
     ]},
    {"crash fences tokens; the live fenced owner keeps its slot until it dies",
     %{slots: 2, units: 10, ttl: 5},
     [
       {:reserve, 0, 0, 4},
       {:authorize, 0},
       :crash_authority,
       {:settle, 0, 4},
       {:cancel, 0},
       {:reserve, 0, 0, 4},
       {:reserve, 1, 1, 3},
       {:reserve, 2, 2, 1},
       {:kill_owner, 0},
       {:reserve, 2, 2, 1},
       {:reconcile, 0, 4},
       {:reconcile, 0, 4},
       {:cancel, 1}
     ]},
    {"late timer from a dead incarnation cannot release the new hold",
     %{slots: 2, units: 10, ttl: 5},
     [
       {:reserve, 0, 0, 2},
       {:advance, 1},
       :crash_authority,
       {:reserve, 1, 1, 2},
       {:advance, 4},
       {:advance, 1},
       {:advance, 10}
     ]},
    {"all-or-none reserve and op-id idempotence", %{slots: 1, units: 5, ttl: 3},
     [
       {:reserve, 0, 0, 3},
       {:reserve, 0, 0, 3},
       {:reserve, 0, 1, 3},
       {:reserve, 0, 0, 2},
       {:reserve, 1, 1, 1},
       {:cancel, 0},
       {:reserve, 1, 1, 6},
       {:reserve, 1, 1, 5},
       {:authorize, 1},
       {:cancel, 1},
       {:reserve, 2, 1, 1},
       {:settle, 1, 0},
       {:reserve, 2, 1, 5}
     ]}
  ]

  for {{name, config, cmds}, i} <- Enum.with_index(@regressions) do
    @tag regression: i
    test "regression: #{name}", %{tmp_dir: dir} do
      run_sequence!(unquote(Macro.escape(config)), unquote(Macro.escape(cmds)), dir)
    end
  end

  property "authority agrees with the reference model after every transition", %{tmp_dir: dir} do
    check all config <- config_gen(),
              cmds <- commands_gen(),
              max_runs: 300 do
      run_sequence!(config, cmds, dir)
    end
  end

  # -- generators --------------------------------------------------------------

  defp config_gen do
    gen all slots <- integer(1..3), units <- integer(3..12), ttl <- integer(1..6) do
      %{slots: slots, units: units, ttl: ttl}
    end
  end

  # Mix of single commands and short lifecycle chunks, so that deep states
  # (settled, unresolved → reconciled, fenced live grants) are reached often.
  # The flattened list still shrinks element-wise and by chunk.
  defp commands_gen do
    frequency([{3, map(command_gen(), &[&1])}, {2, lifecycle_gen()}])
    |> list_of(max_length: 25)
    |> map(&Enum.concat/1)
  end

  defp lifecycle_gen do
    gen all op <- integer(0..3),
            o <- integer(0..2),
            u <- integer(1..5),
            a <- integer(0..6),
            tail <-
              member_of([
                [],
                [{:settle, op, a}],
                [{:settle, op, a}, {:settle, op, a}],
                [{:cancel, op}],
                [{:cancel, op}, {:reconcile, op, a}],
                [{:kill_owner, o}],
                [{:kill_owner, o}, {:settle, op, a}],
                [{:kill_owner, o}, {:reconcile, op, a}],
                [:crash_authority, {:settle, op, a}],
                [:crash_authority, {:kill_owner, o}, {:reconcile, op, a}]
              ]) do
      [{:reserve, op, o, u}, {:authorize, op} | tail]
    end
  end

  defp command_gen do
    op = integer(0..3)
    owner = integer(0..2)

    frequency([
      {6, tuple({constant(:reserve), op, owner, integer(1..5)})},
      {4, tuple({constant(:authorize), op})},
      {3, tuple({constant(:cancel), op})},
      {4, tuple({constant(:settle), op, integer(0..6)})},
      {2, tuple({constant(:reconcile), op, integer(0..6)})},
      {2, tuple({constant(:kill_owner), owner})},
      {1, constant(:crash_authority)},
      {2, tuple({constant(:advance), integer(1..4)})}
    ])
  end

  # -- driver ------------------------------------------------------------------

  defmodule NoTransport do
    @moduledoc false
    @behaviour Telelem.Instance.Transport
    def checkout(_, _), do: {:error, :not_used}
    def dispatch(_, _), do: {:error, :not_used}
    def close(_), do: :ok
  end

  def run_sequence!(config, cmds, dir) do
    uniq = System.unique_integer([:positive])
    clock_id = {:clock, uniq}
    inst_id = {:model, uniq}
    clock = start_supervised!({ManualClock, id: clock_id})

    sup =
      start_supervised!(
        {Instance,
         id: inst_id,
         scope: %{route: "model"},
         limits: %{slots: config.slots, units: config.units},
         spool_dir: Path.join(dir, "run-#{uniq}"),
         transport: {NoTransport, nil},
         clock: {ManualClock, clock},
         reserve_ttl: config.ttl,
         max_restarts: 10_000,
         max_seconds: 1}
      )

    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    Process.put(:admission_model_owners, %{})
    ctx = %{inst: inst, clock: clock, tag: inst.tag, owners: %{}, gids: %{}}

    try do
      cmds
      |> Enum.with_index()
      |> Enum.reduce({Model.new(config), ctx, []}, fn {cmd, i}, {m, ctx, trace} ->
        {expected, m} = Model.step(m, cmd)
        {actual, ctx} = exec(ctx, cmd)
        want = Model.observe(m)
        got = observe(ctx)
        trace = [{cmd, actual} | trace]

        if expected != actual or want != got do
          flunk(divergence(config, cmds, i, cmd, expected, actual, want, got, trace))
        end

        {m, ctx, trace}
      end)
    after
      for {_, pid} <- ctx_owners(), do: Process.exit(pid, :kill)
      stop_supervised(inst_id)
      stop_supervised(clock_id)
      flush(ctx.tag)
    end

    :ok
  end

  # Owners are tracked in the process dictionary as well, so `after` can kill
  # them even when a step raises.
  defp ctx_owners, do: Process.get(:admission_model_owners, %{})

  defp put_owners(ctx, owners) do
    Process.put(:admission_model_owners, owners)
    %{ctx | owners: owners}
  end

  defp exec(ctx, {:reserve, op, ix, units}) do
    {pid, ctx} = ensure_owner(ctx, ix)
    r = Instance.reserve(ctx.inst, op, pid, units)

    ctx =
      case r do
        {tag, gid} when tag in [:ok, :duplicate] -> %{ctx | gids: Map.put_new(ctx.gids, op, gid)}
        _ -> ctx
      end

    {norm(r), ctx}
  end

  defp exec(ctx, {:authorize, op}), do: {norm(Instance.authorize(ctx.inst, gid(ctx, op))), ctx}
  defp exec(ctx, {:cancel, op}), do: {norm(Instance.cancel(ctx.inst, gid(ctx, op))), ctx}

  defp exec(ctx, {:settle, op, a}),
    do: {norm(Instance.settle(ctx.inst, gid(ctx, op), a)), ctx}

  defp exec(ctx, {:reconcile, op, a}),
    do: {norm(Instance.reconcile(ctx.inst, gid(ctx, op), a)), ctx}

  defp exec(ctx, {:kill_owner, ix}) do
    case Map.fetch(ctx.owners, ix) do
      :error ->
        {{:killed, false}, ctx}

      {:ok, pid} ->
        # Whether the authority still holds a live grant for this owner, so
        # that we wait for exactly the observation it must emit.
        live? =
          Enum.any?(Instance.snapshot(ctx.inst).grants, fn {_, g} ->
            g.owner == pid and g.state in [:reserved, :dispatched]
          end)

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        await_down(ref, pid, "owner")
        tag = ctx.tag

        seen =
          live? and
            receive do
              {:telelem_admission, ^tag, {:owner_down, ^pid, _}} -> true
            after
              @wait -> :missing_owner_down_observation
            end

        {{:killed, seen}, put_owners(ctx, Map.delete(ctx.owners, ix))}
    end
  end

  defp exec(ctx, :crash_authority) do
    old = Instance.authority(ctx.inst)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    await_down(ref, old, "authority")
    tag = ctx.tag

    receive do
      {:telelem_admission, ^tag, {:authority_up, new, _inc}} when new != old -> {:crashed, ctx}
    after
      @wait -> {:no_successor, ctx}
    end
  end

  defp exec(ctx, {:advance, ms}) do
    fired = ManualClock.advance(ctx.clock, ms)
    tag = ctx.tag

    results =
      for {{:telelem_reservation_ttl, gid}, pid} <- fired do
        if pid == nil do
          :undelivered
        else
          receive do
            {:telelem_admission, ^tag, {:timer, ^gid, r}} -> norm(r)
          after
            @wait -> :missing_timer_observation
          end
        end
      end

    {{:timers, results}, ctx}
  end

  defp await_down(ref, pid, what) do
    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      @wait -> flunk("#{what} #{inspect(pid)} did not exit")
    end
  end

  defp ensure_owner(ctx, ix) do
    case Map.fetch(ctx.owners, ix) do
      {:ok, pid} ->
        {pid, ctx}

      :error ->
        pid = spawn(fn -> receive do: (:never -> :ok) end)
        {pid, put_owners(ctx, Map.put(ctx.owners, ix, pid))}
    end
  end

  # Unknown ops get a well-formed token for the current incarnation that names no grant.
  defp gid(ctx, op),
    do: Map.get_lazy(ctx.gids, op, fn -> {Instance.incarnation(ctx.inst), 1_000_000 + op} end)

  defp norm({:ok, _}), do: :ok
  defp norm({:duplicate, _}), do: :duplicate
  defp norm({:error, r}), do: {:error, r}

  defp observe(ctx) do
    s = Instance.snapshot(ctx.inst)

    %{
      incarnation: s.incarnation,
      active: s.active,
      available_slots: s.available_slots,
      reserved_units: s.reserved_units,
      encumbered_units: s.encumbered_units,
      spent_units: s.spent_units,
      available_units: s.available_units,
      states: Map.new(s.grants, fn {_, g} -> {g.op_id, g.state} end)
    }
  end

  defp flush(tag) do
    receive do
      {:telelem_admission, ^tag, _} -> flush(tag)
    after
      0 -> :ok
    end
  end

  defp divergence(config, cmds, i, cmd, expected, actual, want, got, trace) do
    seed = ExUnit.configuration()[:seed]

    """
    admission model diverged at step #{i}: #{inspect(cmd)}
      model result:     #{inspect(expected)}
      authority result: #{inspect(actual)}
      model observation:     #{inspect(want)}
      authority observation: #{inspect(got)}
      trace (cmd, authority result): #{inspect(Enum.reverse(trace), limit: :infinity)}

    rerun: scripts/mix test test/telelem/admission_model_test.exs --seed #{seed}
    regression case (add to @regressions):
      {"name", #{inspect(config)},
       #{inspect(cmds, limit: :infinity)}}
    """
  end
end
