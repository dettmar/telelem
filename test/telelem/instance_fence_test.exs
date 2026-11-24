defmodule Telelem.InstanceFenceTest do
  @moduledoc """
  Fencing across authority restarts and instance lives (TEL-002/006 follow-up).

    * C1: the parent restarts the instance after restart intensity is
      exceeded. The new life has a new instance id. Old tokens are
      `:foreign_instance`, old handles are `:instance_unavailable`, and the
      prior exposure is reported as unknown, not zero.
    * C2: a live direct owner's never-dispatched grant is retired at the
      fence (no leaked slot/units, no revivable `:duplicate`). A dispatched
      grant stays held until the owner's monitored death.
    * Tokens from another instance in the same VM never act.

  Ordering comes from monitors, admission observations and clock return
  values. There are no sleeps.

      scripts/mix test test/telelem/instance_fence_test.exs
  """
  use ExUnit.Case, async: true

  alias Telelem.Instance
  alias Telelem.Instance.ManualClock

  @moduletag :tmp_dir
  @t 1_000

  defmodule NoTransport do
    @moduledoc false
    @behaviour Telelem.Instance.Transport
    def checkout(_, _), do: {:error, :not_used}
    def dispatch(_, _), do: {:error, :not_used}
    def close(_), do: :ok
  end

  defp spec(id, dir, clock, limits, extra \\ []) do
    {Instance,
     [
       id: id,
       scope: %{route: "chat", tenant: "acme"},
       limits: limits,
       spool_dir: Path.join(dir, to_string(id)),
       transport: {NoTransport, nil},
       clock: {ManualClock, clock},
       reserve_ttl: 50
     ] ++ extra}
  end

  defp up!(sup) do
    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    inst
  end

  defp owner do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp kill_authority!(inst) do
    old = Instance.authority(inst)
    ref = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old, :killed}, @t
    old
  end

  defp monitored_by?(pid, owner) do
    {:monitors, ms} = Process.info(pid, :monitors)
    Enum.any?(ms, &match?({:process, ^owner}, &1))
  end

  # Bounded wait (no sleep) for the parent to restart the child: each step is
  # a synchronous call to the parent.
  defp restarted_child!(parent, id, old, n \\ 1_000) do
    case List.keyfind(Supervisor.which_children(parent), id, 0) do
      {^id, pid, _, _} when is_pid(pid) and pid != old -> pid
      _ when n > 0 -> restarted_child!(parent, id, old, n - 1)
      other -> flunk("parent did not restart #{inspect(id)}: #{inspect(other)}")
    end
  end

  test "C1: a parent-restarted instance is a new life: old tokens are foreign, exposure unknown",
       %{tmp_dir: dir} do
    clock = start_supervised!({ManualClock, id: :clock})

    child =
      Supervisor.child_spec(spec(:gw, dir, clock, %{slots: 2, units: 10}, max_restarts: 1), [])

    parent =
      start_supervised!(%{
        id: :parent,
        type: :supervisor,
        start: {Supervisor, :start_link, [[child], [strategy: :one_for_one, max_restarts: 5]]}
      })

    [{:gw, sup1, _, _}] = Supervisor.which_children(parent)
    inst1 = up!(sup1)
    tag1 = inst1.tag

    # A fresh spool: this life cannot speak for any other, but saw none.
    assert %{prior_instances: 0, prior_exposure: :no_prior_instance_in_spool} =
             Instance.snapshot(inst1).coverage

    o_old = owner()
    {:ok, g_old} = Instance.reserve(inst1, "op-old", o_old, 4)
    {:ok, ^g_old} = Instance.authorize(inst1, g_old)
    assert %{encumbered_units: 4, available_units: 6} = Instance.snapshot(inst1)

    # Exceed the instance's restart intensity (1): the parent restarts it.
    smon = Process.monitor(sup1)
    kill_authority!(inst1)
    assert_receive {:telelem_admission, ^tag1, {:authority_up, _, 2}}, @t
    kill_authority!(inst1)
    assert_receive {:DOWN, ^smon, :process, ^sup1, :shutdown}, @t

    sup2 = restarted_child!(parent, :gw, sup1)
    inst2 = up!(sup2)
    assert Instance.instance_id(inst2) != Instance.instance_id(inst1)

    snap2 = Instance.snapshot(inst2)
    assert %{incarnation: 1, encumbered_units: 0, available_units: 10} = snap2

    # ...but the empty ledger is not evidence of zero historical exposure.
    assert %{
             ledger: :volatile,
             covers: :this_instance_life,
             prior_instances: 1,
             prior_exposure: :unknown
           } = snap2.coverage

    assert snap2.coverage.instance_id == Base.encode16(Instance.instance_id(inst2), case: :lower)

    # The old handle cannot reach the new life.
    assert {:error, :instance_unavailable} = Instance.cancel(inst1, g_old)
    assert {:error, :instance_unavailable} = Instance.reserve(inst1, "x", owner(), 1)

    # Old life's TTL timer for g_old is due at 50; move past nothing yet.
    assert [] = ManualClock.advance(clock, 30)

    # A fresh, unrelated holder in the new life, with the same incarnation and seq numbers.
    o_new = owner()
    {:ok, g_new} = Instance.reserve(inst2, "op-new", o_new, 3)
    assert {_, 1, 1} = g_new
    assert {_, 1, 1} = g_old
    assert g_new != g_old
    before = Instance.snapshot(inst2)

    # The stale holder's token cannot act through the new handle.
    assert {:error, :foreign_instance} = Instance.cancel(inst2, g_old)
    assert {:error, :foreign_instance} = Instance.authorize(inst2, g_old)
    assert {:error, :foreign_instance} = Instance.settle(inst2, g_old, 0)
    assert {:error, :foreign_instance} = Instance.reconcile(inst2, g_old, 0)

    # The old life's timers are not delivered: their directory is gone.
    assert [{{:telelem_reservation_ttl, ^g_old}, nil}] = ManualClock.advance(clock, 20)

    after_ = Instance.snapshot(inst2)
    assert after_.grants[g_new].state == :reserved
    assert Map.drop(after_, [:stale_rejections]) == Map.drop(before, [:stale_rejections])

    # The op id is unknown to the new life: a retry is a fresh grant, not the old one.
    {:ok, g_retry} = Instance.reserve(inst2, "op-old", o_old, 4)
    assert g_retry != g_old
  end

  test "C2: a live direct owner's never-dispatched grant is retired at the fence", %{tmp_dir: dir} do
    clock = start_supervised!({ManualClock, id: :clock})
    inst = up!(start_supervised!(spec(:gw, dir, clock, %{slots: 1, units: 10})))
    tag = inst.tag

    o = owner()
    {:ok, g} = Instance.reserve(inst, "op-a", o, 5)
    assert {:duplicate, ^g} = Instance.reserve(inst, "op-a", o, 5)

    kill_authority!(inst)
    # Bounded recovery: retired by the time the successor is observable.
    assert_receive {:telelem_admission, ^tag, {:fenced, 2, %{released: [^g], held: []}}}, @t
    assert_receive {:telelem_admission, ^tag, {:authority_up, auth2, 2}}, @t

    assert %{active: 0, reserved_units: 0, encumbered_units: 0, available_units: 10} =
             snap = Instance.snapshot(inst)

    assert snap.grants[g].state == :released
    assert Process.alive?(o)
    refute monitored_by?(auth2, o)

    # Nothing can revive or reuse the retired hold.
    assert {:error, :stale_incarnation} = Instance.reserve(inst, "op-a", o, 5)
    assert {:error, :stale_incarnation} = Instance.authorize(inst, g)
    assert {:error, :stale_incarnation} = Instance.cancel(inst, g)
    assert {:error, {:not_unresolved, :released}} = Instance.reconcile(inst, g, 0)
    assert [{{:telelem_reservation_ttl, ^g}, ^auth2}] = ManualClock.advance(clock, 50)
    assert_receive {:telelem_admission, ^tag, {:timer, ^g, {:error, :stale_incarnation}}}, @t

    # The only slot is free for new work.
    {:ok, g2} = Instance.reserve(inst, "op-b", owner(), 1)
    assert {_, 2, _} = g2

    # Late owner death changes nothing.
    ref = Process.monitor(o)
    Process.exit(o, :kill)
    assert_receive {:DOWN, ^ref, :process, ^o, :killed}, @t

    assert %{active: 1, reserved_units: 1, encumbered_units: 0, spent_units: 0} =
             Instance.snapshot(inst)
  end

  test "C2: a live direct owner's dispatched grant stays held until its monitored death",
       %{tmp_dir: dir} do
    clock = start_supervised!({ManualClock, id: :clock})
    inst = up!(start_supervised!(spec(:gw, dir, clock, %{slots: 2, units: 10})))
    tag = inst.tag

    o = owner()
    {:ok, g} = Instance.reserve(inst, "op-d", o, 5)
    {:ok, ^g} = Instance.authorize(inst, g)

    kill_authority!(inst)
    assert_receive {:telelem_admission, ^tag, {:fenced, 2, %{released: [], held: [^g]}}}, @t
    assert_receive {:telelem_admission, ^tag, {:authority_up, auth2, 2}}, @t
    assert monitored_by?(auth2, o)

    # Conservative: the possibly running work keeps its slot and its exposure.
    assert %{active: 1, encumbered_units: 5, available_units: 5} = Instance.snapshot(inst)
    assert {:error, :stale_incarnation} = Instance.reserve(inst, "op-d", o, 5)
    assert {:error, :stale_incarnation} = Instance.settle(inst, g, 0)
    assert {:error, :stale_incarnation} = Instance.cancel(inst, g)
    assert {:error, {:not_unresolved, :dispatched}} = Instance.reconcile(inst, g, 0)
    assert [{{:telelem_reservation_ttl, ^g}, ^auth2}] = ManualClock.advance(clock, 50)
    assert_receive {:telelem_admission, ^tag, {:timer, ^g, {:error, :stale_incarnation}}}, @t

    # Owner death frees the slot once; the spend stays encumbered.
    Process.exit(o, :kill)
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^o, [{^g, {:ok, ^g}}]}}, @t
    snap = Instance.snapshot(inst)
    assert %{active: 0, encumbered_units: 5, spent_units: 0} = snap
    assert snap.grants[g].state == :unresolved

    assert {:ok, ^g} = Instance.reconcile(inst, g, 5)
    assert {:duplicate, ^g} = Instance.reconcile(inst, g, 5)
    assert {:error, :settle_conflict} = Instance.reconcile(inst, g, 4)
    assert %{active: 0, encumbered_units: 0, spent_units: 5} = Instance.snapshot(inst)
  end

  test "tokens and handles from another instance in the same VM never act", %{tmp_dir: dir} do
    ca = start_supervised!({ManualClock, id: :ca})
    cb = start_supervised!({ManualClock, id: :cb})
    a = up!(start_supervised!(spec(:a, dir, ca, %{slots: 2, units: 10})))
    b = up!(start_supervised!(spec(:b, dir, cb, %{slots: 2, units: 10})))
    assert Instance.instance_id(a) != Instance.instance_id(b)

    o = owner()
    {:ok, ga} = Instance.reserve(a, "op", o, 3)
    {:ok, gb} = Instance.reserve(b, "op", o, 3)
    {:ok, ^gb} = Instance.authorize(b, gb)
    # Same op, owner, incarnation and seq; different instances.
    assert {_, 1, 1} = ga
    assert {_, 1, 1} = gb
    assert ga != gb

    a0 = Instance.snapshot(a)
    b0 = Instance.snapshot(b)

    for {inst, foreign} <- [{a, gb}, {b, ga}] do
      assert {:error, :foreign_instance} = Instance.authorize(inst, foreign)
      assert {:error, :foreign_instance} = Instance.cancel(inst, foreign)
      assert {:error, :foreign_instance} = Instance.settle(inst, foreign, 0)
      assert {:error, :foreign_instance} = Instance.reconcile(inst, foreign, 0)
    end

    assert {:error, :invalid_token} = Instance.cancel(a, {1, 1})

    # No stolen or refunded units anywhere.
    assert Instance.snapshot(a) == a0
    assert Instance.snapshot(b) == b0
    assert Instance.snapshot(a).grants[ga].state == :reserved
    assert Instance.snapshot(b).grants[gb].state == :dispatched

    # A stopped instance's handle is unavailable; its tokens are foreign to the survivor.
    :ok = stop_supervised(:a)
    assert {:error, :instance_unavailable} = Instance.cancel(a, ga)
    assert {:error, :foreign_instance} = Instance.cancel(b, ga)
    assert Instance.snapshot(b) == b0
  end
end
