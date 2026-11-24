defmodule Telelem.InstanceTest do
  @moduledoc """
  TEL-002: two gateway subtrees in one BEAM under ExUnit.

  Ordering comes from monitors, admission observations, the manual clock's
  return value and TCP reads. Bounded timeouts only detect failure; there
  are no sleeps.

      scripts/mix test test/telelem/instance_test.exs
  """
  use ExUnit.Case, async: true

  alias Telelem.Instance
  alias Telelem.Instance.{ManualClock, Worker}
  alias Telelem.Test.Admission.TcpTransport

  @moduletag :tmp_dir
  @scope %{route: "chat", tenant: "acme"}

  defp start_instance!(id, label, limits, dir, clock, port) do
    sup =
      start_supervised!(
        {Instance,
         id: id,
         scope: @scope,
         limits: limits,
         spool_dir: Path.join(dir, label),
         transport: {TcpTransport, %{port: port, label: label}},
         clock: {ManualClock, clock},
         reserve_ttl: 50}
      )

    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    inst
  end

  defp recv!(peer), do: :gen_tcp.recv(peer, 0, 1_000)

  defp lines(path), do: path |> File.read!() |> String.split("\n", trim: true)

  test "killing A's authority fences A while B serves; stopping A closes its transport", %{
    tmp_dir: dir
  } do
    {la, pa} = TcpTransport.listen!()
    {lb, pb} = TcpTransport.listen!()
    ca = start_supervised!({ManualClock, id: :clock_a})
    cb = start_supervised!({ManualClock, id: :clock_b})

    a = start_instance!(:gw_a, "A", %{slots: 2, units: 10}, dir, ca, pa)
    b = start_instance!(:gw_b, "B", %{slots: 3, units: 20}, dir, cb, pb)
    tag_a = a.tag

    # Same human scope, separate everything else.
    assert Instance.meta(a).scope == Instance.meta(b).scope
    assert Instance.meta(a).spool_dir != Instance.meta(b).spool_dir
    assert Instance.authority(a) != Instance.authority(b)
    assert Instance.snapshot(a).limits == %{slots: 2, units: 10}
    assert Instance.snapshot(b).limits == %{slots: 3, units: 20}

    # B serves a dispatched request on a held socket.
    {:ok, wb} = Instance.start_work(b, "op-1", 4)
    {sb, "hello B op-1"} = TcpTransport.accept!(lb)
    assert {:ok, gb} = Worker.dispatch(wb, "req-b")
    assert {:ok, "dispatch req-b\n"} = recv!(sb)

    # A: the same op id (separate ledger), dispatched on a held socket, plus a
    # reserved grant. The authority armed TTL timers for both on A's clock.
    {:ok, wa1} = Instance.start_work(a, "op-1", 3)
    {sa1, "hello A op-1"} = TcpTransport.accept!(la)
    assert {:ok, ga1} = Worker.dispatch(wa1, "req-a1")
    assert {:ok, "dispatch req-a1\n"} = recv!(sa1)
    {:ok, wa2} = Instance.start_work(a, "op-2", 2)
    {sa2, "hello A op-2"} = TcpTransport.accept!(la)
    %{grant: ga2} = Worker.info(wa2)

    assert [{50, {:telelem_reservation_ttl, ^ga1}}, {50, {:telelem_reservation_ttl, ^ga2}}] =
             ManualClock.pending(ca)

    assert [] == ManualClock.advance(ca, 30)
    assert ManualClock.pending(cb) == [{50, {:telelem_reservation_ttl, gb}}]

    assert %{incarnation: 1, active: 2, reserved_units: 2, encumbered_units: 3} =
             Instance.snapshot(a)

    b_auth = Instance.authority(b)
    b_workers_sup = Instance.workers_sup(b)
    b_snap = Instance.snapshot(b)
    assert %{incarnation: 1, active: 1, encumbered_units: 4, spent_units: 0} = b_snap

    # Kill A's authority. Monitor the old pids first.
    old_auth = Instance.authority(a)
    for p <- [old_auth, wa1, wa2], do: Process.monitor(p)
    Process.exit(old_auth, :kill)

    assert_receive {:DOWN, _, :process, ^old_auth, :killed}
    # rest_for_one: A's workers exit and their sockets close before the successor starts.
    assert_receive {:DOWN, _, :process, ^wa1, _}
    assert_receive {:DOWN, _, :process, ^wa2, _}
    assert {:error, :closed} = recv!(sa1)
    assert {:error, :closed} = recv!(sa2)

    assert_receive {:telelem_admission, ^tag_a, {:authority_up, new_auth, 2}}
    assert new_auth != old_auth and Instance.authority(a) == new_auth
    # The successor re-monitors the fenced owners and sees each die exactly once.
    assert_receive {:telelem_admission, ^tag_a, {:owner_down, ^wa1, [{^ga1, {:ok, ^ga1}}]}}
    assert_receive {:telelem_admission, ^tag_a, {:owner_down, ^wa2, [{^ga2, {:ok, ^ga2}}]}}

    snap_a = Instance.snapshot(a)
    assert %{incarnation: 2, active: 0, reserved_units: 0, spent_units: 0} = snap_a
    # The dispatched spend may be billed: the worker's death does not refund it.
    assert snap_a.encumbered_units == 3
    assert snap_a.grants[ga1].state == :unresolved
    assert snap_a.grants[ga2].state == :released

    # B was untouched: same authority, worker supervisor, worker and balances.
    assert Instance.authority(b) == b_auth
    assert Instance.workers_sup(b) == b_workers_sup
    assert Instance.workers(b) == [wb]
    assert Instance.snapshot(b) == b_snap

    # A new hold in A's incarnation 2.
    {:ok, wa3} = Instance.start_work(a, "op-3", 2)
    {sa3, "hello A op-3"} = TcpTransport.accept!(la)
    %{grant: ga3, incarnation: 2} = Worker.info(wa3)
    assert elem(ga3, 0) == 2

    # Late timers armed by incarnation 1 reach incarnation 2 (the manual clock
    # resolves the destination at fire time) and are fenced.
    assert [
             {{:telelem_reservation_ttl, ^ga1}, ^new_auth},
             {{:telelem_reservation_ttl, ^ga2}, ^new_auth}
           ] = ManualClock.advance(ca, 20)

    assert_receive {:telelem_admission, ^tag_a, {:timer, ^ga1, {:error, :stale_incarnation}}}
    assert_receive {:telelem_admission, ^tag_a, {:timer, ^ga2, {:error, :stale_incarnation}}}

    # Stale worker callbacks from incarnation 1 are rejected as well.
    assert {:error, :stale_incarnation} = Instance.settle(a, ga1, 0)
    assert {:error, :stale_incarnation} = Instance.cancel(a, ga2)
    assert {:error, :stale_incarnation} = Instance.authorize(a, ga2)

    snap_a = Instance.snapshot(a)

    assert %{active: 1, reserved_units: 2, encumbered_units: 3, stale_rejections: 5} = snap_a
    assert snap_a.grants[ga3].state == :reserved
    assert Process.alive?(wa3)

    # Fenced unresolved spend is resolved only by reconciliation.
    assert {:ok, ^ga1} = Instance.reconcile(a, ga1, 3)
    assert {:duplicate, ^ga1} = Instance.reconcile(a, ga1, 3)
    assert %{encumbered_units: 0, spent_units: 3, active: 1} = Instance.snapshot(a)

    # Stop A while B has work in flight.
    Process.monitor(wa3)
    Process.monitor(new_auth)
    :ok = stop_supervised(:gw_a)
    assert_receive {:DOWN, _, :process, ^wa3, :shutdown}
    assert_receive {:DOWN, _, :process, ^new_auth, :shutdown}
    assert {:error, :closed} = recv!(sa3)

    # B still serves: the in-flight worker settles, and new work is admitted and settled.
    assert Process.alive?(wb)
    Process.monitor(wb)
    assert {:ok, ^gb} = Worker.complete(wb, 4)
    assert {:error, :closed} = recv!(sb)
    assert_receive {:DOWN, _, :process, ^wb, :normal}

    {:ok, wb2} = Instance.start_work(b, "op-2", 5)
    {sb2, "hello B op-2"} = TcpTransport.accept!(lb)
    assert {:ok, gb2} = Worker.dispatch(wb2, "req-b2")
    assert {:ok, "dispatch req-b2\n"} = recv!(sb2)
    assert {:ok, ^gb2} = Worker.complete(wb2, 5)

    assert %{incarnation: 1, active: 0, encumbered_units: 0, spent_units: 9, authority: ^b_auth} =
             Instance.snapshot(b)

    # Spools are per instance.
    assert [_, _] = lines(Path.join([dir, "A", "authority.log"]))
    assert [_] = lines(Path.join([dir, "B", "authority.log"]))
    assert [a_receipt] = lines(Path.join([dir, "A", "receipts.log"]))
    assert a_receipt =~ "grant=#{inspect(ga1)}" and a_receipt =~ "actual=3"
    assert [_, _] = lines(Path.join([dir, "B", "receipts.log"]))
  end

  test "limits are per instance, reservation is all-or-none, duplicates are safe", %{
    tmp_dir: dir
  } do
    {la, pa} = TcpTransport.listen!()
    {lb, pb} = TcpTransport.listen!()
    ca = start_supervised!({ManualClock, id: :clock_a})
    cb = start_supervised!({ManualClock, id: :clock_b})
    a = start_instance!(:gw_a, "A", %{slots: 1, units: 5}, dir, ca, pa)
    b = start_instance!(:gw_b, "B", %{slots: 2, units: 8}, dir, cb, pb)

    {:ok, _wa} = Instance.start_work(a, "op-x", 3)
    {_sa, "hello A op-x"} = TcpTransport.accept!(la)
    before = Instance.snapshot(a)

    # A has one slot: refusal holds nothing and opens no connection.
    assert {:error, {:shutdown, {:admission, :no_slot}}} = Instance.start_work(a, "op-y", 1)
    assert Instance.snapshot(a) == before
    assert {:error, :timeout} = :gen_tcp.accept(la, 0)

    # B, with the same scope and op ids, uses its own limits.
    {:ok, wbx} = Instance.start_work(b, "op-x", 3)
    {_sbx, "hello B op-x"} = TcpTransport.accept!(lb)
    {:ok, _wby} = Instance.start_work(b, "op-y", 1)
    {_sby, "hello B op-y"} = TcpTransport.accept!(lb)
    b_before = Instance.snapshot(b)
    assert %{active: 2, reserved_units: 4, available_units: 4} = b_before

    # Out of slots and units at once: refused, nothing held.
    assert {:error, {:shutdown, {:admission, :no_slot}}} = Instance.start_work(b, "op-z", 5)
    assert Instance.snapshot(b) == b_before

    # A second owner reusing an op id conflicts and holds nothing.
    assert {:error, {:shutdown, {:admission, :op_conflict}}} = Instance.start_work(b, "op-x", 3)
    assert Instance.snapshot(b) == b_before

    # Duplicate dispatch and settle are idempotent; conflicting late settle is rejected.
    assert {:ok, gx} = Worker.dispatch(wbx, "r")
    assert {:duplicate, ^gx} = Instance.authorize(b, gx)
    assert {:ok, ^gx} = Worker.complete(wbx, 3)
    assert {:duplicate, ^gx} = Instance.settle(b, gx, 3)
    assert {:error, :settle_conflict} = Instance.settle(b, gx, 2)
    assert {:duplicate, ^gx} = Instance.cancel(b, gx)

    assert %{active: 1, reserved_units: 1, encumbered_units: 0, spent_units: 3} =
             Instance.snapshot(b)

    # Units are all-or-none too: 8 - 3 spent - 1 held = 4 available.
    assert {:error, {:shutdown, {:admission, :insufficient_units}}} =
             Instance.start_work(b, "op-z", 5)

    assert %{active: 1, reserved_units: 1} = Instance.snapshot(b)
    assert Instance.snapshot(a) == before
  end
end
