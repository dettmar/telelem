defmodule Telelem.HandoffInstanceTest do
  @moduledoc """
  TEL-003 across the admission boundary: the handoff monitors a real
  `Telelem.Instance.authority/1`, and each attempt owns a real
  `Telelem.Instance.Worker` whose grant is authorized by an actual
  `Worker.dispatch/2` over a loopback TCP socket. The fault is the
  foundational authority's death after the first semantic fragment.

  This shows policy and accounting across the boundary. It is not the TEL-005
  wire gate: no Mint/Bandit, TLS, SDK or downstream socket.

      scripts/mix test test/telelem/handoff_instance_test.exs
  """
  use ExUnit.Case, async: true

  alias Telelem.{Handoff, Instance}
  alias Telelem.Instance.Worker
  alias Telelem.Test.Admission.TcpTransport

  @moduletag :tmp_dir
  @t 1_000
  @units %{a: 3, b: 5}

  # Attempt process: runs closures sent by the test (the barrier is its
  # receive) and forwards every handoff message.
  defp attempt(id) do
    test = self()

    pid =
      spawn(fn ->
        Stream.repeatedly(fn ->
          receive do
            {:run, ref, fun} -> send(test, {:ran, id, ref, fun.()})
            {:telelem_handoff, _h, msg} -> send(test, {:attempt, id, :msg, msg})
          end
        end)
        |> Stream.run()
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp run!(pid, id, fun) do
    ref = make_ref()
    send(pid, {:run, ref, fun})
    assert_receive {:ran, ^id, ^ref, result}, @t
    result
  end

  defp forwarder(tag) do
    test = self()

    pid =
      spawn(fn ->
        Stream.repeatedly(fn ->
          receive do: ({:telelem_handoff, _h, m} -> send(test, {tag, m}))
        end)
        |> Stream.run()
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp watch(pid), do: {pid, Process.monitor(pid)}

  defp req_events(acc \\ []) do
    receive do
      {:req, {:closed, _} = ev} -> Enum.reverse([ev | acc])
      {:req, ev} -> req_events([ev | acc])
    after
      @t -> flunk("no {:closed, _} after #{inspect(Enum.reverse(acc))}")
    end
  end

  test "authority death after the first fragment ends the stream incomplete; successor holds both dispatches as unresolved exposure",
       %{tmp_dir: dir} do
    {listen, port} = TcpTransport.listen!()

    sup =
      start_supervised!(
        {Instance,
         id: :gw_handoff,
         limits: %{slots: 4, units: 20},
         spool_dir: Path.join(dir, "A"),
         transport: {TcpTransport, %{port: port, label: "A"}},
         reserve_ttl: nil}
      )

    inst = Instance.handle(sup)
    tag = inst.tag
    :ok = Instance.subscribe(inst)
    auth1 = Instance.authority(inst)
    inc1 = Instance.incarnation(inst)

    req = forwarder(:req)

    h =
      start_supervised!(
        {Handoff, requester: req, attempts: [:a, :b], authority: auth1, trace: self()}
      )

    hmon = Process.monitor(h)
    att = %{a: attempt(:a), b: attempt(:b)}

    # Each attempt: attach, start its worker (reserve + TCP checkout), cross the
    # dispatch boundary, then let admission authorize and send the request.
    work =
      for id <- [:a, :b], into: %{} do
        p = att[id]
        assert {:ok, _} = run!(p, id, fn -> Handoff.attach(h, id) end)
        assert {:ok, w} = run!(p, id, fn -> Instance.start_work(inst, "op-#{id}", @units[id]) end)
        {peer, hello} = TcpTransport.accept!(listen)
        assert hello == "hello A op-#{id}"
        assert :ok = run!(p, id, fn -> Handoff.begin_dispatch(h, id) end)
        assert {:ok, gid} = run!(p, id, fn -> Worker.dispatch(w, "req-#{id}") end)
        assert :gen_tcp.recv(peer, 0, @t) == {:ok, "dispatch req-#{id}\n"}
        {id, %{w: w, gid: gid, peer: peer}}
      end

    snap = Instance.snapshot(inst)
    assert snap.active == 2 and snap.encumbered_units == 8
    assert snap.grants[work.a.gid].state == :dispatched
    assert snap.grants[work.b.gid].state == :dispatched

    # Both are at the barrier; :a reaches ready first and wins.
    assert :release = run!(att.a, :a, fn -> Handoff.ready(h, :a) end)
    assert_receive {:attempt, :b, :msg, {:cancel, {:lost_to, :a}}}, @t
    assert {:cancel, {:lost_to, :a}} = run!(att.b, :b, fn -> Handoff.ready(h, :b) end)

    # The loser cancels its worker. Admission keeps the spend (dispatched → unresolved),
    # and the handoff does not take the :none claim as proof of no dispatch.
    {wb, wbmon} = watch(work.b.w)
    gb = work.b.gid
    assert {:ok, ^gb} = run!(att.b, :b, fn -> Worker.cancel(wb) end)
    assert_receive {:DOWN, ^wbmon, :process, ^wb, :normal}, @t
    assert {:error, :closed} = :gen_tcp.recv(work.b.peer, 0, @t)
    assert :ok = run!(att.b, :b, fn -> Handoff.cleaned(h, :b, :none) end)
    assert Instance.snapshot(inst).grants[gb].state == :unresolved

    # First semantic fragment: committed.
    assert :ok = run!(att.a, :a, fn -> Handoff.transfer(h, "tool-fragment") end)

    # ── Fault: kill the foundational authority. Monitor old PIDs first. ──
    {_, amon} = watch(auth1)
    {wa, wamon} = watch(work.a.w)
    Process.exit(auth1, :kill)
    assert_receive {:DOWN, ^amon, :process, ^auth1, :killed}, @t
    assert_receive {:telelem_handoff_trace, ^h, {:authority_down, :committed, :killed}}, @t
    # rest_for_one fences the instance: the winner's worker and socket go too.
    assert_receive {:DOWN, ^wamon, :process, ^wa, _}, @t
    assert {:error, :closed} = :gen_tcp.recv(work.a.peer, 0, @t)

    # The open stream ends; nothing is extended, reselected or completed.
    reason = {:authority_down, :killed}
    assert_receive {:attempt, :a, :msg, {:cancel, ^reason}}, @t
    assert {:error, :cancelled} = run!(att.a, :a, fn -> Handoff.transfer(h, "more") end)
    assert {:error, :cancelled} = run!(att.a, :a, fn -> Handoff.finish(h, :synthetic) end)
    # The closed loser can't come back either.
    assert {:cancel, :closed} = run!(att.b, :b, fn -> Handoff.ready(h, :b) end)
    # Explicit cleanup acknowledgement from the winner (its transport is already closed).
    assert :ok = run!(att.a, :a, fn -> Handoff.cleaned(h, :a, :none) end)
    assert_receive {:DOWN, ^hmon, :process, ^h, :normal}, @t

    assert [{:elected, :a}, {:data, "tool-fragment"}, {:incomplete, ^reason}, {:closed, r}] =
             req_events()

    assert r.authority == :lost and r.committed and r.winner == :a
    assert r.attempts.a.cleanup == :ack and r.attempts.b.cleanup == :ack
    assert r.possible_charges == [:a, :b]

    # ── Successor: fenced incarnation, both dispatches still encumbered. ──
    assert_receive {:telelem_admission, ^tag, {:authority_up, auth2, inc2}}, @t
    assert auth2 != auth1 and inc2 == inc1 + 1
    ga = work.a.gid
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^wa, [{^ga, {:ok, ^ga}}]}}, @t

    snap = Instance.snapshot(inst)
    assert snap.incarnation == inc2
    assert snap.grants[ga].state == :unresolved
    assert snap.grants[gb].state == :unresolved
    assert snap.active == 0
    assert snap.encumbered_units == @units.a + @units.b
    assert snap.spent_units == 0
    assert snap.available_units == 20 - 8
    assert Instance.workers(inst) == []

    # A late old-incarnation callback cannot refund the exposure.
    assert {:error, :stale_incarnation} = Instance.cancel(inst, ga)
    assert Instance.snapshot(inst).encumbered_units == 8
  end
end
