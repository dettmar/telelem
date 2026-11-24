defmodule Telelem.HandoffTest do
  # TEL-003: policy-ordering cases over a fake, barrier-controlled transport.
  # These do not test TLS/socket ownership or downstream delivery (TEL-005).
  # Ordering comes from messages and synchronous calls; timeouts only bound
  # failure detection. No Process.sleep.
  use ExUnit.Case, async: true

  alias Telelem.Handoff

  @t 1_000

  # ── Fakes ────────────────────────────────────────────────────────────────

  # Fake transport attempt. It parks in `receive` between steps; that is the
  # barrier. Each step's result and every async message from the handoff are
  # reported to the test. After a successful {:clean, _} the transport is
  # closed and the process exits normally.
  defp attempt(h, id) do
    test = self()
    pid = spawn(fn -> attempt_loop(test, h, id) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp attempt_loop(test, h, id) do
    receive do
      {:do, op} ->
        result = run(op, h, id)
        send(test, {:attempt, id, op, result})
        if match?({{:clean, _}, :ok}, {op, result}), do: :ok, else: attempt_loop(test, h, id)

      {:telelem_handoff, ^h, msg} ->
        send(test, {:attempt, id, :msg, msg})
        attempt_loop(test, h, id)

      {:DOWN, _, :process, ^h, reason} ->
        send(test, {:attempt, id, :handoff_down, reason})
        attempt_loop(test, h, id)
    end
  end

  defp run(:attach, h, id) do
    case Handoff.attach(h, id) do
      {:ok, mref} when is_reference(mref) -> :ok
      other -> other
    end
  end

  defp run(:ready, h, id), do: Handoff.ready(h, id)
  defp run({:transfer, chunk}, h, _), do: Handoff.transfer(h, chunk)
  defp run({:finish, term}, h, _), do: Handoff.finish(h, term)
  defp run({:clean, charge}, h, id), do: Handoff.cleaned(h, id, charge)
  defp run({:clean_stay, charge}, h, id), do: Handoff.cleaned(h, id, charge)
  defp run({:clean_as, other, charge}, h, _), do: Handoff.cleaned(h, other, charge)

  # Requester and authority fakes forward everything to the test.
  defp forwarder(tag) do
    test = self()

    pid =
      spawn(fn ->
        Stream.repeatedly(fn ->
          receive do
            {:telelem_handoff, _h, msg} ->
              send(test, {tag, msg})

            {:do_cancel, h, reason} ->
              send(test, {tag, :cancel_result, Handoff.cancel(h, reason)})
          end
        end)
        |> Stream.run()
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp start(ids, opts \\ []) do
    req = forwarder(:req)
    auth = forwarder(:auth)
    base = [requester: req, attempts: ids, authority: auth, trace: self()]
    id = Keyword.get(opts, :child_id, Handoff)

    h =
      start_supervised!(
        Supervisor.child_spec({Handoff, Keyword.merge(base, opts) |> Keyword.delete(:child_id)},
          id: id
        )
      )

    %{
      h: h,
      hmon: Process.monitor(h),
      req: req,
      auth: auth,
      att: Map.new(ids, &{&1, attempt(h, &1)})
    }
  end

  defp do!(c, id, op) do
    send(c.att[id], {:do, op})
    assert_receive {:attempt, ^id, ^op, result}, @t
    result
  end

  defp to_barrier(c, ids), do: for(id <- ids, do: assert(do!(c, id, :attach) == :ok))

  defp until_closed(tag, acc \\ []) do
    receive do
      {^tag, {:closed, _} = ev} -> Enum.reverse([ev | acc])
      {^tag, ev} -> until_closed(tag, [ev | acc])
    after
      @t -> flunk("#{tag}: no {:closed, _} after #{inspect(Enum.reverse(acc))}")
    end
  end

  defp traced(c, ev), do: assert_receive({:telelem_handoff_trace, h, ^ev} when h == c.h, @t)

  defp assert_stopped(c),
    do: assert_receive({:DOWN, ref, :process, _, :normal} when ref == c.hmon, @t)

  defp kill_and_observe(pid) do
    mref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^mref, :process, ^pid, :killed}, @t
  end

  # ── Election, both orders ────────────────────────────────────────────────

  for {w, l} <- [{:a, :b}, {:b, :a}] do
    @w w
    @l l

    test "both at barrier, #{w} ready first: #{w} wins, #{l} cancelled and never released" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])

      assert do!(c, w, :ready) == :release
      assert_receive {:attempt, ^l, :msg, {:cancel, {:lost_to, ^w}}}, @t
      assert do!(c, l, :ready) == {:cancel, {:lost_to, w}}
      assert do!(c, l, {:transfer, "x"}) == {:error, :not_winner}
      # Claims :none, but it reached ready, so it may have been billed.
      assert do!(c, l, {:clean_stay, :none}) == :ok
      assert do!(c, l, {:clean, :none}) == {:error, :duplicate}
      traced(c, {:duplicate_cleanup, l})

      assert do!(c, w, :ready) == :release
      assert do!(c, w, {:transfer, "e1"}) == :ok
      assert do!(c, w, {:transfer, "e2"}) == :ok
      assert do!(c, w, {:finish, :stop}) == :ok
      assert do!(c, w, {:clean, :possible}) == :ok
      assert_stopped(c)

      assert [{:elected, ^w}, {:data, "e1"}, {:data, "e2"}, {:done, :stop}, {:closed, r}] =
               until_closed(:req)

      assert r.winner == w and r.committed and r.authority == :up and r.unreported == []
      assert r.attempts[w] == %{cleanup: :ack, charge: :possible, ready: true}
      assert r.attempts[l] == %{cleanup: :ack, charge: :possible, ready: true}

      assert [
               {:elected, ^w},
               {:attempt_closed, ^l, %{cleanup: :ack, charge: :possible}},
               {:committed, ^w},
               {:outcome, {:done, :stop}},
               {:attempt_closed, ^w, %{cleanup: :ack, charge: :possible}},
               {:closed, ^r}
             ] = until_closed(:auth)
    end

    # ── Kill requester before election ───────────────────────────────────

    test "requester killed before election (#{w} reaches ready later): no winner, both cancelled" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])

      kill_and_observe(c.req)
      traced(c, {:requester_down, :killed})
      reason = {:requester_down, :killed}
      assert_receive {:attempt, :a, :msg, {:cancel, ^reason}}, @t
      assert_receive {:attempt, :b, :msg, {:cancel, ^reason}}, @t

      # The barrier opens after the fault: a ready attempt cannot win.
      assert do!(c, w, :ready) == {:cancel, reason}
      assert do!(c, w, {:transfer, "x"}) == {:error, :not_winner}
      assert do!(c, w, {:clean, :none}) == :ok
      # l never reached ready and never dispatched.
      assert do!(c, l, {:clean, :none}) == :ok
      assert_stopped(c)

      refute_received {:attempt, _, :ready, :release}

      assert [
               {:outcome, {:failed, ^reason}},
               {:attempt_closed, ^w, %{cleanup: :ack, charge: :possible}},
               {:attempt_closed, ^l, %{cleanup: :ack, charge: :none}},
               {:closed, r}
             ] = until_closed(:auth)

      assert r.winner == nil and not r.committed
      assert r.possible_charges == [w]
    end

    # ── Kill winner during transfer ──────────────────────────────────────

    test "winner #{w} killed during transfer: incomplete, #{l} never released, close waits for #{l}'s ack" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])

      assert do!(c, w, :ready) == :release
      assert_receive {:attempt, ^l, :msg, {:cancel, {:lost_to, ^w}}}, @t
      # l is still parked at the barrier and has not acknowledged cleanup.
      assert do!(c, w, {:transfer, "tool-fragment"}) == :ok

      kill_and_observe(c.att[w])
      traced(c, {:attempt_down, w, :killed})

      st = Handoff.status(c.h)
      assert st.outcome == {:incomplete, {:winner_down, :killed}}
      assert st.attempts[l].cleanup == nil

      # Partial transfer and winner death do not reopen selection.
      assert do!(c, l, :ready) == {:cancel, {:lost_to, w}}
      assert do!(c, l, {:transfer, "replay"}) == {:error, :not_winner}
      assert Process.alive?(c.h)
      assert do!(c, l, {:clean, :possible}) == :ok
      assert_stopped(c)

      assert [
               {:elected, ^w},
               {:data, "tool-fragment"},
               {:incomplete, {:winner_down, :killed}},
               {:closed, r}
             ] = until_closed(:req)

      assert r.attempts[w] == %{cleanup: {:down, :killed}, charge: :possible, ready: true}
      assert r.attempts[l] == %{cleanup: :ack, charge: :possible, ready: true}
      assert r.possible_charges == [:a, :b]

      ev = until_closed(:auth)
      assert Enum.count(ev, &match?({:attempt_closed, ^w, _}, &1)) == 1
      assert Enum.count(ev, &match?({:attempt_closed, ^l, _}, &1)) == 1
      assert Enum.count(ev, &match?({:elected, _}, &1)) == 1
    end

    # ── Kill authority after commit ──────────────────────────────────────

    test "authority killed after #{w} committed: stream completes, no replay, charges stay possible and unreported" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])

      assert do!(c, w, :ready) == :release
      assert do!(c, l, {:clean, :possible}) == :ok
      assert do!(c, w, {:transfer, "c1"}) == :ok

      # Everything up to commit has been delivered to the old authority.
      assert_receive {:auth, {:elected, ^w}}, @t
      assert_receive {:auth, {:attempt_closed, ^l, %{charge: :possible}}}, @t
      assert_receive {:auth, {:committed, ^w}}, @t

      kill_and_observe(c.auth)
      traced(c, {:authority_down, :committed, :killed})

      assert do!(c, w, {:transfer, "c2"}) == :ok
      assert do!(c, w, {:finish, :end}) == :ok
      assert do!(c, w, {:clean, :none}) == :ok
      assert_stopped(c)

      assert [{:elected, ^w}, {:data, "c1"}, {:data, "c2"}, {:done, :end}, {:closed, r}] =
               until_closed(:req)

      # Local cleanup is complete...
      assert Enum.all?(r.attempts, fn {_, a} -> a.cleanup == :ack end)
      refute Process.alive?(c.att[w]) or Process.alive?(c.att[l])
      # ...but nothing has been settled: both dispatches may still be billed, and the
      # winner's close never reached an authority.
      assert r.authority == :lost
      assert r.possible_charges == [:a, :b]

      assert r.unreported == [
               {:outcome, {:done, :end}},
               {:attempt_closed, w, %{cleanup: :ack, charge: :possible}}
             ]
    end

    # ── Authority loss before commit fails closed ────────────────────────

    test "authority killed after electing #{w} but before commit: fails closed, no bytes" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])
      assert do!(c, w, :ready) == :release

      kill_and_observe(c.auth)
      traced(c, {:authority_down, :elected, :killed})
      assert_receive {:attempt, ^w, :msg, {:cancel, {:authority_down, :killed}}}, @t
      assert do!(c, w, {:transfer, "late"}) == {:error, :cancelled}
      assert do!(c, l, :ready) == {:cancel, {:lost_to, w}}
      assert do!(c, w, {:clean, :possible}) == :ok
      assert do!(c, l, {:clean, :possible}) == :ok
      assert_stopped(c)

      assert [{:elected, ^w}, {:failed, {:authority_down, :killed}}, {:closed, r}] =
               until_closed(:req)

      refute r.committed
      assert r.possible_charges == [:a, :b]
    end

    # ── Failed downstream write ──────────────────────────────────────────

    test "downstream write fails after #{w} committed: incomplete, selection stays closed" do
      {w, l} = {@w, @l}
      c = start([:a, :b])
      to_barrier(c, [:a, :b])
      assert do!(c, w, :ready) == :release
      assert do!(c, w, {:transfer, "part"}) == :ok

      send(c.req, {:do_cancel, c.h, :write_failed})
      assert_receive {:req, :cancel_result, :ok}, @t
      assert_receive {:attempt, ^w, :msg, {:cancel, :write_failed}}, @t
      assert do!(c, w, {:transfer, "more"}) == {:error, :cancelled}
      assert do!(c, l, :ready) == {:cancel, {:lost_to, w}}
      assert do!(c, w, {:clean, :possible}) == :ok
      assert do!(c, l, {:clean, :possible}) == :ok
      assert_stopped(c)

      assert [{:elected, ^w}, {:data, "part"}, {:incomplete, :write_failed}, {:closed, _}] =
               until_closed(:req)
    end
  end

  # ── Pre-dispatch refusal, registration, deadlines ──────────────────────

  test "refusal before dispatch allows the other attempt to win; refused attempt is :none" do
    c = start([:a, :b])
    assert do!(c, :a, :attach) == :ok
    assert do!(c, :a, {:clean, :none}) == :ok
    assert Handoff.status(c.h).outcome == nil
    to_barrier(c, [:b])
    assert do!(c, :b, :ready) == :release
    assert do!(c, :b, {:finish, :ok}) == :ok
    assert do!(c, :b, {:clean, :possible}) == :ok
    assert_stopped(c)
    assert [{:elected, :b}, {:done, :ok}, {:closed, r}] = until_closed(:req)
    assert r.possible_charges == [:b]
  end

  test "all attempts refused before dispatch: failed :no_winner, nothing charged" do
    c = start([:a, :b])
    to_barrier(c, [:a, :b])
    assert do!(c, :a, {:clean, :none}) == :ok
    assert do!(c, :b, {:clean, :none}) == :ok
    assert_stopped(c)
    assert [{:failed, :no_winner}, {:closed, r}] = until_closed(:req)
    assert r.possible_charges == []
  end

  test "attempt that never attaches is closed at the register deadline as :never_registered" do
    c = start([:a, :b], register_timeout: 50)
    to_barrier(c, [:a])
    assert do!(c, :a, {:clean, :none}) == :ok
    assert_stopped(c)
    assert [{:failed, :no_winner}, {:closed, r}] = until_closed(:req)
    assert r.attempts[:b] == %{cleanup: :never_registered, charge: :none, ready: false}
  end

  test "late, unknown, duplicate and foreign registrations are refused" do
    c = start([:a, :b, :c])
    to_barrier(c, [:a, :b])
    z = attempt(c.h, :z)
    c = %{c | att: Map.put(c.att, :z, z)}
    assert do!(c, :z, :attach) == {:error, :unknown_attempt}
    assert do!(c, :z, {:clean_as, :b, :none}) == {:error, :not_owner}

    assert do!(c, :a, :ready) == :release
    # :c never attached before election, so it must not dispatch now.
    assert do!(c, :c, :attach) == {:cancel, :closed}
    assert do!(c, :b, {:clean, :none}) == :ok
    assert do!(c, :a, {:finish, :ok}) == :ok
    assert do!(c, :a, {:clean, :possible}) == :ok
    assert_stopped(c)
    assert [{:elected, :a}, {:done, :ok}, {:closed, r}] = until_closed(:req)
    assert r.attempts[:c] == %{cleanup: :never_registered, charge: :none, ready: false}
    assert r.attempts[:b].charge == :none
  end

  test "loser that never acknowledges is killed at the cleanup deadline; charge stays possible" do
    c = start([:a, :b], cleanup_timeout: 50)
    to_barrier(c, [:a, :b])
    assert do!(c, :b, :ready) == :release
    assert_receive {:attempt, :a, :msg, {:cancel, {:lost_to, :b}}}, @t
    traced(c, {:cleanup_deadline, :a})
    assert do!(c, :b, {:finish, :ok}) == :ok
    assert do!(c, :b, {:clean, :possible}) == :ok
    assert_stopped(c)
    assert [{:elected, :b}, {:done, :ok}, {:closed, r}] = until_closed(:req)

    assert r.attempts[:a] == %{
             cleanup: {:killed, :cleanup_timeout},
             charge: :possible,
             ready: false
           }
  end

  test "acknowledgement before the cleanup deadline cancels the kill" do
    c = start([:a, :b], cleanup_timeout: 50)
    to_barrier(c, [:a, :b])
    lmon = Process.monitor(c.att[:a])
    assert do!(c, :b, :ready) == :release
    assert do!(c, :a, {:clean_stay, :none}) == :ok
    # Bounded wait past the deadline: proves absence of a kill, not ordering.
    refute_receive {:DOWN, ^lmon, _, _, _}, 150
    refute_received {:telelem_handoff_trace, _, {:cleanup_deadline, :a}}
    assert Handoff.status(c.h).attempts[:a].charge == :none
  end

  test "requester may not start_link the coordinator" do
    Process.flag(:trap_exit, true)

    assert {:error, :requester_must_not_be_parent} =
             Handoff.start_link(requester: self(), attempts: [:a])

    assert {:ok, h} = Handoff.start(requester: self(), attempts: [:a], register_timeout: 10)
    ref = Process.monitor(h)
    assert_receive {:telelem_handoff, ^h, {:failed, :no_winner}}, @t
    assert_receive {:DOWN, ^ref, _, _, :normal}, @t
  end

  # ── Seeded schedules vs. an independent reference ──────────────────────

  @ids [:a, :b, :c]

  test "seeded random schedules: one winner, one close per attempt, no data after outcome, charges never refunded" do
    seed = ExUnit.configuration()[:seed]
    :rand.seed(:exsss, {seed, 3, 7})

    for i <- 1..150 do
      schedule = for _ <- 1..Enum.random(1..14), do: gen_op()

      try do
        run_schedule(i, schedule)
      rescue
        e in ExUnit.AssertionError ->
          reraise %{
                    e
                    | message:
                        "#{e.message}\nseed=#{seed} iteration=#{i} schedule=#{inspect(schedule)}"
                  },
                  __STACKTRACE__
      end
    end
  end

  defp gen_op do
    id = Enum.random(@ids)

    Enum.random([
      {id, :attach},
      {id, :attach},
      {id, :ready},
      {id, :ready},
      {id, {:transfer, make_ref()}},
      {id, {:finish, :t}},
      {id, {:clean, Enum.random([:none, :possible])}},
      {id, :kill},
      :kill_requester,
      :kill_authority,
      :requester_cancel
    ])
  end

  defp run_schedule(i, schedule) do
    c = start(@ids, child_id: {:h, i}, cleanup_timeout: 60_000, register_timeout: 60_000)

    m = %{
      a: Map.new(@ids, &{&1, %{attached: false, ready: false, closed: nil, alive: true}}),
      releases: [],
      ok_data: [],
      ok_finish: 0,
      req_alive: true,
      auth_alive: true
    }

    m = Enum.reduce(schedule, m, &step(c, &1, &2))

    # Drive to closure: end the request, then acknowledge every live attempt.
    m = if m.req_alive, do: step(c, :kill_requester, m), else: m

    m =
      Enum.reduce(@ids, m, fn id, m ->
        if m.a[id].alive and m.a[id].closed == nil,
          do: step(c, {id, {:clean, :possible}}, m),
          else: m
      end)

    assert_receive {:telelem_handoff_trace, h, {:closed, r}} when h == c.h, @t
    assert_stopped(c)

    # At most one release, and it matches the report's winner.
    assert length(Enum.uniq(m.releases)) <= 1
    assert r.winner == List.first(m.releases)
    assert r.committed == (m.ok_data != [] or m.ok_finish > 0)

    for id <- @ids do
      a = m.a[id]

      {cleanup, charge} =
        case a.closed do
          {:ack, claim} -> {:ack, if(a.ready, do: :possible, else: claim)}
          :killed -> {{:down, :killed}, :possible}
          nil -> {:never_registered, :none}
        end

      assert r.attempts[id].cleanup == cleanup,
             "#{id}: #{inspect(r.attempts[id])} model #{inspect(a)}"

      assert r.attempts[id].charge == charge,
             "#{id}: #{inspect(r.attempts[id])} model #{inspect(a)}"
    end

    # Requester observations (prefix up to its death) are well formed.
    req = drain(:req)
    check_requester(req, Enum.reverse(m.ok_data))

    # A live authority saw exactly one close per attempt.
    if m.auth_alive do
      auth = until_closed(:auth)

      for id <- @ids,
          do: assert(Enum.count(auth, &match?({:attempt_closed, ^id, _}, &1)) == 1)
    else
      drain(:auth)
    end

    flush_attempts()
  end

  defp step(c, {id, :kill}, m) do
    a = m.a[id]

    if a.alive do
      kill_and_observe(c.att[id])
      # The coordinator monitors only registered, unclosed attempts.
      if a.attached and a.closed == nil, do: traced(c, {:attempt_down, id, :killed})
      closed = if a.attached and a.closed == nil, do: :killed, else: a.closed
      put_in(m.a[id], %{a | alive: false, closed: closed})
    else
      m
    end
  end

  defp step(c, :kill_requester, m) do
    if m.req_alive and alive?(c) do
      kill_and_observe(c.req)
      traced(c, {:requester_down, :killed})
    end

    %{m | req_alive: false}
  end

  defp step(c, :kill_authority, m) do
    if m.auth_alive and alive?(c) do
      kill_and_observe(c.auth)
      assert_receive {:telelem_handoff_trace, h, {:authority_down, _, :killed}} when h == c.h, @t
    end

    %{m | auth_alive: false}
  end

  defp step(c, :requester_cancel, m) do
    if m.req_alive do
      send(c.req, {:do_cancel, c.h, :client_gone})
      assert_receive {:req, :cancel_result, res}, @t
      assert res in [:ok, {:error, :handoff_down}]
    end

    m
  end

  defp step(c, {id, op}, m) do
    a = m.a[id]

    if not a.alive do
      m
    else
      res = do!(c, id, op)

      case {op, res} do
        {:attach, :ok} ->
          put_in(m.a[id].attached, true)

        {:ready, :release} ->
          %{put_in(m.a[id].ready, true) | releases: [id | m.releases]}

        {:ready, {:cancel, reason}} when reason != :closed ->
          put_in(m.a[id].ready, true)

        {{:transfer, chunk}, :ok} ->
          %{m | ok_data: [chunk | m.ok_data]}

        {{:finish, _}, :ok} ->
          %{m | ok_finish: m.ok_finish + 1}

        {{:clean, claim}, :ok} ->
          assert a.closed == nil, "second successful cleanup for #{id}"
          put_in(m.a[id], %{a | closed: {:ack, claim}, alive: false})

        _ ->
          m
      end
    end
  end

  # A coordinator that answers a call will also process later signals;
  # Process.alive?/1 can't tell that from one that is already stopping.
  defp alive?(c), do: is_map(Handoff.status(c.h))

  defp drain(tag, acc \\ []) do
    receive do
      {^tag, :cancel_result, _} -> drain(tag, acc)
      {^tag, ev} -> drain(tag, [ev | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp check_requester(events, ok_data) do
    outcomes = Enum.filter(events, &match?({k, _} when k in [:done, :incomplete, :failed], &1))
    assert length(outcomes) <= 1
    assert Enum.count(events, &match?({:elected, _}, &1)) <= 1
    {before_outcome, after_outcome} = Enum.split_while(events, &(&1 not in outcomes))
    assert Enum.all?(after_outcome, &(not match?({:data, _}, &1))), inspect(events)
    data = for {:data, d} <- before_outcome, do: d
    # The requester saw an in-order prefix of the accepted chunks.
    assert data == Enum.take(ok_data, length(data))

    case Enum.find_index(events, &match?({:data, _}, &1)) do
      nil -> :ok
      i -> assert Enum.any?(Enum.take(events, i), &match?({:elected, _}, &1)), inspect(events)
    end
  end

  defp flush_attempts do
    receive do
      {:attempt, _, _, _} -> flush_attempts()
      {:attempt, _, _} -> flush_attempts()
      {:telelem_handoff_trace, _, _} -> flush_attempts()
      {:DOWN, _, _, _, _} -> flush_attempts()
    after
      0 -> :ok
    end
  end
end
