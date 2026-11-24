defmodule Telelem.HandoffTerminalTest do
  @moduledoc """
  Terminal authorization serialized through the pinned admission authority,
  the shared terminal deadline, and upstream completion vs. confirmed
  downstream write (M2a).

  The authority is a real `Telelem.Admission.Authority` (from a real
  `Telelem.Instance`), pinned by PID. Orders are forced, not timed: the
  authority and/or coordinator is `:sys.suspend`ed while a request, grant or
  `:DOWN` is queued in a known mailbox position; the only wait for ordering
  is a bounded spin on `message_queue_len`. Deadline cases wait for a short
  absolute deadline by design.

  Probes for review (each a single test below):

    * P1 death wins: authority dies with the grant request unanswered →
      refused, marker handed to no one.
    * P2 grant wins: grant queued at the coordinator ahead of the authority's
      `:DOWN` → authorized; the death is only reported.
    * P3 pause after the grant (Sol's finish-first commit callback): the
      `:on_event` callback blocks inside the authorizing step while the
      authority dies → still authorized (the grant is the linearization point).
    * P4/P5 deadline: grant still pending at the deadline → refused, a later
      grant is stale; coordinator wedged past deadline + grace → the winner
      kills it and gets `:unconfirmed`, and the queued grant never releases.
    * P5b finish/4 with `deadline:`: a coordinator that never sends the
      initial reply is fenced at deadline + grace, not the 5s call timeout;
      a genuine `{:pending, d}` reply the caller only consumes after d (but
      before d + grace) does not restart the grace: fenced at d + grace, not
      at consumption + grace. Controls on the same schedules: legacy finish/3
      keeps its 5s initial-reply wait and restarts the grace from the late
      reply.
    * P6 write result pending at the deadline (requester alive, silent) →
      `{:incomplete, {:terminal_unconfirmed, :deadline}}`, never done.
    * P7 nil/empty marker; legacy fence-only fakes cannot be terminal
      authority or combine with `terminal_ack`.

      scripts/mix test test/telelem/handoff_terminal_test.exs
  """
  use ExUnit.Case, async: false

  alias Telelem.{Handoff, Instance}

  @moduletag :tmp_dir
  @t 1_000
  @marker "data: [DONE]\n\n"

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!(
        {Instance,
         id: :terminal_test,
         limits: %{slots: 4, units: 20},
         spool_dir: Path.join(dir, "spool"),
         transport: {Telelem.Test.Admission.TcpTransport, %{port: 1, label: "unused"}},
         reserve_ttl: nil}
      )

    inst = Instance.handle(sup)
    %{inst: inst, auth: Instance.authority(inst)}
  end

  defp spawn_tracked(fun) do
    pid = spawn(fun)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # Runs closures on request and forwards every handoff message as {tag, msg}.
  defp actor(tag) do
    test = self()

    spawn_tracked(fn ->
      Stream.repeatedly(fn ->
        receive do
          {:call, f} -> send(test, {:res, tag, f.()})
          {:telelem_handoff, _, m} -> send(test, {tag, m})
        end
      end)
      |> Stream.run()
    end)
  end

  defp run(pid, f), do: send(pid, {:call, f})

  defp run!(c, tag, f) do
    run(c.p[tag], f)
    assert_receive {:res, ^tag, r}, @t
    r
  end

  defp in_ms(ms), do: System.monotonic_time(:millisecond) + ms

  # Elects :a (:b loses and closes). Unless transfer: false, :a commits "c1".
  defp start(ctx, opts \\ []) do
    {transfer?, opts} = Keyword.pop(opts, :transfer, true)
    req = actor(:req)
    obs = actor(:obs)

    base = [
      requester: req,
      attempts: [:a, :b],
      admission_authority: ctx.auth,
      terminal_deadline: in_ms(10_000),
      observer: obs,
      trace: self()
    ]

    {:ok, h} = Handoff.start(Keyword.merge(base, opts))
    on_exit(fn -> Process.exit(h, :kill) end)
    hm = Process.monitor(h)
    c = %{h: h, hm: hm, auth: ctx.auth, p: %{req: req, obs: obs, a: actor(:a), b: actor(:b)}}

    for id <- [:a, :b], do: assert(run!(c, id, fn -> Handoff.attach(h, id) |> elem(0) end) == :ok)
    assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == :release
    assert run!(c, :b, fn -> Handoff.ready(h, :b) end) == {:cancel, {:lost_to, :a}}
    assert run!(c, :b, fn -> Handoff.cleaned(h, :b, :possible) end) == :ok
    if transfer?, do: assert(run!(c, :a, fn -> Handoff.transfer(h, "c1") end) == :ok)
    c
  end

  defp wait_q(p, n, until \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      elem(Process.info(p, :message_queue_len), 1) >= n ->
        :ok

      System.monotonic_time(:millisecond) > until ->
        flunk("queue of #{inspect(p)} never reached #{n}")

      true ->
        wait_q(p, n, until)
    end
  end

  defp kill(p) do
    m = Process.monitor(p)
    Process.exit(p, :kill)
    assert_receive {:DOWN, ^m, _, _, _}, @t
  end

  defp drain(tag, acc \\ []) do
    receive do
      {^tag, {:closed, _} = e} -> Enum.reverse([e | acc])
      {^tag, e} -> drain(tag, [e | acc])
    after
      @t -> Enum.reverse(acc)
    end
  end

  defp traces(h, acc \\ []) do
    receive do
      {:telelem_handoff_trace, ^h, ev} -> traces(h, [ev | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp marker_msgs(evs),
    do: for(e <- evs, match?({:terminal, _, _}, e) or e == {:data, @marker}, do: e)

  defp outcomes(evs), do: for({k, _} = e <- evs, k in [:done, :incomplete, :failed], do: e)

  # Starts finish/3 on :a with the authority suspended; returns once the grant
  # request sits unanswered in the authority's mailbox.
  defp finish_with_authority_suspended(c, marker \\ @marker) do
    :sys.suspend(c.auth)
    run(c.p.a, fn -> Handoff.finish(c.h, {:upstream_done, "stop"}, marker) end)
    wait_q(c.auth, 1)
  end

  # ── P1: death wins ──

  for ack <- [false, true] do
    @ack ack
    test "P1 authority dies before answering the grant request (terminal_ack: #{ack}): refused, no marker",
         ctx do
      c = start(ctx, terminal_ack: @ack)
      finish_with_authority_suspended(c)
      kill(c.auth)

      assert_receive {:res, :a, {:error, :cancelled}}, @t
      assert run!(c, :a, fn -> Handoff.cleaned(c.h, :a, :possible) end) == :ok
      assert_receive {:DOWN, _, _, _, :normal}, @t

      reason = {:authority_down, :killed}
      evs = drain(:req)
      assert marker_msgs(evs) == []
      assert [{:elected, :a}, {:data, "c1"}, {:incomplete, ^reason}, {:closed, r}] = evs
      assert r.authority == :lost and r.committed and r.terminal == nil
      assert r.authorization == :admission
      tr = traces(c.h)
      assert {:terminal_requested, :a} in tr
      refute Enum.any?(tr, &match?({:terminal_granted, _, _}, &1))
      refute Enum.any?(drain(:obs), &match?({:outcome, {:done, _}}, &1))
    end
  end

  test "P1 finish first (nothing committed): refused before any byte is failed, not incomplete",
       ctx do
    c = start(ctx, transfer: false)
    finish_with_authority_suspended(c)
    kill(c.auth)
    assert_receive {:res, :a, {:error, :cancelled}}, @t
    assert run!(c, :a, fn -> Handoff.cleaned(c.h, :a, :possible) end) == :ok

    assert [{:elected, :a}, {:failed, {:authority_down, :killed}}, {:closed, r}] = drain(:req)
    refute r.committed
  end

  # ── P2: grant wins over a death queued behind it ──

  for ack <- [false, true] do
    @ack ack
    test "P2 grant queued ahead of the authority's :DOWN (terminal_ack: #{ack}): authorized, death only reported",
         ctx do
      c = start(ctx, terminal_ack: @ack)
      h = c.h
      finish_with_authority_suspended(c)
      :sys.suspend(h)
      # The authority answers into the suspended coordinator, then dies.
      :sys.resume(c.auth)
      wait_q(h, 1)
      kill(c.auth)
      wait_q(h, 2)
      :sys.resume(h)

      assert_receive {:res, :a, :ok}, @t
      tr = traces(h) ++ (Process.sleep(50) && traces(h))
      ig = Enum.find_index(tr, &match?({:terminal_granted, :a, _}, &1))
      id = Enum.find_index(tr, &match?({:authority_down, _, :killed}, &1))
      assert ig != nil and id != nil and ig < id

      if @ack do
        assert_receive {:req, {:terminal, @marker, d}} when is_integer(d), @t
        assert Handoff.status(h).outcome == nil
        assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == :ok
      end

      assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
      assert_receive {:DOWN, _, _, _, :normal}, @t
      evs = drain(:req)
      assert outcomes(evs) == [{:done, {:upstream_done, "stop"}}]
      {:closed, r} = List.last(evs)
      assert r.authority == :lost

      assert r.terminal.downstream == if(@ack, do: :written, else: :not_tracked)
      unless @ack, do: assert({:data, @marker} in evs)
    end
  end

  # ── P3: a pause after the grant does not reopen the decision ──

  test "P3 finish-first commit callback blocks inside the authorizing step while the authority dies: still authorized",
       ctx do
    test = self()

    on_event = fn
      {:committed, _} ->
        send(test, {:paused_in_commit, self()})
        receive do: (:go -> :ok)

      _ ->
        :ok
    end

    c = start(ctx, transfer: false, terminal_ack: true, on_event: on_event)
    h = c.h
    run(c.p.a, fn -> Handoff.finish(h, {:upstream_done, "stop"}, @marker) end)
    assert_receive {:paused_in_commit, ^h}, @t
    assert {:terminal_granted, :a, _} = List.last(traces(h))
    kill(c.auth)
    send(h, :go)

    assert_receive {:res, :a, :ok}, @t
    assert_receive {:req, {:terminal, @marker, _}}, @t
    assert_receive {:telelem_handoff_trace, ^h, {:authority_down, :committed, :killed}}, @t
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == :ok
    assert Handoff.status(h).outcome == {:done, {:upstream_done, "stop"}}
  end

  # ── P4/P5: deadline on the pending grant, no late side effect ──

  test "P4 grant still pending at the deadline: refused; the grant that arrives later is stale",
       ctx do
    c = start(ctx, terminal_deadline: in_ms(300))
    finish_with_authority_suspended(c)
    assert_receive {:res, :a, {:error, :cancelled}}, 2_000
    assert_receive {:telelem_handoff_trace, _, {:terminal_deadline, :pending_grant}}, @t
    :sys.resume(c.auth)
    assert_receive {:telelem_handoff_trace, _, :stale_terminal_grant}, @t
    assert run!(c, :a, fn -> Handoff.cleaned(c.h, :a, :possible) end) == :ok

    evs = drain(:req)
    assert marker_msgs(evs) == []
    assert outcomes(evs) == [{:incomplete, :terminal_deadline}]
  end

  test "P4 finish at or after the deadline is refused without asking the authority", ctx do
    c = start(ctx, terminal_deadline: in_ms(-1))
    assert run!(c, :a, fn -> Handoff.finish(c.h, :t, @marker) end) == {:error, :cancelled}
    refute Enum.any?(traces(c.h), &match?({:terminal_requested, _}, &1))
    assert marker_msgs(drain(:req)) == []
  end

  test "P5 coordinator wedged past deadline + grace with a grant queued: the winner fences it, unconfirmed, no marker ever",
       ctx do
    c = start(ctx, terminal_deadline: in_ms(200))
    h = c.h
    finish_with_authority_suspended(c)
    :sys.suspend(h)
    :sys.resume(c.auth)
    # The grant (and the deadline timer) now sit in the wedged coordinator.
    wait_q(h, 1)

    assert_receive {:res, :a, {:error, :unconfirmed}}, 3_000
    assert_receive {:DOWN, ref, :process, ^h, :killed} when ref == c.hm, @t
    refute Process.alive?(h)
    assert marker_msgs(drain(:req)) == []
  end

  # finish/4 with the shared deadline: the caller times both waits against one
  # absolute bound, deadline + grace (1_000 ms in Handoff).
  @grace 1_000

  defp timed_finish(c, d) do
    run(c.p.a, fn ->
      r = Handoff.finish(c.h, {:upstream_done, "stop"}, @marker, deadline: d)
      {r, System.monotonic_time(:millisecond)}
    end)
  end

  test "P5b coordinator never sends the initial reply: finish/4 fences at deadline + grace, not the 5s call timeout",
       ctx do
    d = in_ms(200)
    c = start(ctx, terminal_deadline: d)
    h = c.h
    # Wedged before it can even see the finish call.
    :sys.suspend(h)
    timed_finish(c, d)
    wait_q(h, 1)

    assert_receive {:res, :a, {{:error, :unconfirmed}, t}}, 3_000
    assert t >= d + @grace
    assert t < d + @grace + 1_000
    assert_receive {:DOWN, ref, :process, ^h, :killed} when ref == c.hm, @t
    refute Enum.any?(traces(h), &match?({:terminal_requested, _}, &1))
    assert marker_msgs(drain(:req)) == []
  end

  # The two-phase schedule shared by the timed test and its legacy control.
  # The coordinator genuinely replies {:pending, d} before d (grant request
  # stuck at the suspended authority) and then wedges before its deadline
  # timer can act. The caller is suspended with that reply already in its
  # mailbox and resumed only at r, after d but before d + grace, so it
  # consumes the reply (ends phase one) at or after r. Returns {r, actor}.
  # Synchronization is by mailbox contents, not timing; the only sleep is the
  # wait for the absolute instant r, by design.
  @late_by 500

  defp pending_reply_consumed_after_deadline(c, d, finish_fun) do
    h = c.h
    a = c.p.a
    :sys.suspend(c.auth)
    :sys.suspend(h)
    run(a, finish_fun)
    # The finish call is queued at the wedged coordinator; the caller waits.
    wait_q(h, 1)
    true = :erlang.suspend_process(a)
    :sys.resume(h)
    assert_receive {:telelem_handoff_trace, ^h, {:terminal_requested, :a}}, @t
    # Synchronous: returns after the coordinator finished the finish call.
    :sys.suspend(h)
    wait_msg(a, &match?({_, {:pending, ^d}}, &1))
    assert System.monotonic_time(:millisecond) < d, "pending reply not queued before d"

    Process.sleep(max(d + @late_by - System.monotonic_time(:millisecond), 0))
    r = System.monotonic_time(:millisecond)
    true = :erlang.resume_process(a)
    assert r > d and r < d + @grace
    r
  end

  defp wait_msg(p, pred, until \\ System.monotonic_time(:millisecond) + 5_000) do
    {:messages, msgs} = Process.info(p, :messages)

    cond do
      Enum.any?(msgs, pred) ->
        :ok

      System.monotonic_time(:millisecond) > until ->
        flunk("no expected message in #{inspect(msgs)}")

      true ->
        wait_msg(p, pred, until)
    end
  end

  test "P5b pending reply consumed after d but before d + grace: finish/4 does not restart the grace",
       ctx do
    d = in_ms(400)
    c = start(ctx, terminal_deadline: d)
    h = c.h

    r =
      pending_reply_consumed_after_deadline(c, d, fn ->
        res = Handoff.finish(h, {:upstream_done, "stop"}, @marker, deadline: d)
        {res, System.monotonic_time(:millisecond)}
      end)

    assert_receive {:res, :a, {{:error, :unconfirmed}, t}}, 3_000
    # Fenced at the absolute bound...
    assert t >= d + @grace
    # ...not at (reply consumed) + grace, which is >= r + grace.
    assert t < r + @grace, "grace restarted: t=#{t - d}ms after d, reply consumed at #{r - d}ms"
    assert_receive {:DOWN, ref, :process, ^h, :killed} when ref == c.hm, @t
    :sys.resume(c.auth)
    assert marker_msgs(drain(:req)) == []
  end

  test "P5b control: on the same schedule legacy finish/3 restarts the grace from the late reply",
       ctx do
    d = in_ms(400)
    c = start(ctx, terminal_deadline: d)
    h = c.h

    r =
      pending_reply_consumed_after_deadline(c, d, fn ->
        res = Handoff.finish(h, {:upstream_done, "stop"}, @marker)
        {res, System.monotonic_time(:millisecond)}
      end)

    assert_receive {:res, :a, {{:error, :unconfirmed}, t}}, 3_000
    # Legacy (unchanged): max(reported deadline, arrival) + grace. This is the
    # behavior the timed test above must not show.
    assert t >= r + @grace
    assert_receive {:DOWN, ref, :process, ^h, :killed} when ref == c.hm, @t
    :sys.resume(c.auth)
    assert marker_msgs(drain(:req)) == []
  end

  test "P5b legacy finish/3 (no caller deadline) keeps the 5s initial-reply wait", ctx do
    d = in_ms(100)
    c = start(ctx, terminal_deadline: d)
    h = c.h
    :sys.suspend(h)
    run(c.p.a, fn -> Handoff.finish(h, :t, @marker) end)
    wait_q(h, 1)
    # Well past deadline + grace the legacy caller is still waiting...
    refute_receive {:res, :a, _}, d + @grace + 300 - System.monotonic_time(:millisecond)
    # ...so the reply still reaches it: refused at the deadline, no fence.
    :sys.resume(h)
    assert_receive {:res, :a, {:error, :cancelled}}, @t
    assert Process.alive?(h)
    assert marker_msgs(drain(:req)) == []
  end

  test "P5 coordinator lost while the grant is pending: unconfirmed, never a refusal", ctx do
    c = start(ctx)
    finish_with_authority_suspended(c)
    kill(c.h)
    assert_receive {:res, :a, {:error, :unconfirmed}}, @t
    assert marker_msgs(drain(:req)) == []
  end

  test "P5 requester dies while the grant is pending: refused, the late grant is stale", ctx do
    c = start(ctx)
    finish_with_authority_suspended(c)
    kill(c.p.req)
    assert_receive {:res, :a, {:error, :cancelled}}, @t
    :sys.resume(c.auth)
    assert_receive {:telelem_handoff_trace, _, :stale_terminal_grant}, @t
  end

  # ── P6: write result bounded independently of the requester ──

  test "P6 requester alive but silent at the deadline: incomplete/unconfirmed, a late ack can't make it done",
       ctx do
    c = start(ctx, terminal_ack: true, terminal_deadline: in_ms(300))
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert_receive {:req, {:terminal, @marker, _}}, @t
    assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok

    assert_receive {:req, {:incomplete, {:terminal_unconfirmed, :deadline}}}, 2_000
    assert_receive {:req, {:closed, r}}, @t
    assert r.terminal == %{upstream: :complete, downstream: :unconfirmed}
    assert r.possible_charges == [:a, :b]
  end

  test "P6 ack queued before the deadline but processed after it: refused as late, never done",
       ctx do
    d = in_ms(300)
    c = start(ctx, terminal_ack: true, terminal_deadline: d)
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert_receive {:req, {:terminal, @marker, ^d}}, @t
    :sys.suspend(h)
    run(c.p.req, fn -> Handoff.terminal_written(h, :ok) end)
    wait_q(h, 1)
    Process.sleep(max(d - System.monotonic_time(:millisecond), 0) + 20)
    :sys.resume(h)

    assert_receive {:res, :req, {:error, :deadline}}, @t
    assert Handoff.status(h).outcome == {:incomplete, {:terminal_unconfirmed, :deadline}}
  end

  # ── existing ack-mode contract, now against the real authority ──

  test "downstream write failure after authorization: incomplete, never done", ctx do
    for {how, expect} <- [
          {fn h -> Handoff.terminal_written(h, {:error, :closed}) end,
           {:incomplete, {:downstream_write, :closed}}},
          {fn h -> Handoff.cancel(h, {:downstream_closed, :closed}) end,
           {:incomplete, {:downstream_closed, :closed}}}
        ] do
      c = start(ctx, terminal_ack: true)
      h = c.h
      assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
      assert_receive {:req, {:terminal, @marker, _}}, @t
      assert run!(c, :req, fn -> how.(h) end) == :ok
      assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
      assert_receive {:DOWN, _, _, _, :normal}, @t

      evs = drain(:req)
      assert outcomes(evs) == [expect]
      {:closed, r} = List.last(evs)
      assert r.terminal.upstream == :complete and r.terminal.downstream != :written
    end
  end

  test "requester dies holding the marker: incomplete, never done", ctx do
    c = start(ctx, terminal_ack: true)
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert_receive {:req, {:terminal, @marker, _}}, @t
    kill(c.p.req)

    assert_receive {:telelem_handoff_trace, ^h,
                    {:outcome, {:incomplete, {:requester_down, :killed}}}},
                   @t
  end

  @tag :capture_log
  test "coordinator exits (parent) while the write result is pending: incomplete, never done",
       ctx do
    test = self()
    req = actor(:req)
    auth = ctx.auth

    parent =
      spawn_tracked(fn ->
        {:ok, h} =
          Handoff.start_link(
            requester: req,
            attempts: [:a],
            admission_authority: auth,
            terminal_ack: true,
            terminal_deadline: in_ms(10_000)
          )

        send(test, {:h, h})
        receive do: (:never -> :ok)
      end)

    assert_receive {:h, h}, @t
    hm = Process.monitor(h)
    c = %{h: h, p: %{req: req, a: actor(:a)}}
    assert run!(c, :a, fn -> Handoff.attach(h, :a) |> elem(0) end) == :ok
    assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == :release
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    Process.exit(parent, :kill)
    assert_receive {:DOWN, ^hm, _, _, :killed}, @t

    assert [{:elected, :a}, {:terminal, @marker, _}, {:incomplete, {:handoff_down, :killed}}] =
             drain(:req)
  end

  test "after authorization nothing else is accepted; write result only from the requester, once",
       ctx do
    c = start(ctx, terminal_ack: true)
    h = c.h
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == {:error, :no_terminal}
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert run!(c, :a, fn -> Handoff.transfer(h, "late") end) == {:error, :cancelled}
    assert run!(c, :a, fn -> Handoff.finish(h, :again, @marker) end) == {:error, :cancelled}
    assert run!(c, :a, fn -> Handoff.begin_dispatch(h, :a) end) == {:cancel, :ended}
    assert run!(c, :a, fn -> Handoff.terminal_written(h, :ok) end) == {:error, :not_requester}
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == :ok
    assert run!(c, :req, fn -> Handoff.terminal_written(h, {:error, :x}) end) == {:error, :ended}
    assert Handoff.status(h).outcome == {:done, :t}
  end

  test "while a grant is pending the winner can't transfer or finish again", ctx do
    c = start(ctx)
    h = c.h
    finish_with_authority_suspended(c)
    # A second process can't act as the winner either.
    assert Handoff.transfer(h, "x") == {:error, :not_winner}
    assert Handoff.status(h).terminal_pending
    :sys.resume(c.auth)
    assert_receive {:res, :a, :ok}, @t
  end

  # ── P7: marker and interface validation ──

  test "P7 nil or empty marker is rejected in admission and ack modes; nothing is requested",
       ctx do
    for opts <- [[], [terminal_ack: true]] do
      c = start(ctx, opts)
      assert run!(c, :a, fn -> Handoff.finish(c.h, :t, nil) end) == {:error, :invalid_marker}
      assert run!(c, :a, fn -> Handoff.finish(c.h, :t, "") end) == {:error, :invalid_marker}
      refute Enum.any?(traces(c.h), &match?({:terminal_requested, _}, &1))
      assert run!(c, :a, fn -> Handoff.finish(c.h, :t, @marker) end) == :ok
    end
  end

  test "P7 interface: a generic PID can't be terminal authority, and fence-only can't do terminal_ack",
       ctx do
    fake = spawn_tracked(fn -> receive do: (:never -> :ok) end)
    base = [requester: self(), attempts: [:a], terminal_deadline: in_ms(1_000)]

    assert Handoff.start(base ++ [admission_authority: fake]) ==
             {:error, {:bad_option, :admission_authority}}

    assert Handoff.start(base ++ [authority: fake, terminal_ack: true]) ==
             {:error, {:bad_option, {:terminal_ack, :requires_admission_authority}}}

    assert Handoff.start(base ++ [terminal_ack: true]) ==
             {:error, {:bad_option, {:terminal_ack, :requires_admission_authority}}}

    assert Handoff.start(base ++ [authority: fake, admission_authority: ctx.auth]) ==
             {:error, {:bad_option, :authority}}

    assert Handoff.start(
             requester: self(),
             attempts: [:a],
             admission_authority: ctx.auth
           ) == {:error, {:bad_option, :terminal_deadline}}

    dead = spawn(fn -> :ok end)
    kill(dead)

    assert Handoff.start(base ++ [admission_authority: dead]) ==
             {:error, {:bad_option, :admission_authority}}
  end

  test "P7 legacy fence-only authority: mailbox order decides, no serialization claimed" do
    for {order, expect} <- [down_behind_finish: :ok, down_ahead_of_finish: {:error, :cancelled}] do
      req = actor(:req)
      fake = actor(:fake)
      {:ok, h} = Handoff.start(requester: req, attempts: [:a], authority: fake)
      c = %{h: h, p: %{req: req, a: actor(:a)}}
      assert run!(c, :a, fn -> Handoff.attach(h, :a) |> elem(0) end) == :ok
      assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == :release
      :sys.suspend(h)

      if order == :down_ahead_of_finish do
        kill(fake)
        wait_q(h, 1)
        run(c.p.a, fn -> Handoff.finish(h, :t, @marker) end)
      else
        run(c.p.a, fn -> Handoff.finish(h, :t, @marker) end)
        wait_q(h, 1)
        kill(fake)
      end

      wait_q(h, 2)
      :sys.resume(h)
      assert_receive {:res, :a, ^expect}, @t
      assert Handoff.status(h).authorization == :fence_only
      _ = drain(:req)
    end
  end
end
