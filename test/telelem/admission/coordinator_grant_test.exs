defmodule Telelem.Admission.CoordinatorGrantTest do
  @moduledoc """
  TEL-011/TEL-012: the opt-in coordinator-owned admission grant of
  `Telelem.Handoff` (`coordinator_grant: {:one_primary, units}`).

  Listener-free. One `Telelem.Instance` with no workers (no transport is
  started), its real `Telelem.Admission.Authority` pinned by pid, one real
  handoff per request and one scripted fake attempt. The fake opens no
  socket: "connect" is simulated and its HTTP send is a `{:fake_http, fake,
  marker}` message to the test. The fake sends only after
  `begin_dispatch/3 == :ok`, while it has seen neither a handoff cancel nor
  the pinned authority's `:DOWN`, and only if its own clock check
  (`now < deadline`), made immediately before the send, passes.

  Ordering. `:sys.suspend/1` holds the authority or the coordinator; an
  order is asserted from the suspended process's actual mailbox
  (`Process.info/2`, re-read until the expected message is there, bounded
  by `@t`) before resuming it. A receive trace on the authority records
  every request it got. Deadline cases use a short real deadline
  (`@window`); the coordinator's own deadline timer message, observed in its
  mailbox or trace, is the barrier that the deadline has passed. There are
  no sleeps; receive timeouts only detect failure.

  Only the authority's ledger is evidence of reservation and exposure.

      scripts/mix test test/telelem/admission/coordinator_grant_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.{Authority, Ledger}
  alias Telelem.{Handoff, Instance}
  alias Telelem.Instance.Directory

  @moduletag :tmp_dir
  @t 1_000
  # Handoff's grace after the deadline for a bounded caller wait.
  @grace 1_000
  # Deadline for expiry cases, from the handoff's start.
  @window 300
  @slots 2
  @limit 10
  @units 4
  @left @limit - @units
  @http_marker "POST /v1/fake (simulated, no socket)"
  @marker "data: [DONE]\n\n"

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        id: :coordinator_grant_gw,
        limits: %{slots: @slots, units: @limit},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)

    on_exit(fn ->
      try do
        if Process.alive?(auth), do: :sys.resume(auth)
      catch
        :exit, _ -> :ok
      end
    end)

    %{inst: inst, auth: auth, tag: inst.tag}
  end

  # ── options ─────────────────────────────────────────────────────────────

  test "opt-in is explicit and validated; without it the status shape is unchanged", ctx do
    req = idle()
    d = now() + 60_000
    base = [requester: req, attempts: [:p], admission_authority: ctx.auth, terminal_deadline: d]

    for bad <- [
          [coordinator_grant: {:one_primary, 0}],
          [coordinator_grant: {:one_primary, -1}],
          [coordinator_grant: {:one_primary, 1.5}],
          [coordinator_grant: {:one_primary, :four}],
          [coordinator_grant: {:two_primaries, @units}],
          [coordinator_grant: @units],
          [coordinator_grant: {:one_primary, @units}, attempts: [:p, :q]]
        ] do
      assert {:error, {:bad_option, :coordinator_grant}} =
               Handoff.start(Keyword.merge(base, bad)),
             inspect(bad)
    end

    # A fence-only authority cannot own grants.
    assert {:error, {:bad_option, :coordinator_grant}} =
             Handoff.start(
               requester: req,
               attempts: [:p],
               authority: idle(),
               terminal_deadline: d,
               coordinator_grant: {:one_primary, @units}
             )

    # The admission authority already requires the absolute deadline.
    assert {:error, {:bad_option, :terminal_deadline}} =
             Handoff.start(
               requester: req,
               attempts: [:p],
               admission_authority: ctx.auth,
               coordinator_grant: {:one_primary, @units}
             )

    {:ok, h} = Handoff.start(base)
    on_exit(fn -> Process.exit(h, :kill) end)
    refute Map.has_key?(Handoff.status(h), :grant)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  # ── happy path ──────────────────────────────────────────────────────────

  test "reserve for the attached pid before connect, authorize after it, one permission, one send, marker, nothing private outward",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake} = r

    assert {:ok, _mref} = step!(fake, :attach)

    # Reserved, owned and monitored as exactly the attached pid, before connect.
    {gid, g} = only_grant!(ctx.auth)
    assert %{owner: ^fake, state: :reserved, units: @units} = g
    assert {:process, fake} in elem(Process.info(ctx.auth, :monitors), 1)

    assert %{grant: :reserved, attempts: %{p: %{pid: ^fake, dispatched: false}}} =
             Handoff.status(h)

    assert {:ok, :simulated} = step!(fake, :connect)
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_gate, ^fake, :open}, @t
    assert_receive {:fake_http, ^fake, @http_marker}, @t
    assert Authority.snapshot(ctx.auth).grants[gid].state == :dispatched
    status = Handoff.status(h)
    assert %{grant: :authorized, attempts: %{p: %{dispatched: true}}} = status

    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:transfer, "chunk"})
    assert :ok = step!(fake, {:finish, :t, @marker})

    assert_receive {:req, {:telelem_handoff, ^h, {:elected, :p}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:data, "chunk"}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:data, @marker}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:done, :t}}}, @t

    hmon = Process.monitor(h)
    assert :ok = step!(fake, {:cleanup, :possible})
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, report}}}, @t
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert %{outcome: {:done, :t}, grant: :authorized, possible_charges: [:p]} = report

    # One reserve, one authorize, one terminal request; no cancel, settle or retry.
    assert [{:reserve, _, ^fake, @units}, {:authorize, ^gid}, :terminal] =
             authority_requests!(ctx.auth, h)

    assert grant_phases(r) == [:reserving, :reserved, :authorizing, :authorized]
    refute_received {:fake_http, _, _}
    assert_private!(ctx, r, gid, [status, report])

    # The coordinator settles nothing: the owner's exit leaves it encumbered.
    leave!(ctx, fake, gid)
    assert_ledger!(ctx, gid, :unresolved, encumbered: @units, available: @left)
  end

  # ── release order ───────────────────────────────────────────────────────

  for ack <- [false, true] do
    @ack ack
    test "terminal_ack=#{ack}: a blocked observer callback does not hold back the marker or the winner's decision",
         ctx do
      test = self()
      gate = make_ref()

      on_event = fn
        {:committed, _} = ev ->
          send(test, {:ev_blocked, gate, ev})
          receive do: ({:ev_go, ^gate} -> :ok)

        _ ->
          :ok
      end

      r = start_h!(ctx, on_event: on_event, terminal_ack: @ack)
      %{h: h, fake: fake, d: d} = r
      permit!(r)
      assert :release = step!(fake, :ready)

      fref = step_async(fake, {:finish, :t, @marker})
      assert_receive {:ev_blocked, ^gate, {:committed, :p}}, @t

      # The coordinator is still inside the callback: everything below was
      # sent before it.
      if @ack do
        assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, ^d}}}, @t
      else
        assert_receive {:req, {:telelem_handoff, ^h, {:data, @marker}}}, @t
        assert_receive {:req, {:telelem_handoff, ^h, {:done, :t}}}, @t
      end

      assert :ok = await_step!(fake, fref)
      send(h, {:ev_go, gate})

      if @ack do
        ref = req_do(r, fn -> Handoff.terminal_written(h, :ok) end)
        assert_receive {:req_did, ^ref, :ok}, @t
        assert_receive {:req, {:telelem_handoff, ^h, {:done, :t}}}, @t
      end

      assert :ok = step!(fake, {:cleanup, :possible})
      assert_receive {:req, {:telelem_handoff, ^h, {:closed, report}}}, @t
      downstream = if @ack, do: :written, else: :not_tracked

      assert %{outcome: {:done, :t}, terminal: %{upstream: :complete, downstream: ^downstream}} =
               report

      refute_received {:req, {:telelem_handoff, ^h, {:data, @marker}}}
    end
  end

  # ── bypasses ────────────────────────────────────────────────────────────

  test "ready, transfer and finish before the permission are refused and change nothing", ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake} = r
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    {gid, _} = only_grant!(ctx.auth)

    assert {:error, :not_permitted} = step!(fake, :ready)
    assert {:error, :not_winner} = step!(fake, {:transfer, "early"})
    assert {:error, :not_winner} = step!(fake, {:finish, :t, @marker})

    assert %{
             phase: :open,
             winner: nil,
             grant: :reserved,
             attempts: %{p: %{ready: false, dispatched: false}}
           } = Handoff.status(h)

    assert Authority.snapshot(ctx.auth).grants[gid].state == :reserved
    refute_received {:req, {:telelem_handoff, ^h, _}}

    # The legitimate path still works afterwards, once.
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_http, ^fake, @http_marker}, @t
    assert :release = step!(fake, :ready)
    assert [{:reserve, _, ^fake, @units}, {:authorize, ^gid}] = authority_requests!(ctx.auth, h)
  end

  test "attach and begin_dispatch need the shared deadline; no second reservation or permission",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake, d: d} = r

    assert {:error, :deadline_required} = step!(fake, {:attach, []})
    assert {:error, :deadline_required} = step!(fake, {:attach, [deadline: d + 1]})
    assert %{attempts: %{p: %{pid: nil}}, grant: :idle} = Handoff.status(h)
    assert Authority.snapshot(ctx.auth).grants == %{}

    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, _} = step!(fake, :attach)
    {gid, _} = only_grant!(ctx.auth)

    intruder = intruder(h, d)
    assert {:error, :already_attached} = intrude!(intruder, :attach)
    assert {:error, :not_attached} = intrude!(intruder, :begin_dispatch)

    assert {:ok, :simulated} = step!(fake, :connect)
    assert {:error, :deadline_required} = step!(fake, {:begin_dispatch, []})

    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, {:error, :deadline_required}}}},
                   @t

    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_gate, ^fake, :open}, @t
    assert {:error, :duplicate} = step!(fake, :begin_dispatch)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, {:error, :duplicate}}}}, @t

    assert_receive {:fake_http, ^fake, @http_marker}, @t
    refute_received {:fake_http, _, _}
    assert [{:reserve, _, ^fake, @units}, {:authorize, ^gid}] = authority_requests!(ctx.auth, h)
  end

  # ── uncertainty and abandonment ─────────────────────────────────────────

  test "requester cancel while authorize is outstanding: refused, no send; the abandoned authorize still encumbers",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake} = r
    {gid, ref} = authorizing!(ctx, r)

    cref = req_do(r, fn -> Handoff.cancel(h, :client_gone) end)
    assert_receive {:req_did, ^cref, :ok}, @t
    assert {:cancel, :client_gone} = await_step!(fake, ref)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, {:cancel, :client_gone}}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :client_gone}}}, @t
    assert %{grant: :abandoned} = Handoff.status(h)
    assert Ledger.grant(Directory.ledger(ctx.inst.table), gid).state == :reserved
    # Abandoned: the request monitor is gone, only the pinned one is left.
    assert [{:process, _}] = for(m <- monitors(h), m == {:process, ctx.auth}, do: m)

    # The authority still applies it; the reply goes nowhere (the request's
    # alias is inactive), so it never reaches the held coordinator.
    :ok = :sys.suspend(h)
    :ok = :sys.resume(ctx.auth)
    assert_ledger!(ctx, gid, :dispatched, active: 1, encumbered: @units, available: @left)
    assert [] = mailbox(h)
    :ok = :sys.resume(h)
    assert %{grant: :abandoned} = Handoff.status(h)

    close!(r, :none)
    leave!(ctx, fake, gid)
    assert_ledger!(ctx, gid, :unresolved, encumbered: @units, available: @left)

    refute {:grant, :p, :authorized} in collected(r)
    assert {:grant, :p, {:abandoned, :authorizing}} in collected(r)
    assert [{:reserve, _, ^fake, @units}, {:authorize, ^gid}] = authority_requests!(ctx.auth, h)
    refute_received {:fake_http, _, _}
    assert_private!(ctx, r, gid, [])
  end

  test "a fresh reply already queued behind the cancel is taken by the abandonment and discarded",
       ctx do
    r = start_h!(ctx)
    %{h: h, fake: fake, req: req} = r
    {gid, ref} = authorizing!(ctx, r)

    :ok = :sys.suspend(h)
    cref = req_do(r, fn -> Handoff.cancel(h, :client_gone) end)
    await_mailbox!(h, &match?([{:"$gen_call", {^req, _}, {:cancel, :client_gone}}], &1))
    :ok = :sys.resume(ctx.auth)

    assert [{:"$gen_call", {^req, _}, {:cancel, :client_gone}}, {_, {:ok, ^gid}}] =
             await_mailbox!(h, &match?([_, {_, {:ok, ^gid}}], &1))

    :ok = :sys.resume(h)
    assert_receive {:req_did, ^cref, :ok}, @t
    assert {:cancel, :client_gone} = await_step!(fake, ref)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, _}}}, @t

    assert %{grant: :abandoned, outcome: {:failed, :client_gone}} = Handoff.status(h)
    assert {:messages, []} = Process.info(h, :messages)
    assert [{:process, _}] = for(m <- monitors(h), m == {:process, ctx.auth}, do: m)
    refute {:grant, :p, :authorized} in collected(r)
    assert_ledger!(ctx, gid, :dispatched, active: 1, encumbered: @units, available: @left)
    refute_received {:fake_http, _, _}
  end

  for stage <- [:reserve, :authorize] do
    @stage stage
    test "#{stage}: a fresh ok reply handled after the deadline is refused by the clock check",
         ctx do
      r = start_h!(ctx, deadline: now() + @window)
      %{h: h, fake: fake, d: d} = r

      if @stage == :authorize do
        assert {:ok, _} = step!(fake, :attach)
        assert {:ok, :simulated} = step!(fake, :connect)
      end

      assert now() < d, "setup took longer than the #{@window} ms window"
      :ok = :sys.suspend(ctx.auth)
      ref = step_async(fake, if(@stage == :reserve, do: :attach, else: :begin_dispatch))

      await_mailbox!(ctx.auth, fn
        [{:"$gen_call", {^h, _}, req} | _] -> elem(req, 0) == @stage
        _ -> false
      end)

      refute_received {:fake, ^fake, ^ref, _}

      :ok = :sys.suspend(h)
      :ok = :sys.resume(ctx.auth)
      await_mailbox!(h, &match?([{_, {:ok, _}}], &1))
      # The coordinator's deadline timer, queued behind the reply: the
      # deadline has passed before the reply is handled.
      assert [{_, {:ok, gid}}, {:terminal_deadline, _}] =
               await_mailbox!(h, &match?([_, {:terminal_deadline, _}], &1), d + @t)

      :ok = :sys.resume(h)
      assert {:cancel, :dispatch_deadline} = await_step!(fake, ref)
      assert_receive {:req, {:telelem_handoff, ^h, {:failed, :dispatch_deadline}}}, @t
      assert {:grant, :p, {:refused, :dispatch_deadline}} in collected(r)

      if @stage == :authorize do
        assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, _}}}, @t
        assert_ledger!(ctx, gid, :dispatched, active: 1, encumbered: @units, available: @left)
        close!(r, :none)
        leave!(ctx, fake, gid)
        assert_ledger!(ctx, gid, :unresolved, encumbered: @units, available: @left)
      else
        assert_ledger!(ctx, gid, :reserved, active: 1, reserved: @units, available: @left)
        close!(r, :none)
        leave!(ctx, fake, gid)
        assert_ledger!(ctx, gid, :released, available: @limit)
      end

      refute_received {:fake_http, _, _}
    end
  end

  test "the fake's own clock check: a permission held past the deadline sends nothing", ctx do
    r = start_h!(ctx, deadline: now() + @window)
    %{fake: fake, d: d} = r
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    assert now() < d, "setup took longer than the #{@window} ms window"

    hold = make_ref()
    ref = step_async(fake, {:begin_dispatch_hold, hold})
    assert_receive {:fake_permitted, ^fake, ^hold}, @t
    # The coordinator's deadline passed after the permission was released.
    assert_receive {:tr, {:terminal_deadline, :no_terminal}}, max(d - now(), 0) + @t
    send(fake, {:go, hold})

    assert :ok = await_step!(fake, ref)
    assert_receive {:fake_gate, ^fake, {:closed, :deadline}}, @t
    refute_received {:fake_http, _, _}
    {gid, _} = only_grant!(ctx.auth)
    assert_ledger!(ctx, gid, :dispatched, active: 1, encumbered: @units, available: @left)
  end

  # ── expiry ──────────────────────────────────────────────────────────────

  for phase <- [:reserving, :reserved, :authorizing] do
    @phase phase
    test "deadline while #{phase}: the request ends, no permission, the ledger releases or encumbers by owner exit",
         ctx do
      trace_receive!(ctx.auth)
      r = start_h!(ctx, deadline: now() + @window)
      %{h: h, fake: fake, d: d} = r

      ref =
        case @phase do
          :reserving ->
            :ok = :sys.suspend(ctx.auth)
            ref = step_async(fake, :attach)
            await_mailbox!(ctx.auth, &match?([{:"$gen_call", {^h, _}, {:reserve, _, _, _}}], &1))
            ref

          :reserved ->
            assert {:ok, _} = step!(fake, :attach)
            assert {:ok, :simulated} = step!(fake, :connect)
            nil

          :authorizing ->
            {_gid, ref} = authorizing!(ctx, r)
            ref
        end

      assert now() < d, "setup took longer than the #{@window} ms window"
      assert_receive {:tr, {:grant_deadline, :p, @phase}}, max(d - now(), 0) + @t
      assert_receive {:req, {:telelem_handoff, ^h, {:failed, :dispatch_deadline}}}, @t

      if ref, do: assert({:cancel, :dispatch_deadline} = await_step!(fake, ref))

      if @phase == :reserved do
        assert_receive {:fake_cancelled, ^fake, :dispatch_deadline}, @t
        assert {:cancel, :dispatch_deadline} = step!(fake, :begin_dispatch)
      end

      if @phase != :reserved, do: :ok = :sys.resume(ctx.auth)
      {gid, _} = only_grant!(ctx.auth)
      close!(r, :none)
      leave!(ctx, fake, gid)

      if @phase == :authorizing do
        assert_ledger!(ctx, gid, :unresolved, encumbered: @units, available: @left)
        assert [{:reserve, _, _, _}, {:authorize, ^gid}] = authority_requests!(ctx.auth, h)
      else
        assert_ledger!(ctx, gid, :released, available: @limit)
        assert [{:reserve, _, _, _}] = authority_requests!(ctx.auth, h)
      end

      refute_received {:fake_http, _, _}
    end
  end

  test "a stuck coordinator: begin_dispatch's bound (deadline + grace) fences it; nothing is sent",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx, deadline: now() + @window)
    %{h: h, fake: fake, d: d} = r
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    {gid, _} = only_grant!(ctx.auth)

    hmon = Process.monitor(h)
    :ok = :sys.suspend(h)
    ref = step_async(fake, :begin_dispatch)

    assert {:error, :unconfirmed} = await_step!(fake, ref, max(d + @grace - now(), 0) + @t)
    assert_receive {:DOWN, ^hmon, :process, _, :killed}, @t
    assert now() >= d + @grace
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, {:error, :unconfirmed}}}}, @t

    assert [{:reserve, _, ^fake, @units}] = authority_requests!(ctx.auth, h)
    leave!(ctx, fake, gid)
    assert_ledger!(ctx, gid, :released, available: @limit)
    refute_received {:fake_http, _, _}
  end

  # ── cancellation and loss of requester, owner or coordinator ────────────

  test "requester death while reserving: attach refused; the late reservation is released only by the owner's exit",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake, req: req} = r
    :ok = :sys.suspend(ctx.auth)
    ref = step_async(fake, :attach)
    await_mailbox!(ctx.auth, &match?([{:"$gen_call", {^h, _}, {:reserve, _, ^fake, @units}}], &1))
    refute_received {:fake, ^fake, ^ref, _}

    hmon = Process.monitor(h)
    Process.exit(req, :kill)
    assert {:cancel, {:requester_down, :killed}} = await_step!(fake, ref)
    assert_receive {:fake_cancelled, ^fake, {:requester_down, :killed}}, @t
    assert :ok = step!(fake, {:cleanup, :none})
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t

    # Applied after the coordinator gave it up: held for the live owner.
    :ok = :sys.resume(ctx.auth)
    {gid, g} = only_grant!(ctx.auth)
    assert %{owner: ^fake, state: :reserved} = g
    assert_ledger!(ctx, gid, :reserved, active: 1, reserved: @units, available: @left)

    leave!(ctx, fake, gid)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^fake, @units}] = authority_requests!(ctx.auth, h)
    assert {:grant, :p, {:abandoned, :reserving}} in collected(r)
  end

  test "requester cancel while reserved/connecting: no authorize is ever sent; owner exit releases",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h, fake: fake} = r
    assert {:ok, _} = step!(fake, :attach)
    {gid, _} = only_grant!(ctx.auth)

    cref = req_do(r, fn -> Handoff.cancel(h, :client_gone) end)
    assert_receive {:req_did, ^cref, :ok}, @t
    assert_receive {:fake_cancelled, ^fake, :client_gone}, @t

    assert {:ok, :simulated} = step!(fake, :connect)
    assert {:cancel, :client_gone} = step!(fake, :begin_dispatch)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, _}}}, @t
    assert {:cancel, :client_gone} = step!(fake, :ready)

    close!(r, :none)
    leave!(ctx, fake, gid)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^fake, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:fake_http, _, _}
  end

  for stage <- [:reserve, :authorize] do
    @stage stage
    test "owner death while #{stage} is outstanding: the request ends; the queued request then the owner :DOWN decide the ledger",
         ctx do
      r = start_h!(ctx)
      %{h: h, fake: fake} = r

      {ref, gid} =
        if @stage == :reserve do
          :ok = :sys.suspend(ctx.auth)
          ref = step_async(fake, :attach)

          await_mailbox!(
            ctx.auth,
            &match?([{:"$gen_call", {^h, _}, {:reserve, _, ^fake, _}}], &1)
          )

          {ref, nil}
        else
          {gid, ref} = authorizing!(ctx, r)
          {ref, gid}
        end

      _ = ref
      fmon = Process.monitor(fake)
      Process.exit(fake, :kill)
      assert_receive {:DOWN, ^fmon, :process, _, :killed}, @t

      assert_receive {:req, {:telelem_handoff, ^h, {:failed, :no_winner}}}, @t
      assert_receive {:req, {:telelem_handoff, ^h, {:closed, report}}}, @t
      assert %{grant: :abandoned, attempts: %{p: %{cleanup: {:down, :killed}}}} = report

      if @stage == :reserve do
        # Not applied yet, so the authority does not monitor the owner: the
        # reserve will find it dead (a :noproc :DOWN right after it).
        refute {:process, fake} in elem(Process.info(ctx.auth, :monitors), 1)
        assert [{:"$gen_call", {^h, _}, {:reserve, _, ^fake, _}}] = mailbox(ctx.auth)
      else
        assert [{:"$gen_call", {^h, _}, {:authorize, ^gid}}, {:DOWN, _, :process, ^fake, :killed}] =
                 await_mailbox!(ctx.auth, &match?([_, {:DOWN, _, :process, ^fake, _}], &1))
      end

      :ok = :sys.resume(ctx.auth)
      assert_receive {:telelem_admission, _, {:owner_down, ^fake, [{gid2, {:ok, gid2}}]}}, @t
      if gid, do: assert(gid == gid2)

      if @stage == :reserve,
        do: assert_ledger!(ctx, gid2, :released, available: @limit),
        else: assert_ledger!(ctx, gid2, :unresolved, encumbered: @units, available: @left)

      refute_received {:fake_http, _, _}
    end
  end

  for stage <- [:reserve, :authorize] do
    @stage stage
    test "coordinator killed while #{stage} is outstanding: the attempt's call is unconfirmed and nothing is sent",
         ctx do
      r = start_h!(ctx)
      %{h: h, fake: fake} = r

      ref =
        if @stage == :reserve do
          :ok = :sys.suspend(ctx.auth)
          ref = step_async(fake, :attach)
          await_mailbox!(ctx.auth, &match?([{:"$gen_call", {^h, _}, {:reserve, _, _, _}}], &1))
          ref
        else
          {_gid, ref} = authorizing!(ctx, r)
          ref
        end

      hmon = Process.monitor(h)
      Process.exit(h, :kill)
      assert_receive {:DOWN, ^hmon, :process, _, :killed}, @t
      assert {:error, :unconfirmed} = await_step!(fake, ref)

      :ok = :sys.resume(ctx.auth)
      {gid, g} = only_grant!(ctx.auth)
      assert g.owner == fake
      leave!(ctx, fake, gid)

      if @stage == :reserve,
        do: assert_ledger!(ctx, gid, :released, available: @limit),
        else: assert_ledger!(ctx, gid, :unresolved, encumbered: @units, available: @left)

      refute_received {:fake_http, _, _}
    end
  end

  # ── authority loss ──────────────────────────────────────────────────────

  test "authority death while reserved/connecting: the request fails, no permission, the fence retires the reservation",
       ctx do
    r = start_h!(ctx)
    %{h: h, fake: fake} = r
    assert {:ok, _} = step!(fake, :attach)
    {gid, _} = only_grant!(ctx.auth)

    amon = Process.monitor(ctx.auth)
    Process.exit(ctx.auth, :kill)
    assert_receive {:DOWN, ^amon, :process, _, :killed}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, {:authority_down, :killed}}}}, @t
    assert_receive {:telelem_admission, _, {:fenced, 2, %{released: [^gid], held: []}}}, @t

    assert {:ok, :simulated} = step!(fake, :connect)
    assert {:cancel, {:authority_down, :killed}} = step!(fake, :begin_dispatch)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, _}}}, @t
    assert %{authority: :lost, grant: :reserved} = Handoff.status(h)
    close!(r, :none)
    refute_received {:fake_http, _, _}
  end

  test "authority dead before the coordinator's request: its pinned :DOWN is handled first and the request's :DOWN is consumed",
       ctx do
    r = start_h!(ctx)
    %{h: h, fake: fake} = r

    :ok = :sys.suspend(h)
    ref = step_async(fake, :attach)
    await_mailbox!(h, &match?([{:"$gen_call", {^fake, _}, {:attach, :p, _}}], &1))

    amon = Process.monitor(ctx.auth)
    Process.exit(ctx.auth, :kill)
    assert_receive {:DOWN, ^amon, :process, _, :killed}, @t
    await_mailbox!(h, &match?([_, {:DOWN, _, :process, _, :killed}], &1))
    :ok = :sys.resume(h)

    assert {:cancel, {:authority_down, :killed}} = await_step!(fake, ref)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, {:authority_down, :killed}}}}, @t
    assert %{authority: :lost, grant: :abandoned} = Handoff.status(h)
    assert {:messages, []} = Process.info(h, :messages)

    evs = collected(r)
    assert [{:authority_down, :open, :killed}] = for({:authority_down, _, _} = e <- evs, do: e)

    assert index!(evs, {:authority_down, :open, :killed}) <
             index!(evs, {:grant, :p, {:abandoned, :reserving}})

    # The successor authority never saw a request for this attempt.
    assert_receive {:telelem_admission, _, {:authority_up, new, 2}}, @t
    assert Authority.snapshot(new).grants == %{}
    close!(r, :none)
  end

  test "authority death while authorize is outstanding: either :DOWN order is one loss and no permission",
       ctx do
    r = start_h!(ctx)
    %{h: h, fake: fake} = r
    {gid, ref} = authorizing!(ctx, r)

    :ok = :sys.suspend(h)
    pinned = :sys.get_state(h).authority_mon
    Process.exit(ctx.auth, :kill)

    [{:DOWN, first, :process, _, :killed}, {:DOWN, _, :process, _, :killed}] =
      await_mailbox!(h, &match?([_, _], &1))

    :ok = :sys.resume(h)
    assert {:cancel, {:authority_down, :killed}} = await_step!(fake, ref)
    assert_receive {:fake_gate, ^fake, {:closed, {:not_permitted, _}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, {:authority_down, :killed}}}}, @t
    assert %{authority: :lost, grant: :abandoned} = Handoff.status(h)
    assert {:messages, []} = Process.info(h, :messages)

    evs = collected(r)
    assert [_] = for({:authority_down, _, _} = e <- evs, do: e)
    down = index!(evs, {:authority_down, :open, :killed})
    abandoned = index!(evs, {:grant, :p, {:abandoned, :authorizing}})

    # pinned first: handled as the authority's :DOWN, then the request is
    # abandoned; request monitor first: check_response's :DOWN, then the loss.
    if first == pinned, do: assert(down < abandoned), else: assert(abandoned < down)

    # Never applied (the authority died suspended): the fence retires it.
    assert_receive {:telelem_admission, _, {:fenced, 2, %{released: [^gid], held: []}}}, @t
    close!(r, :none)
    refute_received {:fake_http, _, _}
  end

  # ── terminal ordering ───────────────────────────────────────────────────

  test "a terminal pending at the authority: no second finish, the requester's cancel refuses it, no marker",
       ctx do
    r = start_h!(ctx)
    %{h: h, fake: fake, d: d} = r
    permit!(r)
    assert :release = step!(fake, :ready)

    :ok = :sys.suspend(ctx.auth)
    fref = step_async(fake, {:finish, :t, @marker})
    await_mailbox!(ctx.auth, &match?([{:telelem_terminal_authorize, ^h, _}], &1))
    assert %{terminal_pending: true} = Handoff.status(h)

    intruder = intruder(h, d)
    assert {:error, :already_attached} = intrude!(intruder, :attach)

    cref = req_do(r, fn -> Handoff.cancel(h, :client_gone) end)
    assert_receive {:req_did, ^cref, :ok}, @t
    assert {:error, :cancelled} = await_step!(fake, fref)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :client_gone}}}, @t

    :ok = :sys.resume(ctx.auth)
    assert_receive {:tr, :stale_terminal_grant}, @t
    close!(r, :possible)
    refute_received {:req, {:telelem_handoff, ^h, {:data, @marker}}}
    refute_received {:req, {:telelem_handoff, ^h, {:done, _}}}
  end

  # ── request setup ───────────────────────────────────────────────────────

  defp start_h!(ctx, opts \\ []) do
    req = requester()
    col = collector()
    d = Keyword.get(opts, :deadline, now() + 60_000)
    on_event = Keyword.get(opts, :on_event, fn ev -> send(col, {:ev, ev}) end)

    base = [
      requester: req,
      attempts: [:p],
      admission_authority: ctx.auth,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, @units},
      trace: col,
      on_event: on_event,
      # Forced kills and register expiry stay out of the way.
      cleanup_timeout: 60_000,
      register_timeout: 60_000
    ]

    {:ok, h} = Handoff.start(Keyword.merge(base, Keyword.drop(opts, [:deadline, :on_event])))
    on_exit(fn -> Process.exit(h, :kill) end)
    r = %{h: h, req: req, col: col, d: d, auth: ctx.auth}
    Map.put(r, :fake, fake(r))
  end

  # Attached, reserved, connected and permitted (one fake send).
  defp permit!(%{fake: fake}) do
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_http, ^fake, @http_marker}, @t
  end

  # Attached, reserved and connected; the authority is suspended with the
  # coordinator's authorize queued, the fake blocked in begin_dispatch/3.
  defp authorizing!(ctx, %{h: h, fake: fake}) do
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    {gid, _} = only_grant!(ctx.auth)
    :ok = :sys.suspend(ctx.auth)
    ref = step_async(fake, :begin_dispatch)
    await_mailbox!(ctx.auth, &match?([{:"$gen_call", {^h, _}, {:authorize, ^gid}}], &1))
    refute_received {:fake, ^fake, ^ref, _}
    {gid, ref}
  end

  defp close!(%{h: h, fake: fake}, charge) do
    hmon = Process.monitor(h)
    assert :ok = step!(fake, {:cleanup, charge})
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
  end

  defp leave!(%{tag: tag}, fake, gid) do
    fmon = Process.monitor(fake)
    send(fake, :leave)
    assert_receive {:DOWN, ^fmon, :process, _, :normal}, @t
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^fake, [{^gid, {:ok, ^gid}}]}}, @t
  end

  # ── fake attempt ────────────────────────────────────────────────────────

  defp fake(r) do
    test = self()

    pid =
      spawn(fn ->
        fake_loop(%{
          test: test,
          h: r.h,
          id: :p,
          d: r.d,
          amon: Process.monitor(r.auth),
          connected: false,
          cancelled: nil,
          auth_down: nil
        })
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp fake_loop(s) do
    receive do
      {:step, ref, step} ->
        {reply, s} = fake_step(step, s)
        send(s.test, {:fake, self(), ref, reply})
        fake_loop(s)

      {:telelem_handoff, h, {:cancel, reason}} when h == s.h ->
        send(s.test, {:fake_cancelled, self(), reason})
        fake_loop(%{s | cancelled: reason})

      {:DOWN, ref, :process, _, reason} when ref == s.amon ->
        fake_loop(%{s | auth_down: reason})

      :leave ->
        :ok
    end
  end

  defp fake_step(:attach, s), do: {Handoff.attach(s.h, s.id, deadline: s.d), s}
  defp fake_step({:attach, opts}, s), do: {Handoff.attach(s.h, s.id, opts), s}
  # Simulated successful connect: no socket is opened.
  defp fake_step(:connect, s), do: {{:ok, :simulated}, %{s | connected: true}}

  defp fake_step(:begin_dispatch, s),
    do: dispatch(s, Handoff.begin_dispatch(s.h, s.id, deadline: s.d), nil)

  defp fake_step({:begin_dispatch, opts}, s),
    do: dispatch(s, Handoff.begin_dispatch(s.h, s.id, opts), nil)

  defp fake_step({:begin_dispatch_hold, hold}, s),
    do: dispatch(s, Handoff.begin_dispatch(s.h, s.id, deadline: s.d), hold)

  defp fake_step(:ready, s), do: {Handoff.ready(s.h, s.id), s}
  defp fake_step({:transfer, chunk}, s), do: {Handoff.transfer(s.h, chunk), s}
  defp fake_step({:finish, t, m}, s), do: {Handoff.finish(s.h, t, m, deadline: s.d), s}
  defp fake_step({:cleanup, charge}, s), do: {Handoff.cleaned(s.h, s.id, charge), s}

  # Sends only on :ok, and only if nothing invalidated it; the clock check
  # is the last thing before the send.
  defp dispatch(s, result, hold) do
    if result == :ok and hold do
      send(s.test, {:fake_permitted, self(), hold})
      receive do: ({:go, ^hold} -> :ok)
    end

    s = drain(s)

    decision =
      cond do
        result != :ok -> {:closed, {:not_permitted, result}}
        not s.connected -> {:closed, :not_connected}
        s.auth_down != nil -> {:closed, {:authority_down, s.auth_down}}
        s.cancelled != nil -> {:closed, {:cancelled, s.cancelled}}
        System.monotonic_time(:millisecond) >= s.d -> {:closed, :deadline}
        true -> :open
      end

    if decision == :open, do: send(s.test, {:fake_http, self(), @http_marker})
    send(s.test, {:fake_gate, self(), decision})
    {result, s}
  end

  defp drain(s) do
    receive do
      {:telelem_handoff, h, {:cancel, reason}} when h == s.h ->
        send(s.test, {:fake_cancelled, self(), reason})
        drain(%{s | cancelled: reason})

      {:DOWN, ref, :process, _, reason} when ref == s.amon ->
        drain(%{s | auth_down: reason})
    after
      0 -> s
    end
  end

  defp step_async(fake, step) do
    ref = make_ref()
    send(fake, {:step, ref, step})
    ref
  end

  defp await_step!(fake, ref, wait \\ @t) do
    assert_receive {:fake, ^fake, ^ref, reply}, wait
    reply
  end

  defp step!(fake, step), do: await_step!(fake, step_async(fake, step))

  # Another process trying to act as the primary.
  defp intruder(h, d) do
    test = self()

    pid = spawn(fn -> intruder_loop(test, h, d) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp intruder_loop(test, h, d) do
    receive do
      {:do, ref, :attach} ->
        send(test, {:intruded, ref, Handoff.attach(h, :p, deadline: d)})

      {:do, ref, :begin_dispatch} ->
        send(test, {:intruded, ref, Handoff.begin_dispatch(h, :p, deadline: d)})
    end

    intruder_loop(test, h, d)
  end

  defp intrude!(pid, what) do
    ref = make_ref()
    send(pid, {:do, ref, what})
    assert_receive {:intruded, ^ref, reply}, @t
    reply
  end

  # ── requester and trace collector ───────────────────────────────────────

  # Forwards everything to the test, and runs requester-only calls.
  defp requester do
    test = self()
    pid = spawn(fn -> req_loop(test) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp req_loop(test) do
    receive do
      {:do, ref, fun} -> send(test, {:req_did, ref, fun.()})
      msg -> send(test, {:req, msg})
    end

    req_loop(test)
  end

  defp req_do(%{req: req}, fun) do
    ref = make_ref()
    send(req, {:do, ref, fun})
    ref
  end

  # Keeps every trace event and observer event, and forwards trace events.
  defp collector do
    test = self()
    pid = spawn(fn -> col_loop(test, []) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp col_loop(test, acc) do
    receive do
      {:get, from, ref} ->
        send(from, {:collected, ref, Enum.reverse(acc)})
        col_loop(test, acc)

      {:telelem_handoff_trace, _h, ev} ->
        send(test, {:tr, ev})
        col_loop(test, [ev | acc])

      {:ev, ev} ->
        col_loop(test, [{:event, ev} | acc])
    end
  end

  defp collected(%{col: col}) do
    ref = make_ref()
    send(col, {:get, self(), ref})
    assert_receive {:collected, ^ref, evs}, @t
    evs
  end

  defp grant_phases(r), do: for({:grant, :p, p} <- collected(r), do: p)

  defp index!(list, x) do
    i = Enum.find_index(list, &(&1 == x))
    assert i != nil, "#{inspect(x)} not in #{inspect(list)}"
    i
  end

  # Neither the token nor the instance id it embeds is in any outward view.
  defp assert_private!(ctx, r, gid, extra) do
    for term <- collected(r) ++ extra do
      refute contains?(term, gid), inspect(term)
      refute contains?(term, ctx.inst.instance_id), inspect(term)
    end
  end

  defp contains?(term, needle) when term === needle, do: true
  defp contains?(t, n) when is_tuple(t), do: t |> Tuple.to_list() |> contains?(n)
  defp contains?([h | t], n), do: contains?(h, n) or contains?(t, n)
  defp contains?(%{} = m, n), do: m |> Map.to_list() |> contains?(n)
  defp contains?(_, _), do: false

  # ── authority observation ───────────────────────────────────────────────

  defp only_grant!(auth) do
    assert [{gid, g}] = Map.to_list(Authority.snapshot(auth).grants)
    {gid, g}
  end

  defp assert_ledger!(ctx, gid, state, counts) do
    snap = Authority.snapshot(Instance.authority(ctx.inst))
    assert snap.grants[gid].state == state

    assert %{
             active: Keyword.get(counts, :active, 0),
             reserved_units: Keyword.get(counts, :reserved, 0),
             encumbered_units: Keyword.get(counts, :encumbered, 0),
             spent_units: 0,
             available_units: Keyword.fetch!(counts, :available)
           } ==
             Map.take(snap, [
               :active,
               :reserved_units,
               :encumbered_units,
               :spent_units,
               :available_units
             ])

    assert Ledger.consistent?(Directory.ledger(ctx.inst.table))
  end

  defp trace_receive!(auth), do: 1 = :erlang.trace(auth, true, [:receive, {:tracer, self()}])

  # Requests the traced authority received from the coordinator `h`. Every
  # other request it received is one of the test's own snapshots.
  defp authority_requests!(auth, h) do
    1 = :erlang.trace(auth, false, [:receive])
    ref = :erlang.trace_delivered(auth)
    assert_receive {:trace_delivered, ^auth, ^ref}, @t
    me = self()
    msgs = traced([])

    for {:"$gen_call", {from, _}, req} <- msgs,
        from != h,
        do: assert({from, req} == {me, :snapshot})

    for m <- msgs, r = from_h(m, h), r != nil, do: r
  end

  defp from_h({:"$gen_call", {h, _}, req}, h), do: req
  defp from_h({:telelem_terminal_authorize, h, _}, h), do: :terminal
  defp from_h(_, _), do: nil

  defp traced(acc) do
    receive do
      {:trace, _, :receive, msg} -> traced([msg | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Re-read the process's actual mailbox until `pred` holds, bounded by
  # `until` (absolute ms) or @t from now. Each read is a round trip; no sleep.
  defp await_mailbox!(pid, pred, until \\ nil) do
    until = until || now() + @t
    {:messages, msgs} = Process.info(pid, :messages)

    cond do
      pred.(msgs) -> msgs
      now() >= until -> flunk("mailbox of #{inspect(pid)}: #{inspect(msgs)}")
      true -> await_mailbox!(pid, pred, until)
    end
  end

  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)
  defp monitors(pid), do: elem(Process.info(pid, :monitors), 1)

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp now, do: System.monotonic_time(:millisecond)
end
