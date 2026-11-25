defmodule Telelem.Admission.AttemptGrantLifetimeTest do
  @moduledoc """
  TEL-012: how long a grant reserved for one request's attempt holds its slot
  and units, and what an uncertain authorization leaves in the ledger.

  Listener-free. One `Telelem.Instance` with no workers (its transport is
  never started), one real `Telelem.Handoff` per request, and one fake
  attempt per request. The fake opens no socket: "connect" is simulated and
  its HTTP send is a `{:fake_http, fake, grant, marker}` message to the test.

  Per request:

    1. the fake attaches to the real handoff;
    2. the test reserves on the fake's behalf on the **pinned** authority pid,
       with the fake's own pid as owner (the authority monitors it);
    3. the fake simulates a successful connect and calls
       `Handoff.begin_dispatch/2`. That is only the coordinator's
       may-have-dispatched flag (a charge hint), not spend authority;
    4. a separate scripted caller, holding only the pinned authority pid (no
       instance handle, so no replacement lookup), makes the real
       `Authority.authorize/3` call and sends its outcome to the fake.

  The fake's gate (test policy, not a production guarantee) opens its HTTP
  send only for `{:ok, grant}` from that caller, while it has seen neither the
  pinned authority's `:DOWN` nor a handoff cancel. On anything else,
  including a timeout, it sends nothing and does nothing else: no retry,
  cancel, settle or reconcile.

  Ordering and synchronisation. `:sys.suspend/1` holds the authority, so a
  call to it cannot be answered and the caller's wait must expire. The
  caller then reads the authority's mailbox; that read is ordered after its
  own request (same sender, same receiver). The owner-down signal comes from
  the runtime, so its arrival is observed by re-reading the authority's
  mailbox (`Process.info/2`) until it is there, bounded by `@t`. Each order
  is established only after one message is already observed in the
  mailbox, and the exact mailbox is asserted before resuming. A receive
  trace on the authority records every request it received. There are no
  sleeps; receive timeouts only detect failure.

  The handoff's cleanup `charge` is not used as evidence of exposure here:
  only the authority's ledger is.

      scripts/mix test test/telelem/admission/attempt_grant_lifetime_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.{Authority, Ledger}
  alias Telelem.{Handoff, Instance}
  alias Telelem.Instance.Directory

  @moduletag :tmp_dir
  @t 1_000
  # Caller wait for the uncertain authorize. The authority is suspended, so
  # this expires whatever its value.
  @short 10
  @slots 2
  @limit 10
  @units 4
  @left @limit - @units
  @http_marker "POST /v1/fake (simulated, no socket)"

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        # No worker is started, so no transport is ever opened.
        id: :grant_lifetime_gw,
        limits: %{slots: @slots, units: @limit},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(sup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)

    on_exit(fn ->
      # Never leave a suspended authority behind if an assertion failed.
      try do
        if Process.alive?(auth), do: :sys.resume(auth)
      catch
        :exit, _ -> :ok
      end
    end)

    %{inst: inst, auth: auth, tag: inst.tag}
  end

  # ── confirmed authorization ─────────────────────────────────────────────

  test "confirmed: coordinator kill leaves the slot held while the fake lives; replacement holds it; owner_down unresolves it",
       ctx do
    r = request!(ctx)
    authorize_confirmed!(ctx, r)

    hmon = Process.monitor(r.h)
    Process.exit(r.h, :kill)
    assert_receive {:DOWN, ^hmon, :process, _, :killed}, @t

    assert Process.alive?(r.fake)
    assert_held!(ctx.auth, r, incarnation: 1)

    replace_then_release!(ctx, r)
  end

  test "confirmed: requester loss leaves the slot held while the fake lives; replacement holds it; owner_down unresolves it",
       ctx do
    r = request!(ctx)
    authorize_confirmed!(ctx, r)

    hmon = Process.monitor(r.h)
    rmon = Process.monitor(r.req)
    Process.exit(r.req, :kill)
    assert_receive {:DOWN, ^rmon, :process, _, :killed}, @t

    # The coordinator cancels the fake; the fake closes its (simulated)
    # transport and acknowledges, but stays alive as the grant's owner.
    assert_receive {:fake_cancelled, fake, {:requester_down, :killed}}, @t
    assert fake == r.fake
    assert :ok = step!(r.fake, :cleanup)
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t

    assert Process.alive?(r.fake)
    assert_held!(ctx.auth, r, incarnation: 1)

    replace_then_release!(ctx, r)
  end

  # ── uncertain authorization: both mailbox orders ────────────────────────

  test "uncertain: authorize queued before the owner-down signal: no send, then unresolved with units encumbered",
       ctx do
    %{auth: auth, tag: tag} = ctx
    r = request!(ctx)
    %{caller: caller, fake: fake, gid: gid} = r
    trace_receive!(auth)

    :ok = :sys.suspend(auth)
    ref = make_ref()
    send(caller, {:authorize, ref, gid, @short, fake})

    assert_receive {:caller, ^ref, outcome, queued}, @t
    assert {:exit, {:timeout, {GenServer, :call, [^auth, {:authorize, ^gid}, @short]}}} = outcome
    # Ordered after the caller's request (same sender, same receiver).
    assert [{:"$gen_call", {^caller, _}, {:authorize, ^gid}}] = queued

    # The fake got the timeout and sent nothing.
    assert_receive {:fake_gate, ^fake, ^gid, {:closed, {:not_confirmed, ^outcome}}}, @t
    refute_received {:fake_http, _, _, _}

    # Now the owner-down signal: the authorize is already in the mailbox, so
    # the :DOWN can only be queued after it.
    fmon = Process.monitor(fake)
    Process.exit(fake, :kill)
    assert_receive {:DOWN, ^fmon, :process, _, :killed}, @t

    msgs = await_mailbox!(auth, &match?([_, {:DOWN, _, :process, ^fake, _}], &1))

    assert [
             {:"$gen_call", {^caller, _}, {:authorize, ^gid}},
             {:DOWN, _, :process, ^fake, :killed}
           ] = msgs

    # Nothing applied yet: the committed ledger still holds a reservation.
    assert Ledger.grant(Directory.ledger(ctx.inst.table), gid).state == :reserved

    :ok = :sys.resume(auth)
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^fake, [{^gid, {:ok, ^gid}}]}}, @t

    snap = Authority.snapshot(auth)
    assert snap.grants[gid].state == :unresolved

    assert %{
             active: 0,
             available_slots: @slots,
             reserved_units: 0,
             encumbered_units: @units,
             spent_units: 0,
             available_units: @left
           } = snap

    assert Ledger.consistent?(Directory.ledger(ctx.inst.table))
    assert_only_authorize!(auth, caller, gid)
    refute_received {:fake_http, _, _, _}
  end

  test "uncertain: owner-down signal queued before authorize: no send, then released and never dispatched",
       ctx do
    %{auth: auth, tag: tag} = ctx
    r = request!(ctx)
    %{caller: caller, fake: fake, gid: gid} = r
    trace_receive!(auth)

    :ok = :sys.suspend(auth)

    fmon = Process.monitor(fake)
    Process.exit(fake, :kill)
    assert_receive {:DOWN, ^fmon, :process, _, :killed}, @t

    # The owner-down signal is in the mailbox before the authorize is sent.
    assert [{:DOWN, _, :process, ^fake, :killed}] =
             await_mailbox!(auth, &match?([{:DOWN, _, :process, ^fake, _}], &1))

    ref = make_ref()
    send(caller, {:authorize, ref, gid, @short, fake})

    assert_receive {:caller, ^ref, outcome, queued}, @t
    assert {:exit, {:timeout, {GenServer, :call, [^auth, {:authorize, ^gid}, @short]}}} = outcome

    assert [
             {:DOWN, _, :process, ^fake, :killed},
             {:"$gen_call", {^caller, _}, {:authorize, ^gid}}
           ] = queued

    assert Ledger.grant(Directory.ledger(ctx.inst.table), gid).state == :reserved

    :ok = :sys.resume(auth)
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^fake, [{^gid, {:ok, ^gid}}]}}, @t

    # Ordered after the queued authorize, which found the grant released.
    snap = Authority.snapshot(auth)
    assert snap.grants[gid].state == :released

    assert %{
             active: 0,
             available_slots: @slots,
             reserved_units: 0,
             encumbered_units: 0,
             spent_units: 0,
             available_units: @limit
           } = snap

    assert Ledger.consistent?(Directory.ledger(ctx.inst.table))
    assert_only_authorize!(auth, caller, gid)
    refute_received {:fake_gate, _, _, _}
    refute_received {:fake_http, _, _, _}
  end

  # ── request setup ───────────────────────────────────────────────────────

  # One handoff, one fake attempt and one authorize caller for one request,
  # all pinned to the authority pid of the context. The fake is attached,
  # has a reservation it owns, simulated its connect and crossed the
  # handoff's dispatch flag. Nothing is authorized yet.
  defp request!(%{auth: auth}) do
    req = idle()

    {:ok, h} =
      Handoff.start(
        requester: req,
        attempts: [:a],
        admission_authority: auth,
        terminal_deadline: System.monotonic_time(:millisecond) + 60_000
      )

    on_exit(fn -> Process.exit(h, :kill) end)

    caller = caller(auth)
    fake = fake(h, :a, auth, caller)

    assert {:ok, _mref} = step!(fake, :attach)

    # On the fake's behalf: its exact pid is the owner the authority monitors.
    assert {:ok, gid} = Authority.reserve(auth, {:op, make_ref()}, fake, @units, @t)
    g = Authority.snapshot(auth).grants[gid]
    assert %{owner: ^fake, state: :reserved, units: @units} = g
    assert {:process, fake} in elem(Process.info(auth, :monitors), 1)

    assert {:ok, :simulated} = step!(fake, :connect)
    assert :ok = step!(fake, :begin_dispatch)

    %{req: req, h: h, caller: caller, fake: fake, gid: gid}
  end

  defp authorize_confirmed!(%{auth: auth}, %{caller: caller, fake: fake, gid: gid}) do
    ref = make_ref()
    send(caller, {:authorize, ref, gid, @t, fake})
    assert_receive {:caller, ^ref, {:returned, {:ok, ^gid}}, :not_read}, @t
    assert_receive {:fake_gate, ^fake, ^gid, :open}, @t
    assert_receive {:fake_http, ^fake, ^gid, @http_marker}, @t
    assert Authority.snapshot(auth).grants[gid].state == :dispatched
  end

  defp assert_held!(auth, %{fake: fake, gid: gid}, incarnation: inc) do
    snap = Authority.snapshot(auth)
    assert %{owner: ^fake, state: :dispatched} = snap.grants[gid]

    assert %{
             incarnation: ^inc,
             active: 1,
             available_slots: 1,
             reserved_units: 0,
             encumbered_units: @units,
             spent_units: 0,
             available_units: @left
           } = snap
  end

  # Same-instance authority replacement while the fake lives, then the fake's
  # exit. Only that owner-down frees the slot; the units stay encumbered.
  defp replace_then_release!(%{inst: inst, auth: old, tag: tag}, %{fake: fake, gid: gid} = r) do
    amon = Process.monitor(old)
    Process.exit(old, :kill)
    assert_receive {:DOWN, ^amon, :process, _, :killed}, @t

    assert_receive {:telelem_admission, ^tag, {:fenced, 2, %{released: [], held: [^gid]}}}, @t
    assert_receive {:telelem_admission, ^tag, {:authority_up, new, 2}}, @t
    assert new != old
    assert Instance.authority(inst) == new

    assert Process.alive?(fake)
    assert {:process, fake} in elem(Process.info(new, :monitors), 1)
    assert_held!(new, r, incarnation: 2)
    refute_received {:telelem_admission, ^tag, {:owner_down, _, _}}

    fmon = Process.monitor(fake)
    send(fake, :leave)
    assert_receive {:DOWN, ^fmon, :process, _, :normal}, @t
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^fake, [{^gid, {:ok, ^gid}}]}}, @t

    snap = Authority.snapshot(new)
    assert snap.grants[gid].state == :unresolved

    assert %{
             active: 0,
             available_slots: @slots,
             reserved_units: 0,
             encumbered_units: @units,
             spent_units: 0,
             available_units: @left
           } = snap

    assert Ledger.consistent?(Directory.ledger(inst.table))
    # Exactly one fake send for the request.
    refute_received {:fake_http, _, _, _}
  end

  # ── fake attempt ────────────────────────────────────────────────────────

  defp fake(h, id, auth, caller) do
    test = self()

    pid =
      spawn(fn ->
        fake_loop(%{
          test: test,
          h: h,
          id: id,
          caller: caller,
          amon: Process.monitor(auth),
          hmon: nil,
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

      {:authorization, caller, gid, outcome} when caller == s.caller ->
        s = drain(s)
        decision = gate(s, gid, outcome)
        if decision == :open, do: send(s.test, {:fake_http, self(), gid, @http_marker})
        send(s.test, {:fake_gate, self(), gid, decision})
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

  defp fake_step(:attach, s) do
    case Handoff.attach(s.h, s.id) do
      {:ok, mref} -> {{:ok, mref}, %{s | hmon: mref}}
      other -> {other, s}
    end
  end

  # Simulated successful connect: no socket is opened.
  defp fake_step(:connect, s), do: {{:ok, :simulated}, %{s | connected: true}}

  defp fake_step(:begin_dispatch, %{connected: true} = s),
    do: {Handoff.begin_dispatch(s.h, s.id), s}

  defp fake_step(:begin_dispatch, s), do: {{:error, :not_connected}, s}

  defp fake_step(:cleanup, s), do: {Handoff.cleaned(s.h, s.id, :possible), s}

  # Only a confirmed authorization that nothing has invalidated opens the send.
  defp gate(s, gid, outcome) do
    cond do
      outcome != {:returned, {:ok, gid}} -> {:closed, {:not_confirmed, outcome}}
      s.auth_down != nil -> {:closed, {:authority_down, s.auth_down}}
      s.cancelled != nil -> {:closed, {:cancelled, s.cancelled}}
      true -> :open
    end
  end

  # Take in a cancel or authority :DOWN that arrived with the authorization.
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

  defp step!(fake, step) do
    ref = make_ref()
    send(fake, {:step, ref, step})
    assert_receive {:fake, ^fake, ^ref, reply}, @t
    reply
  end

  # ── scripted authorize caller ───────────────────────────────────────────

  # Holds only the pinned authority pid. Makes one real authorize per
  # instruction, then reports the outcome to the fake and to the test. On a
  # caller-side exit it reads the authority's mailbox, which is ordered after
  # its own request. It never retries, cancels, settles or reconciles.
  defp caller(auth) do
    test = self()
    pid = spawn(fn -> caller_loop(test, auth) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp caller_loop(test, auth) do
    receive do
      {:authorize, ref, gid, wait, fake} ->
        outcome =
          try do
            {:returned, Authority.authorize(auth, gid, wait)}
          catch
            :exit, reason -> {:exit, reason}
          end

        queued =
          case outcome do
            {:exit, _} -> elem(Process.info(auth, :messages), 1)
            {:returned, _} -> :not_read
          end

        send(fake, {:authorization, self(), gid, outcome})
        send(test, {:caller, ref, outcome, queued})
        caller_loop(test, auth)
    end
  end

  # ── observation helpers ─────────────────────────────────────────────────

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # Re-read the authority's actual mailbox until `pred` holds, bounded by @t.
  # Each read is a round trip to the authority; there is no sleep.
  defp await_mailbox!(pid, pred, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @t
    {:messages, msgs} = Process.info(pid, :messages)

    cond do
      pred.(msgs) -> msgs
      System.monotonic_time(:millisecond) >= deadline -> flunk("mailbox: #{inspect(msgs)}")
      true -> await_mailbox!(pid, pred, deadline)
    end
  end

  defp trace_receive!(auth) do
    1 = :erlang.trace(auth, true, [:receive, {:tracer, self()}])
  end

  # Every request the authority received since trace_receive!/1: the
  # caller's single authorize, and otherwise only the test's snapshots.
  defp assert_only_authorize!(auth, caller, gid) do
    1 = :erlang.trace(auth, false, [:receive])
    ref = :erlang.trace_delivered(auth)
    assert_receive {:trace_delivered, ^auth, ^ref}, @t
    calls = traced_calls([])
    me = self()

    assert [{^caller, {:authorize, ^gid}}] = for({p, req} <- calls, p != me, do: {p, req})
    assert Enum.all?(for({^me, req} <- calls, do: req), &(&1 == :snapshot))
  end

  defp traced_calls(acc) do
    receive do
      {:trace, _, :receive, {:"$gen_call", {from, _}, req}} -> traced_calls([{from, req} | acc])
      {:trace, _, :receive, _} -> traced_calls(acc)
    after
      0 -> Enum.reverse(acc)
    end
  end
end
