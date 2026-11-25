defmodule Telelem.WireOwnerGuardTest do
  @moduledoc """
  TEL-012 `Telelem.Wire.OwnerGuard`: one gated `Telelem.Wire.Upstream`
  launched at its fixed guarded entry under a real `DynamicSupervisor`, with
  a real `Telelem.Handoff` (`coordinator_grant`, and a denying legacy one) and
  its real pinned `Telelem.Admission.Authority`.

  Listener-free: no socket is opened. The target is a `VerifiedTarget` for
  `127.0.0.1` port 1 and every owner ends before it dials: the authority is
  held with `:sys.suspend/1` so `attach_grant/3` waits for its reservation,
  or the coordinator refuses the grant attach (a dial would surface as
  `{:connect_failed, _, _}`, which each case refutes).

  Covers: the closed option interface; equality expiry of the one activation
  limit; the metadata-only child entry and late (queued) child start; refused
  startup before `A` (`:start_failed`, also with the helper gone without a
  result); the helper linked to the trapping witness only and released by the
  launcher; the helper killed by the witness on expiry, launcher loss
  (`:normal` included) and abandonment, by the launcher's own failed cleanup
  when the witness cannot act, and by the witness's link when it goes, for
  both orders of launcher and witness loss (a suspended live witness
  included); a closure known at the witness's start (no helper); a forged
  helper `EXIT`; a witness `:DOWN` already delivered behind its acknowledgement
  winning over GO; positive attach by the exact owner pid; the owner's GO
  rechecks at the real entry (token, limit, pinned coordinator, deadline,
  pending stop); cooperative stop at a checkpoint and the forced fence while
  blocked in a call; supervisor shutdown; the witness as the only releaser of
  activation (exactly once, to the exact reported guard, never after it
  closed, including a launcher `:DOWN` or abandonment queued behind the
  helper's `{:ok, guard}`), the helper's death taken only from the witness's
  private helper monitor (the helper gone at every report and loss relay,
  also after a forged `EXIT`), and its three reports: `:owner_down` only from its own monitor,
  `:activation_never_released`, and `:owner_unknown` when a released guard
  went down without announcing an owner (fake launcher, helper, guard and
  owner processes). Reports are asserted only with the exact process already
  dead.

  Ordering uses `:sys.suspend/1`, `:erlang.suspend_process/1` and mailboxes
  read back with `Process.info/2`, re-read until the expected message is
  there, bounded. The only clock waits are for an activation limit or fence
  instant to pass.

  Not covered: TLS/socket teardown, streaming checkpoints (need a
  handshake), the deadline trigger, durability or successor ordering.

      scripts/mix test test/telelem/wire_owner_guard_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.{Authority, Ledger}
  alias Telelem.{Handoff, Instance}
  alias Telelem.Instance.Directory
  alias Telelem.Sandbox.VerifiedTarget
  alias Telelem.Wire.{Limits, OwnerGuard, Upstream}

  @moduletag :tmp_dir
  @t 2_000
  @limit 10
  @units 4
  @secret "sk-test-SECRET-body-marker"

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :wire_owner_guard_gw,
        limits: %{slots: 2, units: @limit},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :guard_sup)

    on_exit(fn ->
      for p <- [auth, sup] do
        try do
          if Process.alive?(p), do: :sys.resume(p)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem guard test CA", [])
    {:ok, target} = VerifiedTarget.build("a", "127.0.0.1", 1, [der])
    %{inst: inst, auth: auth, tag: inst.tag, target: target, sup: sup}
  end

  # ── interface ───────────────────────────────────────────────────────────

  test "the closed option interface is refused in the launcher; nothing starts", ctx do
    r = start_h!(ctx)
    good = gopts(ctx, r, idle())
    up = good[:upstream]
    {:monitors, mons} = Process.info(self(), :monitors)

    bad = [
      {:not_a_list, "keyword list"},
      {[{"upstream", up}], "keyword list"},
      {good ++ [grace_ms: 5], "at most once"},
      {good ++ [extra: 1], ":extra is not accepted"},
      {Keyword.delete(good, :observer), ":observer is required"},
      {Keyword.delete(good, :upstream), ":upstream is required"},
      {Keyword.put(good, :upstream, %{}), ":upstream must be a keyword list"},
      {Keyword.put(good, :upstream, Keyword.delete(up, :gate)), ":gate is required"},
      {Keyword.put(good, :upstream, Keyword.put(up, :gate, nil)), ":gate is required"},
      {Keyword.put(good, :upstream, Keyword.put(up, :body, 1)), ":body must be a binary"},
      {Keyword.put(good, :upstream, up ++ [lane: %{}]), ":lane is not accepted"},
      {Keyword.put(good, :requester, :name), ":requester must be a local pid"},
      {Keyword.put(good, :observer, nil), ":observer must be a local pid"},
      {Keyword.put(good, :grace_ms, 0), ":grace_ms must be an integer"},
      {Keyword.put(good, :grace_ms, OwnerGuard.bounds().grace_ms + 1), ":grace_ms"},
      {Keyword.put(good, :activation_ms, 1.5), ":activation_ms must be an integer"},
      {Keyword.put(good, :activation_ms, OwnerGuard.bounds().activation_ms + 1), ":activation_ms"}
    ]

    for {o, msg} <- bad do
      e = assert_raise ArgumentError, fn -> OwnerGuard.start(ctx.sup, o) end
      assert e.message =~ msg, "#{inspect(msg)}: #{e.message}"
      for s <- [@secret, ctx.target.host], do: refute(e.message =~ s, e.message)
    end

    assert DynamicSupervisor.which_children(ctx.sup) == []
    assert {:monitors, ^mons} = Process.info(self(), :monitors)
    assert {:messages, []} = Process.info(self(), :messages)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "one activation limit A = min(now + budget, D): reached (equality included) spawns nothing",
       ctx do
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:primary])
    on_exit(fn -> Process.exit(h, :kill) end)

    for d <- [now(), now() - 1] do
      r = %{h: h, d: d}
      assert OwnerGuard.start(ctx.sup, gopts(ctx, r, idle())) == {:error, :expired}
    end

    assert DynamicSupervisor.which_children(ctx.sup) == []
    assert {:messages, []} = Process.info(self(), :messages)
  end

  test "child spec: metadata only, temporary, finite shutdown; the entry refuses other shapes and late starts" do
    me = self()

    meta = %{
      ref: make_ref(),
      witness: me,
      observer: me,
      requester: me,
      handoff: me,
      deadline: now() + 60_000,
      until: now() + 60_000,
      grace_ms: 250
    }

    spec = OwnerGuard.child_spec(meta)
    assert %{restart: :temporary, shutdown: 1_500, type: :worker} = spec
    assert spec.shutdown == 2 * 250 + OwnerGuard.bounds().shutdown_slack_ms
    assert {OwnerGuard, :start_link, [^meta]} = spec.start

    for junk <- [
          :junk,
          Map.put(meta, :body, @secret),
          Map.delete(meta, :witness),
          %{meta | until: meta.deadline + 1},
          %{meta | grace_ms: 0},
          %{meta | ref: :ref},
          %{meta | handoff: :name}
        ] do
      assert OwnerGuard.start_link(junk) == {:error, :invalid_metadata}
    end

    # A start processed at or after A does no work.
    for until <- [now(), now() - 1] do
      assert OwnerGuard.start_link(%{meta | until: until}) == {:error, :expired}
    end

    assert {:messages, []} = Process.info(self(), :messages)
  end

  # ── the owner's fixed entry ─────────────────────────────────────────────

  test "fixed owner entry: no GO, wrong token, other coordinator or deadline, late GO and a pending stop attach nothing",
       ctx do
    r = start_h!(ctx, trace: self())
    payload = payload(ctx, r)
    {:ok, other_h} = Handoff.start(requester: idle(), attempts: [:primary])
    on_exit(fn -> Process.exit(other_h, :kill) end)

    entry = fn until, d ->
      ref = make_ref()
      meta = %{ref: ref, until: min(until, d), deadline: d, handoff: r.h}
      {pid, mon} = :erlang.spawn_opt(Upstream, :guarded, [meta], [:monitor])
      {pid, mon, ref}
    end

    # No GO by A, or a GO with another token: expires at A.
    {o, m, _} = entry.(now() + 50, r.d)
    send(o, {:telelem_owner_go, make_ref(), payload})
    assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :expired}}, @t

    # A stop before GO.
    {o, m, ref} = entry.(now() + 60_000, r.d)
    send(o, {:telelem_owner_stop, ref})
    assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :stopped}}, @t

    # GO for another coordinator, another deadline, or a legacy payload.
    for p <- [
          Keyword.put(payload, :handoff, other_h),
          Keyword.put(payload, :gate, target: ctx.target, deadline: r.d + 1),
          Keyword.delete(payload, :gate)
        ] do
      {o, m, ref} = entry.(now() + 60_000, r.d)
      send(o, {:telelem_owner_go, ref, p})
      assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :refused}}, @t
    end

    # GO followed by a stop, both queued before the owner runs.
    {o, m, ref} = entry.(now() + 60_000, r.d)
    true = :erlang.suspend_process(o)
    send(o, {:telelem_owner_go, ref, payload})
    send(o, {:telelem_owner_stop, ref})
    true = :erlang.resume_process(o)
    assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :stopped}}, @t

    # A GO queued before A but handled at or after A.
    a = now() + 50
    {o, m, ref} = entry.(a, r.d)
    true = :erlang.suspend_process(o)
    send(o, {:telelem_owner_go, ref, payload})
    sleep_until(a)
    true = :erlang.resume_process(o)
    assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :expired}}, @t

    # Invalid metadata.
    {o, m} = :erlang.spawn_opt(Upstream, :guarded, [%{ref: make_ref()}], [:monitor])
    assert_receive {:DOWN, ^m, :process, ^o, {:shutdown, :invalid_meta}}, @t

    # Nothing attached, reserved or reported.
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_handoff_trace, _, {:attached, _}}
    refute_received {:telelem_wire, _, _, _}
  end

  # ── launch ──────────────────────────────────────────────────────────────

  test "positive: the exact owner attaches; the scoped stop at its next checkpoint ends it before the dial",
       ctx do
    r = start_h!(ctx)
    greq = idle()
    {:monitors, mons} = Process.info(self(), :monitors)
    :ok = :sys.suspend(ctx.auth)

    assert {:ok, %{guard: g, owner: o, witness: w, ref: ref}} =
             OwnerGuard.start(ctx.sup, gopts(ctx, r, greq, grace_ms: 10_000))

    # The launcher left no monitor, alias reply or message behind.
    assert {:monitors, ^mons} = Process.info(self(), :monitors)
    assert {:messages, []} = Process.info(self(), :messages)

    # The reservation is for exactly this owner, which registered itself.
    await_reserve!(ctx.auth, r.h, o)
    assert %{attempts: %{primary: %{pid: ^o}}} = Handoff.status(r.h)

    # Shape: supervised trapping guard; non-trapping owner linked only to it
    # and monitored by it and the witness; trapping witness, linked to
    # nothing once its helper (which delivered the result) has exited.
    assert [{_, ^g, :worker, _}] = DynamicSupervisor.which_children(ctx.sup)
    assert {:trap_exit, true} = Process.info(g, :trap_exit)
    assert {:links, [^g]} = Process.info(o, :links)
    assert {:trap_exit, false} = Process.info(o, :trap_exit)
    {:monitored_by, by} = Process.info(o, :monitored_by)
    assert g in by and w in by
    assert {:trap_exit, true} = Process.info(w, :trap_exit)
    await_links!(w, [])

    # Hold the owner with the attach reply queued, then trigger the guard.
    true = :erlang.suspend_process(o)
    :ok = :sys.resume(ctx.auth)
    await_mailbox!(o, &Enum.any?(&1, fn m -> match?({_, :ok}, m) end))
    Process.exit(greq, :kill)
    await_mailbox!(o, &({:telelem_owner_stop, ref} in &1))
    true = :erlang.resume_process(o)

    assert_receive {:telelem_wire, ^o, :primary,
                    {:ended, {:stopped, :after_attach, :guard_stop}, stats}},
                   @t

    assert %{claim: :none, cleaned_ack: :ok} = stats

    assert_receive {:telelem_owner_guard, ^ref, :owner_down, :requester_down, :stopped}, @t
    refute Process.alive?(o)
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    refute Process.alive?(g)

    owner_gone!(ctx, o)
    {gid, _} = only_grant!(ctx.auth)
    assert_ledger!(ctx, gid, :released)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "forced: an owner blocked in attach is killed at min(trigger, D) + grace; the witness confirms",
       ctx do
    r = start_h!(ctx)
    greq = idle()
    grace = 50
    :ok = :sys.suspend(ctx.auth)

    assert {:ok, %{owner: o, ref: ref}} =
             OwnerGuard.start(ctx.sup, gopts(ctx, r, greq, grace_ms: grace))

    await_reserve!(ctx.auth, r.h, o)
    t0 = now()
    Process.exit(greq, :kill)

    assert_receive {:telelem_owner_guard, ^ref, :owner_down, :requester_down, :killed}, @t
    assert now() >= t0 + grace
    refute Process.alive?(o)
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    # Killed while blocked: it reported nothing and never dialed.
    refute_received {:telelem_wire, ^o, _, _}

    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
    {gid, _} = only_grant!(ctx.auth)
    assert_ledger!(ctx, gid, :released)
  end

  test "denying coordinator: the GO'd owner's grant attach is refused before any dial", ctx do
    {:ok, h} =
      Handoff.start(
        requester: idle(),
        attempts: [:primary],
        trace: self(),
        register_timeout: 60_000
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    r = %{h: h, d: now() + 60_000}

    assert {:ok, %{owner: o, ref: ref}} = OwnerGuard.start(ctx.sup, gopts(ctx, r, idle()))

    assert_receive {:telelem_wire, ^o, :primary,
                    {:ended, {:attach, :error, :grant_required}, stats}},
                   @t

    assert %{claim: :none, cleaned_ack: :not_sent} = stats
    assert_receive {:telelem_owner_guard, ^ref, :owner_down, :owner_exited, :exited}, @t
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(h)
    refute_received {:telelem_handoff_trace, ^h, _}
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "supervisor shutdown: the guard stops and fences the owner, then exits with the supervisor's reason",
       ctx do
    r = start_h!(ctx)
    :ok = :sys.suspend(ctx.auth)

    assert {:ok, %{guard: g, owner: o, ref: ref}} =
             OwnerGuard.start(ctx.sup, gopts(ctx, r, idle(), grace_ms: 50))

    await_reserve!(ctx.auth, r.h, o)
    gm = Process.monitor(g)
    assert :ok = DynamicSupervisor.terminate_child(ctx.sup, g)

    assert_receive {:telelem_owner_guard, ^ref, :owner_down, :parent_exit, :killed}, @t
    assert_receive {:DOWN, ^gm, :process, ^g, :shutdown}, @t
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
  end

  # ── independent confirmation ────────────────────────────────────────────

  test "guard loss: the link ends the owner and the witness confirms without a guard report",
       ctx do
    r = start_h!(ctx)
    :ok = :sys.suspend(ctx.auth)

    assert {:ok, %{guard: g, owner: o, witness: w, ref: ref}} =
             OwnerGuard.start(ctx.sup, gopts(ctx, r, idle(), grace_ms: 10_000))

    await_reserve!(ctx.auth, r.h, o)
    wm = Process.monitor(w)
    Process.exit(g, :kill)

    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    refute Process.alive?(o)
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
    refute_received {:telelem_owner_guard, ^ref, :owner_down, _, _}
    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
  end

  test "launcher loss after GO (normal exit): the witness outlives it and confirms the later owner end",
       ctx do
    r = start_h!(ctx)
    greq = idle()
    :ok = :sys.suspend(ctx.auth)
    opts = gopts(ctx, r, greq, grace_ms: 50)
    {l, lm} = launcher(fn -> OwnerGuard.start(ctx.sup, opts) end)

    assert_receive {:launched, ^l, {:ok, %{owner: o, witness: w, ref: ref}}}, @t
    send(l, :exit)
    assert_receive {:DOWN, ^lm, :process, ^l, :normal}, @t

    await_reserve!(ctx.auth, r.h, o)
    assert Process.alive?(w) and Process.alive?(o)

    Process.exit(greq, :kill)
    assert_receive {:telelem_owner_guard, ^ref, :owner_down, :requester_down, :killed}, @t
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
  end

  test "launcher loss while the supervisor is busy: the witness kills the exact helper before A; the queued child never gets an owner",
       ctx do
    r = start_h!(ctx)
    act = 1_000
    :ok = :sys.suspend(ctx.sup)
    t0 = now()
    opts = gopts(ctx, r, idle(), activation_ms: act)
    {l, lm} = launcher(fn -> OwnerGuard.start(ctx.sup, opts) end)

    [{:"$gen_call", {helper, _}, {:start_child, _} = call}] =
      await_mailbox!(ctx.sup, &match?([{:"$gen_call", _, {:start_child, _}}], &1))

    # The supervisor got metadata only, never the payload.
    refute contains?(call, @secret)
    refute contains?(call, ctx.target)
    hm = Process.monitor(helper)
    %{witness: w} = find_meta(call)
    assert {:links, [^w]} = Process.info(helper, :links)

    Process.exit(l, :kill)
    assert_receive {:DOWN, ^lm, :process, ^l, :killed}, @t
    assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
    assert now() < t0 + act, "the helper must go on launcher loss, not at A"
    assert_receive {:telelem_owner_witness, _, :activation_never_released}, @t

    # The queued request still starts a guard before A; never activated, it
    # gets no owner and expires at A.
    :ok = :sys.resume(ctx.sup)
    assert [{_, g, :worker, _}] = DynamicSupervisor.which_children(ctx.sup)
    gm = Process.monitor(g)
    assert {:links, [_sup]} = Process.info(g, :links)
    assert_receive {:DOWN, ^gm, :process, ^g, {:shutdown, :expired}}, act + @t
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "late queued start: the launcher expires at A, the witness kills the helper, the late child does no work",
       ctx do
    r = start_h!(ctx)
    :ok = :sys.suspend(ctx.sup)
    sup = ctx.sup
    t0 = now()

    assert OwnerGuard.start(ctx.sup, gopts(ctx, r, idle(), activation_ms: 100)) ==
             {:error, :expired}

    assert now() >= t0 + 100
    [{:"$gen_call", {helper, _}, {:start_child, _}}] = mailbox(ctx.sup)
    assert_receive {:telelem_owner_witness, _, :activation_never_released}, @t
    refute Process.alive?(helper)

    # Processed after A: the child's init refuses and spawns nothing.
    1 = :erlang.trace(sup, true, [:procs, :set_on_spawn, {:tracer, self()}])
    :ok = :sys.resume(sup)
    assert DynamicSupervisor.which_children(sup) == []
    1 = :erlang.trace(sup, false, [:procs, :set_on_spawn])

    assert_receive {:trace, ^sup, :spawn, g, {:proc_lib, :init_p, _}}, @t
    assert_receive {:trace, ^g, :exit, :normal}, @t
    refute_received {:trace, ^g, :spawn, _, _}

    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    # The alias was deactivated and drained: no late reply reached the launcher.
    refute_received {:telelem_owner_guard, _, _, _}
  end

  test "launcher expiry while the witness cannot act: the launcher itself kills the exact helper blocked in start_child",
       ctx do
    r = start_h!(ctx)
    act = 200
    :ok = :sys.suspend(ctx.sup)
    opts = gopts(ctx, r, idle(), activation_ms: act)
    {l, lm} = launcher(fn -> OwnerGuard.start(ctx.sup, opts) end)

    [{:"$gen_call", {helper, _}, {:start_child, call}}] =
      await_mailbox!(ctx.sup, &match?([{:"$gen_call", _, {:start_child, _}}], &1))

    %{witness: w} = find_meta(call)
    await_monitors!(l, w)
    assert {:links, [^w]} = Process.info(helper, :links)
    hm = Process.monitor(helper)
    wm = Process.monitor(w)
    # The witness can neither kill the helper at A nor process the abandon.
    true = :erlang.suspend_process(w)

    assert_receive {:launched, ^l, {:error, :expired}}, act + @t
    # The launcher's own failed cleanup ended the exact helper.
    assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
    assert Process.alive?(w)
    assert {:status, :suspended} = Process.info(w, :status)

    # The witness then goes before ever acting: nobody else was needed.
    Process.exit(w, :kill)
    assert_receive {:DOWN, ^wm, :process, ^w, :killed}, @t
    send(l, :exit)
    assert_receive {:DOWN, ^lm, :process, ^l, :normal}, @t

    # The queued request is processed after A: no work.
    :ok = :sys.resume(ctx.sup)
    assert DynamicSupervisor.which_children(ctx.sup) == []
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "a witness :DOWN already delivered behind its acknowledgement wins over GO; the guard is killed and nothing attaches",
       ctx do
    r = start_h!(ctx, trace: self())
    :ok = :sys.suspend(ctx.sup)
    opts = gopts(ctx, r, idle(), grace_ms: 10_000)
    {l, lm} = launcher(fn -> OwnerGuard.start(ctx.sup, opts) end)

    [{:"$gen_call", _, {:start_child, call}}] =
      await_mailbox!(ctx.sup, &match?([{:"$gen_call", _, {:start_child, _}}], &1))

    %{witness: w, ref: ref} = find_meta(call)
    # The launcher's own witness monitor exists before it is held.
    await_monitors!(l, w)

    # Hold the launcher in its receive, let the witness release activation
    # and acknowledge the exact owner, then kill the witness.
    true = :erlang.suspend_process(l)
    :ok = :sys.resume(ctx.sup)

    [{:telelem_owner_guard, ^ref, :watching, g, o}] =
      for {:telelem_owner_guard, ^ref, :watching, _, _} = m <-
            await_mailbox!(
              l,
              &Enum.any?(&1, fn m -> match?({:telelem_owner_guard, ^ref, :watching, _, _}, m) end)
            ),
          do: m

    om = Process.monitor(o)
    gm = Process.monitor(g)
    Process.exit(w, :kill)
    await_mailbox!(l, &Enum.any?(&1, fn m -> match?({:DOWN, _, :process, ^w, :killed}, m) end))
    true = :erlang.resume_process(l)

    assert_receive {:launched, ^l, {:error, :witness_down}}, @t
    assert_receive {:DOWN, ^gm, :process, ^g, :killed}, @t
    assert_receive {:DOWN, ^om, :process, ^o, :killed}, @t

    # No GO was sent: the owner never attached and nothing was reserved.
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_handoff_trace, _, {:attached, _}}
    refute_received {:telelem_wire, _, _, _}
    refute_received {:telelem_owner_witness, _, _, _}
    send(l, :exit)
    assert_receive {:DOWN, ^lm, :process, ^l, :normal}, @t
  end

  # ── startup lifecycle (real launcher, helper and witness) ───────────────

  test "refused startup before A: :start_failed, the exact helper ends, the witness reports it never released activation",
       ctx do
    r = start_h!(ctx)
    act = 5_000
    opts = gopts(ctx, r, idle(), activation_ms: act)

    # A real supervisor that refuses every child, and a dead one.
    full =
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, max_children: 0},
        id: :full_sup
      )

    dead = spawn(fn -> :ok end)
    dm = Process.monitor(dead)
    assert_receive {:DOWN, ^dm, :process, ^dead, _}, @t

    for sup <- [full, dead] do
      t0 = now()
      assert OwnerGuard.start(sup, opts) == {:error, :start_failed}
      assert now() < t0 + act
      assert_receive {:telelem_owner_witness, _, :activation_never_released}, @t
    end

    assert DynamicSupervisor.which_children(full) == []

    # Exact pids, over a fake supervisor: a refusal, then the helper gone
    # without a result.
    for how <- [:refuse, :helper_killed] do
      sup = recorder()
      t0 = now()
      {l, lm} = launcher(fn -> OwnerGuard.start(sup, opts) end)

      assert_receive {:recorded, ^sup, {:"$gen_call", {helper, _} = from, {:start_child, call}}},
                     @t

      %{witness: w, ref: ref} = find_meta(call)
      assert {:links, [^w]} = Process.info(helper, :links)
      hm = Process.monitor(helper)
      wm = Process.monitor(w)

      case how do
        :refuse -> GenServer.reply(from, {:error, :max_children})
        :helper_killed -> Process.exit(helper, :kill)
      end

      assert_receive {:launched, ^l, {:error, :start_failed}}, @t, inspect(how)
      assert now() < t0 + act
      assert_receive {:DOWN, ^hm, :process, ^helper, _}, @t
      assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
      assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
      send(l, :exit)
      assert_receive {:DOWN, ^lm, :process, ^l, :normal}, @t
    end

    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_owner_guard, _, _, _, _}
  end

  test "launcher gone first while the witness is suspended: the live witness then ends the exact helper, or its link does",
       ctx do
    for then <- [:resume_witness, :kill_witness] do
      %{r: r, l: l, lm: lm, w: w, wm: wm, helper: helper, hm: hm, ref: ref, t0: t0, act: act} =
        blocked_launch!(ctx)

      true = :erlang.suspend_process(w)
      Process.exit(l, :kill)
      assert_receive {:DOWN, ^lm, :process, ^l, :killed}, @t
      await_mailbox!(w, &Enum.any?(&1, fn m -> match?({:DOWN, _, :process, ^l, _}, m) end))
      # Only the live witness holds it now.
      assert Process.alive?(helper)

      case then do
        :resume_witness ->
          true = :erlang.resume_process(w)
          assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
          assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
          assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t

        :kill_witness ->
          Process.exit(w, :kill)
          assert_receive {:DOWN, ^wm, :process, ^w, :killed}, @t
          assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
          refute_received {:telelem_owner_witness, ^ref, _}
      end

      assert now() < t0 + act, "#{then}: the helper must go before A"
      queued_child_expires!(ctx, r, act)
    end

    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "witness gone first while the launcher is suspended: its link ends the exact helper; the launcher then returns :witness_down or goes too",
       ctx do
    for then <- [:resume_launcher, :kill_launcher] do
      %{r: r, l: l, lm: lm, w: w, wm: wm, helper: helper, hm: hm, t0: t0, act: act} =
        blocked_launch!(ctx)

      true = :erlang.suspend_process(l)
      Process.exit(w, :kill)
      assert_receive {:DOWN, ^wm, :process, ^w, :killed}, @t
      assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t

      case then do
        :resume_launcher ->
          true = :erlang.resume_process(l)
          assert_receive {:launched, ^l, {:error, :witness_down}}, @t
          send(l, :exit)
          assert_receive {:DOWN, ^lm, :process, ^l, :normal}, @t

        :kill_launcher ->
          Process.exit(l, :kill)
          assert_receive {:DOWN, ^lm, :process, ^l, :killed}, @t
      end

      assert now() < t0 + act, "#{then}: the helper must go before A"
      refute_received {:telelem_owner_witness, _, _}
      queued_child_expires!(ctx, r, act)
    end

    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  # ── witness (fake launcher, supervisor, guard and owner) ───────────────

  test "witness: a closure known at start spawns no helper; launcher loss ends any helper it spawned" do
    # A reached: nothing is spawned or relayed.
    ref = make_ref()
    l = idle()
    w = spawn_witness(ref, l, recorder(), now())
    wm = Process.monitor(w)
    assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
    refute_received {:telelem_owner_guard, ^ref, :helper, _}

    # A launcher already gone: whichever way the witness learns it, no helper
    # it spawned outlives it, and none was released.
    ref = make_ref()
    l = spawn(fn -> :ok end)
    lm = Process.monitor(l)
    assert_receive {:DOWN, ^lm, :process, ^l, _}, @t
    sup = recorder()
    w = spawn_witness(ref, l, sup, now() + 60_000)
    wm = Process.monitor(w)
    assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t

    receive do
      {:telelem_owner_guard, ^ref, :helper, h} -> refute Process.alive?(h)
    after
      0 -> :ok
    end

    no_activation!(sup, ref)
  end

  test "witness: closed (launcher :normal exit, abandonment, A) before the helper's result, it never releases activation" do
    for name <- [:normal, :abandon, :expiry] do
      ref = make_ref()
      l = spawn(fn -> receive do: (:exit -> :ok) end)
      g = recorder()
      until = if name == :expiry, do: now() + 300, else: now() + 60_000
      %{w: w, helper: helper, hm: hm, from: from} = fake_witness(ref, l, until)
      wm = Process.monitor(w)

      # The witness is held while it is closed and the helper's {:ok, guard}
      # is queued behind that.
      true = :erlang.suspend_process(w)
      assert Process.alive?(helper), "#{name}: the witness closed before it was held"

      case name do
        :normal -> send(l, :exit)
        :abandon -> send(w, {:telelem_owner_guard, ref, :abandon})
        :expiry -> sleep_until(until)
      end

      if name == :normal,
        do: await_mailbox!(w, &Enum.any?(&1, fn m -> match?({:DOWN, _, :process, ^l, _}, m) end))

      GenServer.reply(from, {:ok, g})
      await_mailbox!(w, &Enum.any?(&1, fn m -> match?({_, ^ref, :started, _}, m) end))
      true = :erlang.resume_process(w)

      # The helper delivered its result and ended; the witness recorded,
      # watched and relayed the exact guard, but never activates it.
      assert_receive {:DOWN, ^hm, :process, ^helper, _}, @t, inspect(name)
      assert_receive {:telelem_owner_guard, ^ref, :started, {:ok, ^g}}, @t
      await_watched!(w, g)
      no_activation!(g, ref)
      Process.exit(g, :kill)
      assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
      assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
      refute_received {:telelem_owner_guard, ^ref, :watching, _, _}
      Process.exit(l, :kill)
    end
  end

  test "witness: a launcher :DOWN or abandonment queued behind the helper's {:ok, guard} still closes it before release" do
    for name <- [:normal, :killed, :abandon] do
      ref = make_ref()
      l = spawn(fn -> receive do: (:exit -> :ok) end)
      g = recorder()
      %{w: w, helper: helper, hm: hm, from: from} = fake_witness(ref, l, now() + 60_000)
      wm = Process.monitor(w)

      # The helper's result is delivered first, then the closure, both while
      # the witness is held.
      true = :erlang.suspend_process(w)
      GenServer.reply(from, {:ok, g})
      await_mailbox!(w, &Enum.any?(&1, fn m -> match?({_, ^ref, :started, {:ok, ^g}}, m) end))

      case name do
        :normal -> send(l, :exit)
        :killed -> Process.exit(l, :kill)
        :abandon -> send(w, {:telelem_owner_guard, ref, :abandon})
      end

      closure? = fn
        {:DOWN, _, :process, ^l, _} -> true
        {:telelem_owner_guard, ^ref, :abandon} -> true
        _ -> false
      end

      msgs = await_mailbox!(w, &Enum.any?(&1, closure?))

      assert Enum.find_index(msgs, &match?({_, ^ref, :started, _}, &1)) <
               Enum.find_index(msgs, closure?),
             "#{name}: the result must be ahead of the closure"

      true = :erlang.resume_process(w)

      # Relayed (relay before release), recorded and watched, never activated.
      assert_receive {:telelem_owner_guard, ^ref, :started, {:ok, ^g}}, @t, inspect(name)
      assert_receive {:DOWN, ^hm, :process, ^helper, _}, @t
      await_watched!(w, g)
      no_activation!(g, ref)
      Process.exit(g, :kill)
      assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
      assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
      refute_received {:telelem_owner_guard, ^ref, :watching, _, _}
      Process.exit(l, :kill)
    end
  end

  test "witness: a normal return only after its private helper monitor's :DOWN; the helper is gone at its loss relay and at every report" do
    for how <- [:refuse, :ok_then_guard_down, :helper_killed] do
      ref = make_ref()
      l = spawn(fn -> receive do: (:exit -> :ok) end)
      %{w: w, helper: helper, hm: hm, from: from} = fake_witness(ref, l, now() + 60_000)
      wm = Process.monitor(w)
      # Exactly one monitor of the witness on the exact helper, set at spawn.
      {:monitors, ms} = Process.info(w, :monitors)
      assert Enum.count(ms, &(&1 == {:process, helper})) == 1

      case how do
        :refuse ->
          GenServer.reply(from, {:error, :max_children})
          assert_receive {:telelem_owner_guard, ^ref, :started, :error}, @t

        :ok_then_guard_down ->
          g = recorder()
          GenServer.reply(from, {:ok, g})
          assert_receive {:recorded, ^g, {:telelem_owner_guard, ^ref, :activate}}, @t
          Process.exit(g, :kill)

        :helper_killed ->
          Process.exit(helper, :kill)
          # Relayed only on the :DOWN: the helper is already gone.
          assert_receive {:telelem_owner_guard, ^ref, :started, :error}, @t
          refute Process.alive?(helper)
      end

      assert_receive {:telelem_owner_witness, ^ref, _}, @t, inspect(how)
      refute Process.alive?(helper)
      assert_receive {:DOWN, ^hm, :process, ^helper, _}, @t
      assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
      Process.exit(l, :kill)
    end
  end

  test "witness: one activation to the exact reported guard, relayed first; owner death only from its own exact monitor" do
    ref = make_ref()
    l = spawn(fn -> receive do: (:exit -> :ok) end)
    on_exit(fn -> Process.exit(l, :kill) end)
    g = recorder()
    other = recorder()
    %{w: w, helper: helper, hm: hm, from: from} = fake_witness(ref, l, now() + 60_000)

    GenServer.reply(from, {:ok, g})
    assert_receive {:telelem_owner_guard, ^ref, :started, {:ok, ^g}}, @t
    assert_receive {:recorded, ^g, {:telelem_owner_guard, ^ref, :activate}}, @t
    # The helper delivered its result and ended; its exit removed the link.
    assert_receive {:DOWN, ^hm, :process, ^helper, :normal}, @t
    await_links!(w, [])

    # A second result, or an announcement from any other guard, is ignored.
    send(w, {:telelem_owner_guard, ref, :started, {:ok, other}})
    o = idle()
    send(w, {:telelem_owner_guard, ref, :owner, other, o})
    send(w, {:telelem_owner_guard, make_ref(), :owner, g, o})

    # The announcement from the exact guard is acknowledged, once.
    send(w, {:telelem_owner_guard, ref, :owner, g, o})
    assert_receive {:telelem_owner_guard, ^ref, :watching, ^g, ^o}, @t
    send(w, {:telelem_owner_guard, ref, :owner, g, idle()})

    await_idle!(w)
    no_activation!(other, ref)
    refute_received {:telelem_owner_guard, ^ref, :started, _}

    # The guard goes; the owner is still up: no report.
    gm = Process.monitor(g)
    Process.exit(g, :kill)
    assert_receive {:DOWN, ^gm, :process, ^g, _}, @t

    {:monitored_by, by} = Process.info(o, :monitored_by)
    assert w in by

    # A guard report, a forged :DOWN for another reference or a forged EXIT
    # is not evidence.
    send(w, {:telelem_owner_guard, ref, :owner_down, :owner_exited, :exited})
    send(w, {:DOWN, make_ref(), :process, o, :normal})
    send(w, {:EXIT, o, :normal})
    await_idle!(w)
    assert Process.alive?(w)
    refute_received {:telelem_owner_witness, ^ref, _}
    refute_received {:telelem_owner_witness, ^ref, _, _}

    Process.exit(o, :kill)
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
  end

  test "witness: a forged helper EXIT ends the exact helper and relays a failure; it never releases activation" do
    ref = make_ref()
    l = spawn(fn -> receive do: (:exit -> :ok) end)
    on_exit(fn -> Process.exit(l, :kill) end)
    %{w: w, helper: helper, hm: hm} = fake_witness(ref, l, now() + 60_000)
    wm = Process.monitor(w)

    send(w, {:EXIT, helper, :normal})
    # Not evidence: the failure is relayed only on the helper monitor's :DOWN.
    assert_receive {:telelem_owner_guard, ^ref, :started, :error}, @t
    refute Process.alive?(helper)
    assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
    assert_receive {:telelem_owner_witness, ^ref, :activation_never_released}, @t
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
  end

  test "witness: activation released but the guard went down without announcing an owner: owner state unknown" do
    ref = make_ref()
    l = spawn(fn -> receive do: (:exit -> :ok) end)
    on_exit(fn -> Process.exit(l, :kill) end)
    g = recorder()
    %{w: w, from: from} = fake_witness(ref, l, now() + 60_000)
    wm = Process.monitor(w)

    GenServer.reply(from, {:ok, g})
    assert_receive {:recorded, ^g, {:telelem_owner_guard, ^ref, :activate}}, @t
    Process.exit(g, :kill)

    assert_receive {:telelem_owner_witness, ^ref, :owner_unknown}, @t
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
    refute_received {:telelem_owner_witness, ^ref, :owner_down, _}
    refute_received {:telelem_owner_witness, ^ref, :activation_never_released}
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  defp start_h!(ctx, opts \\ []) do
    d = Keyword.get(opts, :deadline, now() + 60_000)

    {:ok, h} =
      Handoff.start(
        [
          requester: idle(),
          attempts: [:primary],
          admission_authority: ctx.auth,
          terminal_deadline: d,
          coordinator_grant: {:one_primary, @units},
          cleanup_timeout: 60_000,
          register_timeout: 60_000
        ] ++ Keyword.take(opts, [:trace])
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    %{h: h, d: d}
  end

  defp payload(ctx, r) do
    [
      handoff: r.h,
      id: :primary,
      controller: self(),
      body: ~s({"marker":"#{@secret}"}),
      limits: Limits.new(),
      gate: [target: ctx.target, deadline: r.d]
    ]
  end

  defp gopts(ctx, r, requester, extra \\ []) do
    Keyword.merge(
      [
        upstream: payload(ctx, r),
        requester: requester,
        observer: self(),
        grace_ms: 50,
        activation_ms: 5_000
      ],
      extra
    )
  end

  defp launcher(fun) do
    test = self()

    :erlang.spawn_opt(
      fn ->
        send(test, {:launched, self(), fun.()})
        receive do: (:exit -> :ok)
      end,
      [:monitor]
    )
  end

  defp await_reserve!(auth, h, o) do
    await_mailbox!(
      auth,
      &Enum.any?(&1, fn m -> match?({:"$gen_call", {^h, _}, {:reserve, _, ^o, @units}}, m) end)
    )
  end

  defp owner_gone!(%{tag: tag}, o) do
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^o, [{_gid, {:ok, _}}]}}, @t
  end

  defp only_grant!(auth) do
    assert [{gid, g}] = Map.to_list(Authority.snapshot(auth).grants)
    {gid, g}
  end

  defp assert_ledger!(ctx, gid, state) do
    snap = Authority.snapshot(ctx.auth)
    assert snap.grants[gid].state == state

    assert %{active: 0, reserved_units: 0, encumbered_units: 0, available_units: @limit} =
             Map.take(snap, [:active, :reserved_units, :encumbered_units, :available_units])

    assert Ledger.consistent?(Directory.ledger(ctx.inst.table))
  end

  defp await_mailbox!(pid, pred, until \\ nil) do
    until = until || now() + @t
    msgs = mailbox(pid)

    cond do
      pred.(msgs) -> msgs
      now() >= until -> flunk("mailbox of #{inspect(pid)}: #{inspect(msgs)}")
      true -> await_mailbox!(pid, pred, until)
    end
  end

  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)

  defp sleep_until(t) do
    left = t - now()
    if left > 0, do: Process.sleep(left)
    if now() < t, do: sleep_until(t)
  end

  # A real launch whose helper is blocked in start_child at the suspended
  # supervisor; the launcher has released it. Checks the process shape.
  defp blocked_launch!(ctx) do
    r = start_h!(ctx)
    act = 1_000
    :ok = :sys.suspend(ctx.sup)
    t0 = now()
    opts = gopts(ctx, r, idle(), activation_ms: act)
    {l, lm} = launcher(fn -> OwnerGuard.start(ctx.sup, opts) end)

    [{:"$gen_call", {helper, _}, {:start_child, call}}] =
      await_mailbox!(ctx.sup, &match?([{:"$gen_call", _, {:start_child, _}}], &1))

    %{witness: w, ref: ref} = find_meta(call)
    # Linked to the witness only and not trapping; the witness traps and
    # monitors the launcher, which monitors it.
    assert {:links, [^w]} = Process.info(helper, :links)
    assert {:trap_exit, false} = Process.info(helper, :trap_exit)
    assert {:trap_exit, true} = Process.info(w, :trap_exit)
    await_monitors!(w, l)
    await_monitors!(l, w)

    %{
      r: r,
      l: l,
      lm: lm,
      w: w,
      wm: Process.monitor(w),
      helper: helper,
      hm: Process.monitor(helper),
      ref: ref,
      t0: t0,
      act: act
    }
  end

  # The queued request of a helper already gone is not withdrawn: it starts a
  # guard before A that is never activated, gets no owner and expires at A.
  defp queued_child_expires!(ctx, r, act) do
    :ok = :sys.resume(ctx.sup)
    assert [{_, g, :worker, _}] = DynamicSupervisor.which_children(ctx.sup)
    gm = Process.monitor(g)
    assert_receive {:DOWN, ^gm, :process, ^g, {:shutdown, :expired}}, act + @t
    assert %{attempts: %{primary: %{pid: nil}}} = Handoff.status(r.h)
  end

  defp spawn_witness(ref, l, sup, until) do
    me = self()

    meta = %{
      ref: ref,
      witness: nil,
      observer: me,
      requester: me,
      handoff: me,
      deadline: until,
      until: until,
      grace_ms: 50
    }

    spawn(OwnerGuard, :witness, [
      %{launcher: l, sup: sup, alias: me, ref: ref, until: until, observer: me, meta: meta}
    ])
  end

  # A real witness and its real helper over a fake supervisor (a recorder);
  # the test is the launcher's alias (it releases the helper) and the
  # observer. Returns with the helper blocked in its start_child call.
  defp fake_witness(ref, l, until) do
    sup = recorder()
    w = spawn_witness(ref, l, sup, until)
    assert_receive {:telelem_owner_guard, ^ref, :helper, helper}, @t
    assert {:links, [^w]} = Process.info(helper, :links)
    hm = Process.monitor(helper)
    send(helper, {:telelem_owner_guard, ref, :run})
    assert_receive {:recorded, ^sup, {:"$gen_call", {^helper, _} = from, {:start_child, _}}}, @t
    %{w: w, helper: helper, hm: hm, from: from}
  end

  defp await_links!(pid, links, until \\ nil) do
    until = until || now() + @t
    {:links, ls} = Process.info(pid, :links)

    cond do
      Enum.sort(ls) == Enum.sort(links) -> :ok
      now() >= until -> flunk("links of #{inspect(pid)}: #{inspect(ls)}")
      true -> await_links!(pid, links, until)
    end
  end

  # Forwards everything it receives to the test, tagged with its pid.
  defp recorder do
    test = self()
    pid = spawn(fn -> record(test) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp record(test) do
    receive do
      m -> send(test, {:recorded, self(), m})
    end

    record(test)
  end

  defp await_monitors!(pid, target, until \\ nil) do
    until = until || now() + @t
    {:monitors, ms} = Process.info(pid, :monitors)

    cond do
      {:process, target} in ms -> :ok
      now() >= until -> flunk("#{inspect(pid)} does not monitor #{inspect(target)}")
      true -> await_monitors!(pid, target, until)
    end
  end

  # The witness has processed everything delivered to it so far.
  defp await_idle!(w, until \\ nil) do
    until = until || now() + @t

    case Process.info(w, [:message_queue_len, :status]) do
      [message_queue_len: 0, status: :waiting] ->
        :ok

      other ->
        if now() >= until, do: flunk("witness not idle: #{inspect(other)}")
        await_idle!(w, until)
    end
  end

  defp await_watched!(w, g) do
    await_idle!(w)
    {:monitored_by, by} = Process.info(g, :monitored_by)
    assert w in by
  end

  # Anything the idle witness sent `g` is already in g's queue, ahead of
  # this marker.
  defp no_activation!(g, ref) do
    send(g, :marker)
    assert_receive {:recorded, ^g, :marker}, @t
    refute_received {:recorded, ^g, {:telelem_owner_guard, ^ref, :activate}}
  end

  # The guard metadata inside a queued start_child call.
  defp find_meta(%{ref: _, witness: _} = m), do: m
  defp find_meta(%{} = m), do: m |> Map.values() |> find_meta()
  defp find_meta(t) when is_tuple(t), do: t |> Tuple.to_list() |> find_meta()
  defp find_meta([h | t]), do: find_meta(h) || find_meta(t)
  defp find_meta(_), do: nil

  defp contains?(term, needle) when term === needle, do: true
  defp contains?(b, n) when is_binary(b) and is_binary(n), do: String.contains?(b, n)
  defp contains?(t, n) when is_tuple(t), do: t |> Tuple.to_list() |> contains?(n)
  defp contains?([h | t], n), do: contains?(h, n) or contains?(t, n)
  defp contains?(%{} = m, n), do: m |> Map.to_list() |> contains?(n)
  defp contains?(_, _), do: false

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp now, do: System.monotonic_time(:millisecond)
end
