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
  limit; the metadata-only child entry and late (queued) child start; the
  helper killed by the witness on expiry, launcher loss (`:normal` included)
  and abandonment; positive attach by the exact owner pid; the owner's GO
  rechecks at the real entry (token, limit, pinned coordinator, deadline,
  pending stop); cooperative stop at a checkpoint and the forced fence while
  blocked in a call; supervisor shutdown; and the witness's independent
  confirmation on guard, requester and launcher loss. Reports are asserted
  only with the exact process already dead.

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
    # and monitored by it and the witness; witness linked to nothing.
    assert [{_, ^g, :worker, _}] = DynamicSupervisor.which_children(ctx.sup)
    assert {:trap_exit, true} = Process.info(g, :trap_exit)
    assert {:links, [^g]} = Process.info(o, :links)
    assert {:trap_exit, false} = Process.info(o, :trap_exit)
    {:monitored_by, by} = Process.info(o, :monitored_by)
    assert g in by and w in by
    assert {:links, []} = Process.info(w, :links)

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
    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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
    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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
    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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
    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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

    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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
    assert_receive {:telelem_owner_witness, ^ref, :confirmed, {:owner_down, ^o}}, @t
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
    assert {:links, []} = Process.info(helper, :links)

    Process.exit(l, :kill)
    assert_receive {:DOWN, ^lm, :process, ^l, :killed}, @t
    assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t
    assert now() < t0 + act, "the helper must go on launcher loss, not at A"
    assert_receive {:telelem_owner_witness, _, :confirmed, :no_owner}, @t

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
    assert_receive {:telelem_owner_witness, _, :confirmed, :no_owner}, @t
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

  test "witness: the exact helper is killed on launcher :normal exit, on abandonment and at A" do
    for name <- [:normal, :abandon, :expiry] do
      ref = make_ref()
      l = spawn(fn -> receive do: (:exit -> :ok) end)
      test = self()

      helper =
        spawn(fn ->
          receive do
            {:telelem_owner_guard, ^ref, :run, w} -> send(test, {:run, w})
          end

          receive do: (:never -> :ok)
        end)

      hm = Process.monitor(helper)
      until = if name == :expiry, do: now() + 50, else: now() + 60_000

      w =
        spawn(OwnerGuard, :witness, [
          %{launcher: l, helper: helper, alias: self(), ref: ref, until: until, observer: self()}
        ])

      assert_receive {:run, ^w}, @t

      case name do
        :normal -> send(l, :exit)
        :abandon -> send(w, {:telelem_owner_guard, ref, :abandon})
        :expiry -> :ok
      end

      assert_receive {:DOWN, ^hm, :process, ^helper, :killed}, @t, inspect(name)
      assert_receive {:telelem_owner_witness, ^ref, :confirmed, :no_owner}, @t
      Process.exit(l, :kill)
    end
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
