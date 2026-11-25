defmodule Telelem.Serve.PrimaryTest do
  @moduledoc """
  W2c `Telelem.Serve.Primary` on a real `Telelem.Serve.Tree`: one
  `Ingress.prepare/3` (SystemClock), one real coordinator started and
  activated by `Telelem.Serve.CoordinatorStart` on the pinned
  `:coordinators` branch (`coordinator_grant`, `terminal_ack`, receipts into
  a real sandbox `Telelem.Receipts.Store`), one real guarded owner launched by
  `Telelem.Wire.OwnerGuard` on the pinned `:guards` branch, and the real
  pinned `Telelem.Admission.Authority` of a `Telelem.Instance`.

  Listener-free and offline: no socket is opened. The instance has fewer
  units than one reservation needs, so the authority **denies the
  reservation** and the GO'd owner ends before it dials (a dial would surface
  as `{:connect_failed, _, _}` to the controller, which every case refutes).
  The fixture targets are `127.0.0.1` ports 1 and 2 and are never dialed.

  Barriers: `:sys.suspend/1` on the authority (the reservation request is
  read from its mailbox), on a branch supervisor or on the coordinator, and
  `:erlang.suspend_process/1` on a spawned requester; requests are read back
  from mailboxes with `Process.info/2`. Clock waits: only for an absolute
  limit to pass (OwnerGuard's activation limit `A`; the coordinator limit
  `S`). The registration and lifecycle timers are read from an isolated
  `:trace` session on the coordinators branch (the coordinator inherits it).

  Not covered: a dial, the requester's downstream protocol, crossing `D`
  exactly at the checks before the coordinator start or before the launch
  (no deterministic hook), the activation-refused path end to end (no
  documented refusal is reachable from the exact requester's only
  activation; classified directly) and the unconfirmed launch path end to
  end (OwnerGuard raises nothing after a passing preflight; classified
  directly). OwnerGuard's helper/witness lifetime after a launcher failure
  is its own contract and is not asserted here.

      scripts/mix test test/telelem/serve/primary_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.Authority
  alias Telelem.{Handoff, Instance}
  alias Telelem.Ops.{Context, MemoryStore}
  alias Telelem.Receipts.{Receipt, Store}
  alias Telelem.Route.Revision
  alias Telelem.Sandbox.{Bootstrap, Principal, VerifiedTarget}
  alias Telelem.Serve.{Ingress, Primary, Tree}
  alias Telelem.Serve.Primary.Handle
  alias Telelem.Serve.Tree.Pins
  alias Telelem.Wire.{Limits, OwnerGuard}

  @moduletag :tmp_dir
  @t 2_000
  @units 4
  # Fewer than @units: every reservation is refused.
  @instance_units 2
  @coordinator_ms 5_000
  @auth "AUTH-MARKER-3d9a"
  @body ~s({"marker":"BODY-MARKER-71c4"})
  @tenant "local"
  @empty %{active: 0, specs: 0, supervisors: 0, workers: 0}
  @reader %{
    tenant_id: @tenant,
    subject_id: "operator",
    mode: :sandbox,
    permissions: [:receipt_read]
  }

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :serve_primary_gw,
        limits: %{slots: 2, units: @instance_units},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)
    root = start_supervised!({Tree, []}, id: :tree, restart: :temporary)
    {:ok, pins} = Tree.resolve(root)
    store = start_supervised!({Store, mode: :sandbox}, id: :receipts)
    routes = start_supervised!(MemoryStore, id: :routes)
    :ok = MemoryStore.seed(routes, @tenant, rev1())

    on_exit(fn -> for p <- [auth, pins.coordinators, pins.guards], do: resume(p) end)

    {bindings, target} = bindings!(dir)

    %{
      inst: inst,
      auth: auth,
      root: root,
      pins: pins,
      store: store,
      routes: routes,
      bindings: bindings,
      target: target
    }
  end

  # ── setup pins ──────────────────────────────────────────────────────────

  test "config resolves the exact tree once and pins it with a SystemClock ingress, a live Authority and the canonical sandbox store",
       ctx do
    good = cfg_opts(ctx)
    assert {:ok, %Primary.Config{} = cfg} = Primary.config(good)
    assert cfg.authority == ctx.auth and cfg.store == ctx.store
    assert cfg.tree == ctx.pins
    assert %Pins{root: root} = cfg.tree
    assert root == ctx.root
    assert cfg.producer == Store.producer(ctx.store)

    shown = inspect(cfg)
    for s <- [@auth, @body, "127.0.0.1"], do: refute(shown =~ s)

    manual = start_supervised!(Telelem.Instance.ManualClock)

    {:ok, manual_ingress} =
      Ingress.config(ingress_opts(ctx, clock: {Telelem.Instance.ManualClock, manual}))

    serving = start_supervised!({Store, mode: :serving}, id: :serving_store)
    wide = start_supervised!({Store, mode: :sandbox, max_pending: 257}, id: :wide_store)
    dsup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :bare_dsup)
    stopped = start_supervised!({Tree, []}, id: :stopped_tree, restart: :temporary)
    :ok = stop_supervised(:stopped_tree)
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    bad = [
      {Keyword.put(good, :ingress, manual_ingress), :ingress_clock},
      {Keyword.put(good, :ingress, :nope), :ingress},
      {Keyword.put(good, :authority, idle()), :authority},
      {Keyword.put(good, :authority, dead), :authority},
      {Keyword.put(good, :store, serving), :store},
      {Keyword.put(good, :store, wide), :store},
      {Keyword.put(good, :store, idle()), :store},
      {Keyword.put(good, :store, dead), :store},
      {Keyword.put(good, :tree, idle()), :tree},
      {Keyword.put(good, :tree, dsup), :tree},
      {Keyword.put(good, :tree, ctx.pins.coordinators), :tree},
      {Keyword.put(good, :tree, stopped), :tree},
      {Keyword.put(good, :tree, Tree), :tree},
      {Keyword.put(good, :units, 0), :units},
      {Keyword.put(good, :limits, %{}), :limits},
      {Keyword.put(good, :coordinator_ms, 0), :coordinator_ms},
      {Keyword.put(good, :coordinator_ms, 60_001), :coordinator_ms},
      {Keyword.put(good, :grace_ms, 0), :grace_ms},
      {Keyword.put(good, :activation_ms, OwnerGuard.bounds().activation_ms + 1), :activation_ms},
      {Keyword.delete(good, :store), :store},
      {Keyword.delete(good, :tree), :tree},
      {good ++ [supervisor: ctx.pins.guards], :supervisor},
      {good ++ [units: @units], :config},
      {[{"units", 1} | good], :config}
    ]

    for {opts, field} <- bad do
      assert Primary.config(opts) == {:error, {:invalid, field}}, inspect(field)
    end

    assert Primary.config(Map.put(Map.new(good), :extra, 1)) == {:error, {:invalid, :extra}}
    assert Primary.start(:not_a_config, @auth, reader(@body)) == {:error, :invalid_config}
    assert Primary.start(%{cfg | units: 0}, @auth, reader(@body)) == {:error, :invalid_config}

    assert Primary.start(%{cfg | coordinator_ms: 0}, @auth, reader(@body)) ==
             {:error, :invalid_config}

    # Shape only, no lookup: a struct that is not Pins, or repeated pins.
    p = cfg.tree
    assert Primary.start(%{cfg | tree: p.root}, @auth, reader(@body)) == {:error, :invalid_config}

    assert Primary.start(%{cfg | tree: %{p | guards: p.coordinators}}, @auth, reader(@body)) ==
             {:error, :invalid_config}

    for o <- [
          [controller: :name],
          [observer: 1],
          [observer: self(), observer: self()],
          [x: 1],
          :nope
        ] do
      assert Primary.start(cfg, @auth, reader(@body), o) == {:error, {:invalid, :opts}}
    end

    refute_received {:authenticate, _}
    refute_received {:read_body, _, _}
    nothing_started!(ctx)
  end

  # ── the real composition ────────────────────────────────────────────────

  test "GO sent: coordinator on the coordinators pin, guard on the guards pin, one D and S; the denied reservation ends the owner before any dial; receipt and exact witness",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    :ok = :sys.suspend(ctx.auth)

    before = now()

    assert {:ok, %Handle{handoff: h, guard: g, owner: o, witness: w, ref: ref} = handle} =
             Primary.start(cfg, @auth, reader(@body))

    after_ = now()
    wm = Process.monitor(w)
    hm = Process.monitor(h)
    d = handle.deadline_ms

    # Prepared exactly once.
    assert_received {:authenticate, @auth}
    assert_received {:read_body, _, _}
    refute_received {:authenticate, _}
    refute_received {:read_body, _, _}

    # Exactly one child per pinned branch, linked to it.
    assert [{:undefined, ^h, :worker, [Handoff]}] =
             DynamicSupervisor.which_children(ctx.pins.coordinators)

    assert [{_, ^g, :worker, _}] = DynamicSupervisor.which_children(ctx.pins.guards)
    assert {:links, [l]} = Process.info(h, :links)
    assert l == ctx.pins.coordinators

    # The owner registered itself and the reservation is for exactly it.
    await_reserve!(ctx.auth, h, o)

    assert %{
             attempts: %{primary: %{pid: ^o}},
             grant: :reserving,
             receipts: :recording,
             activation: :active
           } = Handoff.status(h)

    # One D everywhere; S = min(t1 + coordinator_ms, D), with t1 in [before, after].
    hs = :sys.get_state(h)
    assert hs.terminal_deadline == d and hs.terminal_ack and hs.order == [:primary]
    assert hs.authority == ctx.auth and hs.requester == self()
    assert %{units: @units} = hs.grant
    assert %{state: :active, limit: s} = hs.activation
    assert s <= d
    assert s >= min(before + @coordinator_ms, d) and s <= min(after_ + @coordinator_ms, d)

    # No body, auth input or target in the coordinator, either branch's
    # state (child specs), the handle or the launcher's mailbox.
    for term <- [
          hs,
          :sys.get_state(ctx.pins.coordinators),
          :sys.get_state(ctx.pins.guards),
          handle,
          mailbox(self())
        ],
        x <- [@auth, @body, ctx.target, ctx.target.host] do
      refute contains?(term, x), inspect(x)
    end

    # Release the authority: the reservation is denied.
    :ok = :sys.resume(ctx.auth)

    assert_receive {:telelem_wire, ^o, :primary,
                    {:ended, {:attach, :cancel, :admission_refused}, stats}},
                   @t

    assert %{claim: :none, cleaned_ack: :ok} = stats
    assert_receive {:telelem_handoff, ^h, {:failed, :admission_refused}}, @t
    assert_receive {:telelem_handoff, ^h, {:closed, report}}, @t
    assert %{primary: %{cleanup: :ack, charge: :none}} = report.attempts

    # Exact witness: its own monitor on this exact owner; then it ends.
    assert_receive {:telelem_owner_witness, ^ref, :owner_down, ^o}, @t
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
    refute Process.alive?(o)

    # The guard's report: the owner's own exit, or the coordinator's exit
    # after it closed the request (whichever :DOWN the guard handled first).
    assert_receive {:telelem_owner_guard, ^ref, :owner_down, cause, how}, @t
    assert {cause, how} in [{:owner_exited, :exited}, {:handoff_down, :stopped}]
    assert_receive {:DOWN, ^hm, :process, ^h, _}, @t

    # Receipt: complete, no dispatch, exposure none.
    assert {:ok, %Receipt{} = r} = Store.lookup(ctx.store, @reader, handle.request_id)
    assert %{completeness: :complete, gaps: [], observed_outcome: :failed, exposure: :none} = r
    assert [{:ingress, 0, :none, :none}, {:terminal, 0, :failed, :none}] = tuples(r)
    for x <- [@auth, @body, ctx.target.host], do: refute(contains?(r, x))

    # Nothing reserved, nothing dialed, nothing else sent here; the tree is
    # the same one.
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
    refute_received {:telelem_wire, _, _, _}
    refute_received {:telelem_handoff, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_owner_witness, _, _, _}
    assert Tree.resolve(ctx.root) == {:ok, ctx.pins}
  end

  test "lifecycle expiry at S; the registration timer, armed at activation, fires no earlier than D; the terminal timer is at D",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    :ok = :sys.suspend(ctx.auth)

    session = :trace.session_create(:serve_primary, self(), [])
    on_exit(fn -> :trace.session_destroy(session) end)
    # The coordinators branch and what it spawns (the coordinator) only.
    flags = [:call, :set_on_spawn, :monotonic_timestamp]
    1 = :trace.process(session, ctx.pins.coordinators, true, flags)
    _ = :trace.function(session, {:erlang, :send_after, :_}, true, [:global])

    before = now()
    assert {:ok, %Handle{handoff: h, deadline_ms: d}} = Primary.start(cfg, @auth, reader(@body))
    after_ = now()
    1 = :trace.process(session, ctx.pins.coordinators, false, flags)
    tref = :trace.delivered(session, h)
    assert_receive {:trace_delivered, ^h, ^tref}, @t

    calls = drain_send_after(h, [])
    assert [{_, s, _, [abs: true]}] = for({_, _, {:activation_expiry, _}, _} = c <- calls, do: c)
    assert s >= min(before + @coordinator_ms, d) and s <= min(after_ + @coordinator_ms, d)
    assert [{at, ms, _, []}] = for({_, _, {:register_deadline, _}, _} = c <- calls, do: c)
    assert ms >= 1 and at + ms >= d, inspect({at, ms, d})
    assert [{_, ^d, _, [abs: true]}] = for({_, _, {:terminal_deadline, _}, _} = c <- calls, do: c)
    :trace.session_destroy(session)
    :ok = :sys.resume(ctx.auth)
    assert_receive {:telelem_handoff, ^h, {:closed, _}}, @t
  end

  # ── stale pins, ordinary preflight and startup failures ─────────────────

  test "stopped tree: the config keeps the old pins; the start on them submits nothing; a replacement tree is never used",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    old = ctx.pins
    ms = for p <- [old.root, old.coordinators, old.guards], do: {p, Process.monitor(p)}
    :ok = stop_supervised(:tree)
    for {p, m} <- ms, do: assert_receive({:DOWN, ^m, :process, ^p, _}, @t)

    new_root = start_supervised!({Tree, []}, id: :replacement_tree, restart: :temporary)
    {:ok, new} = Tree.resolve(new_root)
    assert MapSet.disjoint?(pin_set(old), pin_set(new))

    assert {:error, {:coordinator, {:not_sent, :start_unconfirmed}, failure}} =
             res = Primary.start(cfg, @auth, reader(@body))

    assert %{handoff: nil, activation: :not_sent, go: :not_sent, cancel: :not_submitted} =
             failure

    assert map_size(failure) == 5 and is_binary(failure.request_id)
    for x <- [@auth, @body, ctx.target, ctx.target.host], do: refute(contains?(res, x))

    # Still the old pins; nothing was looked up, rebound or started anywhere.
    assert cfg.tree == old
    assert DynamicSupervisor.count_children(new.coordinators) == @empty
    assert DynamicSupervisor.count_children(new.guards) == @empty
    assert %{pending: 0, retained: 0, receipts: 0} = Store.stats(ctx.store)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_handoff, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_wire, _, _, _}

    # A new setup resolves the new tree.
    assert {:ok, %{tree: ^new}} = Primary.config(Keyword.put(cfg_opts(ctx), :tree, new_root))
  end

  test "ingress failure: nothing read, nothing started", ctx do
    {:ok, ingress} =
      Ingress.config(
        ingress_opts(ctx,
          authenticate: fn input ->
            send(self(), {:authenticate, input})
            :error
          end
        )
      )

    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :ingress, ingress))
    assert Primary.start(cfg, @auth, reader(@body)) == {:error, {:ingress, :unauthenticated}}
    assert_received {:authenticate, @auth}
    refute_received {:read_body, _, _}
    nothing_started!(ctx)
  end

  test "payload preflight failure (body over the gated limit): no coordinator, guard or receipt",
       ctx do
    limits = Limits.new(max_request_body: 8)
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :limits, limits))
    assert {:error, {:preflight, :upstream}} = res = Primary.start(cfg, @auth, reader(@body))
    for x <- [@auth, @body, ctx.target.host], do: refute(contains?(res, x))
    assert_received {:read_body, _, _}
    nothing_started!(ctx)
  end

  test "coordinator start refused (pinned store gone): activation not sent, no cancel, nothing launched",
       ctx do
    store = start_supervised!({Store, mode: :sandbox}, id: :doomed_store)
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :store, store))
    :ok = stop_supervised(:doomed_store)

    assert {:error,
            {:coordinator, {:not_sent, {:start_refused, {:bad_option, :receipts}}},
             %{handoff: nil, activation: :not_sent, go: :not_sent, cancel: :not_submitted}}} =
             Primary.start(cfg, @auth, reader(@body))

    nothing_started!(ctx)
  end

  # ── failures after the coordinator exists ───────────────────────────────

  test "suspended guards branch: confirmed activation, launch expires at A, GO not sent, one async cancel ends the request",
       ctx do
    act = 50
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :activation_ms, act))
    :ok = :sys.suspend(ctx.pins.guards)

    assert {:error, {:launch, :expired, failure}} = res = Primary.start(cfg, @auth, reader(@body))

    assert %{activation: :confirmed, go: :not_sent, cancel: :submitted, handoff: h} = failure
    assert map_size(failure) == 5
    hm = Process.monitor(h)
    launch_failed!(ctx, res, h, failure.request_id)
    assert_receive {:DOWN, ^hm, :process, ^h, _}, @t

    # OwnerGuard's own report, under a ref this API never returned.
    assert_receive {:telelem_owner_witness, wref, :activation_never_released}, @t
    refute contains?(res, wref)

    # The child queued at the suspended branch starts late and does no work.
    :ok = :sys.resume(ctx.pins.guards)
    assert DynamicSupervisor.which_children(ctx.pins.guards) == []
    assert DynamicSupervisor.which_children(ctx.pins.coordinators) == []
    refute_received {:telelem_owner_guard, _, _, _, _}
    refute_received {:telelem_wire, _, _, _}
  end

  test "accepted but late activation: unconfirmed, no GO, exactly one queued cancel, submitted without waiting",
       ctx do
    cm = 750
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :coordinator_ms, cm))
    test = self()
    pins = ctx.pins

    # Hold the coordinators branch so the requester's start request waits there.
    :ok = :sys.suspend(pins.coordinators)
    # Built here, so its markers come to the test process directly.
    rd = reader(@body)
    r = requester!(fn -> Primary.start(cfg, @auth, rd, controller: test, observer: test) end)
    await_mail!(pins.coordinators, &match?({:"$gen_call", _, {:start_child, _}}, &1))

    # The requester is held before the child exists, so it cannot activate
    # before the coordinator is held too.
    :erlang.suspend_process(r)
    :ok = :sys.resume(pins.coordinators)

    assert [{:undefined, h, :worker, [Handoff]}] =
             DynamicSupervisor.which_children(pins.coordinators)

    hm = Process.monitor(h)
    on_exit(fn -> resume(h) end)
    :ok = :sys.suspend(h)
    true = :erlang.resume_process(r)

    # The one activation request, from the requester, with S.
    [{:"$gen_call", _, {:activate, s}}] =
      await_mail!(h, &match?({:"$gen_call", {^r, _}, {:activate, _}}, &1))

    # Hold the requester before it can see the reply; let the coordinator
    # decide, before S: it accepts.
    :erlang.suspend_process(r)
    :ok = :sys.resume(h)
    assert %{activation: :active, receipts: :recording, grant: :idle} = Handoff.status(h)
    assert now() < s

    # Hold the coordinator again, let S pass (clock wait), release the
    # requester: it observes the reply at or after S.
    :ok = :sys.suspend(h)
    wait_until!(s)
    true = :erlang.resume_process(r)

    # It returns while the coordinator is still suspended: the cancel is
    # submitted, not awaited.
    assert_receive {:result, ^r, res}, @t
    assert {:error, {:coordinator, :unconfirmed, failure}} = res

    assert %{handoff: ^h, activation: :unconfirmed, go: :not_sent, cancel: :submitted} =
             failure

    assert map_size(failure) == 5
    for x <- [@auth, @body, ctx.target, ctx.target.host], do: refute(contains?(res, x))

    reason = Primary.cancel_reason()
    from_r = for {:"$gen_call", {^r, _}, req} <- mailbox(h), do: req
    assert from_r == [{:cancel, reason}]

    # No GO: OwnerGuard was never called; no guard, owner or witness.
    assert DynamicSupervisor.count_children(pins.guards) == @empty
    refute_received {:telelem_wire, _, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_owner_witness, _, _, _}

    # The queued cancel ends the active request; its reply is discarded.
    :ok = :sys.resume(h)
    assert_receive {:via, ^r, {:telelem_handoff, ^h, {:failed, ^reason}}}, @t
    assert_receive {:via, ^r, {:telelem_handoff, ^h, {:closed, report}}}, @t
    assert %{primary: %{cleanup: :never_registered, charge: :none}} = report.attempts
    assert_receive {:DOWN, ^hm, :process, ^h, _}, @t
    send(r, {:flush, test})
    assert_receive {:flushed, ^r}, @t
    refute_received {:via, ^r, _}

    assert {:ok, %Receipt{} = rec} = Store.lookup(ctx.store, @reader, failure.request_id)
    assert %{completeness: :complete, observed_outcome: :failed, exposure: :none} = rec
    assert [{:ingress, 0, :none, :none}, {:terminal, 0, :failed, :none}] = tuples(rec)
    assert Authority.snapshot(ctx.auth).grants == %{}
    assert DynamicSupervisor.count_children(pins.guards) == @empty
  end

  # ── classification (pure) ───────────────────────────────────────────────

  test "coordinator classification: only a confirmed activation proceeds; only a known possibly active pid is cancelled" do
    me = self()
    assert Primary.start_result(fn -> {:ok, me} end) == {:ok, me}

    for why <- [:expired, {:start_refused, :other}, :start_unconfirmed, :handoff_down] do
      assert Primary.start_result(fn -> {:error, {:not_sent, why}} end) ==
               {:error, {:not_sent, why}, nil, :not_sent}
    end

    assert Primary.start_result(fn -> {:error, {:refused, :not_requester, me}} end) ==
             {:error, {:refused, :not_requester}, me, :refused}

    assert Primary.start_result(fn -> {:error, {:unconfirmed, me}} end) ==
             {:error, :unconfirmed, me, :unconfirmed}

    for f <- [
          fn -> raise ArgumentError, "CoordinatorStart: x" end,
          fn -> throw(:x) end,
          fn -> exit(:x) end,
          fn -> {:ok, :name} end,
          fn -> {:error, {:refused, {:x, 1}, me}} end,
          fn -> {:error, {:unconfirmed, :name}} end,
          fn -> :ok end
        ] do
      assert Primary.start_result(f) == {:error, :unrecognized, nil, :unconfirmed}
    end
  end

  test "launch classification: only the handle is GO sent; only typed errors are GO not sent" do
    me = self()
    handle = %{guard: me, owner: me, witness: me, ref: make_ref()}
    assert Primary.launch_result(fn -> {:ok, handle} end) == {:go_sent, handle}

    for why <- [:expired, :start_failed, :guard_down, :witness_down] do
      assert Primary.launch_result(fn -> {:error, why} end) == {:not_sent, why}
    end

    for f <- [
          fn -> raise ArgumentError, "OwnerGuard: x" end,
          fn -> throw(:x) end,
          fn -> exit(:x) end,
          fn -> {:error, :other} end,
          fn -> {:ok, Map.put(handle, :extra, 1)} end,
          fn -> {:ok, %{handle | owner: :name}} end,
          fn -> :ok end
        ] do
      assert Primary.launch_result(f) == :unconfirmed
    end
  end

  test "Handoff.submit_cancel/2: one request, abandoned at once; no wait, no reply kept" do
    me = self()

    fake =
      spawn(fn ->
        receive do
          {:"$gen_call", from, msg} ->
            send(me, {:got, msg})
            receive do: (:reply -> GenServer.reply(from, :ok))
            send(me, :replied)
            receive do: (:never -> :ok)
        end
      end)

    on_exit(fn -> Process.exit(fake, :kill) end)
    assert Handoff.submit_cancel(fake, :launch_failed) == :submitted
    assert_receive {:got, {:cancel, :launch_failed}}, @t
    send(fake, :reply)
    assert_receive :replied, @t
    # The reply went to a deactivated alias: dropped by the runtime.
    send(self(), :marker)
    assert_receive :marker
    assert mailbox(self()) == []
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  # After a launch failure with a confirmed activation: the coordinator ended
  # the request on the one cancel; its messages stayed in the requester's
  # mailbox; the receipt is complete with no dispatch; nothing was reserved
  # or dialed; no guard or owner report can be attributed.
  defp launch_failed!(ctx, res, h, id) do
    assert {:error, {:launch, _, _}} = res
    for x <- [@auth, @body, ctx.target, ctx.target.host], do: refute(contains?(res, x))

    reason = Primary.cancel_reason()
    assert_receive {:telelem_handoff, ^h, {:failed, ^reason}}, @t
    assert_receive {:telelem_handoff, ^h, {:closed, report}}, @t
    assert %{primary: %{cleanup: :never_registered, charge: :none}} = report.attempts
    refute_received {:telelem_handoff, ^h, _}

    assert {:ok, %Receipt{} = r} = Store.lookup(ctx.store, @reader, id)
    assert %{completeness: :complete, observed_outcome: :failed, exposure: :none} = r
    assert [{:ingress, 0, :none, :none}, {:terminal, 0, :failed, :none}] = tuples(r)

    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_wire, _, _, _}
  end

  defp nothing_started!(ctx) do
    assert DynamicSupervisor.count_children(ctx.pins.coordinators) == @empty
    assert DynamicSupervisor.count_children(ctx.pins.guards) == @empty
    assert %{pending: 0, retained: 0, receipts: 0} = Store.stats(ctx.store)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_handoff, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_owner_witness, _, _, _}
    refute_received {:telelem_wire, _, _, _}
  end

  # A separate requester process: runs `fun` on :go, reports its result,
  # then relays everything it receives, in order, until told to flush (it
  # stays alive: the coordinator monitors it).
  defp requester!(fun) do
    test = self()

    r =
      spawn(fn ->
        receive do: (:go -> :ok)
        send(test, {:result, self(), fun.()})
        relay(test)
      end)

    on_exit(fn -> Process.exit(r, :kill) end)
    send(r, :go)
    r
  end

  defp relay(test) do
    receive do
      {:flush, ^test} ->
        send(test, {:flushed, self()})
        relay(test)

      msg ->
        send(test, {:via, self(), msg})
        relay(test)
    end
  end

  defp cfg_opts(ctx, extra \\ []) do
    {:ok, ingress} = Ingress.config(ingress_opts(ctx))

    Keyword.merge(
      [
        ingress: ingress,
        authority: ctx.auth,
        store: ctx.store,
        tree: ctx.root,
        units: @units,
        limits: Limits.new(),
        coordinator_ms: @coordinator_ms,
        grace_ms: 5_000,
        activation_ms: 5_000
      ],
      extra
    )
  end

  defp ingress_opts(ctx, extra \\ []) do
    me = self()

    Keyword.merge(
      [
        authenticate: fn input ->
          send(me, {:authenticate, input})
          Principal.new(@tenant, "caller", :sandbox, [:dispatch])
        end,
        context: %Context{mode: :sandbox, store: {MemoryStore, ctx.routes}, revisions: Revision},
        bindings: ctx.bindings,
        route_id: "chat",
        max_attempts: 1,
        deadline_ceiling_ms: 60_000,
        max_body_bytes: 4096,
        clock: {Telelem.Instance.SystemClock, nil}
      ],
      extra
    )
  end

  defp reader(body) do
    me = self()

    fn max, remaining ->
      send(me, {:read_body, max, remaining})
      {:ok, body}
    end
  end

  defp rev1 do
    Revision.new!(
      route_id: "chat",
      rev: 1,
      deployments: ["a", "b"],
      preferred: "a",
      fallback_on_refusal: false,
      deadline_ms: 30_000,
      max_attempts: 1
    )
  end

  defp bindings!(dir) do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem primary test CA", [])
    pem = :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
    fx = Path.join(dir, "fx")
    sb = Path.join(dir, "sbx")
    store_dir = Path.join(sb, "store")
    File.mkdir_p!(fx)
    File.mkdir_p!(store_dir)
    File.chmod!(store_dir, 0o700)

    doc = %{
      "version" => 2,
      "tenant_id" => @tenant,
      "mode" => "sandbox",
      "fixture_targets" => %{
        "a" => %{"host" => "127.0.0.1", "port" => 1, "ca_pem" => pem},
        "b" => %{"host" => "127.0.0.1", "port" => 2, "ca_pem" => pem}
      },
      "sandbox_store_root" => store_dir,
      "sandbox_signer_ref" => "sandbox:local-fixture"
    }

    {:ok, b} = Bootstrap.from_map(doc, fixture_root: fx, sandbox_root: sb)
    {:ok, p} = Principal.new(@tenant, "caller", :sandbox, [:dispatch])
    {:ok, %VerifiedTarget{} = t} = Telelem.Sandbox.resolve(p, b, "a")
    {b, t}
  end

  defp await_reserve!(auth, h, o) do
    await_mail!(auth, &match?({:"$gen_call", {^h, _}, {:reserve, _, ^o, @units}}, &1))
  end

  # Bounded re-read of `pid`'s mailbox until a message matches; returns the
  # matching messages.
  defp await_mail!(pid, pred, until \\ nil) do
    until = until || now() + @t

    case Enum.filter(mailbox(pid), pred) do
      [] ->
        if now() >= until,
          do: flunk("no matching message in #{inspect(pid)}"),
          else: await_mail!(pid, pred, until)

      found ->
        found
    end
  end

  defp wait_until!(s) do
    left = s - now()

    if left >= 0 do
      receive do
      after
        left + 1 -> wait_until!(s)
      end
    end
  end

  defp drain_send_after(h, acc) do
    receive do
      {:trace_ts, ^h, :call, {:erlang, :send_after, [ms, _dest, msg | opts]}, ts} ->
        at = System.convert_time_unit(ts, :native, :millisecond)
        drain_send_after(h, [{at, ms, msg, List.flatten(opts)} | acc])

      {:trace_ts, _, _, _, _} ->
        drain_send_after(h, acc)
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp pin_set(%Pins{} = p), do: MapSet.new([p.root, p.coordinators, p.guards])

  defp tuples(%Receipt{events: evs}),
    do: for(e <- evs, do: {e.kind, e.attempt, e.outcome, e.exposure})

  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)

  defp resume(p) do
    if Process.alive?(p), do: :sys.resume(p)
  catch
    :exit, _ -> :ok
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
