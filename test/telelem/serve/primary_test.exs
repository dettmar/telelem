defmodule Telelem.Serve.PrimaryTest do
  @moduledoc """
  M2a `Telelem.Serve.Primary`: one `Ingress.prepare/3` (SystemClock), one real
  `Telelem.Handoff` (`coordinator_grant`, `terminal_ack`, receipts into a real
  sandbox `Telelem.Receipts.Store`), one real guarded owner launched by
  `Telelem.Wire.OwnerGuard` under a real `DynamicSupervisor`, and the real
  pinned `Telelem.Admission.Authority` of a `Telelem.Instance`.

  Listener-free: no socket is opened. The instance has fewer units than one
  reservation needs, so the authority **denies the reservation** and the
  GO'd owner ends before it dials (a dial would surface as
  `{:connect_failed, _, _}` to the controller, which every case refutes).
  The fixture targets are `127.0.0.1` ports 1 and 2 and are never dialed.

  Ordering uses `:sys.suspend/1` on the authority (so the reservation request
  can be read from its mailbox while it waits) or on the guard supervisor
  (so the launch expires at its activation limit). The only clock wait is for
  that activation limit to pass. The registration timer is read from an
  isolated `:trace` session on the coordinator's own `erlang:send_after` call.

  Not covered: a dial, the requester's downstream protocol, the deadline
  rechecks immediately before and after `Handoff.start/1` (no deterministic
  way to cross `D` exactly there without a hook), and the unconfirmed launch
  path end to end (OwnerGuard raises nothing after a passing preflight; its
  classification is tested directly).

      scripts/mix test test/telelem/serve/primary_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.Authority
  alias Telelem.{Handoff, Instance}
  alias Telelem.Ops.{Context, MemoryStore}
  alias Telelem.Receipts.{Receipt, Store}
  alias Telelem.Route.Revision
  alias Telelem.Sandbox.{Bootstrap, Principal, VerifiedTarget}
  alias Telelem.Serve.{Ingress, Primary}
  alias Telelem.Serve.Primary.Handle
  alias Telelem.Wire.{Limits, OwnerGuard}

  @moduletag :tmp_dir
  @t 2_000
  @units 4
  # Fewer than @units: every reservation is refused.
  @instance_units 2
  @auth "AUTH-MARKER-3d9a"
  @body ~s({"marker":"BODY-MARKER-71c4"})
  @tenant "local"
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
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :guard_sup)
    store = start_supervised!({Store, mode: :sandbox}, id: :receipts)
    routes = start_supervised!(MemoryStore, id: :routes)
    :ok = MemoryStore.seed(routes, @tenant, rev1())

    on_exit(fn ->
      for p <- [auth, sup] do
        try do
          if Process.alive?(p), do: :sys.resume(p)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    {bindings, target} = bindings!(dir)

    %{
      inst: inst,
      auth: auth,
      sup: sup,
      store: store,
      routes: routes,
      bindings: bindings,
      target: target
    }
  end

  # ── setup pins ──────────────────────────────────────────────────────────

  test "config pins a SystemClock ingress, a live Authority, the canonical sandbox store and a DynamicSupervisor",
       ctx do
    good = cfg_opts(ctx)
    assert {:ok, %Primary.Config{} = cfg} = Primary.config(good)
    assert cfg.authority == ctx.auth and cfg.store == ctx.store and cfg.supervisor == ctx.sup
    assert cfg.producer == Store.producer(ctx.store)

    shown = inspect(cfg)
    for s <- [@auth, @body, "127.0.0.1"], do: refute(shown =~ s)

    manual = start_supervised!(Telelem.Instance.ManualClock)

    {:ok, manual_ingress} =
      Ingress.config(ingress_opts(ctx, clock: {Telelem.Instance.ManualClock, manual}))

    serving = start_supervised!({Store, mode: :serving}, id: :serving_store)
    wide = start_supervised!({Store, mode: :sandbox, max_pending: 257}, id: :wide_store)
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
      {Keyword.put(good, :supervisor, idle()), :supervisor},
      {Keyword.put(good, :units, 0), :units},
      {Keyword.put(good, :limits, %{}), :limits},
      {Keyword.put(good, :grace_ms, 0), :grace_ms},
      {Keyword.put(good, :activation_ms, OwnerGuard.bounds().activation_ms + 1), :activation_ms},
      {Keyword.delete(good, :store), :store},
      {good ++ [extra: 1], :extra},
      {good ++ [units: @units], :config},
      {[{"units", 1} | good], :config}
    ]

    for {opts, field} <- bad do
      assert Primary.config(opts) == {:error, {:invalid, field}}, inspect(field)
    end

    assert Primary.config(Map.put(Map.new(good), :extra, 1)) == {:error, {:invalid, :extra}}
    assert Primary.start(:not_a_config, @auth, reader(@body)) == {:error, :invalid_config}
    assert Primary.start(%{cfg | units: 0}, @auth, reader(@body)) == {:error, :invalid_config}

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

  test "GO sent: exact primary with ack/receipts and one D; the denied reservation ends the owner before any dial; receipt and exact witness",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    :ok = :sys.suspend(ctx.auth)

    assert {:ok, %Handle{handoff: h, guard: g, owner: o, witness: w, ref: ref} = handle} =
             Primary.start(cfg, @auth, reader(@body))

    wm = Process.monitor(w)
    d = handle.deadline_ms

    # Prepared exactly once.
    assert_received {:authenticate, @auth}
    assert_received {:read_body, _, _}
    refute_received {:authenticate, _}
    refute_received {:read_body, _, _}

    # The owner registered itself and the reservation is for exactly it.
    await_reserve!(ctx.auth, h, o)

    assert %{attempts: %{primary: %{pid: ^o}}, grant: :reserving, receipts: :recording} =
             Handoff.status(h)

    # One D everywhere: coordinator deadline, gate and terminal deadline.
    hs = :sys.get_state(h)
    assert hs.terminal_deadline == d and hs.terminal_ack and hs.order == [:primary]
    assert hs.authority == ctx.auth and hs.requester == self()
    assert %{units: @units} = hs.grant

    # No body, auth input or target in the coordinator, the guard's child
    # spec, the handle or the launcher's mailbox.
    for term <- [hs, :sys.get_state(ctx.sup), handle, mailbox(self())],
        s <- [@auth, @body, ctx.target, ctx.target.host] do
      refute contains?(term, s), inspect(s)
    end

    assert [{_, ^g, :worker, _}] = DynamicSupervisor.which_children(ctx.sup)

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

    # Receipt: complete, no dispatch, exposure none.
    assert {:ok, %Receipt{} = r} = Store.lookup(ctx.store, @reader, handle.request_id)

    assert %{completeness: :complete, gaps: [], observed_outcome: :failed, exposure: :none} = r
    assert [{:ingress, 0, :none, :none}, {:terminal, 0, :failed, :none}] = tuples(r)
    for s <- [@auth, @body, ctx.target.host], do: refute(contains?(r, s))

    # Nothing reserved, nothing dialed, nothing else sent here.
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
    refute_received {:telelem_wire, _, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_owner_witness, _, _, _}
  end

  test "the coordinator's registration timer fires no earlier than D; its terminal timer is at D",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    :ok = :sys.suspend(ctx.auth)

    session = :trace.session_create(:serve_primary, self(), [])
    on_exit(fn -> :trace.session_destroy(session) end)
    # Only processes this test spawns from here on (the coordinator included).
    1 = :trace.process(session, self(), true, [:call, :set_on_spawn, :monotonic_timestamp])
    _ = :trace.function(session, {:erlang, :send_after, :_}, true, [:global])

    assert {:ok, %Handle{handoff: h, deadline_ms: d}} = Primary.start(cfg, @auth, reader(@body))
    1 = :trace.process(session, self(), false, [:call, :set_on_spawn])
    tref = :trace.delivered(session, h)
    assert_receive {:trace_delivered, ^h, ^tref}, @t

    calls = drain_send_after(h, [])
    assert [{at, ms, _, []}] = for({_, _, {:register_deadline, _}, _} = c <- calls, do: c)
    assert ms >= 1 and at + ms >= d, inspect({at, ms, d})
    assert [{_, ^d, _, [abs: true]}] = for({_, _, {:terminal_deadline, _}, _} = c <- calls, do: c)
    :trace.session_destroy(session)
    :ok = :sys.resume(ctx.auth)
    assert_receive {:telelem_handoff, ^h, {:closed, _}}, @t
  end

  # ── ordinary preflight and startup failures ─────────────────────────────

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
    for s <- [@auth, @body, ctx.target.host], do: refute(contains?(res, s))
    assert_received {:read_body, _, _}
    nothing_started!(ctx)
  end

  test "coordinator start failure (pinned store gone): bounded reason, nothing launched", ctx do
    store = start_supervised!({Store, mode: :sandbox}, id: :doomed_store)
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :store, store))
    :ok = stop_supervised(:doomed_store)

    assert Primary.start(cfg, @auth, reader(@body)) ==
             {:error, {:handoff_start, {:bad_option, :receipts}}}

    nothing_started!(ctx)
  end

  test "launch failure after a confirmed coordinator start (guard supervisor gone): GO not sent, one cancel, evidence kept",
       ctx do
    gs = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :doomed_sup)
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :supervisor, gs))
    :ok = stop_supervised(:doomed_sup)

    assert {:error, {:launch, :start_failed, failure}} =
             res = Primary.start(cfg, @auth, reader(@body))

    assert %{go: :not_sent, cancel: :ok, handoff: h, request_id: id} = failure
    assert map_size(failure) == 4
    launch_failed!(ctx, res, h, id)

    # The witness's report carries a ref this API never returned.
    assert_receive {:telelem_owner_witness, wref, :activation_never_released}, @t
    refute contains?(res, wref)
  end

  test "launch expiry after a confirmed coordinator start (busy supervisor): GO not sent, one cancel, no owner",
       ctx do
    act = 50
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :activation_ms, act))
    :ok = :sys.suspend(ctx.sup)

    assert {:error,
            {:launch, :expired, %{go: :not_sent, cancel: :ok, handoff: h, request_id: id}}} =
             res = Primary.start(cfg, @auth, reader(@body))

    launch_failed!(ctx, res, h, id)
    assert_receive {:telelem_owner_witness, wref, :activation_never_released}, @t
    refute contains?(res, wref)

    # The queued child starts late and does no work.
    :ok = :sys.resume(ctx.sup)
    assert DynamicSupervisor.which_children(ctx.sup) == []
    refute_received {:telelem_owner_guard, _, _, _, _}
    refute_received {:telelem_wire, _, _, _}
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

  # ── helpers ─────────────────────────────────────────────────────────────

  # After a launch failure: the coordinator ended the request on the one
  # cancel; its messages stayed in the requester's mailbox; the receipt is
  # complete with no dispatch; nothing was reserved or dialed; no guard or
  # owner report can be attributed.
  defp launch_failed!(ctx, res, h, id) do
    assert {:error, {:launch, _, _}} = res
    for s <- [@auth, @body, ctx.target, ctx.target.host], do: refute(contains?(res, s))

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
    assert DynamicSupervisor.which_children(ctx.sup) == []
    assert %{pending: 0, retained: 0, receipts: 0} = Store.stats(ctx.store)
    assert Authority.snapshot(ctx.auth).grants == %{}
    refute_received {:telelem_handoff, _, _}
    refute_received {:telelem_owner_witness, _, _}
    refute_received {:telelem_owner_witness, _, _, _}
    refute_received {:telelem_wire, _, _, _}
  end

  defp cfg_opts(ctx, extra \\ []) do
    {:ok, ingress} = Ingress.config(ingress_opts(ctx))

    Keyword.merge(
      [
        ingress: ingress,
        authority: ctx.auth,
        store: ctx.store,
        supervisor: ctx.sup,
        units: @units,
        limits: Limits.new(),
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

  defp await_reserve!(auth, h, o, until \\ nil) do
    until = until || now() + @t
    msgs = mailbox(auth)

    cond do
      Enum.any?(msgs, &match?({:"$gen_call", {^h, _}, {:reserve, _, ^o, @units}}, &1)) -> :ok
      now() >= until -> flunk("no reservation for the owner: #{inspect(msgs)}")
      true -> await_reserve!(auth, h, o, until)
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

  defp tuples(%Receipt{events: evs}),
    do: for(e <- evs, do: {e.kind, e.attempt, e.outcome, e.exposure})

  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)

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
