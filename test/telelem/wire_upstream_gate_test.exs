defmodule Telelem.WireUpstreamGateTest do
  @moduledoc """
  The opt-in gated `Telelem.Wire.Upstream` (`gate: [target:, deadline:]`).

  Listener-free: no socket is opened by any test here. The target is a
  `VerifiedTarget` for `127.0.0.1` port 1 with a throwaway test CA; every
  case ends before the attempt dials (a dial would surface as a
  `{:connect_failed, _, _}` message, which each case refutes).

  What this covers: the closed option interface (refused in the caller, no
  process spawned) including a duplicate `:gate`, the gated numeric limit
  ranges and Handoff's shared deadline bound, the grant-specific attach (`Handoff.attach_grant/3`: a typed
  `:grant_required` refusal from a coordinator without `coordinator_grant`,
  `[:primary]` included, with nothing changed), legacy defaults and ordinary
  attach, the pure timeout clamps and the
  controller's sanitized vocabulary, and pre-dial denial by a real
  `Telelem.Handoff` in `coordinator_grant` mode with its real pinned
  `Telelem.Admission.Authority` (ledger and receive trace are the evidence).
  Ordering uses `:sys.suspend/1` on the authority or coordinator and
  `:erlang.suspend_process/1` on the attempt, asserted from actual mailboxes
  (re-read until the expected message is there, bounded). No sleeps.

  What it does not cover: TLS/socket behaviour, the post-TLS and pre-request
  checks (they need a handshake), any SDK, hard expiry, blocked teardown or
  slot quiescence.

      scripts/mix test test/telelem/wire_upstream_gate_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.{Authority, Ledger}
  alias Telelem.{Handoff, Instance}
  alias Telelem.Instance.Directory
  alias Telelem.Sandbox.VerifiedTarget
  alias Telelem.Wire.{Limits, Upstream}

  @moduletag :tmp_dir
  @t 1_000
  @window 300
  @limit 10
  @units 4

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        id: :wire_upstream_gate_gw,
        limits: %{slots: 2, units: @limit},
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

    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem gate test CA", [])
    {:ok, target} = VerifiedTarget.build("a", "127.0.0.1", 1, [der])
    %{inst: inst, auth: auth, tag: inst.tag, target: target, der: der}
  end

  # ── interface ───────────────────────────────────────────────────────────

  test "the gate interface is closed and validated in the caller; nothing is spawned", ctx do
    d = now() + 60_000
    h = idle()
    t = ctx.target
    {:ok, v6} = VerifiedTarget.build("a", "::1", 1, [ctx.der])
    l = Limits.new()

    good = [
      handoff: h,
      id: :primary,
      controller: self(),
      body: "{}",
      limits: l,
      gate: [target: t, deadline: d]
    ]

    bad = [
      lane:
        Keyword.put(good, :lane, %{name: "a", host: "127.0.0.1", port: 1, cacerts: [ctx.der]}),
      recorder: Keyword.put(good, :recorder, self()),
      rid: Keyword.put(good, :rid, "r1"),
      path: Keyword.put(good, :path, "/v1/other"),
      unknown: Keyword.put(good, :transport_opts, verify: :verify_none),
      duplicate: good ++ [body: "{}"],
      leading_nil_gate: [{:gate, nil} | good],
      trailing_nil_gate: good ++ [gate: nil],
      two_gates: good ++ [gate: [target: t, deadline: d]],
      two_nil_gates_only: [handoff: h, id: :primary, gate: nil, gate: nil],
      missing_body: Keyword.delete(good, :body),
      missing_controller: Keyword.delete(good, :controller),
      gate_map: Keyword.put(good, :gate, %{target: t, deadline: d}),
      gate_extra: Keyword.put(good, :gate, target: t, deadline: d, cacertfile: "/x"),
      gate_no_target: Keyword.put(good, :gate, deadline: d),
      gate_float_deadline: Keyword.put(good, :gate, target: t, deadline: d * 1.0),
      plain_map_target: Keyword.put(good, :gate, target: Map.from_struct(t), deadline: d),
      tampered_port: Keyword.put(good, :gate, target: %{t | port: 0}, deadline: d),
      tampered_host: Keyword.put(good, :gate, target: %{t | host: "127.0.0.2"}, deadline: d),
      tampered_digest: Keyword.put(good, :gate, target: %{t | ca_sha256: ["00"]}, deadline: d),
      ipv6: Keyword.put(good, :gate, target: v6, deadline: d),
      other_deadline: Keyword.put(good, :terminal_deadline, d + 1),
      # Beyond Handoff's shared bound (2^32 - 1 - grace ms ahead), with 60 s
      # of margin so the test's own runtime cannot bring it back in range.
      distant_deadline:
        Keyword.put(good, :gate,
          target: t,
          deadline: now() + Handoff.max_deadline_ahead_ms() + 60_000
        ),
      overflow_deadline: Keyword.put(good, :gate, target: t, deadline: now() + 4_294_967_295),
      huge_deadline: Keyword.put(good, :gate, target: t, deadline: now() + 1_000_000_000_000_000),
      handoff_not_pid: Keyword.put(good, :handoff, :name),
      controller_not_pid: Keyword.put(good, :controller, nil),
      limits_map: Keyword.put(good, :limits, Map.from_struct(l)),
      limits_inconsistent: Keyword.put(good, :limits, %{l | high_water: l.hard_cap}),
      zero_connect_timeout: Keyword.put(good, :limits, %{l | connect_timeout_ms: 0}),
      body_too_large:
        good |> Keyword.put(:limits, %{l | max_request_body: 2}) |> Keyword.put(:body, "{ }"),
      body_not_binary: Keyword.put(good, :body, ~c"{}")
    ]

    {:links, links} = Process.info(self(), :links)

    for {name, opts} <- bad do
      e = assert_raise ArgumentError, fn -> Upstream.start_link(opts) end
      assert String.starts_with?(e.message, "Upstream gate: "), "#{name}: #{e.message}"
      # Field named, value never copied.
      for secret <- [t.host, hd(t.ca_sha256), "/x", "/v1/other", "r1"],
          do: refute(String.contains?(e.message, secret), "#{name}: #{e.message}")
    end

    assert {:links, ^links} = Process.info(self(), :links)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "a duplicate :gate is refused before mode selection, with the gate's message", ctx do
    d = now() + 60_000
    gate = [target: ctx.target, deadline: d]

    # Each would otherwise select legacy (first gate: nil) or one of two gates.
    for opts <- [
          [gate: nil, gate: gate],
          [gate: nil, gate: nil],
          [gate: gate, gate: nil],
          [gate: gate, gate: gate],
          [gate: nil, terminal_deadline: :soon, gate: nil]
        ] do
      e = assert_raise ArgumentError, fn -> Upstream.start_link(opts) end
      assert e.message == "Upstream gate: :gate must be given at most once", inspect(opts)
    end
  end

  test "every numeric limit of a gated attempt is range-checked before spawn; defaults pass",
       ctx do
    d = now() + 60_000
    h = idle()
    l = Limits.new()
    ranges = Upstream.gate_limit_ranges()

    # Every numeric field of Limits is covered, and every default is in range.
    numeric = for {k, v} <- Map.from_struct(l), is_integer(v), do: k
    assert Enum.sort(numeric) == Enum.sort(Keyword.keys(ranges))
    for {k, max} <- ranges, do: assert(Map.fetch!(l, k) in 1..max, "default #{k}")

    # The legacy defaults themselves are unchanged.
    assert Map.from_struct(l) == %{
             window: 65_536,
             max_event: 65_536,
             high_water: 131_072,
             hard_cap: 262_144,
             overflow: :abort,
             cleanup_timeout_ms: 500,
             cleanup_deadline_ms: 1_000,
             connect_timeout_ms: 2_000,
             register_timeout_ms: 2_000,
             response_timeout_ms: 30_000,
             max_request_body: 1_048_576,
             upstream_recbuf: 16_384,
             upstream_read_buffer: 16_384,
             downstream_sndbuf: 65_536
           }

    good = [
      handoff: h,
      id: :primary,
      controller: self(),
      body: "{}",
      limits: l,
      gate: [target: ctx.target, deadline: d]
    ]

    {:links, links} = Process.info(self(), :links)

    for {k, max} <- ranges, v <- [0, -1, max + 1, 1.0 * Map.fetch!(l, k), :x, nil, "1"] do
      opts = Keyword.put(good, :limits, Map.put(l, k, v))
      e = assert_raise ArgumentError, fn -> Upstream.start_link(opts) end
      assert e.message == "Upstream gate: :limits #{k} must be an integer in 1..#{max}"
    end

    # A missing field is refused too (as nil).
    e =
      assert_raise ArgumentError, fn ->
        Upstream.start_link(Keyword.put(good, :limits, Map.delete(l, :window)))
      end

    assert e.message =~ ":limits window must be an integer"

    assert {:links, ^links} = Process.info(self(), :links)
  end

  test "upper-bound limits are accepted and the attempt still stops at the grant check", ctx do
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:primary])
    on_exit(fn -> Process.exit(h, :kill) end)
    ranges = Map.new(Upstream.gate_limit_ranges())

    l =
      Limits.new(
        connect_timeout_ms: ranges.connect_timeout_ms,
        response_timeout_ms: ranges.response_timeout_ms,
        hard_cap: ranges.hard_cap,
        max_request_body: ranges.max_request_body
      )

    {:ok, target} = VerifiedTarget.build("a", "127.0.0.1", 1, [ctx.der])

    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :primary,
        controller: self(),
        body: "{}",
        limits: l,
        gate: [target: target, deadline: now() + 60_000]
      )

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:attach, :error, :grant_required}, _}},
                   @t

    refute_received {:telelem_wire, ^up, _, {:connect_failed, _, _}}
  end

  test "a gate deadline at the shared bound, or already past, spawns; attach_grant's wait does not overflow",
       ctx do
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:primary])
    on_exit(fn -> Process.exit(h, :kill) end)

    # The test's `now` is not later than the caller's check, so `at` is at
    # most the bound ahead there and inside the attempt.
    for d <- [now() + Handoff.max_deadline_ahead_ms(), now() - 1] do
      {:ok, up} =
        Upstream.start_link(
          handoff: h,
          id: :primary,
          controller: self(),
          body: "{}",
          limits: Limits.new(),
          gate: [target: ctx.target, deadline: d]
        )

      assert_receive {:telelem_wire, ^up, :primary,
                      {:ended, {:attach, :error, :grant_required}, _}},
                     @t

      refute_received {:telelem_wire, ^up, _, {:connect_failed, _, _}}
    end
  end

  test "legacy deadlines are not range-checked: a distant legacy :terminal_deadline still spawns",
       ctx do
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:other])
    on_exit(fn -> Process.exit(h, :kill) end)
    lane = %{name: "a", host: "127.0.0.1", port: 1, cacerts: [ctx.der]}

    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :primary,
        controller: self(),
        lane: lane,
        body: "{}",
        limits: Limits.new(),
        terminal_deadline: now() + 1_000_000_000_000_000
      )

    mon = Process.monitor(up)
    # Attach refused (unknown id): legacy ends silently, before any dial.
    assert_receive {:DOWN, ^mon, :process, _, :normal}, @t
    refute_received {:telelem_wire, ^up, _, _}
  end

  test "legacy limits are not range-checked: a legacy attempt still spawns with them", ctx do
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:other])
    on_exit(fn -> Process.exit(h, :kill) end)
    lane = %{name: "a", host: "127.0.0.1", port: 1, cacerts: [ctx.der]}

    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :primary,
        controller: self(),
        lane: lane,
        body: "{}",
        limits: %{Limits.new() | connect_timeout_ms: 0, upstream_recbuf: 1.5}
      )

    mon = Process.monitor(up)
    # Attach refused (unknown id): legacy ends silently, before any dial.
    assert_receive {:DOWN, ^mon, :process, _, :normal}, @t
    refute_received {:telelem_wire, ^up, _, _}
  end

  # ── grant-specific attach ───────────────────────────────────────────────

  test "attach_grant/3 on a coordinator without coordinator_grant: typed refusal, nothing changed" do
    req = idle()

    {:ok, h} =
      Handoff.start(requester: req, attempts: [:primary], trace: self(), register_timeout: 60_000)

    on_exit(fn -> Process.exit(h, :kill) end)
    d = now() + 60_000
    before = Handoff.status(h)

    assert Handoff.attach_grant(h, :primary, deadline: d) == {:error, :grant_required}
    # Mode first: also for an unknown id.
    assert Handoff.attach_grant(h, :other, deadline: d) == {:error, :grant_required}

    assert Handoff.status(h) == before
    assert %{phase: :open, outcome: nil, attempts: %{primary: %{pid: nil}}} = before
    refute_received {:telelem_handoff_trace, ^h, _}
    refute_received {:DOWN, _, :process, ^h, _}

    # The deadline is required in the caller, before any call.
    assert_raise ArgumentError, fn -> Handoff.attach_grant(h, :primary, []) end
    assert_raise ArgumentError, fn -> Handoff.attach_grant(h, :primary, deadline: :soon) end

    # Ordinary attach is unchanged for legacy callers.
    assert {:ok, mref} = Handoff.attach(h, :primary)
    assert is_reference(mref)
    assert_receive {:telelem_handoff_trace, ^h, {:attached, :primary}}, @t
    me = self()
    assert %{attempts: %{primary: %{pid: ^me}}} = Handoff.status(h)
  end

  test "a gated attempt on a legacy [:primary] coordinator ends with a typed refusal before any dial",
       ctx do
    req = idle()

    {:ok, h} =
      Handoff.start(requester: req, attempts: [:primary], trace: self(), register_timeout: 60_000)

    on_exit(fn -> Process.exit(h, :kill) end)
    d = now() + 60_000
    {:ok, target} = VerifiedTarget.build("a", "127.0.0.1", 1, [ctx.der])

    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :primary,
        controller: self(),
        body: "{}",
        limits: Limits.new(),
        gate: [target: target, deadline: d]
      )

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:attach, :error, :grant_required}, stats}},
                   @t

    # Not registered, so never acknowledged and never dispatched.
    assert %{claim: :none, cleaned_ack: :not_sent, unsent_pending: 0} = stats
    refute_received {:telelem_wire, ^up, _, {:connect_failed, _, _}}

    assert %{
             phase: :open,
             outcome: nil,
             attempts: %{primary: %{pid: nil, dispatched: false, cleanup: nil}}
           } = Handoff.status(h)

    # The status reply follows any trace h sent before it: there was none.
    refute_received {:telelem_handoff_trace, ^h, _}
  end

  test "attach_grant/3 on a coordinator_grant coordinator reserves for the caller only", ctx do
    r = start_h!(ctx)
    %{h: h, d: d} = r

    # Same checks as attach/3 with deadline: a wrong deadline changes nothing.
    assert Handoff.attach_grant(h, :primary, deadline: d + 1) == {:error, :deadline_required}
    assert %{grant: :idle, attempts: %{primary: %{pid: nil}}} = Handoff.status(h)
    assert Authority.snapshot(ctx.auth).grants == %{}

    me = self()
    assert {:ok, _} = Handoff.attach_grant(h, :primary, deadline: d)
    assert %{grant: :reserved, attempts: %{primary: %{pid: ^me}}} = Handoff.status(h)
    {_gid, g} = only_grant!(ctx.auth)
    assert %{owner: ^me, state: :reserved, units: @units} = g
  end

  test "legacy defaults are unchanged: no gate, or gate: nil", ctx do
    # The legacy validation and its message.
    for gate <- [[], [gate: nil]] do
      e =
        assert_raise ArgumentError, fn ->
          Upstream.start_link(gate ++ [terminal_deadline: :soon])
        end

      assert e.message == "Upstream :terminal_deadline must be an integer or nil, got: :soon"
    end

    # A legacy attempt whose attach is refused tells the controller nothing.
    {:ok, h} = Handoff.start(requester: idle(), attempts: [:other])
    on_exit(fn -> Process.exit(h, :kill) end)

    lane = %{name: "a", host: "127.0.0.1", port: 1, cacerts: [ctx.der]}

    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :primary,
        controller: self(),
        lane: lane,
        body: "{}",
        limits: Limits.new()
      )

    mon = Process.monitor(up)
    assert_receive {:DOWN, ^mon, :process, _, :normal}, @t
    refute_received {:telelem_wire, ^up, _, _}
  end

  test "a gated attempt whose attach is refused reports one sanitized end and never acknowledges",
       ctx do
    # A coordinator_grant coordinator (attempts: [:primary]) and an id it
    # does not know: refused by the grant-specific attach, nothing reserved.
    %{h: h, d: d} = start_h!(ctx)

    # terminal_deadline equal to the gate's is accepted.
    {:ok, up} =
      Upstream.start_link(
        handoff: h,
        id: :other,
        controller: self(),
        body: "{}",
        limits: Limits.new(),
        gate: [target: ctx.target, deadline: d],
        terminal_deadline: d
      )

    assert_receive {:telelem_wire, ^up, :other,
                    {:ended, {:attach, :error, :unknown_attempt}, stats}},
                   @t

    assert %{claim: :none, cleaned_ack: :not_sent, unsent_pending: 0} = stats
    refute_received {:telelem_wire, ^up, _, {:connect_failed, _, _}}
    assert %{grant: :idle, attempts: %{primary: %{pid: nil}}} = Handoff.status(h)
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  # ── timing and vocabulary (pure) ────────────────────────────────────────

  test "supported clamps: connect gets half the time left, waits get at most the time left" do
    assert Upstream.connect_clamp(2_000, 10_000, 0) == 2_000
    assert Upstream.connect_clamp(2_000, 1_000, 0) == 500
    assert Upstream.connect_clamp(2_000, 1_001, 0) == 500
    # Below 1 ms the attempt does not dial.
    assert Upstream.connect_clamp(2_000, 1, 0) == 0
    assert Upstream.connect_clamp(2_000, 5, 5) == 0
    assert Upstream.connect_clamp(2_000, 0, 5) == 0

    # Credit wait (legacy :infinity) and upstream wait.
    assert Upstream.wait_clamp(:infinity, 700, 200) == 500
    assert Upstream.wait_clamp(30_000, 700, 200) == 500
    assert Upstream.wait_clamp(100, 700, 200) == 100
    assert Upstream.wait_clamp(:infinity, 0, 5) == 0
    assert Upstream.wait_clamp(100, 0, 5) == 0
  end

  test "controller reasons keep only fixed atoms and status codes" do
    secret = "sk-test-SECRET-marker"

    cases = [
      {{:upstream_status, 500, secret}, {:upstream_status, 500}},
      {{:upstream_status, 999, secret}, {:upstream_status, :other}},
      {{:upstream_error_event, secret}, :upstream_error_event},
      {{:upstream_broken, {:tls_alert, {:x, ~c"#{secret}"}}, %{done_held: true}},
       :upstream_broken},
      {{:dispatch_error, {:closed, secret}}, :dispatch_error},
      {{:handoff_down, {:shutdown, secret}}, :handoff_down},
      {{:upstream_timeout, 30_000}, :upstream_timeout},
      {{:cancelled, {:requester_down, {:killed, secret}}}, {:cancelled, :requester_down}},
      {{:cancelled, {:client_gone, secret}}, {:cancelled, :other}},
      {{:cancelled, secret}, {:cancelled, :other}},
      {{:cancelled, :arbitrary_atom}, {:cancelled, :other}},
      {{:cancelled_before_dispatch, :dispatch_deadline},
       {:cancelled_before_dispatch, :dispatch_deadline}},
      {{:dispatch_refused, :unconfirmed}, {:dispatch_refused, :unconfirmed}},
      {{:finish_refused, :cancelled}, {:finish_refused, :cancelled}},
      {{:lost, {:lost_to, secret}}, {:lost, :lost_to}},
      {{:transfer_refused, :timeout}, {:transfer_refused, :timeout}},
      {{:protocol_violation, {:unsupported_framing, {:http_version, secret}}},
       {:protocol_violation, :unsupported_framing}},
      {{:protocol_violation, {:bad_sentinel, 12}}, {:protocol_violation, :bad_sentinel}},
      {{:protocol_violation, secret}, {:protocol_violation, :other}},
      {{:overflow, {:hard_cap, 300_000}}, {:overflow, :hard_cap}},
      {{:overflow, secret}, {:overflow, :other}},
      {{:unsupported_finish_reason, secret}, :unsupported_finish_reason},
      {{:done_without_finish_reason, :no_finish}, :done_without_finish_reason},
      {{:attach, :cancel, {:requester_down, secret}}, {:attach, :cancel, :requester_down}},
      {{:attach, :error, :unconfirmed}, {:attach, :error, :unconfirmed}},
      {{:attach, :error, :grant_required}, {:attach, :error, :grant_required}},
      {{:attach, :other, :x}, :other},
      {{:stopped, :after_attach, {:cancelled, secret}},
       {:stopped, :after_attach, {:cancelled, :other}}},
      {{:stopped, :before_request, :deadline}, {:stopped, :before_request, :deadline}},
      {{:stopped, :elsewhere, :deadline}, :other},
      # OwnerGuard's scoped cooperative stop (guarded owners only).
      {{:stopped, :after_attach, :guard_stop}, {:stopped, :after_attach, :guard_stop}},
      {:guard_stop, :guard_stop},
      {:finished, :finished},
      {:deadline, :deadline},
      {secret, :other},
      {{secret}, :other},
      {{}, :other}
    ]

    for {internal, public} <- cases do
      assert Upstream.public_reason(internal) == public, inspect(internal)
    end
  end

  # ── pre-dial denial by a real coordinator (coordinator_grant) ───────────

  test "deadline while reserving: attach is cancelled, acknowledged :none, nothing dialed; the late reservation is released by the attempt's exit",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx, deadline: now() + @window)
    %{h: h, d: d} = r

    :ok = :sys.suspend(ctx.auth)
    up = up!(ctx, r)
    await_reserve!(ctx.auth, h, up)
    assert now() < d, "setup took longer than the #{@window} ms window"

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:attach, :cancel, :dispatch_deadline}, stats}},
                   max(d - now(), 0) + @t

    assert %{claim: :none, cleaned_ack: :ok} = stats
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :dispatch_deadline}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, report}}}, @t
    assert %{possible_charges: []} = report

    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, up)
    {gid, _} = only_grant!(ctx.auth)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^up, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "requester cancel while reserving: the controller sees only fixed atoms", ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h} = r
    secret = "sk-test-SECRET-marker"

    :ok = :sys.suspend(ctx.auth)
    up = up!(ctx, r)
    await_reserve!(ctx.auth, h, up)

    ref = req_do(r, fn -> Handoff.cancel(h, {:client_gone, secret}) end)
    assert_receive {:req_did, ^ref, :ok}, @t

    assert_receive {:telelem_wire, ^up, :primary, {:ended, reason, stats}}, @t
    assert reason == {:attach, :cancel, :other}
    assert %{claim: :none, cleaned_ack: :ok} = stats
    refute contains?({reason, stats}, secret)

    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, up)
    {gid, _} = only_grant!(ctx.auth)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^up, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "cancel delivered after the reservation, before the dial: the post-attach check stops it",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h} = r
    up = reserved_suspended!(ctx, r)

    # Reserved for exactly this pid while it has not dialed.
    {gid, g} = only_grant!(ctx.auth)
    assert %{owner: ^up, state: :reserved, units: @units} = g

    ref = req_do(r, fn -> Handoff.cancel(h, :client_gone) end)
    assert_receive {:req_did, ^ref, :ok}, @t

    await_mailbox!(
      up,
      &Enum.any?(&1, fn m -> match?({:telelem_handoff, ^h, {:cancel, _}}, m) end)
    )

    true = :erlang.resume_process(up)

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:stopped, :after_attach, {:cancelled, :other}}, stats}},
                   @t

    assert %{claim: :none, cleaned_ack: :ok} = stats
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, %{possible_charges: []}}}}, @t

    owner_gone!(ctx, up)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^up, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "coordinator lost after the reservation, before the dial: the post-attach check stops it",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx)
    %{h: h} = r
    up = reserved_suspended!(ctx, r)
    {gid, _} = only_grant!(ctx.auth)

    Process.exit(h, :kill)

    await_mailbox!(
      up,
      &Enum.any?(&1, fn m -> match?({:DOWN, _, :process, ^h, :killed}, m) end)
    )

    true = :erlang.resume_process(up)

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:stopped, :after_attach, :handoff_down}, stats}},
                   @t

    # The acknowledgement could not reach a dead coordinator.
    assert %{claim: :none, cleaned_ack: :error} = stats

    owner_gone!(ctx, up)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^up, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  test "deadline passed after the reservation, before the dial: the attempt's own clock check stops it",
       ctx do
    trace_receive!(ctx.auth)
    r = start_h!(ctx, deadline: now() + @window)
    %{h: h, d: d} = r
    up = reserved_suspended!(ctx, r)
    assert now() < d, "setup took longer than the #{@window} ms window"
    {gid, _} = only_grant!(ctx.auth)

    # Hold the coordinator so its deadline cannot cancel the attempt first;
    # its own deadline timer in its mailbox is the barrier that d passed.
    :ok = :sys.suspend(h)

    await_mailbox!(
      h,
      &Enum.any?(&1, fn m -> match?({:terminal_deadline, _}, m) end),
      d + @t
    )

    assert now() >= d
    true = :erlang.resume_process(up)

    # The attempt decided on its own clock (no cancel was sent yet) and
    # acknowledges :none.
    await_mailbox!(
      h,
      &Enum.any?(&1, fn m -> match?({:"$gen_call", {^up, _}, {:cleaned, :primary, :none}}, m) end)
    )

    refute Enum.any?(mailbox(up), &match?({:telelem_handoff, ^h, _}, &1))
    :ok = :sys.resume(h)

    assert_receive {:telelem_wire, ^up, :primary,
                    {:ended, {:stopped, :after_attach, :deadline}, stats}},
                   @t

    assert %{claim: :none} = stats
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :dispatch_deadline}}}, @t

    owner_gone!(ctx, up)
    assert_ledger!(ctx, gid, :released, available: @limit)
    assert [{:reserve, _, ^up, @units}] = authority_requests!(ctx.auth, h)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  defp start_h!(ctx, opts \\ []) do
    req = requester()
    d = Keyword.get(opts, :deadline, now() + 60_000)

    {:ok, h} =
      Handoff.start(
        requester: req,
        attempts: [:primary],
        admission_authority: ctx.auth,
        terminal_deadline: d,
        coordinator_grant: {:one_primary, @units},
        cleanup_timeout: 60_000,
        register_timeout: 60_000
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    %{h: h, req: req, d: d}
  end

  defp up!(ctx, r) do
    {:ok, up} =
      Upstream.start_link(
        handoff: r.h,
        id: :primary,
        controller: self(),
        body: "{}",
        limits: Limits.new(),
        gate: [target: ctx.target, deadline: r.d]
      )

    up
  end

  # The attempt is blocked in attach/3 and the reservation is queued at the
  # suspended authority; the attempt is then suspended, the authority
  # resumed, and the attach reply (:ok, reserved) sits unread in its mailbox.
  defp reserved_suspended!(ctx, %{h: h} = r) do
    :ok = :sys.suspend(ctx.auth)
    up = up!(ctx, r)
    await_reserve!(ctx.auth, h, up)
    true = :erlang.suspend_process(up)
    :ok = :sys.resume(ctx.auth)
    await_mailbox!(up, &Enum.any?(&1, fn m -> match?({_, :ok}, m) end))
    assert %{grant: :reserved} = Handoff.status(h)
    up
  end

  defp await_reserve!(auth, h, up) do
    await_mailbox!(
      auth,
      &match?([{:"$gen_call", {^h, _}, {:reserve, _, ^up, @units}}], &1)
    )
  end

  defp owner_gone!(%{tag: tag}, up) do
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^up, [{_gid, {:ok, _}}]}}, @t
  end

  defp only_grant!(auth) do
    assert [{gid, g}] = Map.to_list(Authority.snapshot(auth).grants)
    {gid, g}
  end

  defp assert_ledger!(ctx, gid, state, counts) do
    snap = Authority.snapshot(ctx.auth)
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

  # Requests the traced authority received from coordinator `h`; anything
  # else it received is one of the test's own snapshots.
  defp authority_requests!(auth, h) do
    1 = :erlang.trace(auth, false, [:receive])
    ref = :erlang.trace_delivered(auth)
    assert_receive {:trace_delivered, ^auth, ^ref}, @t
    me = self()
    msgs = traced([])

    for {:"$gen_call", {from, _}, req} <- msgs,
        from != h,
        do: assert({from, req} == {me, :snapshot})

    for {:"$gen_call", {^h, _}, req} <- msgs, do: req
  end

  defp traced(acc) do
    receive do
      {:trace, _, :receive, msg} -> traced([msg | acc])
    after
      0 -> Enum.reverse(acc)
    end
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
