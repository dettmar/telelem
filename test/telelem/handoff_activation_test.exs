defmodule Telelem.HandoffActivationTest do
  @moduledoc """
  W1: the opt-in inactive start of `Telelem.Handoff`
  (`activation_limit: s`, on top of `coordinator_grant`, `terminal_ack: true`
  and `receipts`) and its one activation (`Handoff.activate/2`).

  Listener-free and offline. One `Telelem.Instance` with no workers and its
  real `Telelem.Admission.Authority`, one real `Telelem.Receipts.Store` per
  case, real coordinators, and a scripted fake attempt (no socket: its HTTP
  send is a `{:fake_http, fake, marker}` message to the test). Evidence is
  read through `Store.lookup/3`, `Store.stats/1`, `Authority.snapshot/1`,
  `status/1`, the coordinator's monitors, and isolated `:trace` sessions
  (sends and selected calls, in the coordinator's own order). No hook in
  the product code. Barriers are messages, `:trace.delivered/2` and
  monitors; the only clock wait is "until `s` has passed".

  Interleavings are forced by suspending the coordinator
  (`:erlang.suspend_process/1`) while a request sits in its mailbox. The
  case "check passed before `s`, reply after the caller's own bound" cannot
  be forced without a seam between the coordinator's clock check and its
  reply; it is covered by the two lost-reply cases (the caller abandons its
  request, and the caller dies), which reach the same coordinator state.
  The abandon case sends the documented request term `{:activate, s}`
  through `:gen_server.send_request/2` and abandons it with
  `receive_response(req, 0)`, which is what `activate/2`'s own timeout does
  (the reply alias is deactivated).

      scripts/mix test test/telelem/handoff_activation_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.Authority
  alias Telelem.{Handoff, Instance}
  alias Telelem.Receipts.{Producer, Receipt, Store, Writer}

  @moduletag :tmp_dir
  @t 2_000
  # How far ahead `s` is when a case needs to cross it.
  @window 500
  @units 4
  @tenant "local"
  @dep "a"
  @http_marker "POST /v1/fake (simulated, no socket)"
  @marker "data: [DONE] MARKER-a41c\n\n"
  @chunk "data: {\"x\":\"CHUNK-a41c\"}\n\n"
  @terminal {:upstream_terminal, "TERMINAL-a41c"}

  @reader %{
    tenant_id: @tenant,
    subject_id: "operator",
    mode: :sandbox,
    permissions: [:receipt_read]
  }

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        id: :handoff_activation_gw,
        limits: %{slots: 4, units: 40},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(sup)
    %{auth: Instance.authority(inst), store: start_store!(:store)}
  end

  # ── options ─────────────────────────────────────────────────────────────

  test "opt-in needs receipts (so grant and terminal_ack) and now < s <= D; default unchanged",
       ctx do
    {good, _} = h_opts(ctx, "opts", [])
    d = good[:terminal_deadline]

    for {opts, why} <- [
          {Keyword.put(good, :activation_limit, :soon), :activation_limit},
          {Keyword.put(good, :activation_limit, 1.0e12), :activation_limit},
          {Keyword.put(good, :activation_limit, d + 1), :activation_limit},
          {Keyword.put(good, :activation_limit, now() - 1), :activation_limit},
          {Keyword.delete(good, :receipts), :activation_limit},
          # The receipts option itself needs the grant and terminal_ack.
          {Keyword.delete(good, :coordinator_grant), :receipts},
          {Keyword.delete(good, :terminal_ack), :receipts},
          {Keyword.put(good, :register_timeout, -1), :register_timeout},
          {Keyword.put(good, :register_timeout, :soon), :register_timeout},
          {Keyword.put(good, :register_timeout, 4_294_967_296), :register_timeout}
        ] do
      assert {:error, {:bad_option, ^why}} = Handoff.start(opts), inspect(why)
    end

    # The binding is checked at init, inactive or not.
    other = start_store!(:other)

    assert {:error, {:bad_option, :receipts}} =
             Handoff.start(put_in(good, [:receipts, :producer], Store.producer(other)))

    # s == D is allowed.
    {:ok, h_eq} = Handoff.start(Keyword.put(good, :activation_limit, d))
    on_exit(fn -> Process.exit(h_eq, :kill) end)
    assert %{activation: :inactive} = Handoff.status(h_eq)

    assert %{pending: 0, retained: 0, receipts: 0, rejected: 0} = Store.stats(ctx.store)
    assert Authority.snapshot(ctx.auth).grants == %{}

    # Default (no activation_limit): no activation key, ingress at start.
    {:ok, h0} = Handoff.start(Keyword.delete(good, :activation_limit))
    on_exit(fn -> Process.exit(h0, :kill) end)
    st = Handoff.status(h0)
    refute Map.has_key?(st, :activation)
    assert %{receipts: :recording, grant: :idle} = st
    assert [:ingress] = kinds!(ctx.store, "opts")
    assert {:error, :no_activation} = Handoff.activate(h0, now() + 60_000)
  end

  # ── inactive start, traced ──────────────────────────────────────────────

  test "inactive init publishes, arms, sends and registers nothing; activation checks, then publishes and replies",
       ctx do
    {hopts, r} = h_opts(ctx, "traced", [])
    {session, h} = traced_start!(hopts)
    r = Map.put(r, :h, h)

    st = Handoff.status(h)
    assert %{activation: :inactive, phase: :open, outcome: nil, grant: :idle} = st
    refute Map.has_key?(st, :receipts)

    init = trace_split!(session, h)

    # One binding read, no writer, no publication, no request timer: the one
    # timer is the private lifecycle expiry at exactly s. The only sends are
    # proc_lib's start ack and the status reply.
    assert Enum.count(init, &match?({:call, {Producer, :binding, _}}, &1)) == 1
    refute Enum.any?(init, &match?({:call, {Writer, :open, _}}, &1)), inspect(init)
    refute Enum.any?(init, &match?({:insert, _}, &1)), inspect(init)
    assert [{:timer, :activation_expiry, s}] = for({:timer, _, _} = t <- init, do: t)
    assert s == r.s

    sends = for {:send, msg, to} <- init, do: {msg, to}
    assert length(sends) == 2, inspect(sends)
    assert Enum.any?(sends, &match?({{_tag, %{activation: :inactive}}, _}, &1))
    refute Enum.any?(sends, fn {msg, to} -> forbidden_send?(msg, to, r) end), inspect(sends)

    # Monitors only the requester, the authority and the observer.
    assert monitored(h) == MapSet.new([r.req, ctx.auth, r.col])
    assert Store.stats(ctx.store).receipts == 0
    assert collected(r) == []

    assert :ok = activate!(r)
    act = trace_split!(session, h)
    :trace.session_destroy(session)

    bind = index!(act, &match?({:call, {Producer, :binding, _}}, &1))
    open = index!(act, &match?({:call, {Writer, :open, _}}, &1))
    insert = index!(act, &match?({:insert, :ingress}, &1))
    # The decision's clock read: the first one after the writer was prepared.
    check = open + 1 + index!(Enum.drop(act, open + 1), &(&1 == :clock))
    reply = index!(act, &match?({:send, {_tag, :ok}, _}, &1))
    cancel = index!(act, &match?({:call, {:erlang, :cancel_timer, _}}, &1))
    timers = for {{:timer, tag, _}, i} <- Enum.with_index(act), do: {tag, i}

    assert bind < open and open < check and check < insert and insert < reply, inspect(act)
    # The expiry is cancelled, then the request timers are armed, after the
    # publication and before the reply.
    assert Enum.count(act, &match?({:call, {:erlang, :cancel_timer, _}}, &1)) == 1
    assert insert < cancel and cancel < reply, inspect(act)
    assert [{:register_deadline, i1}, {:terminal_deadline, i2}] = timers
    assert cancel < i1 and i2 < reply, inspect(act)

    # Nothing is sent between the prepared writer and the publication.
    refute Enum.any?(Enum.slice(act, open..insert), &match?({:send, _, _}, &1)), inspect(act)
    assert Enum.count(act, &match?({:insert, _}, &1)) == 1

    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "traced")
    assert %{activation: :active, receipts: :recording, grant: :idle} = Handoff.status(h)
    assert [{:receipt, :ingress, :ok}, :activated] = collected(r)
  end

  # ── the gate ────────────────────────────────────────────────────────────

  test "inactive: every call but status and the exact requester's activation is refused, every info ignored",
       ctx do
    r = start_inactive!(ctx, "gate", register_timeout: 0)
    %{h: h, d: d, s: s} = r
    mons = monitored(h)

    attempt_calls = [
      &Handoff.attach(&1, :primary),
      &Handoff.attach(&1, :primary, deadline: d),
      &Handoff.attach_grant(&1, :primary, deadline: d),
      &Handoff.begin_dispatch(&1, :primary),
      &Handoff.begin_dispatch(&1, :primary, deadline: d),
      &Handoff.ready(&1, :primary),
      &Handoff.transfer(&1, @chunk),
      &Handoff.finish(&1, @terminal, @marker),
      &Handoff.finish(&1, @terminal, @marker, deadline: d),
      &Handoff.cleaned(&1, :primary, :none),
      &Handoff.cleaned(&1, :primary, :possible),
      &Handoff.cancel(&1, :other),
      &Handoff.terminal_written(&1, :ok)
    ]

    for call <- attempt_calls, do: assert({:error, :inactive} = in_proc(fn -> call.(h) end))

    # The requester's own calls other than activation, too.
    for call <- [
          &Handoff.cancel(&1, :gone),
          &Handoff.terminal_written(&1, :ok),
          &Handoff.terminal_written(&1, {:error, :closed})
        ] do
      assert {:error, :inactive} = req_call!(r, fn -> call.(h) end)
    end

    # A raw call of an unknown shape is refused the same way.
    assert {:error, :inactive} = GenServer.call(h, {:no_such_call, @marker})

    # Not the requester, or not exactly its limit: refused, no change.
    assert {:error, :not_requester} = Handoff.activate(h, s)
    assert {:error, :not_requester} = in_proc(fn -> Handoff.activate(h, s) end)

    assert {:error, :activation_limit_mismatch} =
             req_call!(r, fn -> Handoff.activate(h, s + 1) end)

    assert {:error, :activation_limit_mismatch} =
             req_call!(r, fn -> Handoff.activate(h, s - 1) end)

    # Every info is ignored, with the real timer token, too.
    token = :sys.get_state(h).register_token

    for msg <- [
          {:register_deadline, token},
          {:terminal_deadline, token},
          {:cleanup_deadline, :primary, token},
          # Not the expiry's own token: ignored.
          {:activation_expiry, token},
          {:activation_expiry, make_ref()},
          {:telelem_terminal_grant, ctx.auth, make_ref(), 1},
          {:DOWN, make_ref(), :process, self(), :boom},
          {:EXIT, self(), :boom},
          {make_ref(), {:ok, {:grant, 1}}},
          :junk
        ] do
      send(h, msg)
    end

    assert %{
             activation: :inactive,
             phase: :open,
             outcome: nil,
             winner: nil,
             grant: :idle,
             terminal: nil,
             authority: :up,
             attempts: %{primary: %{pid: nil, cleanup: nil, charge: nil, dispatched: false}}
           } = Handoff.status(h)

    assert monitored(h) == mons
    assert_quiet!(r)
    assert Store.stats(ctx.store).receipts == 0
    assert Authority.snapshot(ctx.auth).grants == %{}

    # Still activatable; the registration timer (0 ms) counts from here.
    assert :ok = activate!(r)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :no_winner}}}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, _}}}, @t
    assert [:ingress, :terminal] = kinds!(ctx.store, "gate")
  end

  test ":sys.get_status of an inactive coordinator shows the category only", ctx do
    r = start_inactive!(ctx, "redact-" <> "a41c")

    {:status, _, _, [_, _, _, _, [header: _, data: _, data: [{~c"State", state}]]]} =
      :sys.get_status(r.h)

    assert %{
             activation: :inactive,
             receipts: nil,
             grant: %{phase: :idle, request?: false},
             phase: :open,
             outcome: nil
           } = state

    shown = inspect(state, limit: :infinity)
    refute shown =~ "a41c"
    refute shown =~ Integer.to_string(r.s)
    refute shown =~ inspect(r.store)

    assert %{message: {:call, :activate}} =
             Handoff.format_status(%{
               message: {:"$gen_call", {self(), make_ref()}, {:activate, 1}}
             })

    assert :ok = activate!(r)

    {:status, _, _, [_, _, _, _, [header: _, data: _, data: [{~c"State", state}]]]} =
      :sys.get_status(r.h)

    assert %{activation: :active, receipts: %{state: :recording}} = state
  end

  # ── silent lifecycle exits while inactive ───────────────────────────────

  test "requester exit while inactive: the coordinator stops :normal, silently", ctx do
    r = start_inactive!(ctx, "req-exit")
    hmon = Process.monitor(r.h)
    Process.exit(r.req, :kill)

    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert collected(r) == []
    assert Store.stats(ctx.store).receipts == 0
    assert {:error, :not_found} = Store.lookup(ctx.store, @reader, "req-exit")
  end

  test "authority exit while inactive: the coordinator stops :normal, silently", ctx do
    r = start_inactive!(ctx, "auth-exit")
    hmon = Process.monitor(r.h)
    Process.exit(ctx.auth, :kill)

    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert_quiet!(r)
    assert {:error, :not_found} = Store.lookup(ctx.store, @reader, "auth-exit")
  end

  # The parent's :boom stop is logged by gen_server; keep it out of the output.
  @tag :capture_log
  test "parent exit while inactive: terminate/2 sends, emits and publishes nothing", ctx do
    test = self()
    {hopts, r} = h_opts(ctx, "parent-exit", [])

    parent =
      spawn(fn ->
        send(test, {:started, Handoff.start_link(hopts)})
        receive do: (:never -> :ok)
      end)

    assert_receive {:started, {:ok, h}}, @t
    on_exit(fn -> Process.exit(h, :kill) end)
    r = Map.put(r, :h, h)
    hmon = Process.monitor(h)
    Process.exit(parent, :boom)

    assert_receive {:DOWN, ^hmon, :process, _, :boom}, @t
    # No {:failed, {:handoff_down, _}}, no {:terminated, _, _} event.
    assert_quiet!(r)
    assert {:error, :not_found} = Store.lookup(ctx.store, @reader, "parent-exit")
  end

  test "observer exit while inactive is recorded silently; an attempt that called in and died changes nothing",
       ctx do
    obs = idle()
    r = start_inactive!(ctx, "obs-exit", observer: obs)
    omon = Process.monitor(obs)
    Process.exit(obs, :kill)
    assert_receive {:DOWN, ^omon, :process, _, :killed}, @t

    assert {:error, :inactive} = in_proc(fn -> Handoff.attach(r.h, :primary) end)
    assert %{activation: :inactive, observer: :lost} = Handoff.status(r.h)
    assert monitored(r.h) == MapSet.new([r.req, ctx.auth])
    assert_quiet!(r)

    assert :ok = activate!(r)
    assert [:ingress] = kinds!(ctx.store, "obs-exit")
  end

  # ── activation ──────────────────────────────────────────────────────────

  test "timely activation: one ingress, duplicates refused, then the unchanged active protocol",
       ctx do
    r = start_inactive!(ctx, "timely")
    h = r.h

    assert :ok = activate!(r)
    assert {:error, :duplicate} = req_call!(r, fn -> Handoff.activate(h, r.s) end)
    assert {:error, :duplicate} = req_call!(r, fn -> Handoff.activate(h, r.s + 1) end)
    assert {:error, :not_requester} = Handoff.activate(h, r.s)
    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "timely")
    assert %{activation: :active, grant: :idle, receipts: :recording} = Handoff.status(h)

    fake = fake(r)
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_http, ^fake, @http_marker}, @t
    assert :release = step!(fake, :ready)
    assert_receive {:req, {:telelem_handoff, ^h, {:elected, :primary}}}, @t
    assert :ok = step!(fake, {:transfer, @chunk})
    assert_receive {:req, {:telelem_handoff, ^h, {:data, @chunk}}}, @t
    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t
    assert :ok = req_call!(r, fn -> Handoff.terminal_written(h, :ok) end)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t
    assert :ok = step!(fake, {:cleanup, :possible})
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, %{outcome: {:done, @terminal}}}}}, @t

    assert [:ingress, :dispatch, :commit, :terminal] = kinds!(ctx.store, "timely")

    assert %{completeness: :complete, observed_outcome: :done} =
             lookup!(ctx.store, "timely")
  end

  test "activation re-checks the pinned binding: a store gone since init is refused, nothing published",
       ctx do
    store = start_supervised!({Store, mode: :sandbox}, id: :gone, restart: :temporary)
    r = start_inactive!(ctx, "gone", store: store)
    hmon = Process.monitor(r.h)
    stop_supervised!(:gone)

    assert {:error, :receipts_unbound} = activate!(r)
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert_quiet!(r)
  end

  test "an activation after s is not submitted; the coordinator expires on its own, silently",
       ctx do
    r = start_inactive!(ctx, "late", activation_limit: now() + 50)
    h = r.h
    hmon = Process.monitor(h)
    session = :trace.session_create(:handoff_activation_late, self(), [])
    1 = :trace.process(session, r.req, true, [:send])
    wait_until(r.s)

    assert {:error, :activation_expired} = activate!(r)
    # The requester sent the coordinator nothing.
    ref = :trace.delivered(session, r.req)
    assert_receive {:trace_delivered, _, ^ref}, @t
    :trace.session_destroy(session)
    refute_received {:trace, _, :send, _, ^h}

    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert_quiet!(r)
    assert Store.stats(ctx.store).receipts == 0

    # Outside the waitable range, or not an integer: refused in the caller.
    assert_raise ArgumentError, fn -> Handoff.activate(r.h, now() + 4_294_967_295) end
    assert_raise ArgumentError, fn -> Handoff.activate(r.h, :soon) end
  end

  test "an activation queued across s: the caller's wait ends :unconfirmed at s, the coordinator refuses and publishes nothing",
       ctx do
    r = start_inactive!(ctx, "queued", activation_limit: now() + @window)
    h = r.h
    hmon = Process.monitor(h)
    session = :trace.session_create(:handoff_activation_queued, self(), [])
    1 = :trace.process(session, h, true, [:send])
    true = :erlang.suspend_process(h)

    result =
      try do
        # Submitted before s (now < s), handled only after it; the expiry
        # message (due at s) is queued behind it.
        req_call!(r, fn -> Handoff.activate(h, r.s) end, @window + @t)
      after
        true = :erlang.resume_process(h)
      end

    assert result == {:error, :unconfirmed}
    assert now() >= r.s

    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    # The activation itself was handled and refused by its own clock check
    # (now >= s), not by the expiry; it published nothing.
    assert_receive {:trace, ^h, :send, {_tag, {:error, :activation_expired}}, _}, @t
    :trace.session_destroy(session)
    # The refusal went to the deactivated alias: the requester sees nothing.
    assert_quiet!(r)
    assert {:error, :not_found} = Store.lookup(ctx.store, @reader, "queued")
    assert Store.stats(ctx.store).receipts == 0
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "an accepted activation whose reply the live caller abandoned stays active: no rollback",
       ctx do
    r = start_inactive!(ctx, "abandoned")
    h = r.h
    true = :erlang.suspend_process(h)

    abandoned =
      try do
        req_call!(r, fn ->
          req = :gen_server.send_request(h, {:activate, r.s})
          :gen_server.receive_response(req, 0)
        end)
      after
        true = :erlang.resume_process(h)
      end

    assert abandoned == :timeout
    # Queued after the activation: it sees the decision.
    assert %{activation: :active, receipts: :recording} = Handoff.status(h)
    # No reply (the alias is gone) and nothing else reached the requester.
    req_quiet!(r)
    assert [{:receipt, :ingress, :ok}, :activated] = collected(r)
    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "abandoned")

    # The request goes on as any active one.
    assert :ok = req_call!(r, fn -> Handoff.cancel(h, :gone) end)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :gone}}}, @t
    assert [:ingress, :terminal] = kinds!(ctx.store, "abandoned")
    assert %{observed_outcome: :failed} = lookup!(ctx.store, "abandoned")
  end

  test "an accepted activation whose caller died before the reply stays active; the death is then handled as active",
       ctx do
    r = start_inactive!(ctx, "caller-died")
    h = r.h
    hmon = Process.monitor(h)

    session = :trace.session_create(:handoff_activation_caller, self(), [])
    1 = :trace.process(session, r.req, true, [:send])
    true = :erlang.suspend_process(h)

    try do
      _ = req_do(r, fn -> Handoff.activate(h, r.s) end)
      # The activation is in the coordinator's mailbox before the death.
      assert_receive {:trace, _, :send, {:"$gen_call", _, {:activate, _}}, ^h}, @t
      Process.exit(r.req, :kill)
    after
      :trace.session_destroy(session)
      true = :erlang.resume_process(h)
    end

    # The activation is handled first (same sender as the :DOWN), then the
    # requester's death ends the now active request.
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert [:ingress, :terminal] = kinds!(ctx.store, "caller-died")
    assert %{observed_outcome: :failed} = lookup!(ctx.store, "caller-died")
    assert {:receipt, :ingress, :ok} in collected(r)
    assert :activated in collected(r)
  end

  # ── lifecycle expiry ────────────────────────────────────────────────────

  test "never activated: the coordinator stops :normal at s, silently, with requester, authority and parent alive",
       ctx do
    test = self()
    {hopts, r} = h_opts(ctx, "expired", activation_limit: now() + @window)

    parent =
      spawn(fn ->
        send(test, {:started, Handoff.start_link(hopts)})
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(parent, :kill) end)
    assert_receive {:started, {:ok, h}}, @t
    on_exit(fn -> Process.exit(h, :kill) end)
    r = Map.put(r, :h, h)
    hmon = Process.monitor(h)
    assert %{activation: :inactive} = Handoff.status(h)

    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @window + @t
    assert now() >= r.s
    assert Process.alive?(r.req) and Process.alive?(ctx.auth) and Process.alive?(parent)
    assert_quiet!(r)
    assert {:error, :not_found} = Store.lookup(ctx.store, @reader, "expired")
    assert Store.stats(ctx.store).receipts == 0
    assert Authority.snapshot(ctx.auth).grants == %{}
  end

  test "after a timely activation the expiry is cancelled and an exact stale copy is ignored",
       ctx do
    r = start_inactive!(ctx, "stale-expiry", activation_limit: now() + @window)
    h = r.h
    {tref, etoken} = :sys.get_state(h).activation.expiry
    assert is_integer(:erlang.read_timer(tref))

    assert :ok = activate!(r)
    assert :erlang.read_timer(tref) == false

    # A copy with the exact token (as if it had been queued before the
    # cancel), then past s: the active request is untouched.
    send(h, {:activation_expiry, etoken})
    wait_until(r.s)

    assert %{activation: :active, outcome: nil, phase: :open, grant: :idle} = Handoff.status(h)
    req_quiet!(r)
    assert [{:receipt, :ingress, :ok}, :activated] = collected(r)

    assert :ok = req_call!(r, fn -> Handoff.cancel(h, :gone) end)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :gone}}}, @t
    assert [:ingress, :terminal] = kinds!(ctx.store, "stale-expiry")
  end

  # ── setup helpers ───────────────────────────────────────────────────────

  defp start_store!(id, opts \\ []),
    do: start_supervised!({Store, Keyword.merge([mode: :sandbox], opts)}, id: id)

  defp meta(request_id),
    do: %{
      tenant_id: @tenant,
      request_id: request_id,
      route_rev: 1,
      deployment_id: @dep,
      mode: :sandbox
    }

  defp start_inactive!(ctx, request_id, opts \\ []) do
    {hopts, r} = h_opts(ctx, request_id, opts)
    {:ok, h} = Handoff.start(hopts)
    on_exit(fn -> Process.exit(h, :kill) end)
    Map.put(r, :h, h)
  end

  defp h_opts(ctx, request_id, opts) do
    store = Keyword.get(opts, :store, ctx.store)
    req = requester()
    col = collector()
    d = now() + 60_000
    s = Keyword.get(opts, :activation_limit, now() + 30_000)

    hopts = [
      requester: req,
      attempts: [:primary],
      admission_authority: ctx.auth,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, @units},
      terminal_ack: true,
      receipts: [
        store: store,
        producer: Store.producer(store),
        store_mode: :sandbox,
        meta: meta(request_id)
      ],
      activation_limit: s,
      observer: Keyword.get(opts, :observer, col),
      trace: col,
      cleanup_timeout: 60_000,
      register_timeout: Keyword.get(opts, :register_timeout, 60_000)
    ]

    {hopts, %{req: req, col: col, d: d, s: s, auth: ctx.auth, store: store}}
  end

  defp activate!(r), do: req_call!(r, fn -> Handoff.activate(r.h, r.s) end)

  # Nothing reached the requester (barrier: a later request it handles) and
  # nothing reached the observer/trace collector.
  defp assert_quiet!(r) do
    req_quiet!(r)
    assert collected(r) == []
  end

  defp req_quiet!(r) do
    assert :pong = req_call!(r, fn -> :pong end)
    refute_received {:req, _}
  end

  defp forbidden_send?(msg, to, r) do
    to in [r.req, r.col, r.auth, r.store] or match?({:telelem_handoff, _, _}, msg) or
      match?({:telelem_handoff_trace, _, _}, msg) or match?({:"$gen_call", _, _}, msg)
  end

  # The pids the coordinator monitors.
  defp monitored(h) do
    {:monitors, ms} = Process.info(h, :monitors)
    MapSet.new(for {:process, p} <- ms, do: p)
  end

  # ── coordinator trace session ───────────────────────────────────────────

  # The coordinator is started by a traced starter (set_on_spawn), so its
  # init/1 is traced from its first instruction.
  defp traced_start!(hopts) do
    test = self()
    Enum.each([Producer, Writer, Process, System], &Code.ensure_loaded!/1)
    session = :trace.session_create(:handoff_activation, self(), [])

    starter =
      spawn(fn ->
        receive do: (:go -> send(test, {:started, self(), Handoff.start(hopts)}))
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(starter, :kill) end)
    1 = :trace.process(session, starter, true, [:send, :call, :set_on_spawn])

    for mfa <- [
          {:ets, :insert_new, 2},
          {System, :monotonic_time, 1},
          {Process, :send_after, :_},
          # Process.cancel_timer/1 is inlined to the BIF.
          {:erlang, :cancel_timer, :_},
          {Producer, :binding, 2},
          {Writer, :open, 3}
        ] do
      assert :trace.function(session, mfa, true, []) > 0
    end

    send(starter, :go)
    assert_receive {:started, ^starter, {:ok, h}}, @t
    on_exit(fn -> Process.exit(h, :kill) end)
    {session, h}
  end

  # Everything the coordinator did since the last split, in its order:
  # {:send, msg, to}, :clock, {:insert, kind}, {:timer, msg_tag, time} and
  # {:call, {m, f, arity}}.
  defp trace_split!(session, h) do
    ref = :trace.delivered(session, h)
    assert_receive {:trace_delivered, ^h, ^ref}, @t
    drain_trace(h, [])
  end

  defp drain_trace(h, acc) do
    receive do
      {:trace, ^h, :send, msg, to} ->
        drain_trace(h, [{:send, msg, to} | acc])

      {:trace, ^h, :call, {System, :monotonic_time, _}} ->
        drain_trace(h, [:clock | acc])

      {:trace, ^h, :call, {:ets, :insert_new, [_, row]}} ->
        drain_trace(h, [{:insert, elem(row, tuple_size(row) - 1).kind} | acc])

      {:trace, ^h, :call, {Process, :send_after, [_dest, msg, time | _]}} ->
        drain_trace(h, [{:timer, elem(msg, 0), time} | acc])

      {:trace, ^h, :call, {m, f, args}} ->
        drain_trace(h, [{:call, {m, f, length(args)}} | acc])

      {:trace, ^h, _, _} ->
        drain_trace(h, acc)

      {:trace, ^h, _, _, _} ->
        drain_trace(h, acc)
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp index!(list, pred) do
    i = Enum.find_index(list, pred)
    assert i != nil, "not found in #{inspect(list, limit: :infinity)}"
    i
  end

  # ── fake attempt (no socket) ────────────────────────────────────────────

  defp fake(r) do
    test = self()

    pid =
      spawn(fn ->
        fake_loop(%{test: test, h: r.h, d: r.d, connected: false, cancelled: nil})
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
        fake_loop(%{s | cancelled: reason})
    end
  end

  defp fake_step(:attach, s), do: {Handoff.attach(s.h, :primary, deadline: s.d), s}
  defp fake_step(:connect, s), do: {{:ok, :simulated}, %{s | connected: true}}

  defp fake_step(:begin_dispatch, s) do
    result = Handoff.begin_dispatch(s.h, :primary, deadline: s.d)

    if result == :ok and s.connected and s.cancelled == nil and now() < s.d,
      do: send(s.test, {:fake_http, self(), @http_marker})

    {result, s}
  end

  defp fake_step(:ready, s), do: {Handoff.ready(s.h, :primary), s}
  defp fake_step({:transfer, chunk}, s), do: {Handoff.transfer(s.h, chunk), s}
  defp fake_step({:finish, t, m}, s), do: {Handoff.finish(s.h, t, m, deadline: s.d), s}
  defp fake_step({:cleanup, charge}, s), do: {Handoff.cleaned(s.h, :primary, charge), s}

  defp step!(fake, step) do
    ref = make_ref()
    send(fake, {:step, ref, step})
    assert_receive {:fake, ^fake, ^ref, reply}, @t
    reply
  end

  # ── requester, collector, one-shot callers ──────────────────────────────

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

  defp req_call!(r, fun, timeout \\ @t) do
    ref = req_do(r, fun)
    assert_receive {:req_did, ^ref, result}, timeout
    result
  end

  # Runs `fun` in a fresh process (neither requester nor attempt).
  defp in_proc(fun) do
    test = self()
    ref = make_ref()
    spawn(fn -> send(test, {:in_proc, ref, fun.()}) end)
    assert_receive {:in_proc, ^ref, result}, @t
    result
  end

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
        col_loop(test, [ev | acc])

      {:telelem_handoff, _h, ev} ->
        col_loop(test, [{:event, ev} | acc])
    end
  end

  defp collected(%{col: col}) do
    ref = make_ref()
    send(col, {:get, self(), ref})
    assert_receive {:collected, ^ref, evs}, @t
    evs
  end

  # ── evidence reads ──────────────────────────────────────────────────────

  defp lookup!(store, request_id) do
    assert {:ok, %Receipt{} = receipt} = Store.lookup(store, @reader, request_id)
    receipt
  end

  defp events!(store, id) do
    for e <- lookup!(store, id).events,
        do: {e.kind, e.attempt, e.deployment_id, e.outcome, e.exposure}
  end

  defp kinds!(store, id), do: Enum.map(lookup!(store, id).events, & &1.kind)

  # ── misc ────────────────────────────────────────────────────────────────

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp wait_until(t) do
    case t - now() do
      left when left > 0 ->
        receive do
        after
          left -> wait_until(t)
        end

      _ ->
        :ok
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
