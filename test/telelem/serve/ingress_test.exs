defmodule Telelem.Serve.IngressTest.CountingStore do
  @moduledoc false
  # Wraps a real `RevisionStore`, reports each fetch to the test process and
  # runs an injected hook first (to move the clock or fail).
  @behaviour Telelem.Ops.RevisionStore

  @impl true
  def fetch({mod, arg, pid, hook}, tenant, route) do
    send(pid, {:store_fetch, tenant, route})

    case hook.() do
      :pass -> mod.fetch(arg, tenant, route)
      {:reply, reply} -> reply
    end
  end

  @impl true
  def compare_and_write({mod, arg, _pid, _hook}, tenant, route, expected, new, digest),
    do: mod.compare_and_write(arg, tenant, route, expected, new, digest)
end

defmodule Telelem.Serve.IngressTest.ScriptedClock do
  @moduledoc false
  # `Instance.Clock` over a `ManualClock`, with a per-reading script held in
  # an Agent: reading N (1-based) runs `script[N]` if present, else reads the
  # manual clock. Each reading is reported to the test process.
  @behaviour Telelem.Instance.Clock

  alias Telelem.Instance.ManualClock

  @impl true
  def send_after(_arg, _dest, _msg, _ms), do: raise("not used")

  @impl true
  def now({manual, agent, pid}) do
    n = Agent.get_and_update(agent, fn {i, script} -> {i + 1, {i + 1, script}} end)
    send(pid, {:clock_read, n})

    case Agent.get(agent, fn {_, script} -> Map.get(script, n, :pass) end) do
      :pass -> ManualClock.now(manual)
      {:return, v} -> v
      :raise -> raise "clock fault"
      :throw -> throw(:clock_fault)
      :exit -> exit(:clock_fault)
    end
  end
end

defmodule Telelem.Serve.IngressTest do
  @moduledoc """
  Offline contract tests for `Telelem.Serve.Ingress.prepare/3`: a real temp
  `Telelem.Ops.FileStore` behind the same `Ops.Context` the route operations
  use, real `Sandbox.Bootstrap` bindings, and `Instance.ManualClock` time.

  Not covered here: a listener, Plug body reading, real time-outs of blocking
  calls (nothing is cancelled; see the module's *Cancellation limits*),
  admission, receipts, dispatch.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Telelem.Instance.ManualClock
  alias Telelem.Ops.{Context, FileStore, MemoryStore, Route}
  alias Telelem.Route.{Plan, Revision}
  alias Telelem.Sandbox.{Bootstrap, Id, Principal, VerifiedTarget}
  alias Telelem.Serve.Ingress
  alias Telelem.Serve.Ingress.Prepared
  alias Telelem.Serve.IngressTest.{CountingStore, ScriptedClock}

  @moduletag :tmp_dir
  @max_body 4096
  @ceiling 10_000

  setup %{tmp_dir: d} do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem ingress test CA", [])
    pem = :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
    fx = Path.join(d, "fx")
    sb = Path.join(d, "sbx")
    File.mkdir_p!(fx)
    File.mkdir_p!(Path.join(sb, "store"))

    bindings = fn targets ->
      doc = %{
        "version" => 2,
        "tenant_id" => "local",
        "mode" => "sandbox",
        "fixture_targets" =>
          Map.new(targets, fn {id, port} ->
            {id, %{"host" => "127.0.0.1", "port" => port, "ca_pem" => pem}}
          end),
        "sandbox_store_root" => Path.join(sb, "store"),
        "sandbox_signer_ref" => "sandbox:local-fixture"
      }

      {:ok, b} = Bootstrap.from_map(doc, fixture_root: fx, sandbox_root: sb)
      b
    end

    store_dir = Path.join(sb, "store")
    File.chmod!(store_dir, 0o700)
    :ok = FileStore.provision(store_dir, "local", rev1())
    name = :"ingress_fs_#{System.unique_integer([:positive])}"
    {:ok, pid} = FileStore.start(name: name, dir: store_dir, tenant_id: "local", route_id: "chat")
    on_exit(fn -> if Process.alive?(pid), do: FileStore.stop(pid) end)

    clock = start_supervised!(ManualClock)
    me = self()
    store = {CountingStore, {FileStore, name, me, fn -> :pass end}}

    %{
      bindings: bindings.(%{"a" => 18443, "b" => 18444}),
      make_bindings: bindings,
      fs: name,
      store: store,
      clock: clock,
      me: me
    }
  end

  defp rev1(attrs \\ []) do
    Revision.new!(
      Keyword.merge(
        [
          route_id: "chat",
          rev: 1,
          deployments: ["a", "b"],
          preferred: "a",
          fallback_on_refusal: false,
          deadline_ms: 5000,
          max_attempts: 1
        ],
        attrs
      )
    )
  end

  defp principal(tenant \\ "local", mode \\ :sandbox, perms \\ [:dispatch]) do
    {:ok, p} = Principal.new(tenant, "caller", mode, perms)
    p
  end

  defp ctx(store), do: %Context{mode: :sandbox, store: store, revisions: Revision}

  # Config whose callbacks report to the test process. `auth` maps the opaque
  # auth input to a callback result.
  defp cfg(t, overrides \\ []) do
    me = t.me

    base = [
      authenticate: fn input ->
        send(me, {:authenticate, input})
        {:ok, principal()}
      end,
      context: ctx(t.store),
      bindings: t.bindings,
      route_id: "chat",
      max_attempts: 1,
      deadline_ceiling_ms: @ceiling,
      max_body_bytes: @max_body,
      clock: {ManualClock, t.clock},
      new_gateway_id: fn ->
        send(me, :gateway_id)
        Ingress.random_id()
      end
    ]

    {:ok, c} = Ingress.config(Keyword.merge(base, overrides))
    c
  end

  defp auth_returning(t, result) do
    me = t.me

    fn input ->
      send(me, {:authenticate, input})
      result.()
    end
  end

  defp reader(t, body, hook \\ fn -> :ok end) do
    me = t.me

    fn max, remaining ->
      send(me, {:read_body, max, remaining})
      hook.()
      {:ok, body}
    end
  end

  defp refute_side_effects(what) do
    if :store in what, do: refute_received({:store_fetch, _, _})
    if :body in what, do: refute_received({:read_body, _, _})
    if :id in what, do: refute_received(:gateway_id)
  end

  ## Happy path

  test "prepares one immutable request from the pinned FileStore revision", t do
    assert {:ok, %Prepared{} = p} = Ingress.prepare(cfg(t), :credential, reader(t, "{}"))

    assert_received {:authenticate, :credential}
    assert_received {:store_fetch, "local", "chat"}
    assert_received {:read_body, @max_body, 5000}
    assert_received :gateway_id

    assert %Prepared{
             tenant_id: "local",
             subject_id: "caller",
             mode: :sandbox,
             route_id: "chat",
             max_attempts: 1,
             accepted_at_ms: 0,
             deadline_ms: 5000,
             body: "{}",
             revision: %Revision{rev: 1, preferred: "a"} = rev,
             plan: %Plan{rev: 1, primary: "a", backup: :none, max_attempts: 1} = plan,
             target: %VerifiedTarget{deployment_id: "a", port: 18443}
           } = p

    assert plan.digest == rev.digest
    assert plan.absolute_deadline_ms == 5000
    assert Revision.verify(rev) == :ok
    assert Id.valid?(p.gateway_id) and byte_size(p.gateway_id) == 22
    refute_received {:store_fetch, _, _}
  end

  test "gateway IDs are fresh per request and never derived from caller data", t do
    c = cfg(t, new_gateway_id: &Ingress.random_id/0)
    ids = for _ <- 1..20, do: elem(Ingress.prepare(c, "req-1", reader(t, "req-1")), 1).gateway_id
    assert length(Enum.uniq(ids)) == 20
    refute "req-1" in ids
  end

  test "body and auth input never choose tenant, route, model or deployment", t do
    body = ~s({"model":"b","tenant_id":"other","route":"x","deployment":"b","stream":true})

    assert {:ok, p} =
             Ingress.prepare(cfg(t), %{tenant_id: "other", model: "b"}, reader(t, body))

    assert {p.tenant_id, p.route_id, p.plan.primary, p.target.deployment_id} ==
             {"local", "chat", "a", "a"}

    assert p.body == body
  end

  ## No body read before authentication and dispatch authorization

  for {label, result} <- [
        error: quote(do: fn -> :error end),
        bare_principal: quote(do: fn -> principal() end),
        lookalike_map:
          quote(
            do: fn ->
              {:ok,
               %{
                 tenant_id: "local",
                 subject_id: "x",
                 mode: :sandbox,
                 permissions: MapSet.new([:dispatch])
               }}
            end
          ),
        tampered_struct:
          quote(do: fn -> {:ok, %{principal() | permissions: MapSet.new([:root])}} end),
        # `Principal.check/1` raises on these; that must stay contained.
        malformed_mapset:
          quote(
            do: fn -> {:ok, %{principal() | permissions: Map.put(MapSet.new(), :map, :bad)}} end
          ),
        list_permissions: quote(do: fn -> {:ok, %{principal() | permissions: [:dispatch]}} end),
        non_binary_tenant: quote(do: fn -> {:ok, %{principal() | tenant_id: {:t, "local"}}} end),
        raises: quote(do: fn -> raise "boom" end),
        throws: quote(do: fn -> throw(:boom) end),
        exits: quote(do: fn -> exit(:boom) end)
      ] do
    test "unauthenticated (#{label}): no store fetch, no body read, no ID", t do
      c = cfg(t, authenticate: auth_returning(t, unquote(result)))
      assert Ingress.prepare(c, :cred, reader(t, "secret")) == {:error, :unauthenticated}
      assert_received {:authenticate, :cred}
      refute_side_effects([:store, :body, :id])
    end
  end

  test "principal without :dispatch is refused before the store and the body", t do
    p = principal("local", :sandbox, [:route_read, :route_write, :receipt_read])
    c = cfg(t, authenticate: auth_returning(t, fn -> {:ok, p} end))
    assert Ingress.prepare(c, :cred, reader(t, "x")) == {:error, :permission_denied}
    refute_side_effects([:store, :body, :id])
  end

  test "serving principal on the sandbox context is a mode mismatch, before any read", t do
    c = cfg(t, authenticate: auth_returning(t, fn -> {:ok, principal("local", :serving)} end))
    assert Ingress.prepare(c, :cred, reader(t, "x")) == {:error, :mode_mismatch}
    refute_side_effects([:store, :body, :id])
  end

  ## Tenant isolation

  test "a foreign tenant gets the missing-route reply without touching the store", t do
    foreign = cfg(t, authenticate: auth_returning(t, fn -> {:ok, principal("other")} end))
    assert Ingress.prepare(foreign, :cred, reader(t, "x")) == {:error, :no_route}
    refute_side_effects([:store, :body, :id])

    missing = cfg(t, route_id: "nope")
    assert Ingress.prepare(missing, :cred, reader(t, "x")) == {:error, :no_route}
    assert_received {:store_fetch, "local", "nope"}
    refute_side_effects([:body, :id])
  end

  test "a store record for another route is an integrity fault, not a route", t do
    store = {CountingStore, {FileStore, t.fs, t.me, fn -> :pass end}}
    {:ok, mem} = MemoryStore.start_link()
    :ok = MemoryStore.seed(mem, "local", rev1(route_id: "other"))

    lying =
      {CountingStore,
       {MemoryStore, mem, t.me, fn -> {:reply, MemoryStore.fetch(mem, "local", "other")} end}}

    assert {:ok, _} = Ingress.prepare(cfg(t, context: ctx(store)), :c, reader(t, "x"))

    assert Ingress.prepare(cfg(t, context: ctx(lying)), :c, reader(t, "x")) ==
             {:error, :store_integrity}
  end

  test "the malformed-MapSet principal really makes Principal.check/1 raise", t do
    bad = %{principal() | permissions: Map.put(MapSet.new(), :map, :bad)}
    assert_raise FunctionClauseError, fn -> Telelem.Sandbox.Principal.check(bad) end

    c = cfg(t, authenticate: auth_returning(t, fn -> {:ok, bad} end))
    assert Ingress.prepare(c, :cred, reader(t, "x")) == {:error, :unauthenticated}
    refute_side_effects([:store, :body, :id])
  end

  ## Pinned revision

  test "the revision is fetched once and stays pinned while the route changes", t do
    {:ok, op} = Principal.new("local", "operator", :sandbox, [:route_read, :route_write])
    ops_ctx = ctx({FileStore, t.fs})

    apply_b = fn ->
      {:ok, %{plan_digest: d}} = Route.plan(op, ops_ctx, "chat", 1, "b")
      {:ok, %{effective_revision: %{rev: 2}}} = Route.apply(op, ops_ctx, "chat", 1, "b", d)
    end

    assert {:ok, p} = Ingress.prepare(cfg(t), :c, reader(t, "x", apply_b))
    assert_received {:store_fetch, "local", "chat"}
    refute_received {:store_fetch, _, _}
    assert {p.revision.rev, p.plan.rev, p.plan.primary, p.target.port} == {1, 1, "a", 18443}

    assert {:ok, next} = Ingress.prepare(cfg(t), :c, reader(t, "x"))
    assert {next.revision.rev, next.plan.primary, next.target.port} == {2, "b", 18444}
    assert {:ok, %{effective_revision: %{rev: 2}}} = Route.read(op, ops_ctx, "chat")
  end

  describe "store faults never reach the body reader" do
    for {label, hook, expected} <-
          [
            raises: quote(do: fn -> raise "down" end),
            exits: quote(do: fn -> exit(:timeout) end),
            garbage: quote(do: fn -> {:reply, {:ok, :nope}} end),
            tampered_digest:
              quote(
                do: fn ->
                  r = rev1()
                  {:reply, {:ok, %{revision: %{r | preferred: "b"}, last_plan_digest: nil}}}
                end
              ),
            extra_field:
              quote(
                do: fn ->
                  r = Map.put(Map.from_struct(rev1()), :model, "b")
                  {:reply, {:ok, %{revision: r, last_plan_digest: nil}}}
                end
              ),
            # The stored digest is required; it must never be recomputed.
            missing_digest:
              quote(
                do: fn ->
                  r = Map.delete(Map.from_struct(rev1()), :digest)
                  {:reply, {:ok, %{revision: r, last_plan_digest: nil}}}
                end
              ),
            missing_digest_tampered:
              quote(
                do: fn ->
                  r = Map.from_struct(rev1()) |> Map.delete(:digest) |> Map.put(:preferred, "b")
                  {:reply, {:ok, %{revision: r, last_plan_digest: nil}}}
                end
              ),
            nil_digest:
              quote(
                do: fn ->
                  {:reply, {:ok, %{revision: %{rev1() | digest: nil}, last_plan_digest: nil}}}
                end
              ),
            uppercase_digest:
              quote(
                do: fn ->
                  r = rev1()

                  {:reply,
                   {:ok,
                    %{revision: %{r | digest: String.upcase(r.digest)}, last_plan_digest: nil}}}
                end
              ),
            missing_last_plan_digest: quote(do: fn -> {:reply, {:ok, %{revision: rev1()}}} end),
            bad_last_plan_digest:
              quote(do: fn -> {:reply, {:ok, %{revision: rev1(), last_plan_digest: "nope"}}} end),
            uppercase_last_plan_digest:
              quote(
                do: fn ->
                  {:reply,
                   {:ok, %{revision: rev1(), last_plan_digest: String.duplicate("A", 64)}}}
                end
              ),
            integer_last_plan_digest:
              quote(do: fn -> {:reply, {:ok, %{revision: rev1(), last_plan_digest: 7}}} end),
            bare_revision: quote(do: fn -> {:reply, {:ok, rev1()}} end),
            non_map_revision:
              quote(do: fn -> {:reply, {:ok, %{revision: [1], last_plan_digest: nil}}} end),
            foreign_struct_revision:
              quote(
                do: fn ->
                  r = Map.put(rev1(), :__struct__, URI)
                  {:reply, {:ok, %{revision: r, last_plan_digest: nil}}}
                end
              )
          ]
          |> Enum.map(fn {l, h} ->
            {l, h, if(l in [:raises, :exits], do: :store_unavailable, else: :store_integrity)}
          end) do
      test "#{label}", t do
        store = {CountingStore, {FileStore, t.fs, t.me, unquote(hook)}}
        c = cfg(t, context: ctx(store))
        assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, unquote(expected)}
        refute_side_effects([:body, :id])
      end
    end
  end

  test "a well-formed envelope with a plain-map revision and a plan digest is accepted", t do
    hook = fn ->
      r = Map.from_struct(rev1())
      {:reply, {:ok, %{revision: r, last_plan_digest: String.duplicate("0a", 32)}}}
    end

    store = {CountingStore, {FileStore, t.fs, t.me, hook}}
    assert {:ok, p} = Ingress.prepare(cfg(t, context: ctx(store)), :c, reader(t, "x"))
    assert %Revision{} = p.revision
    assert p.revision.digest == rev1().digest
  end

  ## Attempt policy

  for {label, attrs} <- [
        two_attempts_with_fallback: [max_attempts: 2, fallback_on_refusal: true],
        two_attempts_no_fallback: [max_attempts: 2, fallback_on_refusal: false],
        fallback_flag_one_attempt: [max_attempts: 1, fallback_on_refusal: true]
      ] do
    test "a #{label} revision is refused, not downgraded", t do
      {:ok, mem} = MemoryStore.start_link()
      :ok = MemoryStore.seed(mem, "local", rev1(unquote(attrs)))
      c = cfg(t, context: ctx({MemoryStore, mem}))
      assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, :unsupported_attempt_policy}
      refute_side_effects([:body, :id])

      # Also when only one lane is bound, so the selector alone would give 1.
      c1 = cfg(t, context: ctx({MemoryStore, mem}), bindings: t.make_bindings.(%{"a" => 1}))
      assert Ingress.prepare(c1, :c, reader(t, "x")) == {:error, :unsupported_attempt_policy}
    end
  end

  test "configuration must state the one-attempt policy explicitly", t do
    base = Map.from_struct(cfg(t)) |> Map.drop([:__struct__])
    assert Ingress.config(Map.delete(base, :max_attempts)) == {:error, {:invalid, :max_attempts}}

    for n <- [2, 0, :fallback] do
      assert Ingress.config(%{base | max_attempts: n}) == {:error, {:unsupported, :max_attempts}}
    end
  end

  ## Eligibility through sandbox bindings

  test "an unbound preferred lane makes the bound alternate the primary, not a fallback", t do
    c = cfg(t, bindings: t.make_bindings.(%{"b" => 18444}))
    assert {:ok, p} = Ingress.prepare(c, :c, reader(t, "x"))
    assert {p.plan.primary, p.plan.backup, p.plan.max_attempts} == {"b", :none, 1}
    assert p.target.deployment_id == "b"
  end

  test "no bound deployment of the revision fails before the body", t do
    c = cfg(t, bindings: t.make_bindings.(%{"z" => 18445}))
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, :no_eligible_deployment}
    refute_side_effects([:body, :id])
  end

  ## Deadline

  test "the pinned deadline is the smaller of ceiling and route deadline from t0", t do
    ManualClock.advance(t.clock, 100)

    assert {:ok, p} = Ingress.prepare(cfg(t, deadline_ceiling_ms: 1000), :c, reader(t, "x"))
    assert {p.accepted_at_ms, p.deadline_ms, p.plan.absolute_deadline_ms} == {100, 1100, 5100}
    assert_received {:read_body, _, 1000}

    assert {:ok, p} = Ingress.prepare(cfg(t, deadline_ceiling_ms: 60_000), :c, reader(t, "x"))
    assert {p.accepted_at_ms, p.deadline_ms} == {100, 5100}
    assert_received {:read_body, _, 5000}
  end

  test "time spent before the body reduces the reader's remaining budget", t do
    store = {CountingStore, {FileStore, t.fs, t.me, fn -> tick(t, 700) end}}
    assert {:ok, p} = Ingress.prepare(cfg(t, context: ctx(store)), :c, reader(t, "x"))
    assert p.deadline_ms == 5000
    assert_received {:read_body, _, 4300}
  end

  test "deadline crossed during authentication stops before the store", t do
    auth =
      auth_returning(t, fn ->
        tick(t, 1000)
        {:ok, principal()}
      end)

    c = cfg(t, authenticate: auth, deadline_ceiling_ms: 1000)
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, {:deadline_exceeded, :authenticate}}
    refute_side_effects([:store, :body, :id])
  end

  test "deadline crossed during the store fetch stops before the body", t do
    store = {CountingStore, {FileStore, t.fs, t.me, fn -> tick(t, 1000) end}}
    c = cfg(t, context: ctx(store), deadline_ceiling_ms: 1000)
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, {:deadline_exceeded, :route}}
    refute_side_effects([:body, :id])
  end

  test "route deadline below the ceiling applies before the body", t do
    {:ok, mem} = MemoryStore.start_link()
    :ok = MemoryStore.seed(mem, "local", rev1(deadline_ms: 300))
    store = {CountingStore, {MemoryStore, mem, t.me, fn -> tick(t, 300) end}}
    c = cfg(t, context: ctx(store))
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, {:deadline_exceeded, :select}}
    refute_side_effects([:body, :id])
  end

  test "deadline crossed during the body read: no prepared request and no ID", t do
    c = cfg(t, deadline_ceiling_ms: 1000)
    r = reader(t, "x", fn -> ManualClock.advance(t.clock, 1000) end)
    assert Ingress.prepare(c, :c, r) == {:error, {:deadline_exceeded, :body}}
    assert_received {:read_body, _, 1000}
    refute_side_effects([:id])
  end

  test "deadline crossed while the ID is generated: no prepared request", t do
    me = t.me

    late_id = fn ->
      send(me, :gateway_id)
      ManualClock.advance(t.clock, 1000)
      Ingress.random_id()
    end

    c = cfg(t, deadline_ceiling_ms: 1000, new_gateway_id: late_id)
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, {:deadline_exceeded, :gateway_id}}
    assert_received {:read_body, _, 1000}
    assert_received :gateway_id

    # One millisecond before the pinned deadline still prepares.
    almost = fn ->
      ManualClock.advance(t.clock, 999)
      Ingress.random_id()
    end

    assert {:ok, %Prepared{deadline_ms: 2000}} =
             Ingress.prepare(
               cfg(t, deadline_ceiling_ms: 1000, new_gateway_id: almost),
               :c,
               reader(t, "x")
             )
  end

  test "the route deadline also applies after ID generation", t do
    {:ok, mem} = MemoryStore.start_link()
    :ok = MemoryStore.seed(mem, "local", rev1(deadline_ms: 300))

    late_id = fn ->
      :pass = tick(t, 300)
      Ingress.random_id()
    end

    c = cfg(t, context: ctx({MemoryStore, mem}), new_gateway_id: late_id)
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, {:deadline_exceeded, :gateway_id}}
  end

  test "ceiling is bounded and required", t do
    base = Map.from_struct(cfg(t)) |> Map.drop([:__struct__])

    for bad <- [0, -1, Ingress.max_ceiling_ms() + 1, 1.5, nil] do
      assert Ingress.config(%{base | deadline_ceiling_ms: bad}) ==
               {:error, {:invalid, :deadline_ceiling_ms}}
    end

    assert {:error, {:invalid, :deadline_ceiling_ms}} =
             Ingress.config(Map.delete(base, :deadline_ceiling_ms))
  end

  defp tick(t, ms) do
    ManualClock.advance(t.clock, ms)
    :pass
  end

  ## Clock faults

  # Readings in a full run: 1 t0, 2 after auth, 3 after route, 4 after
  # select, 5 body budget, 6 after body, 7 after ID. A fault at reading N
  # stops there: only the steps before it ran.
  @clock_faults [
    :raise,
    :throw,
    :exit,
    {:return, nil},
    {:return, 1.5},
    {:return, "0"},
    {:return, {:ok, 0}}
  ]

  for {n, ran} <- [
        {1, []},
        {2, [:auth]},
        {3, [:auth, :store]},
        {4, [:auth, :store]},
        {5, [:auth, :store]},
        {6, [:auth, :store, :body]},
        {7, [:auth, :store, :body, :id]}
      ] do
    test "clock fault at reading #{n} is :clock_unavailable and nothing later runs", t do
      for fault <- @clock_faults do
        {:ok, agent} = Agent.start_link(fn -> {0, %{unquote(n) => fault}} end)
        c = cfg(t, clock: {ScriptedClock, {t.clock, agent, t.me}})
        log = capture_log(fn -> send(self(), {:res, Ingress.prepare(c, :c, reader(t, "x"))}) end)
        assert_received {:res, {:error, :clock_unavailable}}, inspect(fault)
        assert log == ""

        for i <- 1..unquote(n), do: assert_received({:clock_read, ^i})
        refute_received {:clock_read, _}, "read the clock again after #{inspect(fault)}"

        ran = unquote(ran)

        if :auth in ran,
          do: assert_received({:authenticate, :c}),
          else: refute_received({:authenticate, _})

        if :store in ran,
          do: assert_received({:store_fetch, _, _}),
          else: refute_side_effects([:store])

        if :body in ran,
          do: assert_received({:read_body, _, _}),
          else: refute_side_effects([:body])

        if :id in ran, do: assert_received(:gateway_id), else: refute_side_effects([:id])
        Agent.stop(agent)
      end
    end
  end

  test "an unscripted ScriptedClock run takes exactly seven readings", t do
    {:ok, agent} = Agent.start_link(fn -> {0, %{}} end)
    c = cfg(t, clock: {ScriptedClock, {t.clock, agent, t.me}})
    assert {:ok, %Prepared{}} = Ingress.prepare(c, :c, reader(t, "x"))
    for i <- 1..7, do: assert_received({:clock_read, ^i})
    refute_received {:clock_read, _}
  end

  test "a dead clock process is :clock_unavailable before authentication", t do
    {:ok, manual} = ManualClock.start_link()
    Process.unlink(manual)
    c = cfg(t, clock: {ManualClock, manual})
    GenServer.stop(manual)
    assert Ingress.prepare(c, :c, reader(t, "x")) == {:error, :clock_unavailable}
    refute_received {:authenticate, _}
    refute_side_effects([:store, :body, :id])
  end

  test "a clock module that is missing or lacks now/1 is refused", t do
    good = cfg(t)
    base = Map.from_struct(good) |> Map.drop([:__struct__])

    for bad <- [{Telelem.NoSuchClock, nil}, {Enum, nil}, {:"", nil}, {nil, nil}] do
      assert Ingress.config(%{base | clock: bad}) == {:error, {:invalid, :clock}}

      assert Ingress.prepare(%{good | clock: bad}, :c, reader(t, "x")) ==
               {:error, :invalid_config}
    end

    refute_received {:authenticate, _}
    refute_side_effects([:store, :body, :id])
  end

  ## Body reader

  for {label, ret, expected} <-
        [
          oversized: quote(do: {:ok, :binary.copy("x", 4097)}),
          reports_too_large: quote(do: {:error, :too_large}),
          reports_timeout: quote(do: {:error, :timeout}),
          other_error: quote(do: {:error, :closed}),
          non_binary: quote(do: {:ok, ~c"chars"}),
          more: quote(do: {:more, "partial", :conn})
        ]
        |> Enum.map(fn {l, r} ->
          {l, r,
           case l do
             l when l in [:oversized, :reports_too_large] -> :body_too_large
             :reports_timeout -> :body_timeout
             _ -> :body_unavailable
           end}
        end) do
    test "body reader #{label} yields #{expected} and no ID", t do
      assert Ingress.prepare(cfg(t), :c, fn _, _ -> unquote(ret) end) ==
               {:error, unquote(expected)}

      refute_side_effects([:id])
    end
  end

  test "a raising, throwing or exiting body reader is contained", t do
    for f <- [fn _, _ -> raise "x" end, fn _, _ -> throw(:x) end, fn _, _ -> exit(:x) end] do
      assert Ingress.prepare(cfg(t), :c, f) == {:error, :body_unavailable}
    end

    refute_side_effects([:id])
  end

  test "an exactly-at-bound body is accepted", t do
    body = :binary.copy("y", @max_body)
    assert {:ok, %Prepared{body: ^body}} = Ingress.prepare(cfg(t), :c, reader(t, body))
  end

  ## Gateway ID

  test "an invalid or failing ID source fails preparation", t do
    for src <- [fn -> "not valid!" end, fn -> 42 end, fn -> raise "x" end, fn -> "" end] do
      assert Ingress.prepare(cfg(t, new_gateway_id: src), :c, reader(t, "x")) ==
               {:error, :gateway_id_unavailable}
    end
  end

  ## Payload hygiene

  test "no body or credential marker in logs, inspect output or errors", t do
    marker = "MARKER-#{System.unique_integer([:positive])}"
    body = ~s({"messages":[{"role":"user","content":"#{marker}"}]})

    log =
      capture_log(fn ->
        assert {:ok, p} = Ingress.prepare(cfg(t), {:bearer, marker}, reader(t, body))
        refute inspect(p) =~ marker
        refute inspect(p, limit: :infinity, structs: true) =~ marker

        bad = cfg(t, authenticate: fn _ -> raise marker end)
        e1 = Ingress.prepare(bad, marker, reader(t, body))
        e2 = Ingress.prepare(cfg(t), marker, fn _, _ -> raise marker end)
        e3 = Ingress.prepare(cfg(t), marker, fn _, _ -> {:error, marker} end)

        for e <- [e1, e2, e3], do: refute(inspect(e) =~ marker)
        refute inspect(cfg(t)) =~ marker
      end)

    refute log =~ marker
  end

  ## Configuration

  test "configuration is trusted and validated; a tampered config is refused", t do
    good = cfg(t)
    base = Map.from_struct(good) |> Map.drop([:__struct__])

    assert Ingress.config(%{base | context: %{good.context | mode: :serving}}) ==
             {:error, {:unsupported, :serving}}

    assert Ingress.config(%{base | context: %{good.context | revisions: String}}) ==
             {:error, {:invalid, :context}}

    assert Ingress.config(%{base | bindings: %{good.bindings | tenant_id: "bad tenant"}}) ==
             {:error, {:invalid, :bindings}}

    assert Ingress.config(%{base | route_id: "no/route"}) == {:error, {:invalid, :route_id}}
    assert Ingress.config(Map.put(base, :tenant_id, "x")) == {:error, {:invalid, :tenant_id}}
    assert Ingress.config(Map.put(base, :model, "x")) == {:error, {:invalid, :model}}

    for bad <- [%{good | max_attempts: 2}, %{good | deadline_ceiling_ms: 0}, %{route_id: "chat"}] do
      assert Ingress.prepare(bad, :c, reader(t, "x")) == {:error, :invalid_config}
    end

    refute_side_effects([:store, :body, :id])
    refute_received {:authenticate, _}
  end
end
