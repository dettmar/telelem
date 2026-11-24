# Route operation tests (m2a §4). Test doubles first; one file so no helper
# file sits in test/ outside the `_test.exs` load pattern.

defmodule Telelem.OpsTest.FakeRevisions do
  @moduledoc """
  Stand-in for A1's route revision module. Its digest is deliberately *not*
  the §1 encoding (it hashes an Erlang term), so these tests can only pass if
  ops treats the revision digest as opaque.
  """
  @behaviour Telelem.Ops.Revisions

  def new(route_id, deployments, preferred, rev \\ 1) do
    rehash(%{
      route_id: route_id,
      rev: rev,
      deployments: deployments,
      preferred: preferred,
      fallback_on_refusal: true,
      deadline_ms: 5000,
      max_attempts: 2,
      digest: nil
    })
  end

  @impl true
  def with_preferred(current, preferred) do
    cond do
      rehash(current).digest != current.digest ->
        {:error, :digest_mismatch}

      current.preferred not in current.deployments ->
        {:error, :preferred_not_in_deployments}

      preferred in current.deployments ->
        {:ok, rehash(%{current | preferred: preferred, rev: current.rev + 1})}

      true ->
        {:error, :unknown_deployment}
    end
  end

  def rehash(rev) do
    body = Map.delete(rev, :digest)

    %{
      rev
      | digest: Base.encode16(:crypto.hash(:sha256, :erlang.term_to_binary(body)), case: :lower)
    }
  end
end

defmodule Telelem.OpsTest.TamperingRevisions do
  @moduledoc "Faulty revision module that also changes a fixed control."
  @behaviour Telelem.Ops.Revisions

  @impl true
  def with_preferred(current, preferred) do
    {:ok, proposed} = Telelem.OpsTest.FakeRevisions.with_preferred(current, preferred)
    {:ok, Telelem.OpsTest.FakeRevisions.rehash(%{proposed | deadline_ms: 1})}
  end
end

defmodule Telelem.OpsTest.GatedStore do
  @moduledoc """
  Wraps `Telelem.Ops.MemoryStore`. Each `fetch/3` reports `{:fetched, ref,
  self()}` to the test and blocks until it receives `{:go, ref}`, so a test can
  hold several appliers between their read and their compare-and-write.
  """
  @behaviour Telelem.Ops.RevisionStore

  @impl true
  def fetch({server, test_pid, ref}, tenant, route) do
    result = Telelem.Ops.MemoryStore.fetch(server, tenant, route)
    send(test_pid, {:fetched, ref, self()})

    receive do
      {:go, ^ref} -> result
    end
  end

  @impl true
  def compare_and_write({server, _test_pid, _ref}, tenant, route, expected, new, digest) do
    Telelem.Ops.MemoryStore.compare_and_write(server, tenant, route, expected, new, digest)
  end
end

defmodule Telelem.OpsTest.ScriptedStore do
  @moduledoc """
  Wraps `Telelem.Ops.MemoryStore`. The arg is `%{server: pid}` plus optional
  `:fetch` / `:cas` functions that receive a thunk for the real call and
  decide what the caller sees (call it and lose the reply, skip it and exit,
  return garbage, ...). Deterministic: no timers.
  """
  @behaviour Telelem.Ops.RevisionStore
  alias Telelem.Ops.MemoryStore

  @impl true
  def fetch(%{server: s} = a, tenant, route),
    do: Map.get(a, :fetch, & &1.()).(fn -> MemoryStore.fetch(s, tenant, route) end)

  @impl true
  def compare_and_write(%{server: s} = a, tenant, route, expected, new, digest) do
    Map.get(a, :cas, & &1.()).(fn ->
      MemoryStore.compare_and_write(s, tenant, route, expected, new, digest)
    end)
  end
end

defmodule Telelem.OpsTest.Fixtures do
  alias Telelem.OpsTest.FakeRevisions

  def operator(tenant \\ "local", mode \\ :sandbox) do
    %{
      tenant_id: tenant,
      subject_id: "operator",
      mode: mode,
      permissions: MapSet.new([:route_read, :route_write])
    }
  end

  def reader(tenant \\ "local"),
    do: %{operator(tenant) | subject_id: "viewer", permissions: MapSet.new([:route_read])}

  @doc "MemoryStore with `chat, rev 1, [a,b], preferred a, true, 5000ms, 2` for `local`."
  def seeded_store do
    {:ok, store} = Telelem.Ops.MemoryStore.start_link()
    :ok = Telelem.Ops.MemoryStore.seed(store, "local", FakeRevisions.new("chat", ["a", "b"], "a"))
    store
  end

  def ctx(store, mode \\ :sandbox, revisions \\ FakeRevisions) do
    %Telelem.Ops.Context{
      mode: mode,
      store: {Telelem.Ops.MemoryStore, store},
      revisions: revisions
    }
  end
end

defmodule Telelem.Ops.RouteTest do
  use ExUnit.Case, async: true

  alias Telelem.Ops.{MemoryStore, PlanDigest, Route}
  alias Telelem.OpsTest.{FakeRevisions, TamperingRevisions}
  import Telelem.OpsTest.Fixtures

  setup do
    store = seeded_store()
    %{store: store, ctx: ctx(store)}
  end

  describe "contract example (§4)" do
    test "prefer b against rev 1: plan, apply same inputs and digest, read back rev 2", %{
      ctx: ctx
    } do
      op = operator()
      {:ok, before} = Route.read(op, ctx, "chat")
      assert before.effective_revision.rev == 1
      assert before.last_plan_digest == nil

      assert {:ok, %{expected_rev: 1, plan_digest: digest, proposed: proposed}} =
               Route.plan(op, ctx, "chat", 1, "b")

      assert proposed.rev == 2 and proposed.preferred == "b"

      assert digest ==
               PlanDigest.compute("local", "chat", 1, "b", proposed.digest)

      # Plan does not write.
      assert {:ok, ^before} = Route.read(op, ctx, "chat")

      assert {:ok, %{effective_revision: ^proposed, plan_digest: ^digest}} =
               Route.apply(op, ctx, "chat", 1, "b", digest)

      assert {:ok, %{effective_revision: %{rev: 2, preferred: "b"}, last_plan_digest: ^digest}} =
               Route.read(reader(), ctx, "chat")

      # The old revision now conflicts.
      assert {:conflict,
              %{
                reason: :stale_revision,
                expected_rev: 1,
                current_rev: 2,
                last_plan_digest: ^digest
              }} =
               Route.apply(op, ctx, "chat", 1, "b", digest)

      assert {:conflict, %{reason: :stale_revision, current_rev: 2}} =
               Route.plan(op, ctx, "chat", 1, "b")
    end

    test "fixed controls are unchanged; only preferred, rev and digest move", %{ctx: ctx} do
      {:ok, %{effective_revision: old}} = Route.read(operator(), ctx, "chat")
      {:ok, %{plan_digest: d}} = Route.plan(operator(), ctx, "chat", 1, "b")
      {:ok, %{effective_revision: new}} = Route.apply(operator(), ctx, "chat", 1, "b", d)

      for f <- [:route_id, :deployments, :fallback_on_refusal, :deadline_ms, :max_attempts],
          do: assert(Map.fetch!(new, f) == Map.fetch!(old, f))

      assert new.digest != old.digest
    end

    test "re-preferring the current deployment advances rev (orchestrator decision)", %{ctx: ctx} do
      {:ok, %{plan_digest: d, proposed: p}} = Route.plan(operator(), ctx, "chat", 1, "a")
      assert p.rev == 2 and p.preferred == "a"

      assert {:ok, %{effective_revision: %{rev: 2}}} =
               Route.apply(operator(), ctx, "chat", 1, "a", d)
    end
  end

  describe "plan digest" do
    test "golden vector: version-prefixed ordered compact JSON array" do
      proposed = String.duplicate("ab", 32)

      assert PlanDigest.canonical("local", "chat", 1, "b", proposed) ==
               ~s(["telelem.ops.route_plan/1","local","chat",1,"b","#{proposed}"])

      assert PlanDigest.compute("local", "chat", 1, "b", proposed) ==
               "074be536f4ee49c3fb8f090f06ca292178799d48637e28d522c0d021c1e2d4db"
    end

    test "apply recomputes: a digest for other inputs conflicts and writes nothing", %{ctx: ctx} do
      {:ok, %{plan_digest: for_a}} = Route.plan(operator(), ctx, "chat", 1, "a")

      assert {:conflict, %{reason: :plan_digest_mismatch, current_rev: 1}} =
               Route.apply(operator(), ctx, "chat", 1, "b", for_a)

      {:ok, %{plan_digest: for_b}} = Route.plan(operator(), ctx, "chat", 1, "b")

      tampered =
        String.replace_prefix(
          for_b,
          binary_part(for_b, 0, 1),
          if(String.starts_with?(for_b, "0"), do: "1", else: "0")
        )

      assert {:conflict, %{reason: :plan_digest_mismatch}} =
               Route.apply(operator(), ctx, "chat", 1, "b", tampered)

      assert {:ok, %{effective_revision: %{rev: 1, preferred: "a"}, last_plan_digest: nil}} =
               Route.read(operator(), ctx, "chat")
    end

    test "a plan digest is bound to the tenant", %{store: store, ctx: ctx} do
      :ok = MemoryStore.seed(store, "other", FakeRevisions.new("chat", ["a", "b"], "a"))
      {:ok, %{plan_digest: local_digest}} = Route.plan(operator(), ctx, "chat", 1, "b")

      assert {:conflict, %{reason: :plan_digest_mismatch}} =
               Route.apply(operator("other"), ctx, "chat", 1, "b", local_digest)
    end

    test "malformed digests are invalid before any store access", %{ctx: ctx} do
      for bad <- [
            nil,
            "",
            "ABC",
            String.duplicate("A", 64),
            String.duplicate("g", 64),
            String.duplicate("a", 63)
          ],
          do: assert({:invalid, :plan_digest} = Route.apply(operator(), ctx, "chat", 1, "b", bad))
    end
  end

  describe "authority (§3)" do
    test "missing permissions are denied", %{ctx: ctx} do
      none = %{operator() | permissions: MapSet.new()}
      assert {:denied, :missing_permission} = Route.read(none, ctx, "chat")
      assert {:denied, :missing_permission} = Route.plan(reader(), ctx, "chat", 1, "b")

      assert {:denied, :missing_permission} =
               Route.apply(reader(), ctx, "chat", 1, "b", String.duplicate("0", 64))

      assert {:ok, _} = Route.read(%{operator() | permissions: [:route_read]}, ctx, "chat")
    end

    test "a principal sees only its own tenant's routes", %{store: store, ctx: ctx} do
      assert :not_found = Route.read(operator("other"), ctx, "chat")
      assert :not_found = Route.plan(operator("other"), ctx, "chat", 1, "b")

      assert :not_found =
               Route.apply(operator("other"), ctx, "chat", 1, "b", String.duplicate("0", 64))

      # Other tenant's same-named route is a distinct record.
      :ok = MemoryStore.seed(store, "other", FakeRevisions.new("chat", ["x", "y"], "x"))
      {:ok, %{plan_digest: d}} = Route.plan(operator("other"), ctx, "chat", 1, "y")
      {:ok, _} = Route.apply(operator("other"), ctx, "chat", 1, "y", d)

      assert {:ok, %{effective_revision: %{rev: 1, preferred: "a"}}} =
               Route.read(operator(), ctx, "chat")
    end

    test "mode must match the context", %{store: store} do
      assert {:denied, :mode_mismatch} =
               Route.read(operator("local", :serving), ctx(store), "chat")

      assert {:denied, :mode_mismatch} =
               Route.plan(operator(), ctx(store, :serving), "chat", 1, "b")
    end

    test "unauthenticated or malformed principals fail closed", %{ctx: ctx} do
      for p <- [
            nil,
            %{},
            %{tenant_id: "local", subject_id: "op", permissions: MapSet.new([:route_read])},
            %{operator() | tenant_id: "bad tenant"},
            %{operator() | tenant_id: ""},
            %{operator() | subject_id: nil},
            %{operator() | mode: :admin},
            %{operator() | permissions: :all}
          ] do
        result = Route.read(p, ctx, "chat")

        assert match?({:denied, _}, result),
               "expected denial for #{inspect(p)}, got #{inspect(result)}"
      end
    end
  end

  describe "validation" do
    test "IDs and revisions are checked", %{ctx: ctx} do
      op = operator()
      assert {:invalid, :route_id} = Route.read(op, ctx, "ch at")
      assert {:invalid, :route_id} = Route.plan(op, ctx, String.duplicate("r", 65), 1, "b")
      assert {:invalid, :expected_rev} = Route.plan(op, ctx, "chat", 0, "b")
      assert {:invalid, :expected_rev} = Route.plan(op, ctx, "chat", "1", "b")
      assert {:invalid, :expected_rev} = Route.plan(op, ctx, "chat", 1.0, "b")
      assert {:invalid, :preferred} = Route.plan(op, ctx, "chat", 1, "b/")
      assert {:invalid, :preferred} = Route.plan(op, ctx, "chat", 1, "é")
    end

    test "unknown deployment is rejected by the revision module", %{ctx: ctx} do
      assert {:invalid, {:revision, :unknown_deployment}} =
               Route.plan(operator(), ctx, "chat", 1, "c")
    end

    test "missing route is not_found", %{ctx: ctx} do
      assert :not_found = Route.read(operator(), ctx, "nope")
    end

    test "a revision module that edits a fixed control is a fault, not a write", %{store: store} do
      ctx = ctx(store, :sandbox, TamperingRevisions)

      assert {:error, {:revision_invariant, :deadline_ms}} =
               Route.plan(operator(), ctx, "chat", 1, "b")

      assert {:error, {:revision_invariant, :deadline_ms}} =
               Route.apply(operator(), ctx, "chat", 1, "b", String.duplicate("0", 64))

      assert {:ok, %{effective_revision: %{rev: 1}}} = Route.read(operator(), ctx(store), "chat")
    end
  end
end

defmodule Telelem.Ops.RouteRaceTest do
  @moduledoc """
  Stale-write races for `Telelem.Ops.Route.apply/6`. Interleavings are forced
  with `GatedStore` (each fetch blocks until released), not with sleeps.
  """
  use ExUnit.Case, async: true

  alias Telelem.Ops.{Context, MemoryStore, Route}
  alias Telelem.OpsTest.{FakeRevisions, GatedStore}
  import Telelem.OpsTest.Fixtures

  setup do
    store = seeded_store()
    ref = make_ref()

    gated = %Context{
      mode: :sandbox,
      store: {GatedStore, {store, self(), ref}},
      revisions: FakeRevisions
    }

    %{store: store, ctx: ctx(store), gated: gated, ref: ref}
  end

  defp await_fetched(ref, n) do
    for _ <- 1..n do
      assert_receive {:fetched, ^ref, pid}, 1000
      pid
    end
  end

  test "two appliers past the read: exactly one writes, the other conflicts", c do
    {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")

    tasks =
      for _ <- 1..2, do: Task.async(fn -> Route.apply(operator(), c.gated, "chat", 1, "b", d) end)

    # Both have read rev 1 before either may write.
    pids = await_fetched(c.ref, 2)
    Enum.each(pids, &send(&1, {:go, c.ref}))
    results = Task.await_many(tasks)

    assert [{:ok, %{effective_revision: %{rev: 2, preferred: "b"}, plan_digest: ^d}}] =
             Enum.filter(results, &match?({:ok, _}, &1))

    assert [
             {:conflict,
              %{reason: :stale_revision, expected_rev: 1, current_rev: 2, last_plan_digest: ^d}}
           ] =
             Enum.filter(results, &match?({:conflict, _}, &1))

    assert {:ok, %{effective_revision: %{rev: 2}, last_plan_digest: ^d}} =
             Route.read(operator(), c.ctx, "chat")
  end

  test "a write landing between read and compare-and-write makes the stale apply conflict (ABA)",
       c do
    {:ok, %{plan_digest: stale}} = Route.plan(operator(), c.ctx, "chat", 1, "b")
    task = Task.async(fn -> Route.apply(operator(), c.gated, "chat", 1, "b", stale) end)
    [pid] = await_fetched(c.ref, 1)

    # While the stale applier holds rev 1, another operator goes a -> b -> a.
    {:ok, %{plan_digest: d2}} = Route.plan(operator(), c.ctx, "chat", 1, "b")
    {:ok, _} = Route.apply(operator(), c.ctx, "chat", 1, "b", d2)
    {:ok, %{plan_digest: d3}} = Route.plan(operator(), c.ctx, "chat", 2, "a")
    {:ok, %{effective_revision: rev3}} = Route.apply(operator(), c.ctx, "chat", 2, "a", d3)
    assert rev3.preferred == "a" and rev3.rev == 3

    send(pid, {:go, c.ref})

    assert {:conflict,
            %{reason: :stale_revision, expected_rev: 1, current_rev: 3, last_plan_digest: ^d3}} =
             Task.await(task)

    assert {:ok, %{effective_revision: ^rev3, last_plan_digest: ^d3}} =
             Route.read(operator(), c.ctx, "chat")
  end

  test "compare-and-write is conditioned on the revision digest, not only rev", %{store: store} do
    {:ok, %{revision: current}} = MemoryStore.fetch(store, "local", "chat")
    {:ok, next} = FakeRevisions.with_preferred(current, "b")
    wrong = %{rev: 1, digest: String.duplicate("0", 64)}

    assert {:conflict, %{revision: ^current}} =
             MemoryStore.compare_and_write(
               store,
               "local",
               "chat",
               wrong,
               next,
               String.duplicate("1", 64)
             )

    assert {:conflict, :not_found} =
             MemoryStore.compare_and_write(
               store,
               "other",
               "chat",
               %{rev: 1, digest: current.digest},
               next,
               "x"
             )

    assert {:ok, %{revision: ^current}} = MemoryStore.fetch(store, "local", "chat")
    assert {:error, :exists} = MemoryStore.seed(store, "local", next)
  end

  test "32 concurrent identical applies: one success", c do
    {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")

    results =
      1..32
      |> Enum.map(fn _ ->
        Task.async(fn -> Route.apply(operator(), c.ctx, "chat", 1, "b", d) end)
      end)
      |> Task.await_many()

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1

    assert Enum.count(
             results,
             &match?({:conflict, %{reason: :stale_revision, current_rev: 2}}, &1)
           ) == 31
  end

  test "contended read-plan-apply loops: successful writes form a gapless revision chain", c do
    workers = 8
    rounds = 25

    outcomes =
      1..workers
      |> Enum.map(fn _ ->
        Task.async(fn ->
          for _ <- 1..rounds do
            {:ok, %{effective_revision: cur}} = Route.read(operator(), c.ctx, "chat")
            target = if cur.preferred == "a", do: "b", else: "a"

            case Route.plan(operator(), c.ctx, "chat", cur.rev, target) do
              {:ok, %{plan_digest: d}} ->
                {d, Route.apply(operator(), c.ctx, "chat", cur.rev, target, d)}

              {:conflict, %{reason: :stale_revision}} = conflict ->
                {nil, conflict}
            end
          end
        end)
      end)
      |> Task.await_many(10_000)
      |> List.flatten()

    wins = for {d, {:ok, %{effective_revision: r, plan_digest: d}}} <- outcomes, do: {r.rev, d, r}
    others = Enum.reject(outcomes, &match?({_, {:ok, _}}, &1))

    assert Enum.all?(others, &match?({_, {:conflict, %{reason: :stale_revision}}}, &1))
    assert length(wins) + length(others) == workers * rounds

    {:ok, %{effective_revision: final, last_plan_digest: last}} =
      Route.read(operator(), c.ctx, "chat")

    revs = wins |> Enum.map(&elem(&1, 0)) |> Enum.sort()

    # Every successful apply produced a distinct revision, with no gaps.
    assert revs == Enum.to_list(2..final.rev)
    {_, ^last, ^final} = Enum.max_by(wins, &elem(&1, 0))

    # Each write was the successor of the one before it.
    by_rev = Map.new(wins, fn {rev, _, r} -> {rev, r} end)

    for rev <- 3..final.rev//1 do
      assert by_rev[rev].preferred != by_rev[rev - 1].preferred
    end
  end
end

defmodule Telelem.Ops.CLITest do
  use ExUnit.Case, async: true

  alias Telelem.Ops.{CLI, Route}
  import Telelem.OpsTest.Fixtures

  setup do
    store = seeded_store()
    %{ctx: ctx(store)}
  end

  defp run(verb, req, ctx, principal \\ operator()) do
    {status, out} = CLI.run([verb, JSON.encode!(req)], principal, ctx)
    {status, JSON.decode!(out)}
  end

  test "contract example over JSON matches the direct handlers", %{ctx: ctx} do
    assert {0, %{"ok" => true, "expected_rev" => 1, "plan_digest" => d, "proposed" => proposed}} =
             run("plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}, ctx)

    # The same handler, called directly, yields the same digest.
    assert {:ok, %{plan_digest: ^d}} = Route.plan(operator(), ctx, "chat", 1, "b")
    assert %{"rev" => 2, "preferred" => "b", "deployments" => ["a", "b"]} = proposed

    assert {0, %{"ok" => true, "plan_digest" => ^d, "effective_revision" => ^proposed}} =
             run(
               "apply",
               %{route_id: "chat", expected_rev: 1, preferred: "b", plan_digest: d},
               ctx
             )

    assert {0, %{"effective_revision" => ^proposed, "last_plan_digest" => ^d}} =
             run("read", %{route_id: "chat"}, ctx, reader())

    assert {1,
            %{
              "error" => "conflict",
              "reason" => "stale_revision",
              "expected_rev" => 1,
              "current_rev" => 2
            }} =
             run(
               "apply",
               %{route_id: "chat", expected_rev: 1, preferred: "b", plan_digest: d},
               ctx
             )
  end

  test "tenant (or any extra field) in the request is rejected, never trusted", %{ctx: ctx} do
    assert {2, %{"error" => "invalid", "field" => "tenant_id", "reason" => "unknown_field"}} =
             run("read", %{route_id: "chat", tenant_id: "other"}, ctx)

    assert {2, %{"field" => "principal"}} =
             run(
               "plan",
               %{route_id: "chat", expected_rev: 1, preferred: "b", principal: %{}},
               ctx
             )

    # The authenticated principal alone decides the tenant.
    assert {4, %{"error" => "not_found"}} =
             run("read", %{route_id: "chat"}, ctx, operator("other"))
  end

  test "errors map to stable exit statuses", %{ctx: ctx} do
    assert {3, %{"error" => "denied", "reason" => "missing_permission"}} =
             run("plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}, ctx, reader())

    assert {2, %{"field" => "expected_rev"}} =
             run("plan", %{route_id: "chat", expected_rev: "1", preferred: "b"}, ctx)

    assert {2, %{"field" => "expected_rev"}} =
             run("plan", %{route_id: "chat", expected_rev: 1.0, preferred: "b"}, ctx)

    assert {2, %{"field" => "preferred", "reason" => "unknown_deployment"}} =
             run("plan", %{route_id: "chat", expected_rev: 1, preferred: "c"}, ctx)

    assert {2, %{"field" => "plan_digest", "reason" => "missing_field"}} =
             run("apply", %{route_id: "chat", expected_rev: 1, preferred: "b"}, ctx)

    assert {1, %{"reason" => "plan_digest_mismatch"}} =
             run(
               "apply",
               %{
                 route_id: "chat",
                 expected_rev: 1,
                 preferred: "b",
                 plan_digest: String.duplicate("0", 64)
               },
               ctx
             )

    assert {2, %{"field" => "json"}} = decode(CLI.run(["read", "{"], operator(), ctx))
    assert {2, %{"field" => "json"}} = decode(CLI.run(["read", "[]"], operator(), ctx))

    assert {2, %{"field" => "too_large"}} =
             decode(CLI.run(["read", String.duplicate(" ", 5000)], operator(), ctx))

    assert {2, %{"error" => "usage"}} = decode(CLI.run(["delete", "{}"], operator(), ctx))
    assert {2, %{"error" => "usage"}} = decode(CLI.run([], operator(), ctx))
  end

  defp decode({status, out}), do: {status, JSON.decode!(out)}
end

defmodule Telelem.Ops.StoreFaultTest do
  @moduledoc """
  Store call failures and store-integrity faults. Lost replies and broken
  stores are simulated with `ScriptedStore`; nothing sleeps.
  """
  use ExUnit.Case, async: true

  alias Telelem.Ops.{CLI, Context, MemoryStore, Route}
  alias Telelem.OpsTest.{FakeRevisions, ScriptedStore}
  import Telelem.OpsTest.Fixtures

  @secret "s3cr3t-reason-do-not-echo"
  @lost_reply {:timeout, {GenServer, :call, [:store, @secret, 5000]}}

  setup do
    store = seeded_store()
    %{store: store, ctx: ctx(store)}
  end

  defp scripted(store, script),
    do: %Context{
      mode: :sandbox,
      store: {ScriptedStore, Map.put(script, :server, store)},
      revisions: FakeRevisions
    }

  defp cli(verb, req, ctx) do
    {status, out} = CLI.run([verb, JSON.encode!(req)], operator(), ctx)
    refute out =~ @secret
    refute out =~ "timeout"
    {status, JSON.decode!(out)}
  end

  defp apply_req(d), do: %{route_id: "chat", expected_rev: 1, preferred: "b", plan_digest: d}

  defp corrupt!(store, fun) do
    # Overwrite the stored record behind the store's API (test-only).
    :sys.replace_state(store, fn state -> Map.update!(state, {"local", "chat"}, fun) end)
  end

  describe "compare-and-write with no reply" do
    test "committed write, lost reply: outcome unknown, read-back confirms, re-apply conflicts",
         c do
      {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")
      lossy = scripted(c.store, %{cas: fn real -> real.() && exit(@lost_reply) end})

      assert {:error,
              {:apply_outcome_unknown,
               %{cause: :store_unavailable, expected_rev: 1, plan_digest: ^d}}} =
               Route.apply(operator(), lossy, "chat", 1, "b", d)

      # Read back (§4): last_plan_digest matches, so the plan is applied.
      assert {:ok, %{effective_revision: %{rev: 2, preferred: "b"}, last_plan_digest: ^d}} =
               Route.read(operator(), c.ctx, "chat")

      # A blind reissue cannot double-apply; it conflicts against its own write.
      assert {:conflict, %{reason: :stale_revision, current_rev: 2, last_plan_digest: ^d}} =
               Route.apply(operator(), c.ctx, "chat", 1, "b", d)
    end

    test "same over the CLI: exit 5, bounded body, then read-back", c do
      {0, %{"plan_digest" => d}} =
        cli("plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}, c.ctx)

      lossy = scripted(c.store, %{cas: fn real -> real.() && exit(@lost_reply) end})

      assert {5, body} = cli("apply", apply_req(d), lossy)

      assert body == %{
               "ok" => false,
               "op" => "apply",
               "error" => "apply_outcome_unknown",
               "cause" => "store_unavailable",
               "expected_rev" => 1,
               "plan_digest" => d,
               "resolve" => "read_and_compare_last_plan_digest"
             }

      assert {0, %{"last_plan_digest" => ^d, "effective_revision" => %{"rev" => 2}}} =
               cli("read", %{route_id: "chat"}, c.ctx)
    end

    test "exit, raise or throw before the write commits: still unknown, never failed", c do
      {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")

      for cas <- [
            fn _ -> exit({:noproc, @secret}) end,
            fn _ -> raise RuntimeError, @secret end,
            fn _ -> throw(@secret) end
          ] do
        assert {:error, {:apply_outcome_unknown, %{cause: :store_unavailable, plan_digest: ^d}}} =
                 Route.apply(operator(), scripted(c.store, %{cas: cas}), "chat", 1, "b", d)
      end

      # Here it did not land; read-back shows a mismatch and rev 1.
      assert {:ok, %{effective_revision: %{rev: 1}, last_plan_digest: nil}} =
               Route.read(operator(), c.ctx, "chat")

      # A fresh, explicit apply from the operator then succeeds.
      assert {:ok, %{effective_revision: %{rev: 2}}} =
               Route.apply(operator(), c.ctx, "chat", 1, "b", d)
    end

    test "a real GenServer exit (store gone) during compare-and-write is unknown", c do
      {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")
      stop = scripted(c.store, %{cas: fn real -> GenServer.stop(c.store) && real.() end})

      assert {5, %{"error" => "apply_outcome_unknown", "cause" => "store_unavailable"}} =
               cli("apply", apply_req(d), stop)
    end

    test "malformed or inconsistent replies are unknown with cause store_integrity", c do
      {:ok, %{plan_digest: d}} = Route.plan(operator(), c.ctx, "chat", 1, "b")
      {:ok, %{revision: rev1} = rec1} = MemoryStore.fetch(c.store, "local", "chat")

      replies = [
        :ok,
        {:ok, @secret},
        {:ok, %{revision: %{rev: 2}, last_plan_digest: d}},
        # Committed something other than this proposal.
        {:ok, %{rec1 | last_plan_digest: d}},
        {:conflict, @secret},
        # A "conflict" that reports exactly the expected revision.
        {:conflict, rec1},
        {:conflict, %{rec1 | revision: %{rev1 | route_id: "other"}}}
      ]

      for reply <- replies do
        ctx = scripted(c.store, %{cas: fn _ -> reply end})

        assert {:error,
                {:apply_outcome_unknown,
                 %{cause: :store_integrity, expected_rev: 1, plan_digest: ^d}}} =
                 Route.apply(operator(), ctx, "chat", 1, "b", d),
               inspect(reply)

        assert {5, %{"cause" => "store_integrity"}} = cli("apply", apply_req(d), ctx)
      end
    end
  end

  describe "fetch failures" do
    test "exit, raise or throw while fetching is a bounded store_unavailable", c do
      for fetch <- [
            fn _ -> exit(@lost_reply) end,
            fn _ -> raise ArgumentError, @secret end,
            fn _ -> throw(@secret) end
          ] do
        ctx = scripted(c.store, %{fetch: fetch, cas: fn _ -> flunk("must not write") end})
        assert {:error, :store_unavailable} = Route.read(operator(), ctx, "chat")
        assert {:error, :store_unavailable} = Route.plan(operator(), ctx, "chat", 1, "b")

        assert {:error, :store_unavailable} =
                 Route.apply(operator(), ctx, "chat", 1, "b", String.duplicate("0", 64))

        for {verb, req} <- [
              {"read", %{route_id: "chat"}},
              {"plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}},
              {"apply", apply_req(String.duplicate("0", 64))}
            ] do
          assert {69, body} = cli(verb, req, ctx)

          assert body == %{
                   "ok" => false,
                   "op" => verb,
                   "error" => "unavailable",
                   "reason" => "store_unavailable"
                 }
        end
      end
    end

    test "a stopped MemoryStore (real noproc exit) is store_unavailable", c do
      :ok = GenServer.stop(c.store)
      assert {:error, :store_unavailable} = Route.read(operator(), c.ctx, "chat")
      assert {69, _} = cli("plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}, c.ctx)
    end
  end

  describe "store integrity" do
    defp assert_integrity(ctx) do
      assert {:error, :store_integrity} = Route.read(operator(), ctx, "chat")
      assert {:error, :store_integrity} = Route.plan(operator(), ctx, "chat", 1, "b")

      assert {:error, :store_integrity} =
               Route.apply(operator(), ctx, "chat", 1, "b", String.duplicate("0", 64))

      for {verb, req} <- [
            {"read", %{route_id: "chat"}},
            {"plan", %{route_id: "chat", expected_rev: 1, preferred: "b"}},
            {"apply", apply_req(String.duplicate("0", 64))}
          ] do
        {status, out} = CLI.run([verb, JSON.encode!(req)], operator(), ctx)
        assert status == 70
        refute out =~ @secret

        assert JSON.decode!(out) == %{
                 "ok" => false,
                 "op" => verb,
                 "error" => "internal",
                 "reason" => "store_integrity"
               }
      end
    end

    test "a stored revision whose digest no longer matches (tampered field)", c do
      corrupt!(c.store, fn r -> put_in(r.revision.deadline_ms, 1) end)
      assert_integrity(c.ctx)
      assert_integrity(c.ctx)
    end

    test "stored preferred outside deployments is not a caller preferred error", c do
      corrupt!(c.store, fn r ->
        %{r | revision: FakeRevisions.rehash(%{r.revision | preferred: "z"})}
      end)

      assert_integrity(c.ctx)
    end

    test "malformed stored records or fetch results", c do
      {:ok, good} = MemoryStore.fetch(c.store, "local", "chat")
      rev = good.revision

      results = [
        :error,
        {:ok, @secret},
        {:ok, %{revision: rev}},
        {:ok, %{good | last_plan_digest: @secret}},
        {:ok, %{good | revision: %{rev | rev: "1"}}},
        {:ok, %{good | revision: %{rev | route_id: "other"}}},
        {:ok, %{good | revision: %{rev | deployments: ["a" | "b"]}}},
        {:ok, %{good | revision: %{rev | preferred: @secret <> " "}}},
        {:ok, %{good | revision: %{rev | max_attempts: 3}}},
        {:ok, %{good | revision: %{rev | digest: String.upcase(rev.digest)}}},
        {:ok, %{good | revision: Map.delete(rev, :fallback_on_refusal)}}
      ]

      for result <- results do
        ctx = scripted(c.store, %{fetch: fn _ -> result end, cas: fn _ -> flunk("no write") end})
        assert_integrity(ctx)
      end

      # The real store was never touched.
      assert {:ok, ^good} = MemoryStore.fetch(c.store, "local", "chat")
    end
  end
end
