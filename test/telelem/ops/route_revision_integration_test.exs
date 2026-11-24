defmodule Telelem.Ops.RouteRevisionIntegrationTest.HookStore do
  @moduledoc """
  `MemoryStore` wrapper whose compare-and-write calls `cas.(real)` with a
  thunk for the real call, so a test can interleave a write or replace the
  reply. Each compare-and-write is reported to `test` as `:cas_called`.
  """
  @behaviour Telelem.Ops.RevisionStore
  alias Telelem.Ops.MemoryStore

  @impl true
  def fetch(%{server: s}, tenant, route), do: MemoryStore.fetch(s, tenant, route)

  @impl true
  def compare_and_write(%{server: s, cas: cas, test: test}, tenant, route, expected, new, d) do
    send(test, :cas_called)
    cas.(fn -> MemoryStore.compare_and_write(s, tenant, route, expected, new, d) end)
  end
end

defmodule Telelem.Ops.RouteRevisionIntegrationTest do
  @moduledoc """
  Ops handlers with A1's `Telelem.Route.Revision` injected directly (it
  satisfies `Telelem.Ops.Revisions` structurally). Ops computes no §1 digest;
  these checks confirm what it stores verifies under A1's own rules.
  """
  use ExUnit.Case, async: true

  alias Telelem.Ops.{CLI, Context, MemoryStore, Route}
  alias Telelem.Ops.RouteRevisionIntegrationTest.HookStore
  alias Telelem.Route.Revision

  @op %{
    tenant_id: "local",
    subject_id: "operator",
    mode: :sandbox,
    permissions: MapSet.new([:route_read, :route_write])
  }

  setup do
    {:ok, store} = MemoryStore.start_link()

    rev1 =
      Revision.new!(
        route_id: "chat",
        rev: 1,
        deployments: ["a", "b"],
        preferred: "a",
        fallback_on_refusal: true,
        deadline_ms: 5000,
        max_attempts: 2
      )

    :ok = MemoryStore.seed(store, "local", rev1)

    %{
      rev1: rev1,
      store: store,
      ctx: %Context{mode: :sandbox, store: {MemoryStore, store}, revisions: Revision}
    }
  end

  test "contract example: prefer b against rev 1, read back rev 2", %{ctx: ctx, rev1: rev1} do
    {:ok, %{plan_digest: d, proposed: proposed}} = Route.plan(@op, ctx, "chat", 1, "b")
    assert %Revision{rev: 2, preferred: "b"} = proposed
    assert :ok = Revision.verify(proposed)
    assert proposed.digest != rev1.digest

    assert {:ok, %{effective_revision: ^proposed, plan_digest: ^d}} =
             Route.apply(@op, ctx, "chat", 1, "b", d)

    assert {:ok, %{effective_revision: %Revision{rev: 2}, last_plan_digest: ^d}} =
             Route.read(@op, ctx, "chat")

    assert {:conflict, %{reason: :stale_revision, current_rev: 2}} =
             Route.apply(@op, ctx, "chat", 1, "b", d)
  end

  test "A1 rejections surface as invalid", %{ctx: ctx} do
    assert {:invalid, {:revision, :unknown_deployment}} = Route.plan(@op, ctx, "chat", 1, "c")

    assert {2, out} =
             CLI.run(["plan", ~s({"route_id":"chat","expected_rev":1,"preferred":"c"})], @op, ctx)

    assert %{"field" => "preferred", "reason" => "unknown_deployment"} = JSON.decode!(out)
  end

  test "a tampered stored A1 revision is a store-integrity fault, not a preferred error", %{
    ctx: ctx,
    rev1: rev1
  } do
    {:ok, store} = MemoryStore.start_link()
    # Changed control, stale digest: A1 reports :digest_mismatch.
    :ok = MemoryStore.seed(store, "local", %{rev1 | deadline_ms: 1})
    ctx = %{ctx | store: {MemoryStore, store}}
    assert_integrity(ctx)

    # Digest consistent but preferred outside deployments (A1:
    # :preferred_not_in_deployments); the test, not ops, computes the digest.
    {:ok, store} = MemoryStore.start_link()
    bad = %{rev1 | preferred: "z"}
    digest = :crypto.hash(:sha256, Revision.digest_input(bad)) |> Base.encode16(case: :lower)
    :ok = MemoryStore.seed(store, "local", %{bad | digest: digest})
    assert_integrity(%{ctx | store: {MemoryStore, store}})
  end

  defp assert_integrity(ctx) do
    assert {:error, :store_integrity} = Route.read(@op, ctx, "chat")
    assert {:error, :store_integrity} = Route.plan(@op, ctx, "chat", 1, "b")

    for {verb, req} <- [
          {"read", ~s({"route_id":"chat"})},
          {"plan", ~s({"route_id":"chat","expected_rev":1,"preferred":"b"})}
        ] do
      assert {70, out} = CLI.run([verb, req], @op, ctx)

      assert JSON.decode!(out) == %{
               "ok" => false,
               "op" => verb,
               "error" => "internal",
               "reason" => "store_integrity"
             }
    end
  end

  test "same preferred advances rev via A1", %{ctx: ctx} do
    {:ok, %{plan_digest: d}} = Route.plan(@op, ctx, "chat", 1, "a")

    assert {:ok, %{effective_revision: %Revision{rev: 2, preferred: "a"} = r}} =
             Route.apply(@op, ctx, "chat", 1, "a", d)

    assert :ok = Revision.verify(r)
  end

  describe "post-CAS conflict record under A1's contract" do
    defp hooked(c, cas),
      do: %{c.ctx | store: {HookStore, %{server: c.store, cas: cas, test: self()}}}

    test "a genuine concurrent write is a clean stale revision", c do
      {:ok, %{plan_digest: d}} = Route.plan(@op, c.ctx, "chat", 1, "b")
      {:ok, %{plan_digest: other}} = Route.plan(@op, c.ctx, "chat", 1, "a")

      # Another operator's apply lands after this one read rev 1.
      race = fn real ->
        {:ok, _} = Route.apply(@op, c.ctx, "chat", 1, "a", other)
        real.()
      end

      assert {:conflict,
              %{
                reason: :stale_revision,
                expected_rev: 1,
                current_rev: 2,
                last_plan_digest: ^other
              }} =
               Route.apply(@op, hooked(c, race), "chat", 1, "b", d)

      assert_received :cas_called
      refute_received :cas_called
    end

    test "a conflict record whose A1 digest does not verify is outcome unknown", c do
      {:ok, %{plan_digest: d, proposed: rev2}} = Route.plan(@op, c.ctx, "chat", 1, "b")
      decoy = String.duplicate("e", 64)

      for revision <- [
            # Control changed after hashing (A1: :digest_mismatch).
            %{rev2 | deadline_ms: 1},
            # Digest swapped for another well-formed one.
            %{rev2 | digest: decoy},
            # Same as a plain map, as a decoding store might return it.
            Map.from_struct(%{rev2 | max_attempts: 1})
          ] do
        reply = {:conflict, %{revision: revision, last_plan_digest: decoy}}
        ctx = hooked(c, fn _real -> reply end)

        assert {:error,
                {:apply_outcome_unknown,
                 %{cause: :store_integrity, expected_rev: 1, plan_digest: ^d}}} =
                 Route.apply(@op, ctx, "chat", 1, "b", d)

        assert_received :cas_called
        refute_received :cas_called

        req = JSON.encode!(%{route_id: "chat", expected_rev: 1, preferred: "b", plan_digest: d})
        assert {5, out} = CLI.run(["apply", req], @op, ctx)
        assert_received :cas_called
        refute_received :cas_called
        refute out =~ decoy
        assert %{"cause" => "store_integrity"} = body = JSON.decode!(out)
        refute Map.has_key?(body, "current_rev")
      end

      # Nothing was written by these applies.
      assert {:ok, %{effective_revision: %Revision{rev: 1}, last_plan_digest: nil}} =
               Route.read(@op, c.ctx, "chat")
    end
  end
end
