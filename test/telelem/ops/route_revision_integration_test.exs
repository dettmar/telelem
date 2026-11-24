defmodule Telelem.Ops.RouteRevisionIntegrationTest do
  @moduledoc """
  Ops handlers with A1's `Telelem.Route.Revision` injected directly (it
  satisfies `Telelem.Ops.Revisions` structurally). Ops computes no §1 digest;
  these checks confirm what it stores verifies under A1's own rules.
  """
  use ExUnit.Case, async: true

  alias Telelem.Ops.{CLI, Context, MemoryStore, Route}
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
    %{rev1: rev1, ctx: %Context{mode: :sandbox, store: {MemoryStore, store}, revisions: Revision}}
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

  test "same preferred advances rev via A1", %{ctx: ctx} do
    {:ok, %{plan_digest: d}} = Route.plan(@op, ctx, "chat", 1, "a")

    assert {:ok, %{effective_revision: %Revision{rev: 2, preferred: "a"} = r}} =
             Route.apply(@op, ctx, "chat", 1, "a", d)

    assert :ok = Revision.verify(r)
  end
end
