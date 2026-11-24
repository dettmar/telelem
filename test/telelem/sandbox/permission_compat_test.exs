defmodule Telelem.Sandbox.PermissionCompatTest do
  @moduledoc """
  The sandbox `Principal` enum is the one every consumer checks: a struct
  from `Principal.new/4` is accepted unchanged by `Telelem.Ops.Route` and
  `Telelem.Receipts.Store`, and each permission unlocks exactly its own
  boundary. Read-only use of those modules; no data is written.
  """
  use ExUnit.Case, async: true

  alias Telelem.Ops.{Context, MemoryStore, Route}
  alias Telelem.Receipts.Store
  alias Telelem.Sandbox.Principal

  @digest String.duplicate("0", 64)

  setup do
    {:ok, routes} = MemoryStore.start_link()
    {:ok, receipts} = Store.start_link(mode: :sandbox)

    ctx = %Context{
      mode: :sandbox,
      store: {MemoryStore, routes},
      revisions: Telelem.Route.Revision
    }

    %{ctx: ctx, receipts: receipts}
  end

  defp p(perms) do
    {:ok, p} = Principal.new("local", "operator", :sandbox, perms)
    p
  end

  test "enum is exactly dispatch, route_read, route_write, receipt_read" do
    assert Enum.sort(Principal.permissions()) ==
             Enum.sort([:dispatch, :route_read, :route_write, :receipt_read])

    for old <- [:route_plan, :route_apply, :admin] do
      assert {:error, :invalid_principal} = Principal.new("local", "op", :sandbox, [old])
    end
  end

  test "route_write admits plan/apply; route_read admits read", %{ctx: ctx} do
    w = p([:route_write])
    r = p([:route_read])

    # Authorization passes, then the (absent) route is not found.
    assert :not_found = Route.plan(w, ctx, "chat", 1, "b")
    assert :not_found = Route.apply(w, ctx, "chat", 1, "b", @digest)
    assert :not_found = Route.read(r, ctx, "chat")

    assert {:denied, :missing_permission} = Route.plan(r, ctx, "chat", 1, "b")
    assert {:denied, :missing_permission} = Route.apply(r, ctx, "chat", 1, "b", @digest)
    assert {:denied, :missing_permission} = Route.read(w, ctx, "chat")

    for other <- [[:dispatch], [:receipt_read], []] do
      assert {:denied, :missing_permission} = Route.read(p(other), ctx, "chat")
      assert {:denied, :missing_permission} = Route.plan(p(other), ctx, "chat", 1, "b")
    end
  end

  test "receipt_read admits receipt lookup; nothing else does", %{receipts: s} do
    assert {:error, :not_found} = Store.lookup(s, p([:receipt_read]), "r1")

    for other <- [[:dispatch], [:route_read, :route_write], []] do
      assert {:error, {:denied, :missing_permission}} = Store.lookup(s, p(other), "r1")
    end
  end

  test "the full operator set is accepted at every boundary", %{ctx: ctx, receipts: s} do
    op = p(Principal.permissions())
    assert :not_found = Route.read(op, ctx, "chat")
    assert :not_found = Route.plan(op, ctx, "chat", 1, "b")
    assert {:error, :not_found} = Store.lookup(s, op, "r1")
  end
end
