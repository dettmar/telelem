defmodule Telelem.Receipts.EventTest do
  @moduledoc """
  M2a §2 receipt event: strict payload-free shape, Id rule, canonical
  encoding and the 512-byte ceiling.

  Rerun: scripts/mix test test/telelem/receipts --seed 0 --warnings-as-errors
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Telelem.Receipts.Event

  @marker "SYNTHETIC-MARKER-7f3a"

  # Contract example.
  @example %{
    tenant_id: "local",
    request_id: "r1",
    route_rev: 1,
    attempt: 1,
    deployment_id: "a",
    at_ms: 12,
    kind: :connect_refused,
    outcome: :none,
    exposure: :none
  }

  test "contract example validates and has the canonical encoding" do
    assert {:ok, %Event{} = e} = Event.new(@example)

    assert Event.encode(e) ==
             ~s(["telelem.receipt.event/1","local","r1",1,1,"a",12,"connect_refused","none","none"])

    assert {:ok, ^e} = Event.new(Map.to_list(@example))
    assert {:ok, ^e} = Event.new(e)
  end

  test "deployment :none encodes as null, distinct from a deployment named \"none\"" do
    {:ok, none} = Event.new(%{@example | deployment_id: :none, attempt: 0, kind: :ingress})
    {:ok, named} = Event.new(%{@example | deployment_id: "none"})
    assert Event.encode(none) =~ ~s(,0,null,12,)
    assert Event.encode(named) =~ ~s(,1,"none",12,)
  end

  test "largest valid event stays within the 512-byte ceiling" do
    id = String.duplicate("a", 64)

    {:ok, e} =
      Event.new(%{
        @example
        | tenant_id: id,
          request_id: id,
          deployment_id: id,
          route_rev: 0x7FFFFFFFFFFFFFFF,
          at_ms: -0x8000000000000000,
          kind: :connect_refused,
          outcome: :incomplete,
          exposure: :possible
      })

    assert byte_size(Event.encode(e)) <= 512
  end

  test "free-form or malformed fields are rejected without echoing the value" do
    cases = [
      {Map.put(@example, :message, @marker), {:invalid, :shape}},
      {Map.put(@example, "body", @marker), {:invalid, :shape}},
      {Map.delete(@example, :exposure), {:invalid, :shape}},
      {[{:tenant_id, "local"} | Map.to_list(@example)], {:invalid, :shape}},
      {%{@example | tenant_id: @marker <> "/x"}, {:invalid, :tenant_id}},
      {%{@example | tenant_id: "https://" <> @marker}, {:invalid, :tenant_id}},
      {%{@example | request_id: String.duplicate("r", 65)}, {:invalid, :request_id}},
      {%{@example | request_id: ""}, {:invalid, :request_id}},
      {%{@example | request_id: "rü"}, {:invalid, :request_id}},
      {%{@example | request_id: :r1}, {:invalid, :request_id}},
      {%{@example | route_rev: 0}, {:invalid, :route_rev}},
      {%{@example | route_rev: 1.0}, {:invalid, :route_rev}},
      {%{@example | attempt: 3}, {:invalid, :attempt}},
      {%{@example | deployment_id: nil}, {:invalid, :deployment_id}},
      {%{@example | deployment_id: "Bearer " <> @marker}, {:invalid, :deployment_id}},
      {%{@example | at_ms: 0x8000000000000000}, {:invalid, :at_ms}},
      {%{@example | kind: "ingress"}, {:invalid, :kind}},
      {%{@example | kind: :retry}, {:invalid, :kind}},
      {%{@example | outcome: :ok}, {:invalid, :outcome}},
      {%{@example | exposure: :maybe}, {:invalid, :exposure}},
      {%{@example | kind: :terminal, outcome: :none}, {:invalid, :outcome}},
      {@marker, {:invalid, :shape}},
      {%URI{host: @marker}, {:invalid, :shape}}
    ]

    for {input, reason} <- cases do
      assert {:error, ^reason} = Event.new(input), inspect(input)
      refute inspect(reason) =~ @marker
    end
  end

  test "negative monotonic time is valid" do
    assert {:ok, %Event{at_ms: -5}} = Event.new(%{@example | at_ms: -5})
  end

  property "new/1 never raises and only returns an event or a fixed reason" do
    value = one_of([term(), member_of([:none, :ingress, :terminal, :done, :possible, "a", 1, 0])])

    full =
      Event.fields()
      |> Map.new(&{&1, value})
      |> fixed_map()

    check all(
            input <- one_of([term(), full]),
            drop <- list_of(member_of(Event.fields()), max_length: 2)
          ) do
      input = if is_map(input), do: Map.drop(input, drop), else: input

      case Event.new(input) do
        {:ok, %Event{} = e} -> assert byte_size(Event.encode(e)) <= 512
        {:error, {:invalid, field}} -> assert is_atom(field)
      end
    end
  end
end
