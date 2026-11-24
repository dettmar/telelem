defmodule Telelem.Receipts.ReceiptTest do
  @moduledoc """
  M2a §2 read semantics of `Receipt.derive/3` against an independent oracle:
  sequence order (not input order), complete only for exactly `1..n` with an
  ingress at one and the single terminal at `n`, and completeness separate
  from the observed outcome.

  Rerun: scripts/mix test test/telelem/receipts --seed 0 --warnings-as-errors
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Telelem.Receipts.{Event, Receipt}

  @clean %{lost?: false, evicted?: false, writer_conflict?: false}
  @kinds [:ingress, :dispatch, :connect_refused, :commit, :terminal, :authority_down]

  defp event(kind, outcome \\ :done) do
    {:ok, e} =
      Event.new(%{
        tenant_id: "local",
        request_id: "r1",
        route_rev: 1,
        attempt: 1,
        deployment_id: "a",
        at_ms: 0,
        kind: kind,
        outcome: if(kind == :terminal, do: outcome, else: :none),
        exposure: :none
      })

    e
  end

  defp derive(records, ctx \\ @clean), do: Receipt.derive({"local", "r1"}, records, ctx)

  test "reads in sequence order, not input order" do
    records = [{3, 0, event(:terminal)}, {1, 0, event(:ingress)}, {2, 0, event(:dispatch)}]
    r = derive(records)
    assert r.completeness == :complete
    assert Enum.map(r.events, & &1.kind) == [:ingress, :dispatch, :terminal]
  end

  test "each context flag alone makes evidence incomplete" do
    records = [{1, 0, event(:ingress)}, {2, 0, event(:terminal)}]
    assert derive(records).completeness == :complete

    for {flag, gap} <- [lost?: :loss, evicted?: :evicted, writer_conflict?: :writer_conflict] do
      r = derive(records, %{@clean | flag => true})
      assert r.gaps == [gap]
      assert r.observed_outcome == :done
      assert r.exposure == :unknown
    end
  end

  property "complete exactly when the oracle says so; no terminal is never complete" do
    record = tuple({integer(1..6), member_of([0, 0, 0, 1]), member_of(@kinds)})
    outcome = member_of([:done, :incomplete, :failed])

    check all raw <- list_of(record, max_length: 7),
              outcomes <- list_of(outcome, length: length(raw)),
              flags <- list_of(boolean(), length: 3),
              max_runs: 2_000 do
      records =
        Enum.zip_with(raw, outcomes, fn {seq, d, kind}, o -> {seq, d, event(kind, o)} end)

      [lost, evicted, conflict] = flags
      ctx = %{lost?: lost, evicted?: evicted, writer_conflict?: conflict}
      r = derive(records, ctx)

      sorted = Enum.sort_by(records, &elem(&1, 0))
      kinds = Enum.map(sorted, &elem(&1, 2).kind)
      n = length(sorted)

      oracle =
        n >= 2 and Enum.map(sorted, &elem(&1, 0)) == Enum.to_list(1..n) and
          hd(kinds) == :ingress and Enum.count(kinds, &(&1 == :ingress)) == 1 and
          List.last(kinds) == :terminal and Enum.count(kinds, &(&1 == :terminal)) == 1 and
          Enum.all?(sorted, &(elem(&1, 1) == 0)) and not (lost or evicted or conflict)

      complete? = r.completeness == :complete
      no_gaps? = r.gaps == []
      assert complete? == oracle
      assert no_gaps? == oracle
      assert Enum.all?(r.gaps, &(&1 in Receipt.gaps()))
      assert r.usage == :unknown
      if not oracle, do: assert(r.exposure == :unknown)
      if :terminal not in kinds, do: assert(r.completeness == :incomplete)

      expected_outcome =
        case Enum.find(sorted, &(elem(&1, 2).kind == :terminal)) do
          nil -> :not_observed
          {_, _, e} -> e.outcome
        end

      assert r.observed_outcome == expected_outcome
      assert Enum.map(r.events, & &1.kind) == kinds
    end
  end
end
