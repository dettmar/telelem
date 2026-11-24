defmodule Telelem.Route.SelectorTest do
  @moduledoc """
  M2a §1: pure route selection over one stream and two lanes.

  Rerun: scripts/mix test test/telelem/route --seed <seed>
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Telelem.Route.{Plan, Revision, Selector}

  @example [
    route_id: "chat",
    rev: 1,
    deployments: ["a", "b"],
    preferred: "a",
    fallback_on_refusal: true,
    deadline_ms: 5000,
    max_attempts: 2
  ]

  defp rev(overrides \\ []), do: Revision.new!(Keyword.merge(@example, overrides))

  test "contract example: both eligible → primary a, backup b" do
    r = rev()

    assert Selector.select(r, MapSet.new(["a", "b"]), 1_000) ==
             {:ok,
              %Plan{
                rev: 1,
                digest: r.digest,
                primary: "a",
                backup: "b",
                absolute_deadline_ms: 6_000,
                max_attempts: 2
              }}
  end

  test "only preferred eligible → no backup, one attempt" do
    assert {:ok, %Plan{primary: "a", backup: :none, max_attempts: 1}} =
             Selector.select(rev(), MapSet.new(["a"]), 0)
  end

  test "preferred ineligible → other lane is primary without backup" do
    assert {:ok, %Plan{primary: "b", backup: :none, max_attempts: 1}} =
             Selector.select(rev(), MapSet.new(["b"]), 0)
  end

  test "preferred second in deployment order" do
    assert {:ok, %Plan{primary: "b", backup: "a"}} =
             Selector.select(rev(preferred: "b"), MapSet.new(["a", "b"]), 0)
  end

  test "fallback disabled or single attempt → no backup" do
    both = MapSet.new(["a", "b"])

    assert {:ok, %Plan{primary: "a", backup: :none, max_attempts: 1}} =
             Selector.select(rev(fallback_on_refusal: false), both, 0)

    assert {:ok, %Plan{primary: "a", backup: :none, max_attempts: 1}} =
             Selector.select(rev(max_attempts: 1), both, 0)
  end

  test "no eligible deployment" do
    for eligible <- [MapSet.new(), MapSet.new(["c", "A"]), []] do
      assert Selector.select(rev(), eligible, 0) == {:error, :no_eligible_deployment}
    end
  end

  test "foreign eligible IDs are ignored; list input equals set input" do
    r = rev()
    assert Selector.select(r, ["z", "b", "a"], 7) == Selector.select(r, MapSet.new(["a", "b"]), 7)
  end

  test "deadline uses caller's monotonic timebase, including negative values" do
    now = System.monotonic_time(:millisecond) - 10_000_000_000

    assert {:ok, %Plan{absolute_deadline_ms: deadline}} =
             Selector.select(rev(deadline_ms: 250), ["a"], now)

    assert deadline == now + 250
  end

  test "rejects non-revision input" do
    assert_raise FunctionClauseError, fn -> Selector.select(Map.new(@example), ["a"], 0) end
  end

  test "plan holds no pids, ports or references" do
    {:ok, plan} = Selector.select(rev(), ["a", "b"], 0)

    refute plan
           |> Map.from_struct()
           |> Map.values()
           |> Enum.any?(&(is_pid(&1) or is_port(&1) or is_reference(&1)))
  end

  property "plan invariants over all revisions and eligibility sets" do
    id = StreamData.member_of(["a", "b", "x-1", "Y_2"])

    check all [d1, d2] <- StreamData.uniq_list_of(id, length: 2),
              preferred <- StreamData.member_of([d1, d2]),
              fallback <- StreamData.boolean(),
              deadline_ms <- StreamData.positive_integer(),
              max_attempts <- StreamData.member_of([1, 2]),
              eligible <- StreamData.list_of(StreamData.member_of([d1, d2, "zz"])),
              now <- StreamData.integer() do
      r =
        rev(
          deployments: [d1, d2],
          preferred: preferred,
          fallback_on_refusal: fallback,
          deadline_ms: deadline_ms,
          max_attempts: max_attempts
        )

      set = MapSet.new(eligible)
      [other] = [d1, d2] -- [preferred]

      case Selector.select(r, set, now) do
        {:error, :no_eligible_deployment} ->
          refute MapSet.member?(set, d1) or MapSet.member?(set, d2)

        {:ok, %Plan{} = p} ->
          assert p.rev == r.rev and p.digest == r.digest
          assert p.absolute_deadline_ms == now + deadline_ms
          assert MapSet.member?(set, p.primary)
          assert p.primary == preferred or not MapSet.member?(set, preferred)

          if p.backup == :none do
            assert p.max_attempts == 1
          else
            assert p.primary == preferred and p.backup == other
            assert MapSet.member?(set, other) and fallback and max_attempts == 2
            assert p.max_attempts == 2
          end

          # Selection is deterministic.
          assert Selector.select(r, set, now) == {:ok, p}
      end
    end
  end
end
