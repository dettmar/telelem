defmodule Telelem.Route.RevisionTest do
  @moduledoc """
  M2a §1: route revision validation and digest.

  Rerun: scripts/mix test test/telelem/route --seed <seed>
  """
  use ExUnit.Case, async: true

  alias Telelem.Route.Revision

  # Contract example: chat, rev 1, [a,b], preferred a, true, 5000ms, 2.
  @example [
    route_id: "chat",
    rev: 1,
    deployments: ["a", "b"],
    preferred: "a",
    fallback_on_refusal: true,
    deadline_ms: 5000,
    max_attempts: 2
  ]
  @example_input ~s(["telelem.route_revision.v1","chat",1,["a","b"],"a",true,5000,2])
  # Golden vector, computed outside Elixir: printf '%s' "$input" | sha256sum
  @example_digest "5da0cc790a9f5d00bda93dd616a9fe9f49eb81012eee7e95220d577a1d2780d3"

  describe "digest" do
    test "contract example matches golden input and digest" do
      r = Revision.new!(@example)
      assert Revision.digest_input(r) == @example_input
      assert r.digest == @example_digest
      assert Revision.verify(r) == :ok
    end

    test "given digest is accepted when it matches, rejected otherwise" do
      assert {:ok, %{digest: @example_digest}} =
               Revision.new(Keyword.put(@example, :digest, @example_digest))

      wrong = String.duplicate("0", 64)

      assert {:error, :digest_mismatch} =
               Revision.new(Keyword.put(@example, :digest, wrong))

      assert {:error, {:invalid, :digest}} =
               Revision.new(Keyword.put(@example, :digest, String.upcase(@example_digest)))

      assert {:error, {:invalid, :digest}} = Revision.new(Keyword.put(@example, :digest, 1))
    end

    test "every field and deployment order changes the digest" do
      base = Revision.new!(@example).digest

      variants = [
        route_id: "chat2",
        rev: 2,
        deployments: ["b", "a"],
        preferred: "b",
        fallback_on_refusal: false,
        deadline_ms: 5001,
        max_attempts: 1
      ]

      digests =
        for {k, v} <- variants do
          d = Revision.new!(Keyword.put(@example, k, v)).digest
          refute d == base, "#{k} did not change the digest"
          d
        end

      assert length(Enum.uniq(digests)) == length(variants)
    end

    test "verify detects a tampered struct" do
      r = Revision.new!(@example)
      assert Revision.verify(%{r | deadline_ms: 6000}) == {:error, :digest_mismatch}
    end

    test "map and keyword input are equivalent" do
      assert Revision.new!(Map.new(@example)) == Revision.new!(@example)
    end
  end

  describe "with_preferred/2" do
    test "switches preferred, bumps rev, recomputes digest, keeps the rest" do
      r = Revision.new!(@example)
      assert {:ok, next} = Revision.with_preferred(r, "b")

      assert next.rev == 2 and next.preferred == "b"
      assert Revision.verify(next) == :ok
      refute next.digest == r.digest

      assert Map.drop(Map.from_struct(next), [:rev, :preferred, :digest]) ==
               Map.drop(Map.from_struct(r), [:rev, :preferred, :digest])

      assert next ==
               Revision.new!(Keyword.merge(@example, rev: 2, preferred: "b"))
    end

    test "same preferred is still an explicit new revision" do
      r = Revision.new!(@example)
      assert {:ok, next} = Revision.with_preferred(r, "a")
      assert next.rev == 2 and next.preferred == "a"
      refute next.digest == r.digest
      assert {:ok, %{rev: 3}} = Revision.with_preferred(next, "a")
    end

    test "accepts a plain map carrying the §1 fields and digest" do
      r = Revision.new!(@example)
      map = Map.from_struct(r)
      assert Revision.with_preferred(map, "b") == Revision.with_preferred(r, "b")
    end

    test "valid ID outside the configured lanes → :unknown_deployment" do
      r = Revision.new!(@example)
      assert Revision.with_preferred(r, "c") == {:error, :unknown_deployment}
    end

    test "invalid preferred ID is rejected" do
      r = Revision.new!(@example)

      for bad <- ["", "a b", "a\n", String.duplicate("x", 65), :a, nil] do
        assert Revision.with_preferred(r, bad) == {:error, {:invalid, :preferred}}
      end
    end

    test "tampered or invalid current is rejected before preferred is considered" do
      r = Revision.new!(@example)

      assert Revision.with_preferred(%{r | deadline_ms: 6000}, "b") ==
               {:error, :digest_mismatch}

      assert Revision.with_preferred(%{r | preferred: "b"}, "c") == {:error, :digest_mismatch}
      assert Revision.with_preferred(%{r | digest: nil}, "b") == {:error, {:invalid, :digest}}
      assert Revision.with_preferred(%{r | rev: 0}, "b") == {:error, {:invalid, :rev}}

      assert Revision.with_preferred(%{r | deployments: ["a", "a"]}, "a") ==
               {:error, :duplicate_deployment}

      map = Map.from_struct(r)

      assert Revision.with_preferred(Map.delete(map, :digest), "b") ==
               {:error, {:invalid, :digest}}

      assert Revision.with_preferred(Map.put(map, :model, "m"), "b") ==
               {:error, {:invalid, :model}}

      assert Revision.with_preferred(URI.parse("x"), "b") == {:error, {:invalid, :revision}}
      assert Revision.with_preferred("chat", "b") == {:error, {:invalid, :revision}}
    end
  end

  describe "validation" do
    test "ID charset and length" do
      assert Revision.id?("aZ09_-")
      assert Revision.id?(String.duplicate("x", 64))
      refute Revision.id?(String.duplicate("x", 65))
      refute Revision.id?("")
      refute Revision.id?("a b")
      refute Revision.id?("a\n")
      refute Revision.id?("a.b")
      refute Revision.id?("é")
      refute Revision.id?(:a)
    end

    for {field, bad} <- [
          route_id: "bad id",
          route_id: nil,
          rev: 0,
          rev: -1,
          rev: 1.0,
          deployments: ["a"],
          deployments: ["a", "b", "c"],
          deployments: ["a", "b/"],
          deployments: "ab",
          preferred: "bad!",
          fallback_on_refusal: "true",
          deadline_ms: 0,
          deadline_ms: 5.0e3,
          max_attempts: 0,
          max_attempts: 3
        ] do
      test "rejects #{field}=#{inspect(bad)}" do
        assert {:error, {:invalid, unquote(field)}} =
                 Revision.new(Keyword.put(@example, unquote(field), unquote(Macro.escape(bad))))
      end
    end

    test "rejects missing fields" do
      for {k, _} <- @example do
        assert {:error, {:invalid, ^k}} = Revision.new(Keyword.delete(@example, k))
      end
    end

    test "rejects unknown keys and non-map input" do
      assert {:error, {:invalid, :model}} = Revision.new(Keyword.put(@example, :model, "x"))
      assert {:error, {:invalid, :attrs}} = Revision.new("chat")
    end

    test "rejects duplicate deployments and foreign preferred" do
      assert {:error, :duplicate_deployment} =
               Revision.new(Keyword.merge(@example, deployments: ["a", "a"]))

      assert {:error, :preferred_not_in_deployments} =
               Revision.new(Keyword.put(@example, :preferred, "c"))
    end

    test "new!/1 raises on invalid input" do
      assert_raise ArgumentError, ~r/invalid route revision/, fn ->
        Revision.new!(Keyword.put(@example, :rev, 0))
      end
    end
  end
end
