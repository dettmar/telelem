defmodule Telelem.Serve.DiagnosticsTest do
  # Pure decision over synthetic snapshots, synthetic registry and module
  # level classification, and a read-only live observation. Nothing here
  # changes Logger or telemetry state, attaches a handler or executes a
  # telemetry event. Not async so the live observation is not raced by other
  # test modules that install their own Logger handlers.
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Telelem.Serve.Diagnostics

  @codes [
    :snapshot_invalid,
    :logger_unobservable,
    :logger_level_not_none,
    :logger_handlers_present,
    :logger_module_level_permissive,
    :telemetry_registry_absent,
    :telemetry_registry_unknown,
    :telemetry_handlers_unobservable,
    :telemetry_handlers_present
  ]

  @quiet %{
    logger_level: :none,
    logger_handlers: :none,
    logger_module_levels: :none,
    telemetry_registry: :ets,
    telemetry_handlers: :none
  }

  @passing %{
    logger_level: [:none],
    logger_handlers: [:none],
    logger_module_levels: [:none, :restrictive],
    telemetry_registry: [:ets, :persistent_term],
    telemetry_handlers: [:none]
  }

  @allowed %{
    logger_level: [:none, :other, :unobservable],
    logger_handlers: [:none, :present, :unobservable],
    logger_module_levels: [:none, :restrictive, :permissive, :unobservable],
    telemetry_registry: [:ets, :persistent_term, :absent, :unknown],
    telemetry_handlers: [:none, :present, :unobservable]
  }

  test "fixed refusal codes" do
    assert Diagnostics.codes() == @codes
  end

  describe "check/1" do
    test "passes only a fully quiet snapshot" do
      assert Diagnostics.check(@quiet) == :ok
      assert Diagnostics.check(%{@quiet | telemetry_registry: :persistent_term}) == :ok
      assert Diagnostics.check(%{@quiet | logger_module_levels: :restrictive}) == :ok
    end

    test "each observed condition refuses with its own code" do
      cases = [
        {:logger_level, :other, :logger_level_not_none},
        {:logger_level, :unobservable, :logger_unobservable},
        {:logger_handlers, :present, :logger_handlers_present},
        {:logger_handlers, :unobservable, :logger_unobservable},
        {:logger_module_levels, :permissive, :logger_module_level_permissive},
        {:logger_module_levels, :unobservable, :logger_unobservable},
        {:telemetry_registry, :absent, :telemetry_registry_absent},
        {:telemetry_registry, :unknown, :telemetry_registry_unknown},
        {:telemetry_handlers, :present, :telemetry_handlers_present},
        {:telemetry_handlers, :unobservable, :telemetry_handlers_unobservable}
      ]

      for {key, value, code} <- cases do
        assert Diagnostics.check(Map.put(@quiet, key, value)) == {:refuse, [code]},
               "#{key}: #{value}"
      end
    end

    test "multiple findings are reported once each, in codes/0 order" do
      snapshot = %{
        logger_level: :unobservable,
        logger_handlers: :present,
        logger_module_levels: :unobservable,
        telemetry_registry: :absent,
        telemetry_handlers: :unobservable
      }

      assert Diagnostics.check(snapshot) ==
               {:refuse,
                [
                  :logger_unobservable,
                  :logger_handlers_present,
                  :telemetry_registry_absent,
                  :telemetry_handlers_unobservable
                ]}
    end

    test "anything not exactly the snapshot shape fails closed" do
      for bad <- [
            nil,
            [],
            :ok,
            Map.to_list(@quiet),
            Map.delete(@quiet, :telemetry_handlers),
            Map.put(@quiet, :extra, :none),
            Map.put(Map.delete(@quiet, :logger_level), :level, :none),
            Map.put(@quiet, :__struct__, URI)
          ] do
        assert Diagnostics.check(bad) == {:refuse, [:snapshot_invalid]}
      end
    end

    test "an unexpected value fails closed alongside other findings" do
      assert Diagnostics.check(%{@quiet | logger_level: :debug}) == {:refuse, [:snapshot_invalid]}

      assert Diagnostics.check(%{@quiet | telemetry_handlers: 0}) ==
               {:refuse, [:snapshot_invalid]}

      assert Diagnostics.check(%{@quiet | telemetry_registry: nil}) ==
               {:refuse, [:snapshot_invalid]}

      assert Diagnostics.check(%{@quiet | logger_module_levels: [], logger_handlers: :present}) ==
               {:refuse, [:snapshot_invalid, :logger_handlers_present]}
    end

    property "returns :ok or fixed, ordered, unique codes for any five-key map" do
      value = one_of([member_of([:none, :other, :present, :ets, :absent, :unknown]), term()])

      check all(values <- fixed_list(List.duplicate(value, 5))) do
        snapshot = Map.new(Enum.zip(Map.keys(@quiet), values))

        case Diagnostics.check(snapshot) do
          :ok ->
            for {key, v} <- snapshot, do: assert(v in @passing[key])

          {:refuse, [_ | _] = codes} ->
            assert codes == Enum.filter(@codes, &(&1 in codes))
        end
      end
    end
  end

  describe "module_levels_kind/1" do
    test "classifies :logger module levels without returning them" do
      assert Diagnostics.module_levels_kind([]) == :none
      assert Diagnostics.module_levels_kind([{Bandit, :none}]) == :restrictive
      assert Diagnostics.module_levels_kind([{Bandit, :none}, {Plug, :none}]) == :restrictive
      assert Diagnostics.module_levels_kind([{Bandit, :none}, {Plug, :error}]) == :permissive
      assert Diagnostics.module_levels_kind([{ThousandIsland, :all}]) == :permissive
      assert Diagnostics.module_levels_kind([{"Bandit", :none}]) == :unobservable
      assert Diagnostics.module_levels_kind([:bad]) == :unobservable
      assert Diagnostics.module_levels_kind(:undefined) == :unobservable
    end
  end

  describe "registry_kind/3" do
    setup do
      owner = spawn(fn -> receive(do: (:stop -> :ok)) end)
      other = spawn(fn -> receive(do: (:stop -> :ok)) end)
      on_exit(fn -> Enum.each([owner, other], &send(&1, :stop)) end)
      %{owner: owner, other: other, tid: make_ref(), fun: fn _, _ -> [] end}
    end

    test "known forms", %{owner: owner, tid: tid, fun: fun} do
      assert Diagnostics.registry_kind({:telemetry_ets, fun, tid}, owner, fn ^tid -> owner end) ==
               :ets

      assert Diagnostics.registry_kind({:telemetry_pt, fun, %{}}, owner, fn _ -> :undefined end) ==
               :persistent_term

      assert Diagnostics.registry_kind({:telemetry_pt, fun, %{}}, nil, fn _ -> :undefined end) ==
               :persistent_term
    end

    test "absent registry, table or table process", %{owner: owner, tid: tid, fun: fun} do
      assert Diagnostics.registry_kind(:absent, owner, fn _ -> owner end) == :absent

      assert Diagnostics.registry_kind({:telemetry_ets, fun, tid}, nil, fn _ -> owner end) ==
               :absent

      assert Diagnostics.registry_kind({:telemetry_ets, fun, tid}, owner, fn _ -> :undefined end) ==
               :absent
    end

    test "unknown forms", %{owner: owner, other: other, tid: tid, fun: fun} do
      unknown = [
        {{:telemetry_ets, fun, tid}, owner, fn _ -> other end},
        {{:telemetry_ets, fun, tid}, owner, fn _ -> :weird end},
        {{:telemetry_ets, fn _ -> [] end, tid}, owner, fn _ -> owner end},
        {{:telemetry_ets, fun, :named_table}, owner, fn _ -> owner end},
        {{:telemetry_pt, fun, []}, owner, fn _ -> owner end},
        {{:telemetry_other, fun, tid}, owner, fn _ -> owner end},
        {{:telemetry_ets, fun}, owner, fn _ -> owner end},
        {nil, owner, fn _ -> owner end},
        {%{}, owner, fn _ -> owner end}
      ]

      for {term, pid, lookup} <- unknown do
        assert Diagnostics.registry_kind(term, pid, lookup) == :unknown, inspect(term)
      end
    end
  end

  describe "observe/0 (live, read-only)" do
    test "returns only fixed atoms and changes nothing it reads" do
      before = global_state()
      snapshot = Diagnostics.observe()
      assert global_state() == before

      assert Map.keys(snapshot) |> Enum.sort() == Map.keys(@allowed) |> Enum.sort()

      for {key, value} <- snapshot do
        assert value in @allowed[key], "#{key}: #{inspect(value)}"
      end
    end

    test "agrees with what the pinned test environment can independently observe" do
      snapshot = Diagnostics.observe()
      %{level: level} = :logger.get_primary_config()

      assert snapshot.logger_level == if(level == :none, do: :none, else: :other)

      assert snapshot.logger_handlers ==
               if(:logger.get_handler_ids() == [], do: :none, else: :present)

      # The telemetry application is started under `mix test` and nothing in
      # this suite persists the registry.
      assert snapshot.telemetry_registry == :ets

      attached? =
        :telemetry.list_handlers([:bandit]) != [] or
          :telemetry.list_handlers([:thousand_island]) != []

      assert snapshot.telemetry_handlers == if(attached?, do: :present, else: :none)
    end

    test "check/0 refuses under the ordinary test Logger and returns only codes" do
      # ExUnit runs with a Logger handler installed, so this observation is
      # not quiet; the check must say so without mutating anything.
      assert :logger.get_handler_ids() != []
      assert {:refuse, codes} = Diagnostics.check()
      assert :logger_handlers_present in codes
      assert codes == Enum.filter(@codes, &(&1 in codes))
    end
  end

  defp global_state do
    {:logger.get_primary_config(), Enum.sort(:logger.get_handler_ids()),
     Enum.sort(:logger.get_module_level()), :persistent_term.get(:telemetry, :absent),
     Enum.sort(:telemetry.list_handlers([]) |> Enum.map(& &1.id)), Process.get() |> Enum.sort()}
  end
end
