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
      # The exact dispatch functions pinned `telemetry_handler_table` stores.
      %{
        owner: owner,
        other: other,
        tid: make_ref(),
        ets_fun: &:telemetry_ets.list_for_event/2,
        pt_fun: &:telemetry_pt.list_for_event/2
      }
    end

    test "known forms", %{owner: owner, tid: tid, ets_fun: ets_fun, pt_fun: pt_fun} do
      assert Diagnostics.registry_kind({:telemetry_ets, ets_fun, tid}, owner, fn ^tid -> owner end) ==
               :ets

      assert Diagnostics.registry_kind(
               {:telemetry_pt, pt_fun, %{}},
               owner,
               fn _ -> :undefined end
             ) == :persistent_term

      assert Diagnostics.registry_kind({:telemetry_pt, pt_fun, %{}}, nil, fn _ -> :undefined end) ==
               :persistent_term

      # A capture built at runtime is the same external function.
      assert Diagnostics.registry_kind(
               {:telemetry_ets, Function.capture(:telemetry_ets, :list_for_event, 2), tid},
               owner,
               fn _ -> owner end
             ) == :ets
    end

    test "absent registry, table or table process", %{owner: owner, tid: tid, ets_fun: ets_fun} do
      assert Diagnostics.registry_kind(:absent, owner, fn _ -> owner end) == :absent

      assert Diagnostics.registry_kind({:telemetry_ets, ets_fun, tid}, nil, fn _ -> owner end) ==
               :absent

      assert Diagnostics.registry_kind(
               {:telemetry_ets, ets_fun, tid},
               owner,
               fn _ -> :undefined end
             ) == :absent
    end

    test "a known tag with any other arity-2 dispatch function is unknown",
         %{owner: owner, tid: tid, ets_fun: ets_fun, pt_fun: pt_fun} do
      # Each of these would pass an `is_function(fun, 2)` check. Lookups are
      # synthetic and never call the function.
      others = [
        fn _, _ -> [] end,
        fn tid, event -> :telemetry_ets.list_for_event(tid, event) end,
        &:telemetry_ets.list_by_prefix/2,
        &:telemetry_pt.list_by_prefix/2,
        &:erlang.max/2,
        &Map.get/2
      ]

      for fun <- others do
        assert Diagnostics.registry_kind({:telemetry_ets, fun, tid}, owner, fn _ -> owner end) ==
                 :unknown

        assert Diagnostics.registry_kind({:telemetry_pt, fun, %{}}, owner, fn _ -> owner end) ==
                 :unknown
      end

      # Each form's function under the other form's tag.
      assert Diagnostics.registry_kind({:telemetry_ets, pt_fun, tid}, owner, fn _ -> owner end) ==
               :unknown

      assert Diagnostics.registry_kind({:telemetry_pt, ets_fun, %{}}, owner, fn _ -> owner end) ==
               :unknown

      # A mismatched ETS descriptor is unknown even when the table or table
      # process is gone.
      assert Diagnostics.registry_kind({:telemetry_ets, pt_fun, tid}, nil, fn _ -> owner end) ==
               :unknown

      assert Diagnostics.registry_kind(
               {:telemetry_ets, fn _, _ -> [] end, tid},
               owner,
               fn _ -> :undefined end
             ) == :unknown

      # Pinned `default_impl/0` shape, never stored by the registry.
      assert Diagnostics.registry_kind(
               {:telemetry_ets, fn _, _ -> [] end, %{}},
               owner,
               fn _ -> owner end
             ) == :unknown
    end

    test "unknown forms",
         %{owner: owner, other: other, tid: tid, ets_fun: ets_fun, pt_fun: pt_fun} do
      unknown = [
        {{:telemetry_ets, ets_fun, tid}, owner, fn _ -> other end},
        {{:telemetry_ets, ets_fun, tid}, owner, fn _ -> :weird end},
        {{:telemetry_ets, fn _, _, _ -> [] end, tid}, owner, fn _ -> owner end},
        {{:telemetry_ets, ets_fun, :named_table}, owner, fn _ -> owner end},
        {{:telemetry_pt, pt_fun, []}, owner, fn _ -> owner end},
        {{:telemetry_other, ets_fun, tid}, owner, fn _ -> owner end},
        {{:telemetry_ets, ets_fun}, owner, fn _ -> owner end},
        {{:telemetry_ets, nil, tid}, owner, fn _ -> owner end},
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
      # Compared as a boolean so a failure reports no raw Logger or
      # telemetry configuration.
      unchanged? = global_state() == before
      assert unchanged?, "observe/0 changed Logger, telemetry or process state"

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
      # this suite persists the registry, so the live descriptor is the ETS
      # form with its exact pinned dispatch function (checked as a boolean so
      # nothing raw is reported).
      assert snapshot.telemetry_registry == :ets

      pinned = &:telemetry_ets.list_for_event/2

      pinned_ets? =
        match?({:telemetry_ets, ^pinned, _}, :persistent_term.get(:telemetry, :absent))

      assert pinned_ets?, "live telemetry descriptor is not the pinned ETS form"

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
