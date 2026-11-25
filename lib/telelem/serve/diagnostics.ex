defmodule Telelem.Serve.Diagnostics do
  @moduledoc """
  Offline, fail-closed refusal check for diagnostics that could observe the
  serving listener: Erlang `:logger` and Bandit/Thousand Island telemetry
  registrations (pinned `telemetry` 1.4.3).

  `observe/0` reads a snapshot; `check/1` is a pure decision over a snapshot;
  `check/0` is `check(observe())`. Neither changes Logger, telemetry or any
  other state, and neither returns handler IDs, handler configuration,
  functions, levels other than the fixed atoms below, or exception data.

  ## Snapshot

  A map with exactly these keys and fixed atom values:

    * `logger_level`: `:none` when the primary `:logger` level is `none`,
      `:other` for any other level, `:unobservable` if it could not be read.
    * `logger_handlers`: `:none` when no `:logger` handler is installed,
      `:present`, or `:unobservable`.
    * `logger_module_levels`: `:none` when no module level is set,
      `:restrictive` when every module level set is `none`, `:permissive`
      when any module level is something else (it overrides the primary
      level for that module), or `:unobservable`.
    * `telemetry_registry`: `:ets` or `:persistent_term` for the two
      registry descriptors pinned `telemetry_handler_table` stores, each
      only with its own exact dispatch function
      (`{:telemetry_ets, &:telemetry_ets.list_for_event/2, tid}` with the
      table owned by the registry process, or `{:telemetry_pt,
      &:telemetry_pt.list_for_event/2, map}`); `:absent` when no registry is
      installed or its table or table process is gone (pinned telemetry
      would then report no handlers rather than an error); or `:unknown`
      for any other form, including a known tag with any other function of
      arity 2 (the function `telemetry:execute/3` dispatches through would
      then not be the one `telemetry:list_handlers/1` reflects), for a
      descriptor that changed during the observation, or if it could not be
      read.
    * `telemetry_handlers`: `:none` when no handler is attached to an event
      under `[:bandit]` or `[:thousand_island]`, `:present`, or
      `:unobservable` (always so unless the registry is `:ets` or
      `:persistent_term`).

  ## Result

  `:ok` only when the level is `:none`, there are no handlers, module levels
  are `:none` or `:restrictive`, the registry is known and no Bandit/Thousand
  Island telemetry handler is attached. Otherwise `{:refuse, codes}` with the
  fixed codes in `codes/0` order. A snapshot that is not exactly this shape
  refuses with `:snapshot_invalid`; an unexpected value refuses with that
  field's code.

  ## Limits

  This is a point-in-time snapshot, not an authorization to start a listener
  and not a mute. Logger and telemetry are global: any process may install a
  handler, change a level or attach telemetry right after the check, and
  nothing here prevents or detects that. It does not observe process-local
  log levels of other processes, primary filters, telemetry handlers under
  other prefixes (such as `[:plug]`), tracing, `:sys` debugging or crash
  dumps. Telemetry events are still executed when no handler is attached.

  The snapshot is a sequence of separate reads (the registry descriptor,
  each telemetry prefix in turn, the descriptor again, then the primary
  level, handler IDs and module levels), not one atomic view. An `:ok` therefore does not establish that
  all of these were quiet at the same instant: a handler or level may have
  appeared before one read and been removed before another. The registry
  is read before and after the handler lookups and the result is
  `:unknown` only if the descriptor itself changed. In the `:ets` form
  attaching or detaching a handler changes only the table and stores an
  equal descriptor, so such a change during or between the per-prefix
  lookups leaves the descriptor stable and is not detected (in the
  `:persistent_term` form the descriptor carries the handler map, so a
  change inside that window is detected).
  """

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

  @keys [
    :logger_level,
    :logger_handlers,
    :logger_module_levels,
    :telemetry_registry,
    :telemetry_handlers
  ]

  @prefixes [[:bandit], [:thousand_island]]
  @registry_key :telemetry
  @registry_process :telemetry_handler_table

  @type code ::
          :snapshot_invalid
          | :logger_unobservable
          | :logger_level_not_none
          | :logger_handlers_present
          | :logger_module_level_permissive
          | :telemetry_registry_absent
          | :telemetry_registry_unknown
          | :telemetry_handlers_unobservable
          | :telemetry_handlers_present

  @type snapshot :: %{
          logger_level: :none | :other | :unobservable,
          logger_handlers: :none | :present | :unobservable,
          logger_module_levels: :none | :restrictive | :permissive | :unobservable,
          telemetry_registry: :ets | :persistent_term | :absent | :unknown,
          telemetry_handlers: :none | :present | :unobservable
        }

  @doc "Every refusal code, in the order `check/1` reports them."
  @spec codes() :: [code()]
  def codes, do: @codes

  @doc "Observe the current Logger and telemetry state, then decide."
  @spec check() :: :ok | {:refuse, [code(), ...]}
  def check, do: check(observe())

  @doc """
  Pure decision over a snapshot. Fails closed on any shape or value other
  than the documented ones.
  """
  @spec check(term()) :: :ok | {:refuse, [code(), ...]}
  def check(%{} = snapshot) when map_size(snapshot) == length(@keys) do
    if Enum.all?(@keys, &Map.has_key?(snapshot, &1)) do
      snapshot |> findings() |> result()
    else
      {:refuse, [:snapshot_invalid]}
    end
  end

  def check(_), do: {:refuse, [:snapshot_invalid]}

  defp findings(s) do
    [
      logger_level(s.logger_level),
      logger_handlers(s.logger_handlers),
      logger_module_levels(s.logger_module_levels),
      telemetry_registry(s.telemetry_registry),
      telemetry_handlers(s.telemetry_handlers)
    ]
    |> List.flatten()
  end

  defp logger_level(:none), do: []
  defp logger_level(:other), do: [:logger_level_not_none]
  defp logger_level(:unobservable), do: [:logger_unobservable]
  defp logger_level(_), do: [:snapshot_invalid]

  defp logger_handlers(:none), do: []
  defp logger_handlers(:present), do: [:logger_handlers_present]
  defp logger_handlers(:unobservable), do: [:logger_unobservable]
  defp logger_handlers(_), do: [:snapshot_invalid]

  defp logger_module_levels(v) when v in [:none, :restrictive], do: []
  defp logger_module_levels(:permissive), do: [:logger_module_level_permissive]
  defp logger_module_levels(:unobservable), do: [:logger_unobservable]
  defp logger_module_levels(_), do: [:snapshot_invalid]

  defp telemetry_registry(v) when v in [:ets, :persistent_term], do: []
  defp telemetry_registry(:absent), do: [:telemetry_registry_absent]
  defp telemetry_registry(:unknown), do: [:telemetry_registry_unknown]
  defp telemetry_registry(_), do: [:snapshot_invalid]

  defp telemetry_handlers(:none), do: []
  defp telemetry_handlers(:present), do: [:telemetry_handlers_present]
  defp telemetry_handlers(:unobservable), do: [:telemetry_handlers_unobservable]
  defp telemetry_handlers(_), do: [:snapshot_invalid]

  defp result([]), do: :ok
  defp result(found), do: {:refuse, Enum.filter(@codes, &(&1 in found))}

  @doc """
  Read-only snapshot of the current state; see the module documentation.
  Never raises; anything it cannot read is reported as unobservable or
  unknown.
  """
  @spec observe() :: snapshot()
  def observe do
    {registry, handlers} = observe_telemetry()

    %{
      logger_level: observe_logger_level(),
      logger_handlers: observe_logger_handlers(),
      logger_module_levels: observe_module_levels(),
      telemetry_registry: registry,
      telemetry_handlers: handlers
    }
  end

  defp observe_logger_level do
    guard(:unobservable, fn ->
      case :logger.get_primary_config() do
        %{level: :none} -> :none
        %{level: _} -> :other
        _ -> :unobservable
      end
    end)
  end

  defp observe_logger_handlers do
    guard(:unobservable, fn ->
      case :logger.get_handler_ids() do
        [] -> :none
        [_ | _] -> :present
        _ -> :unobservable
      end
    end)
  end

  defp observe_module_levels do
    guard(:unobservable, fn -> module_levels_kind(:logger.get_module_level()) end)
  end

  @doc false
  # Classifies a `:logger.get_module_level/0` result. Public only for
  # synthetic tests.
  @spec module_levels_kind(term()) :: :none | :restrictive | :permissive | :unobservable
  def module_levels_kind([]), do: :none

  def module_levels_kind(levels) when is_list(levels) do
    cond do
      not Enum.all?(levels, &match?({m, _} when is_atom(m), &1)) -> :unobservable
      Enum.all?(levels, &match?({_, :none}, &1)) -> :restrictive
      true -> :permissive
    end
  end

  def module_levels_kind(_), do: :unobservable

  defp observe_telemetry do
    guard({:unknown, :unobservable}, fn ->
      before = :persistent_term.get(@registry_key, :absent)
      kind = registry_kind(before, Process.whereis(@registry_process), &table_owner/1)

      if kind in [:ets, :persistent_term] do
        attached? = Enum.any?(@prefixes, &(:telemetry.list_handlers(&1) != []))
        after_ = :persistent_term.get(@registry_key, :absent)
        again = registry_kind(after_, Process.whereis(@registry_process), &table_owner/1)

        if after_ === before and again == kind do
          {kind, if(attached?, do: :present, else: :none)}
        else
          {:unknown, :unobservable}
        end
      else
        {kind, :unobservable}
      end
    end)
  end

  defp table_owner(tid), do: :ets.info(tid, :owner)

  @doc false
  # Classifies the pinned telemetry registry term (`persistent_term` key
  # `:telemetry`, set by `telemetry_handler_table`), the registry process and
  # an ETS owner lookup. Known only as `{:telemetry_ets,
  # &:telemetry_ets.list_for_event/2, tid}` or `{:telemetry_pt,
  # &:telemetry_pt.list_for_event/2, map}`. Public only for synthetic tests.
  @spec registry_kind(term(), pid() | nil, (term() -> term())) ::
          :ets | :persistent_term | :absent | :unknown
  def registry_kind(:absent, _registry_pid, _owner), do: :absent

  def registry_kind({:telemetry_ets, fun, tid}, registry_pid, owner) when is_reference(tid) do
    if pinned_dispatch?(:telemetry_ets, fun) do
      case {registry_pid, owner.(tid)} do
        {nil, _} -> :absent
        {_, :undefined} -> :absent
        {pid, pid} when is_pid(pid) -> :ets
        _ -> :unknown
      end
    else
      :unknown
    end
  end

  def registry_kind({:telemetry_pt, fun, map}, _registry_pid, _owner) when is_map(map) do
    if pinned_dispatch?(:telemetry_pt, fun), do: :persistent_term, else: :unknown
  end

  def registry_kind(_, _registry_pid, _owner), do: :unknown

  # `telemetry:execute/3` calls the descriptor's function, while
  # `telemetry:list_handlers/1` calls `Mod:list_by_prefix/2`. Only the exact
  # function pinned `telemetry_handler_table` stores for each form keeps the
  # two in agreement, so anything else (an anonymous or other external
  # function of the same arity, or the other form's function) is unknown.
  # External funs compare by module, name and arity.
  defp pinned_dispatch?(:telemetry_ets, fun), do: fun === (&:telemetry_ets.list_for_event/2)
  defp pinned_dispatch?(:telemetry_pt, fun), do: fun === (&:telemetry_pt.list_for_event/2)

  defp guard(fallback, fun) do
    fun.()
  catch
    _kind, _reason -> fallback
  end
end
