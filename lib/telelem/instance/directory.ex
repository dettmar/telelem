defmodule Telelem.Instance.Directory do
  @moduledoc """
  Per-instance ETS table owned by the instance supervisor (TEL-002).

  Each instance gets its own unnamed table. There are no global names and no
  application env. The table outlives restarts of the admission authority, so
  it holds:

    * the current authority pid and its incarnation (the stable address that
      `{:telelem_instance_authority, table}` timer destinations resolve to)
    * the last committed `Telelem.Admission.Ledger`, so a restarted authority
      fences the previous incarnation instead of forgetting encumbered spend
    * observation subscribers and the instance metadata

  The table is deleted when its owner, the instance supervisor, exits.
  """

  @type t :: :ets.tid()

  @spec new(map()) :: t()
  def new(meta) do
    table = :ets.new(:telelem_instance, [:set, :public, read_concurrency: true])
    :ets.insert(table, {:meta, meta})
    table
  end

  @spec meta(t()) :: map()
  def meta(table), do: :ets.lookup_element(table, :meta, 2)

  @spec put_authority(t(), pid(), pos_integer()) :: true
  def put_authority(table, pid, incarnation),
    do: :ets.insert(table, {:authority, pid, incarnation})

  @doc "Current authority `{pid, incarnation}`, or `nil` while it restarts or after the instance stopped."
  @spec authority(t()) :: {pid(), pos_integer()} | nil
  def authority(table) do
    case :ets.lookup(table, :authority) do
      [{:authority, pid, inc}] -> if Process.alive?(pid), do: {pid, inc}, else: nil
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @spec put_ledger(t(), Telelem.Admission.Ledger.t()) :: true
  def put_ledger(table, ledger), do: :ets.insert(table, {:ledger, ledger})

  @spec ledger(t()) :: Telelem.Admission.Ledger.t() | nil
  def ledger(table) do
    case :ets.lookup(table, :ledger) do
      [{:ledger, l}] -> l
      [] -> nil
    end
  end

  @spec subscribe(t(), pid()) :: true
  def subscribe(table, pid), do: :ets.insert(table, {{:subscriber, pid}, true})

  @spec subscribers(t()) :: [pid()]
  def subscribers(table), do: :ets.select(table, [{{{:subscriber, :"$1"}, :_}, [], [:"$1"]}])

  @doc "Resolve a clock/timer destination at delivery time."
  @spec resolve(pid() | {:telelem_instance_authority, t()}) :: pid() | nil
  def resolve(pid) when is_pid(pid), do: pid

  def resolve({:telelem_instance_authority, table}) do
    case authority(table) do
      {pid, _} -> pid
      nil -> nil
    end
  end
end
