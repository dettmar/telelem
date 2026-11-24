defmodule Telelem.Instance do
  @moduledoc """
  One local gateway subtree, started explicitly with no global names (TEL-002, M1).

      children = [
        {Telelem.Instance,
         id: :gw_a,                                # child-spec id, must be distinct
         scope: %{route: "chat", tenant: "acme"},  # human scope; may equal another instance's
         limits: %{slots: 2, units: 10},
         spool_dir: "/tmp/a",
         transport: {MyTransport, arg},
         clock: {Telelem.Instance.ManualClock, clock_pid},   # default SystemClock
         reserve_ttl: 50}                          # ms, or nil for no TTL
      ]

  Tree (`:rest_for_one`), all owned by the instance supervisor:

      Telelem.Instance (Supervisor; owns the Directory ETS table)
      ├── :authority  Telelem.Admission.Authority
      └── :workers    DynamicSupervisor of Telelem.Instance.Worker (:temporary)

  If the authority dies, the worker supervisor is shut down before the
  authority restarts, so every worker that held an old-incarnation grant has
  exited and closed its transport by the time the successor fences the old
  incarnation. Stopping the instance closes held transports through
  `Worker.terminate/2` and deletes the directory with the supervisor.

  Two instances in one BEAM share nothing: each has its own authority pid,
  ledger, limits, clock, transport and spool directory. Nothing is read from
  application env.
  """
  use Supervisor

  alias Telelem.Admission.Authority
  alias Telelem.Instance.Directory

  @enforce_keys [:sup, :table, :tag]
  defstruct [:sup, :table, :tag]

  @type t :: %__MODULE__{sup: pid(), table: :ets.tid(), tag: reference()}

  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :id),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    limits = Keyword.fetch!(opts, :limits)

    meta = %{
      id: Keyword.fetch!(opts, :id),
      tag: make_ref(),
      scope: Keyword.get(opts, :scope, %{}),
      limits: %{slots: Map.fetch!(limits, :slots), units: Map.fetch!(limits, :units)},
      spool_dir: Keyword.fetch!(opts, :spool_dir),
      transport: Keyword.fetch!(opts, :transport),
      clock: Keyword.get(opts, :clock, {Telelem.Instance.SystemClock, nil}),
      reserve_ttl: Keyword.get(opts, :reserve_ttl)
    }

    table = Directory.new(meta)

    children = [
      %{id: :authority, start: {Authority, :start_link, [[table: table]]}},
      %{
        id: :workers,
        start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
        type: :supervisor
      }
    ]

    Supervisor.init(children,
      strategy: :rest_for_one,
      max_restarts: Keyword.get(opts, :max_restarts, 3),
      max_seconds: Keyword.get(opts, :max_seconds, 5)
    )
  end

  @doc "Handle for a started instance supervisor."
  @spec handle(pid()) :: t()
  def handle(sup) when is_pid(sup) do
    table = GenServer.call(child!(sup, :authority), :table)
    %__MODULE__{sup: sup, table: table, tag: Directory.meta(table).tag}
  end

  @spec meta(t()) :: map()
  def meta(%__MODULE__{table: t}), do: Directory.meta(t)

  @doc "Current authority pid (raises while it is restarting)."
  @spec authority(t()) :: pid()
  def authority(%__MODULE__{table: t}) do
    {pid, _} = Directory.authority(t) || raise "authority unavailable"
    pid
  end

  @spec incarnation(t()) :: pos_integer() | nil
  def incarnation(%__MODULE__{table: t}) do
    case Directory.authority(t) do
      {_, inc} -> inc
      nil -> nil
    end
  end

  @doc """
  Subscribe `pid` to `{:telelem_admission, tag, event}` observations.
  The subscription survives authority restarts.
  """
  @spec subscribe(t(), pid()) :: :ok
  def subscribe(%__MODULE__{table: t}, pid \\ self()) do
    Directory.subscribe(t, pid)
    :ok
  end

  @spec snapshot(t()) :: map()
  def snapshot(inst), do: Authority.snapshot(authority(inst))

  @doc "Start a worker that owns a grant for `op_id` and holds a transport."
  @spec start_work(t(), term(), pos_integer(), map()) :: {:ok, pid()} | {:error, term()}
  def start_work(%__MODULE__{} = inst, op_id, units, meta \\ %{}) do
    DynamicSupervisor.start_child(
      workers_sup(inst),
      {Telelem.Instance.Worker, %{table: inst.table, op_id: op_id, units: units, meta: meta}}
    )
  end

  @spec workers(t()) :: [pid()]
  def workers(inst) do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(workers_sup(inst)),
        is_pid(pid),
        do: pid
  end

  @spec workers_sup(t()) :: pid()
  def workers_sup(%__MODULE__{sup: sup}), do: child!(sup, :workers)

  # Direct grant API (the owner is any pid the authority can monitor).
  def reserve(inst, op_id, owner, units),
    do: Authority.reserve(authority(inst), op_id, owner, units)

  def authorize(inst, gid), do: Authority.authorize(authority(inst), gid)
  def cancel(inst, gid), do: Authority.cancel(authority(inst), gid)
  def settle(inst, gid, actual), do: Authority.settle(authority(inst), gid, actual)
  def reconcile(inst, gid, actual), do: Authority.reconcile(authority(inst), gid, actual)

  defp child!(sup, id) do
    case List.keyfind(Supervisor.which_children(sup), id, 0) do
      {^id, pid, _, _} when is_pid(pid) -> pid
      other -> raise "instance child #{inspect(id)} unavailable: #{inspect(other)}"
    end
  end
end
