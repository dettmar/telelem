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
  application env. Each supervisor start mints a boot-unique `instance_id`
  (128 random bits), which every grant token embeds. A token or handle from
  another instance, or from a previous life of this one, cannot act here.

  **M1 coverage.** The ledger is volatile, in memory for one instance life.
  If the parent restarts the instance (for example after `max_restarts`),
  the new life starts empty, and `snapshot/1`'s `coverage` reports the prior
  exposure as `:unknown`, never zero. Limits are local and soft: there is no
  durable billing or global hard limit.
  """
  use Supervisor

  alias Telelem.Admission.Authority
  alias Telelem.Instance.Directory

  @enforce_keys [:sup, :table, :tag, :instance_id]
  defstruct [:sup, :table, :tag, :instance_id]

  @type t :: %__MODULE__{
          sup: pid(),
          table: :ets.tid(),
          tag: reference(),
          instance_id: binary()
        }

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
      # Boot-unique: a parent restart of this supervisor mints a new one, so
      # tokens from a previous life are foreign here (see Admission.Ledger).
      instance_id: Telelem.Admission.Ledger.new_instance_id(),
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
    meta = Directory.meta(table)
    %__MODULE__{sup: sup, table: table, tag: meta.tag, instance_id: meta.instance_id}
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

  @doc "Opaque boot-unique id of this instance life (embedded in every grant token)."
  @spec instance_id(t()) :: binary()
  def instance_id(%__MODULE__{instance_id: iid}), do: iid

  # Direct grant API (the owner is any pid the authority can monitor). A
  # handle whose instance has stopped or been replaced, or whose authority is
  # restarting, returns {:error, :instance_unavailable}. It never reaches
  # another instance.
  def reserve(inst, op_id, owner, units),
    do: via(inst, &Authority.reserve(&1, op_id, owner, units))

  def authorize(inst, gid), do: via(inst, &Authority.authorize(&1, gid))
  def cancel(inst, gid), do: via(inst, &Authority.cancel(&1, gid))
  def settle(inst, gid, actual), do: via(inst, &Authority.settle(&1, gid, actual))
  def reconcile(inst, gid, actual), do: via(inst, &Authority.reconcile(&1, gid, actual))

  defp via(%__MODULE__{table: t}, fun) do
    case Directory.authority(t) do
      {pid, _} -> fun.(pid)
      nil -> {:error, :instance_unavailable}
    end
  end

  defp child!(sup, id) do
    case List.keyfind(Supervisor.which_children(sup), id, 0) do
      {^id, pid, _, _} when is_pid(pid) -> pid
      other -> raise "instance child #{inspect(id)} unavailable: #{inspect(other)}"
    end
  end
end
