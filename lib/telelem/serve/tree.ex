defmodule Telelem.Serve.Tree.Pins do
  @moduledoc """
  The pids `Telelem.Serve.Tree.resolve/2` returned at setup: the exact root
  and its two branch `DynamicSupervisor`s. A value, never re-resolved.
  """
  @enforce_keys [:root, :coordinators, :guards]
  defstruct @enforce_keys

  @type t :: %__MODULE__{root: pid(), coordinators: pid(), guards: pid()}
end

defmodule Telelem.Serve.Tree do
  @moduledoc """
  W2b: the supervision tree for served attempts. One root supervisor with
  two branch `DynamicSupervisor`s, started in this order:

    1. `:coordinators`: where `Telelem.Serve.CoordinatorStart.start/3`
       submits its temporary `Telelem.Handoff` children.
    2. `:guards`: where `Telelem.Wire.OwnerGuard.start/2` submits its
       temporary guard children.

  Listener-free: it starts no socket, listener, coordinator or guard itself.

  ## Installed specs

  The root (`init/1`): `strategy: :one_for_all`, `max_restarts: 0`
  (`max_seconds: 5`, which is irrelevant at zero). Each branch, exactly:

      %{id: :coordinators | :guards,
        start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
        restart: :permanent, type: :supervisor, shutdown: :infinity,
        modules: [DynamicSupervisor]}

  The root's own `child_spec/1` (only `[]` is accepted) is `type:
  :supervisor`, `shutdown: :infinity`, `restart: :permanent`. `start_link/1`
  registers no name.

  **Branch loss.** If either branch exits, for any reason, the root does not
  restart it (zero restarts allowed): it shuts the remaining branch down and
  exits `:shutdown`. No branch or root is replaced by this module; a parent
  that restarts the root gets a new tree with new pids (see "Pins").

  ## Pins (`resolve/2`, at setup only)

  `resolve(root)` turns an exact root pid into `%Telelem.Serve.Tree.Pins{}`
  once, at setup. Callers pass `pins.coordinators` and `pins.guards` on as
  pids; nothing here, or in either start function, looks anything up again,
  by name or otherwise. Pins are never rebound: if the tree is gone or
  replaced, the old pins keep naming the old (dead) pids, a later
  `CoordinatorStart.start/3` on them (otherwise valid and timely) is
  `{:error, {:not_sent, :start_unconfirmed}}` and a later
  `OwnerGuard.start/2` is `{:error, :start_failed}`, and a new setup must
  resolve the new root.

  Steps, in order; the first failure is returned:

    1. `root` is a local pid, else `{:error, :invalid_root}` (a registered
       name is refused here).
    2. `root` is alive, else `{:error, :root_down}`.
    3. Its initial call is exactly `{:supervisor, Telelem.Serve.Tree, 1}`,
       else `{:error, :not_tree}` (`:root_down` if it died meanwhile). A
       supervisor of another module with the same children (a lookalike), a
       bare `DynamicSupervisor` or any other process is refused without being
       called.
    4. **One** `which_children` snapshot, a call bounded by `timeout` (ms,
       default 5 000, `1..60_000`): an exit with `root` no longer alive is
       `{:error, :root_down}`, any other exit or no reply within `timeout` is
       `{:error, :no_snapshot}`. (The call is the supervisor's own
       `:which_children` request, as `Supervisor.which_children/1` sends it,
       with a finite timeout: a root busy shutting a branch down could
       otherwise block setup without bound.)
    5. The snapshot is exactly two entries, in any order,
       `{:coordinators, c, :supervisor, [DynamicSupervisor]}` and
       `{:guards, g, :supervisor, [DynamicSupervisor]}`, with `c` and `g`
       local pids, alive, distinct from each other and from `root`; anything
       else (a missing, extra or renamed branch, another module or type,
       `:restarting` or `:undefined`, a remote, dead or repeated pid) is
       `{:error, :invalid_snapshot}`.

  Only the snapshot is trusted for the branches; their own initial calls are
  not checked. The initial-call check reads the process dictionary, which a
  process can write for itself: a process that forges it is not detected by
  step 3 (step 5 still applies to whatever it replies). Success says only
  that the three pids were as above when checked; they may die later.

  ## Orderly stop: branch ordering only

  When the root is stopped in order (its parent's shutdown, or
  `Supervisor.stop/3`), OTP terminates its children one at a time in reverse
  start order and, with `shutdown: :infinity`, waits for each to exit before
  the next. So the `:guards` branch is told to shut down first, and the
  `:coordinators` branch is told only after the `:guards` branch has exited,
  which a `DynamicSupervisor` does only after every one of its children
  (each guard, within its own `shutdown`) has exited. Any process that the
  `:coordinators` branch's shutdown reaches therefore finds the `:guards`
  branch and every guard it supervised already dead (exiting or exited).

  That is all this ordering gives. It is **not**:

    * the exit of the exact owners: a guard that reports `:owner_down` has
      seen its owner's `:DOWN`, but a guard killed by its own shutdown timer
      leaves the owner to its link, unconfirmed (see `Telelem.Wire.OwnerGuard`
      "Supervisor shutdown");
    * TLS/socket teardown, connection or admission slot quiescence, or any
      ledger state;
    * an overall time bound: both levels wait with `:infinity`; only each
      child's own `shutdown` is finite.

  It holds only for an orderly stop. On branch loss only the lost branch's
  supervisor pid has exited before its sibling is shut down; if it was lost
  abruptly (e.g. killed), its workers may still be exiting, so no order
  between coordinator and guard exits follows. If the root is killed, both
  branches get its exit signal at once and shut down concurrently. Neither
  abrupt branch loss nor a root kill gives any worker exit order.
  """

  use Supervisor

  alias Telelem.Serve.Tree.Pins

  @branch_ids [:coordinators, :guards]
  @default_timeout 5_000
  @max_timeout 60_000

  @type resolve_error ::
          :invalid_root | :root_down | :not_tree | :no_snapshot | :invalid_snapshot

  @doc "The root's child spec; only `[]` is accepted."
  def child_spec([]) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [[]]},
      restart: :permanent,
      type: :supervisor,
      shutdown: :infinity,
      modules: [__MODULE__]
    }
  end

  def child_spec(_), do: raise(ArgumentError, "Tree: no options are accepted")

  @doc "Starts the root, linked to the caller; registers no name."
  @spec start_link([]) :: Supervisor.on_start()
  def start_link([]), do: Supervisor.start_link(__MODULE__, :ok)
  def start_link(_), do: raise(ArgumentError, "Tree: no options are accepted")

  @impl true
  def init(:ok) do
    children = for id <- @branch_ids, do: branch(id)
    Supervisor.init(children, strategy: :one_for_all, max_restarts: 0)
  end

  defp branch(id) do
    %{
      id: id,
      start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
      restart: :permanent,
      type: :supervisor,
      shutdown: :infinity,
      modules: [DynamicSupervisor]
    }
  end

  @doc """
  Resolves the pins of the exact live local root `root`, once, at setup.
  See "Pins". Raises `ArgumentError` only for a `timeout` outside
  `1..60_000`.
  """
  @spec resolve(pid(), pos_integer()) :: {:ok, Pins.t()} | {:error, resolve_error()}
  def resolve(root, timeout \\ @default_timeout) do
    unless is_integer(timeout) and timeout >= 1 and timeout <= @max_timeout,
      do: raise(ArgumentError, "Tree: timeout must be an integer in 1..#{@max_timeout}")

    cond do
      not local_pid?(root) -> {:error, :invalid_root}
      not Process.alive?(root) -> {:error, :root_down}
      initial_call(root) != {:supervisor, __MODULE__, 1} -> down_or(root, :not_tree)
      true -> root |> snapshot(timeout) |> pins(root)
    end
  end

  defp snapshot(root, timeout) do
    {:ok, GenServer.call(root, :which_children, timeout)}
  catch
    :exit, _ -> down_or(root, :no_snapshot)
  end

  defp pins({:error, _} = e, _root), do: e

  defp pins({:ok, snap}, root) when is_list(snap) and length(snap) == 2 do
    if Enum.all?(snap, &(is_tuple(&1) and tuple_size(&1) == 4)) do
      case List.keysort(snap, 0) do
        [
          {:coordinators, c, :supervisor, [DynamicSupervisor]},
          {:guards, g, :supervisor, [DynamicSupervisor]}
        ] ->
          if Enum.all?([c, g], &(local_pid?(&1) and &1 != root and Process.alive?(&1))) and
               c != g,
             do: {:ok, %Pins{root: root, coordinators: c, guards: g}},
             else: {:error, :invalid_snapshot}

        _ ->
          {:error, :invalid_snapshot}
      end
    else
      {:error, :invalid_snapshot}
    end
  end

  defp pins({:ok, _}, _root), do: {:error, :invalid_snapshot}

  defp down_or(root, why),
    do: if(Process.alive?(root), do: {:error, why}, else: {:error, :root_down})

  defp initial_call(pid) do
    :proc_lib.translate_initial_call(pid)
  catch
    _, _ -> nil
  end

  defp local_pid?(p), do: is_pid(p) and node(p) == node()
end
