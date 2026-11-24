defmodule Telelem.Ops.MemoryStore do
  @moduledoc """
  Reference `Telelem.Ops.RevisionStore`: one GenServer per store, so each
  compare-and-write is a single serialized step (no read-then-write window).

  Records are keyed by `{tenant_id, route_id}`; a lookup can never see another
  tenant's route. State is in memory only; durability ("`up` twice preserves
  state") is not provided here.

  A caller that exits on a `GenServer.call/3` timeout cannot tell whether its
  write landed. It must `fetch/3` and compare `last_plan_digest` with its own
  plan digest rather than retry blindly; a retry of a landed plan conflicts.
  """

  use GenServer
  @behaviour Telelem.Ops.RevisionStore

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.take(opts, [:name]))
  end

  @doc "Insert an initial revision if the route is absent. Never overwrites."
  @spec seed(GenServer.server(), String.t(), Telelem.Ops.Revisions.revision()) ::
          :ok | {:error, :exists}
  def seed(server, tenant_id, %{route_id: route_id} = revision) do
    GenServer.call(server, {:seed, {tenant_id, route_id}, revision})
  end

  @impl Telelem.Ops.RevisionStore
  def fetch(server, tenant_id, route_id) do
    GenServer.call(server, {:fetch, {tenant_id, route_id}})
  end

  @impl Telelem.Ops.RevisionStore
  def compare_and_write(server, tenant_id, route_id, expected, new_revision, plan_digest) do
    GenServer.call(server, {:cas, {tenant_id, route_id}, expected, new_revision, plan_digest})
  end

  @impl GenServer
  def init(:ok), do: {:ok, %{}}

  @impl GenServer
  def handle_call({:seed, key, revision}, _from, state) do
    if Map.has_key?(state, key) do
      {:reply, {:error, :exists}, state}
    else
      {:reply, :ok, Map.put(state, key, %{revision: revision, last_plan_digest: nil})}
    end
  end

  def handle_call({:fetch, key}, _from, state) do
    case state do
      %{^key => record} -> {:reply, {:ok, record}, state}
      _ -> {:reply, :not_found, state}
    end
  end

  def handle_call(
        {:cas, key, %{rev: rev, digest: digest}, new_revision, plan_digest},
        _from,
        state
      ) do
    case state do
      %{^key => %{revision: %{rev: ^rev, digest: ^digest}}} ->
        record = %{revision: new_revision, last_plan_digest: plan_digest}
        {:reply, {:ok, record}, Map.put(state, key, record)}

      %{^key => record} ->
        {:reply, {:conflict, record}, state}

      _ ->
        {:reply, {:conflict, :not_found}, state}
    end
  end
end
