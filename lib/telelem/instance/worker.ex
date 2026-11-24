defmodule Telelem.Instance.Worker do
  @moduledoc """
  One unit of work under an instance's worker supervisor (TEL-002).

  The worker is the grant **owner**: it reserves with `self()`, so the
  authority's monitor releases its slot if it dies. Lifecycle:

    1. `init/1`: reserve (all-or-none), then check out a transport. If the
       reservation is refused, start fails with `{:shutdown, {:admission, reason}}`.
       If checkout fails, the grant is cancelled before the worker exits.
    2. `dispatch/2`: authorize the dispatch, then send the request on the
       held transport.
    3. `complete/2`: settle the actual usage and close the transport, then stop.
       `cancel/1`: close the transport and cancel the grant, then stop.

  The worker traps exits so supervisor shutdown runs `terminate/2`, which
  closes a still-held transport. Restart is `:temporary`.
  """
  use GenServer, restart: :temporary

  alias Telelem.Admission.Authority
  alias Telelem.Instance.Directory

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @spec dispatch(pid(), term()) :: {:ok, term()} | {:duplicate, term()} | {:error, term()}
  def dispatch(w, request), do: GenServer.call(w, {:dispatch, request})

  @spec complete(pid(), non_neg_integer()) ::
          {:ok, term()} | {:duplicate, term()} | {:error, term()}
  def complete(w, actual), do: GenServer.call(w, {:complete, actual})

  @spec cancel(pid()) :: {:ok, term()} | {:duplicate, term()} | {:error, term()}
  def cancel(w), do: GenServer.call(w, :cancel)

  @doc "`%{grant: grant_id, op_id: term, incarnation: pos_integer, conn: term}`"
  def info(w), do: GenServer.call(w, :info)

  @impl true
  def init(%{table: table, op_id: op_id, units: units} = args) do
    Process.flag(:trap_exit, true)
    {tmod, targ} = Directory.meta(table).transport

    with {:auth, {auth, inc}} <- {:auth, Directory.authority(table)},
         {:ok, gid} <- Authority.reserve(auth, op_id, self(), units) do
      case tmod.checkout(targ, Map.merge(Map.get(args, :meta, %{}), %{op_id: op_id, grant: gid})) do
        {:ok, conn} ->
          {:ok, %{table: table, gid: gid, op_id: op_id, incarnation: inc, tmod: tmod, conn: conn}}

        {:error, reason} ->
          _ = Authority.cancel(auth, gid)
          {:stop, {:shutdown, {:transport, reason}}}
      end
    else
      {:auth, nil} -> {:stop, {:shutdown, {:admission, :authority_unavailable}}}
      {:duplicate, _gid} -> {:stop, {:shutdown, {:admission, :duplicate_op}}}
      {:error, reason} -> {:stop, {:shutdown, {:admission, reason}}}
    end
  end

  @impl true
  def handle_call({:dispatch, request}, _from, s) do
    case call_authority(s, &Authority.authorize(&1, s.gid)) do
      {:ok, _} = ok ->
        case s.tmod.dispatch(s.conn, request) do
          :ok -> {:reply, ok, s}
          {:error, reason} -> {:reply, {:error, {:transport, reason}}, s}
        end

      other ->
        {:reply, other, s}
    end
  end

  def handle_call({:complete, actual}, _from, s) do
    result = call_authority(s, &Authority.settle(&1, s.gid, actual))
    {:stop, :normal, result, close(s)}
  end

  def handle_call(:cancel, _from, s) do
    s = close(s)
    {:stop, :normal, call_authority(s, &Authority.cancel(&1, s.gid)), s}
  end

  def handle_call(:info, _from, s),
    do: {:reply, Map.take(s, [:gid, :op_id, :incarnation, :conn]) |> rename_gid(), s}

  @impl true
  def handle_info({:EXIT, _from, reason}, s), do: {:stop, reason, s}
  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def terminate(_reason, s), do: close(s)

  defp call_authority(s, fun) do
    case Directory.authority(s.table) do
      {auth, _} -> fun.(auth)
      nil -> {:error, :authority_unavailable}
    end
  end

  defp close(%{conn: nil} = s), do: s

  defp close(s) do
    :ok = s.tmod.close(s.conn)
    %{s | conn: nil}
  end

  defp rename_gid(m), do: m |> Map.put(:grant, m.gid) |> Map.delete(:gid)
end
