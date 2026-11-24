defmodule Telelem.Wire.Recorder do
  @moduledoc """
  Evidence log for the TEL-005 test gateway: an ordered, timestamped list of
  events per downstream request (`rid`), plus the coordinator trace
  (`{:telelem_handoff_trace, pid, event}` messages, mapped to a `rid` with
  `bind/3`).

  Timestamps are BEAM monotonic microseconds (`:t`) plus OS wall-clock
  nanoseconds (`:wall_ns`) for rough comparison with other processes on the
  same host. It is evidence only: nothing in the data path waits on it.
  """

  use GenServer

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, :ok, opts)

  @doc "Registers a new downstream request and returns its id."
  def ingress(rec, meta), do: GenServer.call(rec, {:ingress, meta})

  @doc "Maps a coordinator PID to a request id so its trace is recorded there."
  def bind(rec, rid, handoff), do: GenServer.call(rec, {:bind, rid, handoff})

  @doc "Appends an event (asynchronous; order is kept per sender)."
  def note(rec, rid, ev), do: send(rec, {:note, rid, stamp(), ev})

  @doc "Events of one request, oldest first: `[%{t:, wall_ns:, ev:}]`."
  def events(rec, rid), do: GenServer.call(rec, {:events, rid})

  @doc "All request ids, in arrival order."
  def rids(rec), do: GenServer.call(rec, :rids)

  @doc """
  Blocks until `fun.(events)` returns a truthy value for request `rid`, or
  `timeout` ms pass. Returns `{:ok, value}` or `{:timeout, events}`.
  """
  def wait(rec, rid, fun, timeout) do
    GenServer.call(rec, {:wait, rid, fun, timeout}, timeout + 1_000)
  end

  defp stamp, do: {System.monotonic_time(:microsecond), System.os_time(:nanosecond)}

  @impl true
  def init(:ok), do: {:ok, %{next: 1, ev: %{}, order: [], handoffs: %{}, waiters: []}}

  @impl true
  def handle_call({:ingress, meta}, _from, s) do
    rid = s.next
    s = %{s | next: rid + 1, order: [rid | s.order]}
    {:reply, rid, add(s, rid, stamp(), {:ingress, meta})}
  end

  def handle_call({:bind, rid, h}, _from, s), do: {:reply, :ok, put_in(s.handoffs[h], rid)}

  def handle_call({:events, rid}, _from, s), do: {:reply, list(s, rid), s}
  def handle_call(:rids, _from, s), do: {:reply, Enum.reverse(s.order), s}

  def handle_call({:wait, rid, fun, timeout}, from, s) do
    case safe(fun, list(s, rid)) do
      v when v not in [nil, false] ->
        {:reply, {:ok, v}, s}

      _ ->
        tref = Process.send_after(self(), {:wait_timeout, from}, timeout)
        {:noreply, %{s | waiters: [{from, rid, fun, tref} | s.waiters]}}
    end
  end

  @impl true
  def handle_info({:note, rid, ts, ev}, s), do: {:noreply, s |> add(rid, ts, ev) |> wake(rid)}

  def handle_info({:telelem_handoff_trace, h, ev}, s) do
    case s.handoffs[h] do
      nil -> {:noreply, s}
      rid -> {:noreply, s |> add(rid, stamp(), {:handoff, ev}) |> wake(rid)}
    end
  end

  def handle_info({:wait_timeout, from}, s) do
    case Enum.split_with(s.waiters, fn {f, _, _, _} -> f == from end) do
      {[{^from, rid, _, _}], rest} ->
        GenServer.reply(from, {:timeout, list(s, rid)})
        {:noreply, %{s | waiters: rest}}

      _ ->
        {:noreply, s}
    end
  end

  def handle_info(_, s), do: {:noreply, s}

  defp add(s, rid, {t, wall}, ev) do
    update_in(s.ev[rid], fn l -> [%{t: t, wall_ns: wall, ev: ev} | l || []] end)
  end

  defp list(s, rid), do: Enum.reverse(s.ev[rid] || [])

  defp wake(s, rid) do
    {done, keep} =
      Enum.split_with(s.waiters, fn {from, r, fun, tref} ->
        r == rid and
          case safe(fun, list(s, rid)) do
            v when v not in [nil, false] ->
              Process.cancel_timer(tref)
              GenServer.reply(from, {:ok, v})
              true

            _ ->
              false
          end
      end)

    _ = done
    %{s | waiters: keep}
  end

  defp safe(fun, events) do
    fun.(events)
  rescue
    _ -> nil
  end
end
