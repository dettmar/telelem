defmodule Telelem.Instance.ManualClock do
  @moduledoc """
  Deterministic clock for tests and reruns (TEL-002). Time moves only through
  `advance/2`.

  Timers fire in `{deadline, arming order}` order. Each destination is
  resolved **when the timer fires**. `advance/2` replies only after every due
  message is sent, and returns `[{msg, pid | nil}]` so a caller can wait for
  exactly that many observations without sleeping.
  """
  use GenServer
  @behaviour Telelem.Instance.Clock

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{id: Keyword.get(opts, :id, __MODULE__), start: {__MODULE__, :start_link, [opts]}}
  end

  @impl Telelem.Instance.Clock
  def send_after(clock, dest, msg, ms) when is_integer(ms) and ms >= 0,
    do: GenServer.call(clock, {:send_after, dest, msg, ms})

  @impl Telelem.Instance.Clock
  def now(clock), do: GenServer.call(clock, :now)

  @spec advance(GenServer.server(), non_neg_integer()) :: [{term(), pid() | nil}]
  def advance(clock, ms) when is_integer(ms) and ms >= 0,
    do: GenServer.call(clock, {:advance, ms})

  @doc "Pending timers as `[{deadline, msg}]` in firing order."
  def pending(clock), do: GenServer.call(clock, :pending)

  @impl GenServer
  def init(_opts), do: {:ok, %{now: 0, seq: 0, timers: []}}

  @impl GenServer
  def handle_call({:send_after, dest, msg, ms}, _from, s) do
    ref = make_ref()
    timer = {{s.now + ms, s.seq}, ref, dest, msg}
    {:reply, ref, %{s | seq: s.seq + 1, timers: Enum.sort([timer | s.timers])}}
  end

  def handle_call(:now, _from, s), do: {:reply, s.now, s}

  def handle_call(:pending, _from, s),
    do: {:reply, for({{d, _}, _, _, m} <- s.timers, do: {d, m}), s}

  def handle_call({:advance, ms}, _from, s) do
    now = s.now + ms
    {due, rest} = Enum.split_while(s.timers, fn {{d, _}, _, _, _} -> d <= now end)

    fired =
      for {_, _ref, dest, msg} <- due do
        pid = Telelem.Instance.Directory.resolve(dest)
        if pid, do: send(pid, msg)
        {msg, pid}
      end

    {:reply, fired, %{s | now: now, timers: rest}}
  end
end
