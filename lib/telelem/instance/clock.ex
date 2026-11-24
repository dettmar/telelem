defmodule Telelem.Instance.Clock do
  @moduledoc """
  Injected clock for an instance (TEL-002). Each instance takes `{module, arg}`.

  The destination is a pid or `{:telelem_instance_authority, table}`. Each
  implementation decides when to resolve it:

    * `Telelem.Instance.SystemClock` resolves when the timer is armed. A
      timer from an incarnation that has since crashed is dropped with its pid.
    * `Telelem.Instance.ManualClock` resolves when the timer fires. A timer
      armed by an old incarnation then reaches its successor: the worst case
      for late callbacks, which the authority must fence.
  """

  @type dest :: pid() | {:telelem_instance_authority, :ets.tid()}

  @callback send_after(arg :: term(), dest(), msg :: term(), ms :: non_neg_integer()) :: term()
  @callback now(arg :: term()) :: integer()
end

defmodule Telelem.Instance.SystemClock do
  @moduledoc "Monotonic wall clock; timers are bound to the pid current at arming time."
  @behaviour Telelem.Instance.Clock

  @impl true
  def send_after(_arg, dest, msg, ms) do
    case Telelem.Instance.Directory.resolve(dest) do
      nil -> nil
      pid -> Process.send_after(pid, msg, ms)
    end
  end

  @impl true
  def now(_arg), do: System.monotonic_time(:millisecond)
end
