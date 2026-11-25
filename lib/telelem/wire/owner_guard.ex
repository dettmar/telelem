defmodule Telelem.Wire.OwnerGuard do
  @moduledoc """
  TEL-012 owner guard, listener-free slice: a temporary supervised guard that
  holds one fixed, inert, non-trapping **owner** process and ends it when the
  request it serves is gone or out of time.

  Nothing here opens a socket, holds a TLS session or talks to a real
  `Telelem.Wire.Upstream`, `Telelem.Handoff` or admission authority. The owner
  is a placeholder for a future socket owner and does no work.

  ## Startup (metadata only, one-shot activation)

  `start/2` runs in the **caller** (the process the guard serves):

    1. Validates a closed option set (`:handoff`, `:cutoff`, `:grace_ms`,
       `:activation_ms`; each required, each once) and raises
       `ArgumentError` naming the field, never its value. A `:cutoff` that has
       already passed returns `{:error, :cutoff}` and starts nothing.
    2. Starts the guard with `DynamicSupervisor.start_child/2` and the spec
       from `child_spec/1`. The child's start arguments are **metadata only**:
       the caller pid, a fresh reference, the pinned Handoff pid and three
       integers. No function, module or caller term is passed; the guard's
       entry (`start_link/1`) re-validates that exact shape and refuses
       anything else with `{:error, :invalid_metadata}`. Before its init ack
       the guard traps exits and monitors the caller and the Handoff, so no
       trigger can be missed.
    3. Only after `start_child` returned `{:ok, guard}`, sends one activation
       message (tagged with the reference). On it, and only once, the guard
       spawns the owner with `spawn_opt(..., [:link, :monitor])` (link and
       monitor are set atomically with the spawn) and acks the owner pid to
       the caller. A repeated or wrongly tagged activation is ignored.
    4. Both sides bound the activation by the same absolute instant,
       `min(now + activation_ms, cutoff)`. The guard exits without spawning an
       owner if it is not activated by then (or if the caller, the Handoff or
       the supervisor goes first) and tells the caller why with a fixed
       category. If the ack does not arrive by then, the caller kills the
       exact guard pid (its link kills an owner, if one was spawned), waits at
       most `grace_ms` for its `:DOWN` and returns `{:error,
       :activation_timeout}`.

  The owner (`inert/0`) does not trap exits, is linked and monitored only by
  the guard, discards every message it receives and never exits by itself.

  ## Triggers and teardown

  While active, the guard ends the owner on the first of:

    * `{:EXIT, parent, _}` from its exact supervisor (`:parent_exit`);
    * `:DOWN` of its exact caller monitor (`:caller_down`);
    * `:DOWN` of its exact pinned Handoff monitor (`:handoff_down`);
    * the absolute `cutoff` (monotonic ms) passing (`:cutoff`).

  It then sends `exit(owner, :kill)` to the exact owner pid, and to no other
  process, and waits for the owner monitor's `:DOWN`. That wait ends at
  `min(now, cutoff) + grace_ms`, so no guard wait ever goes past `cutoff +
  grace_ms`. An owner `:DOWN` before any trigger is `:owner_exited`.

  The guard drains its mailbox in arrival order and re-checks the cutoff (or
  the grace end) against the clock **at every receive turn** before handling
  the message, so a busy mailbox cannot keep it past a bound (`after` alone
  would restart with every message). Messages it does not recognise are
  dropped, including `:EXIT` signals from other pids and `:DOWN` with other
  references.

  ## Report

  Only after the exact owner monitor `:DOWN`, the guard sends the caller once:

      {:telelem_owner_guard, ref, :owner_down, category}

  `category` is one of `categories/0`. The owner's exit reason is never
  forwarded. If the `:DOWN` does not arrive by the grace end, the guard sends
  **no** report and exits `{:shutdown, :owner_down_unconfirmed}` (or with its
  supervisor's reason, see below); the kill has been sent and the link still
  kills the owner when the guard exits.

  There is no accounting, no cleanup ack from the caller and no restart. The
  caller does not answer the report.

  ## Exit reasons

    * reported: `:normal`, or the supervisor's reason after a parent `:EXIT`;
    * unconfirmed: `{:shutdown, :owner_down_unconfirmed}`, or the supervisor's
      reason after a parent `:EXIT`;
    * not activated: `{:shutdown, c}` with `c` in `:activation_timeout`,
      `:cutoff`, `:caller_down`, `:handoff_down`, or the supervisor's reason.

  ## Supervisor shutdown policy (finite)

  `child_spec/1` is `restart: :temporary` with `shutdown: grace_ms + 1000`
  ms. On a supervisor shutdown the guard kills the owner and waits at most
  `grace_ms` for its `:DOWN`, then exits with the supervisor's reason. If the
  guard has not exited when the supervisor's timer ends (for example a busy
  mailbox ahead of the `:EXIT`), the supervisor kills it; the link then kills
  the owner and no report is sent.

  ## Not guaranteed

    * **kill-to-DOWN**: the owner's `:DOWN` is waited for at most until the
      grace end; a later one goes unreported (see above).
    * **TLS/socket teardown**: the owner holds none, and a kill closes nothing
      gracefully (no `close_notify`, no flush). A future socket owner needs its
      own teardown contract.
    * **successor ordering**: a successor attempt's guard or owner may start,
      and its report may arrive, before or after this owner's `:DOWN` or
      report. Nothing orders them.
    * a report after the guard itself is killed (by the supervisor timer or
      anyone else): the link kills the owner, nothing is reported.
    * authenticity of a parent `:EXIT`: any process can send the guard
      `{:EXIT, parent, _}` as a plain message. That can only end the owner
      early. The monitor references cannot be forged the same way.
    * `start/2` reaching a supervisor that does not answer: `start_child` is
      a call with no timeout of its own; the guard's own init is bounded by a
      fixed 5000 ms `:proc_lib` start timeout.

  The guard is a plain `:proc_lib` process without `:sys` support. Its grace
  is its own bounded option: it does not read `Telelem.Handoff`'s decision
  grace (there is no accessor for it); `cutoff + grace_ms` must fit one
  `receive ... after` wait (`2^32 - 1` ms) from now.
  """

  @tag :telelem_owner_guard
  @max_wait 4_294_967_295
  @max_grace_ms 60_000
  @max_activation_ms 60_000
  @shutdown_slack_ms 1_000
  @init_timeout_ms 5_000
  @opt_keys [:handoff, :cutoff, :grace_ms, :activation_ms]
  @categories [:owner_exited, :caller_down, :handoff_down, :parent_exit, :cutoff]
  @refusals [:activation_timeout, :cutoff, :caller_down, :handoff_down, :parent_exit]
  @meta_keys [:caller, :ref, :handoff, :cutoff, :grace_ms, :activation_until]

  @type category :: :owner_exited | :caller_down | :handoff_down | :parent_exit | :cutoff
  @type handle :: %{guard: pid(), owner: pid(), ref: reference()}
  @type start_error ::
          :cutoff
          | :start_failed
          | :activation_timeout
          | :handoff_down
          | :caller_down
          | :parent_exit
          | :guard_down

  @doc "The fixed report categories, in no particular order."
  @spec categories() :: [category()]
  def categories, do: @categories

  @doc "The option bounds (ms): `grace_ms` and `activation_ms` are `1..max`."
  def bounds,
    do: %{
      grace_ms: @max_grace_ms,
      activation_ms: @max_activation_ms,
      shutdown_slack_ms: @shutdown_slack_ms,
      max_wait_ms: @max_wait
    }

  # ── Caller side ─────────────────────────────────────────────────────────

  @doc """
  Starts and activates one guard under the `DynamicSupervisor` `sup` for the
  calling process. See the module doc for options and results.
  """
  @spec start(Supervisor.supervisor(), keyword()) :: {:ok, handle()} | {:error, start_error()}
  def start(sup, opts) do
    {handoff, cutoff, grace, act} = opts!(opts)
    now = now()

    if cutoff <= now do
      {:error, :cutoff}
    else
      meta = %{
        caller: self(),
        ref: make_ref(),
        handoff: handoff,
        cutoff: cutoff,
        grace_ms: grace,
        activation_until: min(now + act, cutoff)
      }

      result =
        try do
          DynamicSupervisor.start_child(sup, child_spec(meta))
        catch
          :exit, _ -> :start_failed
        end

      case result do
        {:ok, guard} when is_pid(guard) -> activate(guard, meta)
        _ -> {:error, :start_failed}
      end
    end
  end

  defp activate(guard, %{ref: ref} = meta) do
    gmon = Process.monitor(guard)
    send(guard, {@tag, ref, :activate})

    receive do
      {@tag, ^ref, :active, owner} ->
        Process.demonitor(gmon, [:flush])
        {:ok, %{guard: guard, owner: owner, ref: ref}}

      {@tag, ^ref, :refused, why} ->
        Process.demonitor(gmon, [:flush])
        {:error, why}

      {:DOWN, ^gmon, :process, ^guard, reason} ->
        receive do
          {@tag, ^ref, :refused, why} -> {:error, why}
        after
          0 -> {:error, down_category(reason)}
        end
    after
      max(meta.activation_until - now(), 0) ->
        Process.exit(guard, :kill)

        receive do
          {:DOWN, ^gmon, :process, ^guard, _} -> :ok
        after
          meta.grace_ms -> Process.demonitor(gmon, [:flush])
        end

        receive do
          {@tag, ^ref, :active, _} -> :ok
        after
          0 -> :ok
        end

        {:error, :activation_timeout}
    end
  end

  defp down_category({:shutdown, c}) when c in @refusals, do: c
  defp down_category(_), do: :guard_down

  defp opts!(opts) do
    unless is_list(opts) and Enum.all?(opts, &match?({k, _} when is_atom(k), &1)),
      do: opt_error("options must be a keyword list")

    keys = Keyword.keys(opts)
    unless length(Enum.uniq(keys)) == length(keys), do: opt_error("each option at most once")

    case keys -- @opt_keys do
      [] -> :ok
      [k | _] -> opt_error("option #{inspect(k)} is not accepted")
    end

    case @opt_keys -- keys do
      [] -> :ok
      [k | _] -> opt_error("option #{inspect(k)} is required")
    end

    handoff = opts[:handoff]

    unless is_pid(handoff) and node(handoff) == node(),
      do: opt_error(":handoff must be a local pid")

    grace = opts[:grace_ms]

    unless is_integer(grace) and grace in 1..@max_grace_ms,
      do: opt_error(":grace_ms must be an integer in 1..#{@max_grace_ms}")

    act = opts[:activation_ms]

    unless is_integer(act) and act in 1..@max_activation_ms,
      do: opt_error(":activation_ms must be an integer in 1..#{@max_activation_ms}")

    cutoff = opts[:cutoff]
    unless is_integer(cutoff), do: opt_error(":cutoff must be an integer (monotonic ms)")

    unless cutoff + grace - now() <= @max_wait,
      do: opt_error(":cutoff plus :grace_ms must be at most #{@max_wait} ms ahead")

    {handoff, cutoff, grace, act}
  end

  defp opt_error(msg), do: raise(ArgumentError, "OwnerGuard: " <> msg)

  # ── Child (metadata-only fixed entry) ────────────────────────────────────

  @doc """
  The temporary child spec for one guard. `meta` is the metadata map built by
  `start/2`; the guard re-validates it and refuses any other shape.
  """
  def child_spec(meta) do
    grace =
      case meta do
        %{grace_ms: g} when is_integer(g) and g in 1..@max_grace_ms -> g
        _ -> 0
      end

    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [meta]},
      restart: :temporary,
      shutdown: grace + @shutdown_slack_ms,
      type: :worker,
      modules: [__MODULE__]
    }
  end

  @doc false
  def start_link(meta),
    do: :proc_lib.start_link(__MODULE__, :init_guard, [self(), meta], @init_timeout_ms)

  @doc false
  def init_guard(parent, meta) do
    case meta(meta) do
      {:ok, s} ->
        Process.flag(:trap_exit, true)
        cmon = Process.monitor(s.caller)
        hmon = Process.monitor(s.handoff)
        :proc_lib.init_ack({:ok, self()})
        await_activation(Map.merge(s, %{parent: parent, cmon: cmon, hmon: hmon}))

      :error ->
        :proc_lib.init_fail({:error, :invalid_metadata}, {:exit, :normal})
    end
  end

  defp meta(
         %{
           caller: caller,
           ref: ref,
           handoff: handoff,
           cutoff: cutoff,
           grace_ms: grace,
           activation_until: until
         } = m
       )
       when map_size(m) == length(@meta_keys) and is_pid(caller) and node(caller) == node() and
              is_reference(ref) and is_pid(handoff) and node(handoff) == node() and
              is_integer(cutoff) and is_integer(grace) and grace >= 1 and
              grace <= @max_grace_ms and is_integer(until) and until <= cutoff do
    if cutoff + grace - now() <= @max_wait,
      do: {:ok, Map.take(m, @meta_keys)},
      else: :error
  end

  defp meta(_), do: :error

  @doc false
  # The fixed owner: does not trap exits, discards every message, never
  # returns. Only an exit signal ends it.
  def inert do
    receive do
      _ -> inert()
    end
  end

  # Not yet activated: no owner exists.
  defp await_activation(s) do
    now = now()

    if now >= s.activation_until do
      refuse(s, if(now >= s.cutoff, do: :cutoff, else: :activation_timeout), nil)
    else
      receive do
        msg -> pre(msg, s)
      after
        s.activation_until - now -> await_activation(s)
      end
    end
  end

  defp pre({@tag, ref, :activate}, %{ref: ref} = s) do
    {owner, omon} = :erlang.spawn_opt(__MODULE__, :inert, [], [:link, :monitor])
    send(s.caller, {@tag, ref, :active, owner})
    active(Map.merge(s, %{owner: owner, omon: omon, reason: nil}))
  end

  defp pre({:EXIT, parent, reason}, %{parent: parent} = s), do: refuse(s, :parent_exit, reason)

  defp pre({:DOWN, m, :process, pid, _}, %{cmon: m, caller: pid} = s),
    do: refuse(s, :caller_down, nil)

  defp pre({:DOWN, m, :process, pid, _}, %{hmon: m, handoff: pid} = s),
    do: refuse(s, :handoff_down, nil)

  defp pre(_, s), do: await_activation(s)

  defp refuse(s, why, parent_reason) do
    send(s.caller, {@tag, s.ref, :refused, why})
    exit(if parent_reason == nil, do: {:shutdown, why}, else: parent_reason)
  end

  # Active: the owner is alive until a trigger or its own exit.
  defp active(s) do
    now = now()

    if now >= s.cutoff do
      force(s, :cutoff, now)
    else
      receive do
        msg -> on_active(msg, s)
      after
        s.cutoff - now -> active(s)
      end
    end
  end

  defp on_active({:DOWN, m, :process, pid, _}, %{omon: m, owner: pid} = s),
    do: done(s, :owner_exited)

  defp on_active({:EXIT, parent, reason}, %{parent: parent} = s),
    do: force(%{s | reason: {:parent, reason}}, :parent_exit, now())

  defp on_active({:DOWN, m, :process, pid, _}, %{cmon: m, caller: pid} = s),
    do: force(s, :caller_down, now())

  defp on_active({:DOWN, m, :process, pid, _}, %{hmon: m, handoff: pid} = s),
    do: force(s, :handoff_down, now())

  defp on_active(_, s), do: active(s)

  defp force(s, cause, now) do
    Process.exit(s.owner, :kill)
    draining(Map.merge(s, %{cause: cause, until: min(now, s.cutoff) + s.grace_ms}))
  end

  # Killed: wait for the exact owner :DOWN until the grace end.
  defp draining(s) do
    now = now()

    if now >= s.until do
      exit(exit_reason(s, {:shutdown, :owner_down_unconfirmed}))
    else
      receive do
        {:DOWN, m, :process, pid, _} when m == s.omon and pid == s.owner ->
          done(s, s.cause)

        {:EXIT, parent, reason} when parent == s.parent ->
          draining(%{s | reason: {:parent, reason}})

        _ ->
          draining(s)
      after
        s.until - now -> draining(s)
      end
    end
  end

  defp done(s, category) do
    send(s.caller, {@tag, s.ref, :owner_down, category})
    exit(exit_reason(s, :normal))
  end

  defp exit_reason(%{reason: {:parent, reason}}, _), do: reason
  defp exit_reason(_, default), do: default

  defp now, do: System.monotonic_time(:millisecond)
end
