defmodule Telelem.Serve.CoordinatorStart do
  @moduledoc """
  W2a: start one inactive `Telelem.Handoff` coordinator under a public
  `DynamicSupervisor` and activate it from the exact requester, all bounded
  by **one absolute instant `S`** (`System.monotonic_time(:millisecond)`;
  equality expires: `now >= S` is expired everywhere).

  Listener-free and transport-free: no socket, no attempt, no owner guard.
  The caller of `start/3` is the requester (`requester: self()`).

  ## The child spec (fixed, temporary, metadata only)

      %{id: Telelem.Handoff,
        start: {Telelem.Handoff, :start_link, [opts]},
        restart: :temporary, shutdown: 5_000, type: :worker,
        modules: [Telelem.Handoff]}

  `opts` are, in this order, the fixed `requester: self()`,
  `attempts: [:primary]`, `terminal_ack: true`, `activation_limit: S`, then
  the trusted options given to `start/3`, in the fixed order of
  `trusted_keys/0`. Only those keys are accepted
  (`:admission_authority`, `:terminal_deadline`, `:coordinator_grant` and
  `:receipts` required; `:observer`, `:trace`, `:cleanup_timeout`,
  `:register_timeout` optional), each at most once; anything else (a fixed
  key, `:on_event`, `:authority`, a request body, headers or authentication
  input) raises `ArgumentError` naming the key, never a value, before
  anything is spawned. Values are trusted setup and are checked by
  `Telelem.Handoff`'s own `init/1` (for example `S <= :terminal_deadline`).

  ## Processes

    * **requester** (the caller): creates a scoped alias, then spawns the
      watchdog with `spawn_opt(..., [:monitor])` (unlinked, monitored in one
      step). Every wait it does is bounded by `S`.
    * **watchdog** (unlinked, traps exits): monitors the requester first;
      only then, if the requester's `:DOWN` is not already delivered and
      `now < S`, spawns the worker with `spawn_link` (linked in the same
      step). It ends on the first of: the worker's `:EXIT` (the worker is
      gone), the requester's `:DOWN` or `now >= S`; in the last two cases it
      sends `exit(worker, :kill)` to the exact worker before it returns
      `:normal`. If the watchdog is killed instead, its link kills the worker
      (the worker does not trap exits). The worker therefore never outlives
      the watchdog.
    * **worker** (linked to the watchdog, not trapping): if `now < S`, calls
      the public `DynamicSupervisor.start_child/2` once (its wait is
      unbounded, which is why it is not the requester) and sends one
      sanitized result to the requester's alias: `{:ok, pid}`,
      `{:refused, category}` (an explicit `{:error, _}` or `:ignore` reply)
      or `:no_reply` (the call exited). Then it returns.

  Killing the watchdog or the worker does **not** withdraw a start request
  already queued at the supervisor. The supervisor may still process it: a
  start processed at or after `S` is refused by the coordinator's own
  `init/1` (`now < S` is required there); one processed before `S` yields an
  inactive coordinator nobody activates. Such a **late child** stays silent
  (`Telelem.Handoff` "Activation": nothing sent, emitted or published) and
  stops `:normal` at `S`, or earlier on the requester's `:DOWN`. Its startup
  reply goes to the deactivated alias (dropped by the runtime) or is
  drained.

  ## Requester steps

    1. `now >= S`: `{:error, {:not_sent, :expired}}`, nothing spawned.
    2. Wait (bounded by `S`) for the worker's result on the alias, or the
       watchdog's `:DOWN` (then a result already delivered is still taken).
       A result counts only if a clock read after receiving it is `< S`.
    3. Always, whatever won: send `exit(watchdog, :kill)` to the exact
       watchdog (no wait for it), deactivate the alias, drain results
       already delivered to it, and demonitor the watchdog with `:flush`.
    4. On a timely `{:ok, pid}` and `now < S`: `Telelem.Handoff.activate(pid,
       S)`, which is itself bounded by `S`. Nothing is retried and nothing is
       rolled back.

  ## Results: observed evidence only

    * `{:ok, pid}`: the activation reply `:ok` was received and checked
      before `S`; `pid` is active.
    * `{:error, {:not_sent, why}}`: no activation was submitted.
        * `:expired`: `S` was reached before submission (before spawning,
          or after a timely start, before calling `activate/2`).
        * `{:start_refused, category}`: an explicit refusal reply from the
          supervisor, observed before `S`. `category` is an atom,
          `{:bad_option, atom}`, `{:linked, atom}`, `{:bad_option, :other}`
          or `:other`; no other part of the reason is copied.
        * `:start_unconfirmed`: no startup result checked before `S` (a
          timeout at `S`, a result observed at or after `S`, the watchdog's
          `:DOWN` without a delivered result, or the worker's call exiting).
          A late child may exist; see above.
        * `:handoff_down`: `activate/2` found the started coordinator not
          alive before sending.
    * `{:error, {:refused, reason, pid}}`: `activate/2` returned
      `{:error, reason}` (one of the coordinator's documented decisions,
      received before `S`). `pid` is not active, except for `:duplicate`,
      which cannot happen here (this is the child's only activation).
      `:activation_expired` is included as W1 reports it: either
      `activate/2`'s own pre-send clock check (nothing sent) or the
      coordinator's check (it stopped); in both cases nothing was published
      and it is not active.
    * `{:error, {:unconfirmed, pid}}`: the activation was submitted and no
      reply was received and checked before `S`. `pid` may be active with
      `ingress` published and will then message the requester as any active
      coordinator. Never retry or treat it as "not activated".

  ## Known limits

    * The bound is a monotonic-clock check, not real-time scheduling: a
      requester scheduled after `S` reports `:start_unconfirmed` or
      `{:unconfirmed, pid}`, never a decision.
    * `:activation_expired` from `activate/2` does not say whether the
      request was submitted (W1 API); both cases are "not active, nothing
      published".
    * The exact watchdog is killed on every return; its worker, if it was
      blocked in `start_child/2`, dies by the link. The supervisor's queued
      request is not withdrawn (see "late child").
  """

  alias Telelem.Handoff

  @tag :telelem_coordinator_start
  @max_wait 4_294_967_295
  @required_keys [:admission_authority, :terminal_deadline, :coordinator_grant, :receipts]
  @optional_keys [:observer, :trace, :cleanup_timeout, :register_timeout]
  @trusted_keys @required_keys ++ @optional_keys

  @type not_sent ::
          :expired
          | {:start_refused, term()}
          | :start_unconfirmed
          | :handoff_down
  @type result ::
          {:ok, pid()}
          | {:error, {:not_sent, not_sent()}}
          | {:error, {:refused, atom(), pid()}}
          | {:error, {:unconfirmed, pid()}}

  @doc "The accepted trusted option keys, in the fixed order they are passed on."
  def trusted_keys, do: @trusted_keys

  @doc """
  Starts one inactive coordinator under the local `DynamicSupervisor` pid
  `sup` and activates it from the caller (its requester) before the
  absolute instant `s`. See the module doc.

  Raises `ArgumentError` (naming the field, never a value), before anything
  is spawned or sent, for a non-pid or remote `sup`, a non-integer `s` or
  one more than `Telelem.Handoff.max_deadline_ahead_ms/0` ahead of now, and
  for options that are not exactly a keyword list of the trusted keys.
  """
  @spec start(pid(), integer(), keyword()) :: result()
  def start(sup, s, opts) do
    spec = spec!(sup, s, opts)

    if now() >= s,
      do: {:error, {:not_sent, :expired}},
      else: launch(sup, s, spec)
  end

  # ── validation and the fixed spec ────────────────────────────────────────

  defp spec!(sup, s, opts) do
    unless is_pid(sup) and node(sup) == node(), do: arg_error("sup must be a local pid")
    unless is_integer(s), do: arg_error("s must be an integer")

    unless Handoff.deadline_in_range?(s),
      do: arg_error("s must be at most #{Handoff.max_deadline_ahead_ms()} ms ahead of now")

    unless is_list(opts) and Enum.all?(opts, &match?({k, _} when is_atom(k), &1)),
      do: arg_error("options must be a keyword list")

    keys = Keyword.keys(opts)
    unless length(Enum.uniq(keys)) == length(keys), do: arg_error("each option at most once")

    case keys -- @trusted_keys do
      [] -> :ok
      [k | _] -> arg_error("option #{inspect(k)} is not accepted")
    end

    case @required_keys -- keys do
      [] -> :ok
      [k | _] -> arg_error("option #{inspect(k)} is required")
    end

    fixed = [requester: self(), attempts: [:primary], terminal_ack: true, activation_limit: s]
    trusted = for k <- @trusted_keys, Keyword.has_key?(opts, k), do: {k, Keyword.fetch!(opts, k)}

    %{
      id: Handoff,
      start: {Handoff, :start_link, [fixed ++ trusted]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker,
      modules: [Handoff]
    }
  end

  defp arg_error(msg), do: raise(ArgumentError, "CoordinatorStart: " <> msg)

  # ── requester ────────────────────────────────────────────────────────────

  defp launch(sup, s, spec) do
    al = Process.alias()

    {wd, wmon} =
      :erlang.spawn_opt(
        __MODULE__,
        :watchdog,
        [%{requester: self(), alias: al, sup: sup, spec: spec, limit: s}],
        [:monitor]
      )

    started = await_start(al, wmon, s)

    # Whatever won: end the exact watchdog without waiting (its worker dies
    # by the link), then close the scope. A queued start is not withdrawn.
    Process.exit(wd, :kill)
    Process.unalias(al)
    drain(al)
    Process.demonitor(wmon, [:flush])

    case started do
      {:ok, h} -> activate(h, s)
      {:error, why} -> {:error, {:not_sent, why}}
    end
  end

  # One receive loop up to S, re-checked every turn.
  defp await_start(al, wmon, s) do
    left = s - now()

    if left <= 0 do
      {:error, :start_unconfirmed}
    else
      receive do
        {@tag, ^al, res} ->
          timely(res, s)

        {:DOWN, ^wmon, :process, _, _} ->
          # A result already delivered (sent before the worker ended) wins.
          receive do
            {@tag, ^al, res} -> timely(res, s)
          after
            0 -> {:error, :start_unconfirmed}
          end
      after
        min(left, @max_wait) -> await_start(al, wmon, s)
      end
    end
  end

  # A result counts only if observed before S (equality expires).
  defp timely(res, s) do
    if now() < s do
      case res do
        {:ok, h} when is_pid(h) -> {:ok, h}
        {:refused, category} -> {:error, {:start_refused, category}}
        _ -> {:error, :start_unconfirmed}
      end
    else
      {:error, :start_unconfirmed}
    end
  end

  defp drain(al) do
    receive do
      {@tag, ^al, _} -> drain(al)
    after
      0 -> :ok
    end
  end

  defp activate(h, s) do
    if now() >= s do
      {:error, {:not_sent, :expired}}
    else
      case Handoff.activate(h, s) do
        :ok -> {:ok, h}
        {:error, :handoff_down} -> {:error, {:not_sent, :handoff_down}}
        {:error, :unconfirmed} -> {:error, {:unconfirmed, h}}
        {:error, reason} -> {:error, {:refused, reason, h}}
      end
    end
  end

  # ── watchdog (unlinked; traps exits) ─────────────────────────────────────

  @doc false
  def watchdog(%{requester: r, limit: s} = a) do
    rmon = Process.monitor(r)
    Process.flag(:trap_exit, true)

    cond do
      down?(rmon) ->
        :ok

      now() >= s ->
        :ok

      true ->
        w = spawn_link(__MODULE__, :worker, [Map.take(a, [:alias, :sup, :spec, :limit])])
        watch(w, rmon, s)
    end
  end

  # Returns :normal only after the worker's own :EXIT, or after sending the
  # exact worker a kill (its link alone would not end it on a :normal exit).
  defp watch(w, rmon, s) do
    left = s - now()

    if left <= 0 do
      Process.exit(w, :kill)
      :ok
    else
      receive do
        {:EXIT, ^w, _} ->
          :ok

        {:DOWN, ^rmon, :process, _, _} ->
          Process.exit(w, :kill)
          :ok
      after
        min(left, @max_wait) -> watch(w, rmon, s)
      end
    end
  end

  defp down?(mon) do
    receive do
      {:DOWN, ^mon, :process, _, _} -> true
    after
      0 -> false
    end
  end

  # ── worker (linked to the watchdog; does not trap exits) ─────────────────

  @doc false
  def worker(%{alias: al, sup: sup, spec: spec, limit: s}) do
    if now() < s do
      res =
        try do
          DynamicSupervisor.start_child(sup, spec)
        catch
          _, _ -> :no_reply
        end

      send(al, {@tag, al, sanitize(res)})
    end

    :ok
  end

  defp sanitize({:ok, h}) when is_pid(h) and node(h) == node(), do: {:ok, h}
  defp sanitize(:no_reply), do: :no_reply
  defp sanitize(:ignore), do: {:refused, :ignore}
  defp sanitize({:error, r}), do: {:refused, category(r)}
  defp sanitize(_), do: {:refused, :other}

  defp category(r) when is_atom(r), do: r
  defp category({:bad_option, a}) when is_atom(a), do: {:bad_option, a}
  defp category({:bad_option, _}), do: {:bad_option, :other}
  defp category({:linked, a}) when is_atom(a), do: {:linked, a}
  defp category(_), do: :other

  defp now, do: System.monotonic_time(:millisecond)
end
