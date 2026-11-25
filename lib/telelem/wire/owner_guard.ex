defmodule Telelem.Wire.OwnerGuard do
  @moduledoc """
  TEL-012 guarded owner launch: one gated `Telelem.Wire.Upstream` attempt
  (the **owner**) started under a `DynamicSupervisor`, held by a temporary
  **guard**, and watched by an independent **witness**. This is the only
  supported launch of a gated attempt: `Upstream.start_link/1` refuses any
  gated payload without spawning. Legacy M1 attempts (`Upstream.start_link/1`
  without `:gate`) do not go through here and are unchanged.

  ## Roles

    * **launcher**: the process calling `start/2`.
    * **witness**: a process outside every supervisor and link set of the
      launcher, requester and guard, spawned by the launcher with an atomic
      monitor. It traps exits (so an exit signal other than `:kill`, and
      other than its helper's, is dropped), holds its own private monitors
      on the launcher, the guard and the owner, is linked only to the
      helper, and is the only process that sends the guard its activation.
    * **helper**: a non-trapping process spawned and linked atomically by the
      witness (and linked to nothing else), so it dies with the witness. It
      wraps the public `DynamicSupervisor.start_child/2` and sends only a
      sanitized result (`{:ok, guard_pid}` or `:error`), to the witness only.
    * **guard**: the temporary supervised child. It monitors the requester and
      the pinned coordinator, traps exits, and owns the owner by link and
      monitor.
    * **owner**: a real `Upstream` process at the fixed entry
      `Upstream.guarded/1`, spawned by the guard with
      `spawn_opt(..., [:link, :monitor])`. It does not trap exits.

  ## Options (`start/2`, validated in the launcher)

  Exactly `:upstream`, `:requester`, `:observer`, `:grace_ms` and
  `:activation_ms`, each once; anything else raises `ArgumentError` naming the
  field, never its value.

    * `:upstream`: the gated `Upstream` options (`gate: [target:,
      deadline:]`, see "Gated attempt" there), validated in full. Its
      `:handoff` (a local pid) is the pinned coordinator and its gate deadline
      is the system deadline `D`. This payload is sent only to the owner,
      in its GO; it is never part of the child spec, the helper's or the
      witness's arguments.
    * `:requester`, `:observer`: local pids. The observer receives the
      reports below.
    * `:grace_ms`, `:activation_ms`: integers in `1..60_000`.

  ## Activation: one absolute limit

  The launcher computes `A = min(now + activation_ms, D)` once. Every
  activation step, in every process, has expired when `now >= A`
  (equality expires). `start/2` returns `{:error, :expired}` without
  spawning anything if `A` is already reached.

    1. The launcher creates an alias and spawns the witness with
       `spawn_opt(..., [:monitor])`. The witness monitors the launcher and
       traps exits, then (unless it already knows it is closed: `A` reached,
       or the launcher's `:DOWN` or abandonment already delivered, in which
       case it spawns no helper) spawns the helper with
       `spawn_opt(..., [:link])` and sends its exact pid to the launcher's
       alias. The helper waits, doing nothing, until the launcher releases
       it; it exits without work at `A`.
    2. The launcher records that exact helper pid first, then, only if no
       witness `:DOWN` is already delivered and `A` is not reached, releases
       it. The helper (before `A`) calls `DynamicSupervisor.start_child/2`
       with a metadata-only child spec: token, witness, observer, requester,
       coordinator, `D`, `A`, `grace_ms`. The guard's entry re-validates that
       exact shape. A child whose start is processed at or after `A` (for
       example queued behind a busy supervisor) fails its init with
       `{:error, :expired}` and does no work. The helper sends its sanitized
       result to the witness only.
    3. **The witness relays the result and is the only releaser of
       activation.** It takes the first result only. On `{:ok, guard}` it
       records and monitors that exact guard and relays the result to the
       launcher's alias, all before any activation; then, before `A` and
       only if it has not closed (see below), it sends that guard the one
       activation. On `:error`, or on the helper's exact `EXIT` without a
       result, it relays `:error` while not closed. It never activates any
       other guard (a late child the supervisor started after the helper was
       killed is never activated; it expires at `A`). The launcher monitors
       the relayed guard and sends it nothing.
    4. The activated guard (before `A`) spawns the owner, waiting at the
       fixed entry, and announces its exact pid to the witness. The guard
       never sends GO. The witness accepts an announcement only from the
       guard it activated, monitors the exact owner with its own reference
       and then acknowledges `{:watching, guard, owner}` to the launcher's
       alias.
    5. On that acknowledgement the launcher first takes any already
       delivered `:DOWN` of the witness or the guard as a failure (no GO),
       then checks `A`, and only then sends the owner one GO with the token
       and the payload and returns `{:ok, %{guard:, owner:, witness:, ref:}}`
       without waiting for anything further. The owner validates the payload
       once, then, directly before `attach_grant/3`, rechecks a pending
       scoped stop and `A` (see `Upstream`). A GO that is late, wrongly
       tagged or for another coordinator or deadline attaches nothing.

  The witness **closes** (it will never release activation) on the
  launcher's abandonment, on the launcher's `:DOWN` whatever its reason
  (`:normal` included), and at `A`; it kills the exact helper then if it is
  still up. It returns (normally) only after the helper's exact `EXIT`, and
  kills that exact pid on that `EXIT` too (a no-op if it is gone; a forged
  `EXIT` message can only end the helper early), so a normal witness exit
  never leaves a helper behind its link. If the witness dies abnormally, the
  link ends the helper. An owner that gets no GO exits at `A` without
  attaching.

  On every return the launcher deactivates its alias, drains exact replies
  already delivered, and flushes its witness and guard monitors. On
  **success** it kills nothing (the helper has delivered its result and is
  ending; the witness waits for its `EXIT`). On **failure** it first tells
  the witness to abandon and kills the exact helper itself if it recorded
  one (or drained one), whatever won: the helper may be blocked in
  `start_child/2`, and the witness may be suspended, or gone with its
  `:DOWN` still queued behind an expiry. A helper it never recorded was
  never released by it and does no work. If the witness is gone
  (`:witness_down`, or its `:DOWN` already delivered), it also kills every
  exact guard it learned of, from its own state or from a drained
  `{:ok, guard}`. Failures: `:expired` (`A` reached, also when a relayed
  failure is handled at or after `A`), `:start_failed` (a relayed `:error`
  before `A`: a refused or raising `start_child/2`, or the helper gone
  without a result), `:guard_down`, `:witness_down`.

  So with both the launcher and the witness gone, in either order, the
  helper is gone too: the witness's close or link ends it. A
  `start_child/2` request it already queued is not withdrawn (see "Not
  guaranteed").

  ## While running: triggers and fence

  The guard ends the owner on the first of: its supervisor's `{:EXIT, parent,
  _}` (`:parent_exit`), the requester's `:DOWN` (`:requester_down`), the
  coordinator's `:DOWN` (`:handoff_down`), or `now >= D` (`:deadline`). At
  that observed trigger time `t` it sends the owner one scoped cooperative
  stop, `{:telelem_owner_stop, token}`, which the owner consumes only at its
  current checkpoints (see `Upstream`). When the guard is next scheduled at
  or after `min(t, D) + grace_ms` and the owner has not gone, it sends
  `exit(owner, :kill)` to the exact owner pid. It then waits at most another
  `grace_ms`, counted from the kill, for the exact `:DOWN`.

  The guard's mailbox is drained in arrival order and every bound is
  re-checked against the clock at each receive turn. Unknown messages,
  `:EXIT` from other pids (including the owner's own) and `:DOWN` with other
  references are dropped.

  ## Reports (to the observer; never before an exact `:DOWN`)

    * guard: `{:telelem_owner_guard, ref, :owner_down, cause, how}` once,
      after the owner monitor's `:DOWN`. `cause` is one of `causes/0`
      (`:owner_exited` when no trigger came first). `how` is `:exited` (no
      stop was sent), `:stopped` (it went down after the scoped stop and
      before the kill; that it stopped *because of* the stop is not proved)
      or `:killed`. If no `:DOWN` arrives within the second grace the guard
      sends nothing and exits `{:shutdown, :owner_down_unconfirmed}`; its
      link still kills the owner.
    * witness: exactly one of the following, from its own records only,
      once the helper's exact `EXIT` (if it spawned one), the guard's
      `:DOWN` (if it learned of a guard) and the owner's `:DOWN` (if it
      watched one) have all arrived. It then exits `:normal`.
        * `{:telelem_owner_witness, ref, :owner_down, owner}`: its own
          monitor on that exact owner pid reported it down. This is the only
          report of an owner's death.
        * `{:telelem_owner_witness, ref, :activation_never_released}`: it
          never sent the activation (it is the only sender), so this launch
          never started an owner. It says nothing about any process being
          gone besides the helper and the guard it learned of.
        * `{:telelem_owner_witness, ref, :owner_unknown}`: it released the
          activation, but the guard went down without announcing an owner
          to it. Whether an owner was spawned (and, if so, that its link
          ended it) is not known to the witness; this is not a confirmation.

      No report is a claim of socket, slot or supervisor quiescence.

  The witness releases the one activation and kills the helper; it does not
  kill the guard or the owner and decides nothing else.

  ## Supervisor shutdown

  `child_spec/1` is `restart: :temporary` with `shutdown: 2 * grace_ms +
  1000`. On a supervisor shutdown the guard runs the trigger path above and
  exits with the supervisor's reason. If the supervisor's timer runs out
  first it kills the guard, and the link kills the owner.

  ## Not guaranteed

    * **TLS/socket teardown**: a kill closes nothing gracefully, and nothing
      here proves when the socket is released.
    * **durability, ledger, successor policy**: no report is stored, nothing
      is accounted, and nothing orders a successor attempt after this one.
    * the observer receiving anything (it is only sent to), or any guard
      report after the guard itself is killed (the link kills the owner; the
      witness still reports, `:owner_down` only if it monitored the owner).
    * authenticity of the activation message: any process that knows the
      token could send one. In this module only the witness does.
    * a cooperative stop while the owner is blocked in a call (it reaches no
      checkpoint): the fence applies.
    * authenticity of a parent `:EXIT`: any process can send one as a plain
      message. That can only end the owner early.
    * a supervisor that never answers: the helper is killed at `A` (by the
      witness and by the launcher's failed return); its queued request is
      not withdrawn and then starts a child that is never activated and
      expires (see step 2).
    * a failure type while the witness cannot run (for example suspended):
      the launcher then returns `:expired` at `A`, not `:start_failed`.
    * authenticity of the helper's result or `EXIT`: any process that knows
      the token could forge a result, and any process can send an `EXIT`
      message. The witness takes the first result only; a forged one can
      fail the launch or take the real guard's place, and the real guard is
      then never activated and expires at `A`.
  """

  alias Telelem.Wire.Upstream

  @tag :telelem_owner_guard
  @witness_tag :telelem_owner_witness
  @go_tag :telelem_owner_go
  @stop_tag :telelem_owner_stop
  @max_wait 4_294_967_295
  @max_grace_ms 60_000
  @max_activation_ms 60_000
  @shutdown_slack_ms 1_000
  @init_timeout_ms 5_000
  @opt_keys [:upstream, :requester, :observer, :grace_ms, :activation_ms]
  @causes [:owner_exited, :requester_down, :handoff_down, :parent_exit, :deadline]
  @meta_keys [:ref, :witness, :observer, :requester, :handoff, :deadline, :until, :grace_ms]

  @type cause :: :owner_exited | :requester_down | :handoff_down | :parent_exit | :deadline
  @type handle :: %{guard: pid(), owner: pid(), witness: pid(), ref: reference()}
  @type start_error :: :expired | :start_failed | :guard_down | :witness_down

  @doc "The guard report causes, in no particular order."
  @spec causes() :: [cause()]
  def causes, do: @causes

  @doc "The option bounds (ms): `grace_ms` and `activation_ms` are `1..max`."
  def bounds,
    do: %{
      grace_ms: @max_grace_ms,
      activation_ms: @max_activation_ms,
      shutdown_slack_ms: @shutdown_slack_ms
    }

  # ── Launcher ────────────────────────────────────────────────────────────

  @doc """
  Launches one guarded owner under the `DynamicSupervisor` `sup`. See the
  module doc.
  """
  @spec start(Supervisor.supervisor(), keyword()) :: {:ok, handle()} | {:error, start_error()}
  def start(sup, opts) do
    o = opts!(opts)
    now = now()
    a = min(now + o.activation_ms, o.deadline)
    if now >= a, do: {:error, :expired}, else: launch(sup, o, a)
  end

  defp launch(sup, o, a) do
    ref = make_ref()
    al = Process.alias()

    meta = %{
      ref: ref,
      witness: nil,
      observer: o.observer,
      requester: o.requester,
      handoff: o.handoff,
      deadline: o.deadline,
      until: a,
      grace_ms: o.grace_ms
    }

    {witness, wmon} =
      :erlang.spawn_opt(
        __MODULE__,
        :witness,
        [
          %{
            launcher: self(),
            sup: sup,
            alias: al,
            ref: ref,
            until: a,
            observer: o.observer,
            meta: meta
          }
        ],
        [:monitor]
      )

    st = %{
      ref: ref,
      alias: al,
      until: a,
      helper: nil,
      witness: witness,
      wmon: wmon,
      guard: nil,
      gmon: nil
    }

    st |> await(o.upstream) |> finish()
  end

  # One receive loop up to A, re-checked every turn.
  defp await(st, payload) do
    left = st.until - now()

    if left <= 0 do
      {:error, :expired, st}
    else
      ref = st.ref
      wmon = st.wmon
      gmon = st.gmon

      receive do
        {@tag, ^ref, :helper, h} when st.helper == nil and is_pid(h) ->
          # Recorded first, so every failure below can still end it. Released
          # only while no witness :DOWN is delivered and A is not reached.
          st = %{st | helper: h}

          cond do
            down?(wmon) ->
              {:error, :witness_down, %{st | wmon: nil}}

            now() >= st.until ->
              {:error, :expired, st}

            true ->
              send(h, {@tag, ref, :run})
              await(st, payload)
          end

        {@tag, ^ref, :started, {:ok, g}} when st.guard == nil and is_pid(g) ->
          # Known either way, so a failed cleanup can still end it.
          if now() >= st.until,
            do: {:error, :expired, %{st | guard: g}},
            else: await(%{st | guard: g, gmon: Process.monitor(g)}, payload)

        {@tag, ^ref, :started, _} when st.guard == nil ->
          if now() >= st.until,
            do: {:error, :expired, st},
            else: {:error, :start_failed, st}

        {@tag, ^ref, :watching, g, owner} when is_pid(g) and g == st.guard and is_pid(owner) ->
          # Evidence already delivered wins over GO; A is checked last.
          cond do
            down?(wmon) -> {:error, :witness_down, %{st | wmon: nil}}
            down?(gmon) -> {:error, :guard_down, %{st | gmon: nil}}
            now() >= st.until -> {:error, :expired, st}
            true -> go(st, owner, payload)
          end

        {:DOWN, ^wmon, :process, _, _} ->
          {:error, :witness_down, %{st | wmon: nil}}

        {:DOWN, ^gmon, :process, _, _} when gmon != nil ->
          {:error, :guard_down, %{st | gmon: nil}}
      after
        min(left, @max_wait) -> await(st, payload)
      end
    end
  end

  defp go(st, owner, payload) do
    send(owner, {@go_tag, st.ref, payload})
    {:ok, %{guard: st.guard, owner: owner, witness: st.witness, ref: st.ref}, st}
  end

  # Success: no kill. The helper has already sent its result and is ending;
  # the witness waits for its exact EXIT and watches the guard and the owner.
  defp finish({:ok, handle, st}) do
    Process.unalias(st.alias)
    _ = drain(st.ref, [])
    flush(st)
    {:ok, handle}
  end

  # Failure, whatever won (expiry over a pending witness :DOWN included): end
  # the exact helper here, since it may be blocked in start_child and the
  # witness may be suspended or gone. If the witness is gone, also end every
  # exact guard this launcher learned of, since no one else watches them.
  defp finish({:error, why, st}) do
    send(st.witness, {@tag, st.ref, :abandon})
    if st.helper, do: Process.exit(st.helper, :kill)
    Process.unalias(st.alias)
    late = drain(st.ref, [])
    for {:helper, h} <- late, do: Process.exit(h, :kill)

    if why == :witness_down or down?(st.wmon) do
      guards = for {:guard, g} <- late, do: g
      for g <- Enum.uniq([st.guard | guards]), is_pid(g), do: Process.exit(g, :kill)
    end

    flush(st)
    {:error, why}
  end

  defp flush(st),
    do: for(m <- [st.wmon, st.gmon], m != nil, do: Process.demonitor(m, [:flush]))

  # Exact replies already delivered before the alias was deactivated; every
  # helper and guard pid among them is returned.
  defp drain(ref, acc) do
    receive do
      {@tag, ^ref, :helper, h} when is_pid(h) -> drain(ref, [{:helper, h} | acc])
      {@tag, ^ref, :helper, _} -> drain(ref, acc)
      {@tag, ^ref, :started, {:ok, g}} when is_pid(g) -> drain(ref, [{:guard, g} | acc])
      {@tag, ^ref, :started, _} -> drain(ref, acc)
      {@tag, ^ref, :watching, _, _} -> drain(ref, acc)
    after
      0 -> acc
    end
  end

  defp down?(nil), do: false

  defp down?(mon) do
    receive do
      {:DOWN, ^mon, :process, _, _} -> true
    after
      0 -> false
    end
  end

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

    up = opts[:upstream]
    unless is_list(up), do: opt_error(":upstream must be a keyword list")
    %{handoff: h, deadline: d} = Upstream.validate_guarded!(up)
    unless local_pid?(h), do: opt_error(":upstream handoff must be a local pid")

    for k <- [:requester, :observer],
        not local_pid?(opts[k]),
        do: opt_error("#{inspect(k)} must be a local pid")

    for {k, max} <- [grace_ms: @max_grace_ms, activation_ms: @max_activation_ms] do
      v = opts[k]

      unless is_integer(v) and v >= 1 and v <= max,
        do: opt_error("#{inspect(k)} must be an integer in 1..#{max}")
    end

    %{
      upstream: up,
      handoff: h,
      deadline: d,
      requester: opts[:requester],
      observer: opts[:observer],
      grace_ms: opts[:grace_ms],
      activation_ms: opts[:activation_ms]
    }
  end

  defp opt_error(msg), do: raise(ArgumentError, "OwnerGuard: " <> msg)

  defp local_pid?(p), do: is_pid(p) and node(p) == node()

  # ── Helper (linked to the witness only, non-trapping, metadata only) ────

  # Waits for the launcher's release; does nothing at or after A. Its
  # sanitized result goes to the witness only.
  @doc false
  def helper(%{sup: sup, meta: %{ref: ref, until: a, witness: w} = meta}) do
    receive do
      {@tag, ^ref, :run} ->
        if now() < a do
          res =
            try do
              DynamicSupervisor.start_child(sup, child_spec(meta))
            catch
              _, _ -> :error
            end

          send(w, {@tag, ref, :started, sanitize(res)})
        end
    after
      min(max(a - now(), 0), @max_wait) -> :ok
    end
  end

  defp sanitize({:ok, g}) when is_pid(g), do: {:ok, g}
  defp sanitize(_), do: :error

  # ── Witness (independent, outside the supervisor, linked to the helper) ─

  @doc false
  def witness(%{launcher: l, sup: sup, alias: al, ref: ref, until: a, observer: obs, meta: meta}) do
    # Before the helper exists: the launcher is watched and the helper's
    # link is trapped, so the helper dies with this process but not with a
    # trapped signal.
    lmon = Process.monitor(l)
    Process.flag(:trap_exit, true)

    w = %{
      ref: ref,
      alias: al,
      until: a,
      observer: obs,
      lmon: lmon,
      helper: nil,
      helper_up: false,
      result: nil,
      # true once it will never release activation (abandon, launcher
      # :DOWN, A); the helper is killed then if still up.
      closed: false,
      released: false,
      guard: nil,
      gmon: nil,
      guard_up: false,
      owner: nil,
      omon: nil,
      owner_up: false
    }

    # A closure already known spawns no helper at all.
    if closed_at_start?(w) do
      w_loop(%{w | closed: true})
    else
      h =
        :erlang.spawn_opt(__MODULE__, :helper, [%{sup: sup, meta: %{meta | witness: self()}}], [
          :link
        ])

      send(al, {@tag, ref, :helper, h})
      w_loop(%{w | helper: h, helper_up: true})
    end
  end

  defp closed_at_start?(%{ref: ref, lmon: lmon, until: a}) do
    now() >= a or
      receive do
        {:DOWN, ^lmon, :process, _, _} -> true
        {@tag, ^ref, :abandon} -> true
      after
        0 -> false
      end
  end

  defp w_loop(w) do
    w = if not w.closed and now() >= w.until, do: close(w), else: w

    if w.helper_up or w.guard_up or w.owner_up do
      %{ref: ref, lmon: lmon, helper: helper, gmon: gmon, omon: omon} = w

      receive do
        {:DOWN, ^lmon, :process, _, _} ->
          w_loop(close(w))

        {@tag, ^ref, :abandon} ->
          w_loop(close(w))

        {:EXIT, ^helper, _} when w.helper_up ->
          # Exact by pid, but a plain message could forge it: end that exact
          # pid either way (a no-op if it is gone), so no helper outlives a
          # normal return.
          Process.exit(helper, :kill)
          w = %{w | helper_up: false}

          # Gone without a result: a start failure, while not closed.
          if w.result == nil and not w.closed,
            do: send(w.alias, {@tag, ref, :started, :error})

          w_loop(%{w | result: w.result || :error})

        {@tag, ^ref, :started, res} when w.result == nil and helper != nil ->
          case sanitize(res) do
            {:ok, g} = ok ->
              # Recorded, monitored and relayed before any activation.
              w = %{w | result: ok, guard: g, gmon: Process.monitor(g), guard_up: true}
              send(w.alias, {@tag, ref, :started, ok})
              w_loop(release(w))

            :error ->
              unless w.closed, do: send(w.alias, {@tag, ref, :started, :error})
              w_loop(%{w | result: :error})
          end

        {@tag, ^ref, :owner, g, o}
        when is_pid(g) and g == w.guard and w.released and is_pid(o) and w.owner == nil ->
          w = %{w | owner: o, omon: Process.monitor(o), owner_up: true}
          send(w.alias, {@tag, ref, :watching, g, o})
          w_loop(w)

        {:DOWN, ^gmon, :process, _, _} when gmon != nil ->
          w_loop(%{w | guard_up: false})

        {:DOWN, ^omon, :process, _, _} when omon != nil ->
          w_loop(%{w | owner_up: false})

        _ ->
          w_loop(w)
      after
        w_timeout(w) -> w_loop(w)
      end
    else
      send(w.observer, w_report(w))
    end
  end

  # Only from its own records: the exact owner its own monitor saw go down;
  # it never released activation (it is the only releaser); or it released
  # and the exact guard went down without announcing an owner to it.
  defp w_report(%{owner: o, ref: ref}) when o != nil, do: {@witness_tag, ref, :owner_down, o}
  defp w_report(%{released: true, ref: ref}), do: {@witness_tag, ref, :owner_unknown}
  defp w_report(%{ref: ref}), do: {@witness_tag, ref, :activation_never_released}

  # The one activation, to the exact guard the helper reported, before A and
  # only while the launcher has not given up.
  defp release(%{closed: false, released: false} = w) do
    if now() < w.until do
      send(w.guard, {@tag, w.ref, :activate})
      %{w | released: true}
    else
      close(w)
    end
  end

  defp release(w), do: w

  defp w_timeout(%{closed: false, until: a}), do: min(max(a - now(), 0), @max_wait)
  defp w_timeout(_), do: :infinity

  defp close(%{closed: true} = w), do: w

  defp close(w) do
    if w.helper_up, do: Process.exit(w.helper, :kill)
    %{w | closed: true}
  end

  # ── Guard (temporary child, metadata-only fixed entry) ──────────────────

  @doc """
  The temporary child spec for one guard. `meta` is the metadata map built by
  the launcher; the guard re-validates it and refuses any other shape.
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
      shutdown: 2 * grace + @shutdown_slack_ms,
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
        if now() >= s.until do
          # Started late (for example queued at a busy supervisor): no work.
          :proc_lib.init_fail({:error, :expired}, {:exit, :normal})
        else
          Process.flag(:trap_exit, true)
          rmon = Process.monitor(s.requester)
          hmon = Process.monitor(s.handoff)
          :proc_lib.init_ack({:ok, self()})
          await_activation(Map.merge(s, %{parent: parent, rmon: rmon, hmon: hmon}))
        end

      :error ->
        :proc_lib.init_fail({:error, :invalid_metadata}, {:exit, :normal})
    end
  end

  defp meta(
         %{
           ref: ref,
           witness: w,
           observer: obs,
           requester: req,
           handoff: h,
           deadline: d,
           until: a,
           grace_ms: grace
         } = m
       )
       when map_size(m) == length(@meta_keys) and is_reference(ref) and is_integer(d) and
              is_integer(a) and a <= d and is_integer(grace) and grace >= 1 and
              grace <= @max_grace_ms do
    if Enum.all?([w, obs, req, h], &local_pid?/1), do: {:ok, m}, else: :error
  end

  defp meta(_), do: :error

  # Not activated: no owner exists.
  defp await_activation(s) do
    left = s.until - now()

    if left <= 0 do
      exit({:shutdown, :expired})
    else
      ref = s.ref

      receive do
        {@tag, ^ref, :activate} -> activate(s)
        msg -> pre(msg, s)
      after
        min(left, @max_wait) -> await_activation(s)
      end
    end
  end

  defp activate(s) do
    if now() >= s.until, do: exit({:shutdown, :expired})

    owner_meta = %{ref: s.ref, until: s.until, deadline: s.deadline, handoff: s.handoff}

    {owner, omon} =
      :erlang.spawn_opt(Upstream, :guarded, [owner_meta], [:link, :monitor])

    send(s.witness, {@tag, s.ref, :owner, self(), owner})
    active(Map.merge(s, %{owner: owner, omon: omon, reason: nil}))
  end

  defp pre({:EXIT, parent, reason}, %{parent: parent}), do: exit(reason)

  defp pre({:DOWN, m, :process, pid, _}, %{rmon: m, requester: pid}),
    do: exit({:shutdown, :requester_down})

  defp pre({:DOWN, m, :process, pid, _}, %{hmon: m, handoff: pid}),
    do: exit({:shutdown, :handoff_down})

  defp pre(_, s), do: await_activation(s)

  # Active: the owner lives until a trigger or its own exit.
  defp active(s) do
    now = now()

    if now >= s.deadline do
      trigger(s, :deadline, now)
    else
      receive do
        msg -> on_active(msg, s)
      after
        min(s.deadline - now, @max_wait) -> active(s)
      end
    end
  end

  defp on_active({:DOWN, m, :process, pid, _}, %{omon: m, owner: pid} = s),
    do: report(s, :owner_exited, :exited)

  defp on_active({:EXIT, parent, reason}, %{parent: parent} = s),
    do: trigger(%{s | reason: {:parent, reason}}, :parent_exit, now())

  defp on_active({:DOWN, m, :process, pid, _}, %{rmon: m, requester: pid} = s),
    do: trigger(s, :requester_down, now())

  defp on_active({:DOWN, m, :process, pid, _}, %{hmon: m, handoff: pid} = s),
    do: trigger(s, :handoff_down, now())

  defp on_active(_, s), do: active(s)

  defp trigger(s, cause, t) do
    send(s.owner, {@stop_tag, s.ref})
    fence = min(t, s.deadline) + s.grace_ms
    stopping(Map.merge(s, %{cause: cause, fence: fence, confirm_by: nil}))
  end

  # Stop sent: wait for the exact owner :DOWN; kill it at the fence; give up
  # one grace later without a report.
  defp stopping(s) do
    now = now()

    s =
      if s.confirm_by == nil and now >= s.fence do
        Process.exit(s.owner, :kill)
        %{s | confirm_by: now + s.grace_ms}
      else
        s
      end

    # Before the kill: the fence. After it: one grace from the kill itself.
    bound = s.confirm_by || s.fence

    if s.confirm_by != nil and now >= bound do
      exit(exit_reason(s, {:shutdown, :owner_down_unconfirmed}))
    else
      %{omon: omon, owner: owner, parent: parent} = s

      receive do
        {:DOWN, ^omon, :process, ^owner, _} ->
          report(s, s.cause, if(s.confirm_by, do: :killed, else: :stopped))

        {:EXIT, ^parent, reason} ->
          stopping(%{s | reason: {:parent, reason}})

        _ ->
          stopping(s)
      after
        min(max(bound - now, 0), @max_wait) -> stopping(s)
      end
    end
  end

  defp report(s, cause, how) do
    send(s.observer, {@tag, s.ref, :owner_down, cause, how})
    exit(exit_reason(s, :normal))
  end

  defp exit_reason(%{reason: {:parent, reason}}, _), do: reason
  defp exit_reason(_, default), do: default

  defp now, do: System.monotonic_time(:millisecond)
end
