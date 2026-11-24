defmodule Telelem.Handoff do
  @moduledoc """
  One coordinator process per downstream request that elects **at most one**
  upstream attempt as the winner and relays its stream to the requester
  (TEL-003). Explicit PID API; no registry or application env.

  ## Roles

    * **requester**: the process that owns the downstream response (for
      example a Bandit handler). It is monitored.
    * **attempts**: a fixed set of attempt ids given at start. An attempt
      process calls `attach/2`, then `begin_dispatch/2` *before* any upstream
      bytes can leave, `ready/2` once it holds semantic content, then
      `transfer/2` / `finish/2` if released, and always `cleaned/3` after its
      transport is closed.
    * **authority**: the instance's foundational admission authority PID
      (`Telelem.Instance.authority/1`). It is monitored. Its death fences the
      instance (`:rest_for_one` takes the workers down with it), so the
      handoff treats it as fatal to the request (see below). It receives no
      messages from the handoff.
    * **observer**: an optional best-effort audit PID that receives events
      (`{:telelem_handoff, handoff_pid, event}`, or through `:on_event`).
      It is monitored. Losing it never affects the request; later events are
      only listed as `unreported`.

  ## Supported topology

  The coordinator traps exits and monitors everyone else, so it has no links
  except to its parent (`start_link/1`). The parent should be a supervisor.
  `init/1` refuses a requester, authority or observer that is linked to the
  coordinator (`{:linked, role}`), and `attach/2` refuses the parent
  (`{:error, :linked_to_handoff}`). If the parent exits anyway, `terminate/2`
  still sends the requester a truthful outcome
  (`{:failed | :incomplete, {:handoff_down, reason}}`) and cancels every
  unclosed attempt. Cleanup acknowledgements can't be collected after that,
  so no `{:closed, _}` follows.

  ## Semantics

    * **Election.** The first `ready/2` processed while the selection is open
      gets `:release`. Every other attempt is cancelled at that moment. The
      selection **never reopens**: if the winner then fails, dies, a
      downstream write fails, or the authority dies, the handoff ends
      `{:failed, _}` (nothing committed) or `{:incomplete, _}` (after commit).
      No other attempt is ever released.
    * **Commit** is the first `transfer/2` or `finish/2` that the coordinator
      accepts. The coordinator marks the handoff committed *before* it forwards
      the bytes. Whether the downstream write then succeeds has no effect on
      selection. Data goes winner → coordinator → requester, so the requester
      sees every accepted chunk before any later outcome message.
    * **Authority death** (observed `:DOWN`) while the stream is open ends it:
      `{:failed, {:authority_down, r}}` before commit,
      `{:incomplete, {:authority_down, r}}` after commit. The winner is
      cancelled; later `transfer/2`/`finish/2` get `{:error, :cancelled}`.
      No completion is synthesized. An outcome already reached (for example
      `{:done, _}`) is never changed afterwards.
    * **Cleanup.** Each expected attempt is closed exactly once, by whichever
      happens first: an explicit `cleaned/3` acknowledgement, the attempt's
      `:DOWN`, a forced kill after `:cleanup_timeout` from its cancel or
      finish, or `:never_registered` (it never attached; later attaches are
      refused so it must not dispatch). Stale timers and duplicate
      acknowledgements are ignored (`{:error, :duplicate}`).

  ## Charges: a local, conservative hint

  The handoff never refunds anything. The **admission authority's ledger is
  the source of truth for spend**: a grant it recorded as dispatched stays
  encumbered (`:dispatched`/`:unresolved`) whatever the handoff reports. The
  handoff's per-attempt `charge` is only an upper-bound hint for the requester
  and observer:

    * `:possible` if the attempt crossed the **may-have-dispatched boundary**
      (`begin_dispatch/2` returned `:ok`), reached `ready/2`, claimed
      `:possible`, exited without acknowledging, or was force-killed.
    * `:none` only if the coordinator never granted that boundary and the
      attempt acknowledged `:none`, or it never registered. The protocol makes
      this a proof of no dispatch: a cooperating attempt sends nothing upstream
      without `begin_dispatch/2 == :ok`.

  ## Messages to the requester (`{:telelem_handoff, handoff_pid, msg}`)

      {:elected, attempt_id}                 # informational; write nothing yet
      {:data, chunk}                         # committed semantic bytes, in order
      {:done, terminal} | {:incomplete, reason} | {:failed, reason}   # exactly one
      {:closed, report}                      # after every attempt is closed

  The requester must not write anything downstream (including status and
  headers) before the first `{:data, _}` or `{:done, _}`.

  ## Messages to attempts

      {:telelem_handoff, handoff_pid, {:cancel, reason}}

  An attempt should also monitor the coordinator; `attach/2` returns that
  monitor ref.

  ## Observer events

      {:elected, id} | {:committed, id} | {:outcome, outcome}
      {:attempt_closed, id, %{cleanup: cleanup, charge: :none | :possible}}
      {:closed, report}
      {:terminated, reason, unclosed_ids}    # coordinator stopped abnormally

  Events are at-most-once and carry no authority.

  ## Limits

  Transfer is synchronous to the coordinator and passive (unbounded) to the
  requester. Buffer caps, overflow handling and credit-driven readers belong to
  TEL-005. The coordinator decides policy order only: it says nothing about
  socket/TLS ownership or about delivery over a real wire.
  """

  use GenServer, restart: :temporary

  @tag :telelem_handoff
  @trace_tag :telelem_handoff_trace
  @call_timeout 5_000

  @type attempt_id :: term()
  @type charge :: :none | :possible
  @type cleanup ::
          :ack | {:down, term()} | {:killed, :cleanup_timeout} | :never_registered
  @type outcome :: {:done, term()} | {:incomplete, term()} | {:failed, term()}

  # ── Owner / requester API ────────────────────────────────────────────────

  @doc """
  Starts a coordinator.

  Options:

    * `:requester` (required). Downstream owner PID.
    * `:attempts` (required). Non-empty list of unique attempt ids.
    * `:authority`. Foundational admission authority PID, or `nil`.
    * `:observer`. Best-effort audit PID that receives events, or `nil`.
    * `:on_event`. A 1-arity function called in the coordinator for each event,
      instead of sending to `:observer`. Keep it non-blocking.
    * `:trace`. A PID that receives `{:telelem_handoff_trace, pid, event}` for
      each internal transition, including when a `:DOWN` is noticed. Use it
      for tests and evidence.
    * `:cleanup_timeout`. Default 5000 ms. The time from a cancel or finish
      until the coordinator force-kills an attempt that has not closed.
    * `:register_timeout`. Default 5000 ms. While the selection is open,
      attempts that have not attached by then are closed as
      `:never_registered`.
  """
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Like `start_link/1` but unlinked."
  def start(opts), do: GenServer.start(__MODULE__, opts)

  @doc "Requester-initiated end, such as a downstream disconnect or write failure."
  def cancel(h, reason), do: call(h, {:cancel, reason})

  @doc "Snapshot of the coordinator state for tests and diagnostics."
  def status(h), do: call(h, :status)

  # ── Attempt API (call from the attempt process itself) ───────────────────

  @doc """
  Registers the calling process as attempt `id` and monitors it. Returns
  `{:ok, monitor_ref_on_handoff}`. On `{:cancel, reason}` or `{:error, _}`
  the attempt must not dispatch. A repeated attach by an attempt that has
  since been cancelled returns that cancellation, and any attach after the
  outcome returns `{:cancel, :ended}`.
  """
  def attach(h, id) do
    mref = Process.monitor(h)

    case call(h, {:attach, id}) do
      :ok ->
        {:ok, mref}

      other ->
        Process.demonitor(mref, [:flush])
        other
    end
  end

  @doc """
  The may-have-dispatched boundary. Call it immediately before the first
  upstream byte can leave (before the admission authorize/dispatch call).
  `:ok` grants permission, and from then on the attempt is recorded as
  `:possible`. On `{:cancel, reason}` or `{:error, _}` it must not dispatch.
  """
  def begin_dispatch(h, id), do: call(h, {:begin_dispatch, id})

  @doc """
  The barrier. Reports that this attempt holds semantic content, and returns
  the decision: `:release` (you are the winner), `{:cancel, reason}` or
  `{:error, _}`. Implies the dispatch boundary.
  """
  def ready(h, id), do: call(h, {:ready, id})

  @doc "Winner only. Forwards one semantic chunk. The first accepted chunk commits."
  def transfer(h, chunk), do: call(h, {:transfer, chunk})

  @doc "Winner only. Ends the stream with a terminal event and commits if no data was sent."
  def finish(h, terminal), do: call(h, {:finish, terminal})

  @doc """
  Explicit cleanup acknowledgement: the transport of `id` is closed. `charge`
  is the attempt's own claim. It can raise the recorded charge to `:possible`
  but never lower it: `:none` is recorded only if the attempt never crossed
  the dispatch boundary. Another process may acknowledge an attempt that
  never attached, for example the spawner when a spawn failed.
  """
  def cleaned(h, id, charge) when charge in [:none, :possible],
    do: call(h, {:cleaned, id, charge})

  defp call(h, msg) do
    GenServer.call(h, msg, @call_timeout)
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :handoff_down}
  end

  # ── Server ───────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    requester = Keyword.fetch!(opts, :requester)
    ids = Keyword.fetch!(opts, :attempts)
    authority = Keyword.get(opts, :authority)
    observer = Keyword.get(opts, :observer)
    {:links, links} = Process.info(self(), :links)

    linked =
      Enum.find([requester: requester, authority: authority, observer: observer], fn {_, p} ->
        p in links
      end)

    cond do
      not is_pid(requester) ->
        {:stop, {:bad_option, :requester}}

      not (is_list(ids) and ids != [] and length(Enum.uniq(ids)) == length(ids)) ->
        {:stop, {:bad_option, :attempts}}

      not (is_nil(authority) or is_pid(authority)) ->
        {:stop, {:bad_option, :authority}}

      not (is_nil(observer) or is_pid(observer)) ->
        {:stop, {:bad_option, :observer}}

      linked != nil ->
        # A linked role's exit (as our parent) would stop us before its
        # :DOWN could be handled.
        {:stop, {:linked, elem(linked, 0)}}

      true ->
        Process.flag(:trap_exit, true)
        me = self()

        on_event =
          case Keyword.fetch(opts, :on_event) do
            {:ok, fun} when is_function(fun, 1) -> fun
            :error when is_pid(observer) -> fn ev -> send(observer, {@tag, me, ev}) end
            :error -> fn _ -> :ok end
          end

        token = make_ref()

        Process.send_after(
          me,
          {:register_deadline, token},
          Keyword.get(opts, :register_timeout, 5_000)
        )

        blank = %{
          pid: nil,
          mon: nil,
          dispatched: false,
          ready: false,
          cancelled: nil,
          cleanup: nil,
          charge: nil,
          deadline: nil,
          forced: false
        }

        {:ok,
         %{
           parent: List.first(links),
           requester: requester,
           requester_mon: Process.monitor(requester),
           authority_mon: authority && Process.monitor(authority),
           authority_state: if(authority, do: :up, else: :none),
           observer_mon: observer && Process.monitor(observer),
           observer_state:
             if(observer || Keyword.has_key?(opts, :on_event), do: :up, else: :none),
           on_event: on_event,
           trace: Keyword.get(opts, :trace),
           cleanup_timeout: Keyword.get(opts, :cleanup_timeout, 5_000),
           register_token: token,
           order: ids,
           attempts: Map.new(ids, &{&1, blank}),
           phase: :open,
           winner: nil,
           outcome: nil,
           unreported: []
         }}
    end
  end

  @impl true
  def handle_call({:attach, id}, {pid, _}, s) do
    case Map.fetch(s.attempts, id) do
      :error ->
        {:reply, {:error, :unknown_attempt}, s}

      _ when pid == s.parent ->
        {:reply, {:error, :linked_to_handoff}, s}

      {:ok, %{cleanup: c}} when c != nil ->
        {:reply, {:cancel, :closed}, s}

      {:ok, %{pid: ^pid, cancelled: reason}} when reason != nil ->
        {:reply, {:cancel, reason}, s}

      {:ok, %{pid: ^pid}} when s.outcome != nil ->
        {:reply, {:cancel, :ended}, s}

      {:ok, %{pid: ^pid}} ->
        {:reply, :ok, s}

      {:ok, %{pid: p}} when is_pid(p) ->
        {:reply, {:error, :already_attached}, s}

      {:ok, a} ->
        # Invariant: an unregistered, unclosed attempt exists only while the
        # selection is open (election and outcome close them all).
        s = put(s, id, %{a | pid: pid, mon: Process.monitor(pid)})
        {:reply, :ok, trace(s, {:attached, id})}
    end
  end

  def handle_call({:begin_dispatch, id}, {pid, _}, s) do
    case owned(s, id, pid) do
      {:error, _} = e ->
        {:reply, e, s}

      {:ok, %{cleanup: c}} when c != nil ->
        {:reply, {:cancel, :closed}, s}

      {:ok, %{cancelled: reason}} when reason != nil ->
        {:reply, {:cancel, reason}, s}

      {:ok, _} when s.outcome != nil ->
        # Even the winner of a finished stream gets no new dispatch permission.
        {:reply, {:cancel, :ended}, s}

      {:ok, a} ->
        s = put(s, id, %{a | dispatched: true})
        {:reply, :ok, trace(s, {:dispatch_boundary, id})}
    end
  end

  def handle_call({:ready, id}, {pid, _}, s) do
    case owned(s, id, pid) do
      {:error, _} = e ->
        {:reply, e, s}

      {:ok, %{cleanup: c}} when c != nil ->
        {:reply, {:cancel, :closed}, s}

      {:ok, a} ->
        s = put(s, id, %{a | ready: true, dispatched: true})

        cond do
          a.cancelled != nil -> {:reply, {:cancel, a.cancelled}, s}
          s.outcome != nil -> {:reply, {:cancel, :ended}, s}
          s.winner == id -> {:reply, :release, s}
          s.phase == :open -> {:reply, :release, elect(s, id)}
          true -> {:reply, {:cancel, {:lost_to, s.winner}}, s}
        end
    end
  end

  def handle_call({:transfer, chunk}, {pid, _}, s) do
    with {:ok, id} <- accepting_winner(s, pid) do
      s = commit(s, id)
      send(s.requester, {@tag, self(), {:data, chunk}})
      {:reply, :ok, s}
    else
      e -> {:reply, e, s}
    end
  end

  def handle_call({:finish, terminal}, {pid, _}, s) do
    with {:ok, id} <- accepting_winner(s, pid) do
      s = s |> commit(id) |> set_outcome({:done, terminal})
      reply_finalize(:ok, s)
    else
      e -> {:reply, e, s}
    end
  end

  def handle_call({:cleaned, id, claim}, {pid, _}, s) do
    case Map.fetch(s.attempts, id) do
      :error ->
        {:reply, {:error, :unknown_attempt}, s}

      {:ok, %{cleanup: c}} when c != nil ->
        {:reply, {:error, :duplicate}, trace(s, {:duplicate_cleanup, id})}

      {:ok, %{pid: p}} when is_pid(p) and p != pid ->
        {:reply, {:error, :not_owner}, s}

      {:ok, a} ->
        if a.mon, do: Process.demonitor(a.mon, [:flush])

        s
        |> close(id, :ack, claim)
        |> after_close(id, :winner_closed)
        |> then(&reply_finalize(:ok, &1))
    end
  end

  def handle_call({:cancel, reason}, {pid, _}, s) do
    if pid == s.requester do
      s = set_outcome(s, ended(s, reason))
      reply_finalize(:ok, s)
    else
      {:reply, {:error, :not_requester}, s}
    end
  end

  def handle_call(:status, _from, s) do
    {:reply,
     %{
       phase: s.phase,
       winner: s.winner,
       outcome: s.outcome,
       authority: s.authority_state,
       observer: s.observer_state,
       attempts:
         Map.new(s.attempts, fn {id, a} ->
           {id, Map.take(a, [:pid, :dispatched, :ready, :cancelled, :cleanup, :charge])}
         end)
     }, s}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{requester_mon: ref} = s) do
    s = trace(s, {:requester_down, reason})
    finalize(set_outcome(s, ended(s, {:requester_down, reason})))
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{authority_mon: ref} = s) do
    # The instance is fenced: end an open stream, never extend or reselect.
    # set_outcome/2 is a no-op if an outcome was already reached.
    s = trace(%{s | authority_state: :lost}, {:authority_down, s.phase, reason})
    finalize(set_outcome(s, ended(s, {:authority_down, reason})))
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{observer_mon: ref} = s) do
    {:noreply, trace(%{s | observer_state: :lost}, {:observer_down, s.phase, reason})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, s) do
    case Enum.find(s.attempts, fn {_, a} -> a.mon == ref end) do
      {id, a} ->
        cleanup = if a.forced, do: {:killed, :cleanup_timeout}, else: {:down, reason}

        s
        |> trace({:attempt_down, id, reason})
        |> close(id, cleanup, :possible)
        |> after_close(id, {:winner_down, reason})
        |> finalize()

      nil ->
        {:noreply, s}
    end
  end

  def handle_info({:cleanup_deadline, id, token}, s) do
    case s.attempts[id] do
      %{cleanup: nil, deadline: {_, ^token}, pid: pid} = a ->
        # The :DOWN that follows closes the attempt; an ack racing ahead of it wins.
        Process.exit(pid, :kill)

        {:noreply,
         s |> put(id, %{a | forced: true, deadline: nil}) |> trace({:cleanup_deadline, id})}

      _stale ->
        {:noreply, s}
    end
  end

  def handle_info({:register_deadline, token}, %{register_token: token} = s) do
    if s.phase == :open and s.outcome == nil do
      s |> close_unregistered() |> check_no_winner() |> finalize()
    else
      {:noreply, s}
    end
  end

  # Non-parent links (none are supported) are trapped and ignored; the
  # parent's EXIT is handled by gen_server and lands in terminate/2.
  def handle_info(_other, s), do: {:noreply, s}

  @impl true
  def terminate(reason, s) do
    unclosed = for id <- s.order, s.attempts[id].cleanup == nil, do: id

    if s.outcome == nil or unclosed != [] do
      # Never leave the requester without an outcome. Acks can't be collected.
      s = if s.outcome == nil, do: set_outcome(s, ended(s, {:handoff_down, reason})), else: s

      # set_outcome/2 already cancelled the rest; this reaches a winner that
      # finished but had not acknowledged cleanup yet.
      for id <- unclosed, is_pid(s.attempts[id].pid), s.attempts[id].cancelled == nil do
        send(s.attempts[id].pid, {@tag, self(), {:cancel, {:handoff_down, reason}}})
      end

      emit(s, {:terminated, reason, unclosed})
    end

    :ok
  end

  # ── Transitions ──────────────────────────────────────────────────────────

  defp elect(s, id) do
    s = %{s | phase: :elected, winner: id}
    s = s |> emit({:elected, id}) |> trace({:elected, id})
    send(s.requester, {@tag, self(), {:elected, id}})
    s |> close_unregistered() |> cancel_others({:lost_to, id})
  end

  defp commit(%{phase: :elected} = s, id),
    do: %{s | phase: :committed} |> emit({:committed, id}) |> trace({:committed, id})

  defp commit(s, _id), do: s

  defp set_outcome(%{outcome: o} = s, _) when o != nil, do: s

  defp set_outcome(s, outcome) do
    s = %{s | outcome: outcome}
    send(s.requester, {@tag, self(), outcome})
    s = s |> emit({:outcome, outcome}) |> trace({:outcome, outcome}) |> close_unregistered()

    case outcome do
      {:done, _} -> arm_deadline(s, s.winner)
      {_, reason} -> cancel_others(s, reason, nil)
    end
  end

  # Cancels every registered, unclosed, not-yet-cancelled attempt except `keep`.
  defp cancel_others(s, reason, keep \\ :winner) do
    keep = if keep == :winner, do: s.winner, else: keep

    Enum.reduce(s.order, s, fn id, s ->
      a = s.attempts[id]

      if id != keep and a.pid != nil and a.cleanup == nil and a.cancelled == nil do
        send(a.pid, {@tag, self(), {:cancel, reason}})
        s |> put(id, %{a | cancelled: reason}) |> arm_deadline(id)
      else
        s
      end
    end)
  end

  defp arm_deadline(s, id) do
    a = s.attempts[id]

    if a.pid != nil and a.cleanup == nil and a.deadline == nil and not a.forced do
      token = make_ref()
      tref = Process.send_after(self(), {:cleanup_deadline, id, token}, s.cleanup_timeout)
      put(s, id, %{a | deadline: {tref, token}})
    else
      s
    end
  end

  defp close_unregistered(s) do
    Enum.reduce(s.order, s, fn id, s ->
      a = s.attempts[id]
      if a.pid == nil and a.cleanup == nil, do: close(s, id, :never_registered, :none), else: s
    end)
  end

  defp close(s, id, cleanup, claim) do
    a = s.attempts[id]
    if a.deadline, do: Process.cancel_timer(elem(a.deadline, 0))

    charge =
      cond do
        cleanup == :never_registered -> :none
        cleanup == :ack and not a.dispatched -> claim
        true -> :possible
      end

    s
    |> put(id, %{a | cleanup: cleanup, charge: charge, deadline: nil, mon: nil})
    |> emit({:attempt_closed, id, %{cleanup: cleanup, charge: charge}})
    |> trace({:attempt_closed, id, cleanup, charge})
  end

  defp after_close(s, id, winner_reason) do
    s = if id == s.winner, do: set_outcome(s, ended(s, winner_reason)), else: s
    check_no_winner(s)
  end

  defp check_no_winner(s) do
    if s.phase == :open and all_closed?(s),
      do: set_outcome(s, {:failed, :no_winner}),
      else: s
  end

  defp ended(%{phase: :committed}, reason), do: {:incomplete, reason}
  defp ended(_s, reason), do: {:failed, reason}

  defp all_closed?(s), do: Enum.all?(s.attempts, fn {_, a} -> a.cleanup != nil end)

  defp finalize(s) do
    if s.outcome != nil and all_closed?(s),
      do: {:stop, :normal, close_report(s)},
      else: {:noreply, s}
  end

  defp reply_finalize(reply, s) do
    if s.outcome != nil and all_closed?(s),
      do: {:stop, :normal, reply, close_report(s)},
      else: {:reply, reply, s}
  end

  defp close_report(s) do
    report = report(s)
    send(s.requester, {@tag, self(), {:closed, report}})
    s |> emit({:closed, report}) |> trace({:closed, report})
  end

  defp report(s) do
    %{
      outcome: s.outcome,
      winner: s.winner,
      committed: s.phase == :committed,
      authority: s.authority_state,
      observer: s.observer_state,
      attempts:
        Map.new(s.attempts, fn {id, a} ->
          {id, %{cleanup: a.cleanup, charge: a.charge, ready: a.ready}}
        end),
      possible_charges: for(id <- s.order, s.attempts[id].charge == :possible, do: id),
      unreported: Enum.reverse(s.unreported)
    }
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp owned(s, id, pid) do
    case Map.fetch(s.attempts, id) do
      :error -> {:error, :unknown_attempt}
      {:ok, %{pid: ^pid} = a} -> {:ok, a}
      {:ok, _} -> {:error, :not_attached}
    end
  end

  defp accepting_winner(s, pid) do
    w = s.winner && s.attempts[s.winner]

    cond do
      w == nil or w.pid != pid -> {:error, :not_winner}
      s.outcome != nil or w.cancelled != nil or w.cleanup != nil -> {:error, :cancelled}
      true -> {:ok, s.winner}
    end
  end

  defp put(s, id, a), do: %{s | attempts: Map.put(s.attempts, id, a)}

  defp emit(%{observer_state: :none} = s, _ev), do: s
  defp emit(%{observer_state: :lost} = s, ev), do: %{s | unreported: [ev | s.unreported]}

  defp emit(s, ev) do
    s.on_event.(ev)
    s
  catch
    kind, reason -> %{s | unreported: [{ev, {:on_event_failed, kind, reason}} | s.unreported]}
  end

  defp trace(%{trace: nil} = s, _ev), do: s

  defp trace(s, ev) do
    send(s.trace, {@trace_tag, self(), ev})
    s
  end
end
