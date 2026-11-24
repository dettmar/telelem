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
      `transfer/2` / `finish/3` if released, and always `cleaned/3` after its
      transport is closed.
    * **authority**: the instance's foundational admission authority, pinned
      as a PID at start and never re-resolved (no replacement). It is
      monitored. Its death fences the instance (`:rest_for_one` takes the
      workers down with it), so the handoff treats it as fatal to the request
      (see below). Two explicit, mutually exclusive options:
        - `:admission_authority`: a live local `Telelem.Admission.Authority`
          (checked at start). Terminal authorization is a grant issued by
          it ("Terminal authorization"). This is the serving mode.
        - `:authority` (legacy, TEL-003/M1): any PID, **fence-only**. It is
          monitored and receives no messages. Generic test fakes use it, and
          it never becomes a terminal authority: terminal authorization is the
          coordinator's own decision in mailbox order (an authority `:DOWN`
          handled before `finish/3` refuses it, one handled after does not),
          which is not serialized against the authority's death. It cannot be
          combined with `terminal_ack: true`.
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
    * **Commit** is the first `transfer/2` or `finish/3` that the coordinator
      accepts. The coordinator marks the handoff committed *before* it forwards
      the bytes. Whether the downstream write then succeeds has no effect on
      selection. Data goes winner → coordinator → requester, so the requester
      sees every accepted chunk before any later outcome message.
    * **Authority death** (observed `:DOWN`) while the stream is open ends it:
      `{:failed, {:authority_down, r}}` before commit,
      `{:incomplete, {:authority_down, r}}` after commit. The winner is
      cancelled; later `transfer/2`/`finish/3` get `{:error, :cancelled}`.
      No completion is synthesized. An authorized terminal or an outcome
      already reached (for example `{:done, _}`) is never changed afterwards.
    * **Terminal authorization** (`finish/3`, `:admission_authority` mode).
      The coordinator asks the pinned authority for a grant
      (`Telelem.Admission.Authority.request_terminal_grant/2`) and waits for
      it without blocking. The authority issuing the grant is the
      linearization point against the fence: the authority's messages and
      its exit reach the coordinator in order, so either the grant arrives
      before the authority's `:DOWN` (authorized; a later death, a slow
      `:on_event` callback or the local write that follows change nothing)
      or the `:DOWN` arrives with no grant (refused: death won). Only on the
      grant does the coordinator commit, record **upstream completion** and
      hand the `marker` (the bytes that declare success to the client, e.g.
      `data: [DONE]`) to the requester, all in that one step, and only then
      answer the winner. Refused, the marker is handed to no one. Every
      other outcome reached while the grant is pending (requester
      cancel/death, winner death, deadline) also refuses it, and a grant
      arriving later is ignored. In this mode, and with `terminal_ack: true`,
      the marker must be a non-empty binary.
    * **Terminal deadline.** `:terminal_deadline` is one absolute instant of
      `System.monotonic_time(:millisecond)`, shared by the coordinator, the
      winner (`finish/3` learns it from the coordinator) and the requester
      (`{:terminal, marker, deadline}`). The coordinator enforces it with one
      timer of its own, independent of the requester: a `finish/3` at or
      after it, or a grant still pending at it, is refused; a write result
      still pending at it ends `{:incomplete, {:terminal_unconfirmed,
      :deadline}}` with `downstream: :unconfirmed`. It does not end an open
      stream that has not finished yet (no finish can succeed after it).
    * **No late side effect after a caller gives up.** `finish/3` and
      `terminal_written/2` never return a local timeout while the coordinator
      can still act on the call: when their bound expires (`finish/3`: the
      deadline plus a grace period; otherwise the call timeout) they kill the
      coordinator, wait for its `:DOWN` and only then return. A decision
      already sent before the kill is returned; otherwise the result is
      `{:error, :unconfirmed}` (the marker may or may not have been handed
      out). Coordinator loss is never a refusal: no retry, replay or refund.
    * **Upstream completion is not downstream success**:
        - `terminal_ack: true` (requires `:admission_authority` and a
          deadline): the requester gets `{:terminal, marker, deadline}` and
          **no outcome yet**. Only `terminal_written(h, :ok)` (the local
          downstream write returned success) processed before the deadline
          gives `{:done, terminal}`. `terminal_written(h, {:error, r})` gives
          `{:incomplete, {:downstream_write, r}}`; requester cancel/death,
          coordinator exit or the deadline before the ack give
          `{:incomplete, _}`. Authority death after authorization changes
          nothing (the terminal was granted before it); it is only reported
          (`authority: :lost`). The winner's cleanup after authorization does
          not end the stream. See "Requester terminal protocol".
        - default (M1 compatibility): `{:data, marker}` (if any) and
          `{:done, terminal}` are sent back to back in the authorizing step.
          `{:done, _}` then means "upstream complete, marker handed to the
          requester", not "written".
    * **Legacy terminal** (`:authority` fence-only, or no authority): the
      same step runs synchronously when the coordinator handles `finish/3`;
      only an authority `:DOWN` handled earlier refuses it.
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
      {:terminal, marker, deadline}          # terminal_ack only: see below
      {:done, terminal} | {:incomplete, reason} | {:failed, reason}   # exactly one
      {:closed, report}                      # after every attempt is closed

  The report's `terminal` is `nil` or `%{upstream: :complete, downstream:
  :unconfirmed | :written | {:error, r} | :not_tracked}`. `authorization` is
  `:admission` | `:fence_only` | `:none` (the configured mode).

  The requester must not write anything downstream (including status and
  headers) before the first `{:data, _}`, `{:terminal, _, _}` or `{:done, _}`.

  ## Requester terminal protocol (`terminal_ack: true`)

    1. On `{:terminal, marker, deadline}`: if
       `System.monotonic_time(:millisecond) >= deadline`, do not start the
       write; call `terminal_written(h, {:error, :deadline})` and end the
       response without the transport terminator.
    2. Otherwise write exactly `marker`, once, and call `terminal_written/2`
       with the local result.
    3. Decide the transport ending only from the outcome message that follows
       (`{:done, _}`: complete the response; anything else, or the
       coordinator's `:DOWN`: end without the terminator). The return value of
       `terminal_written/2` is not the outcome; `{:error, :unconfirmed}` means
       the coordinator is gone.

  Limits, stated rather than solved: a locally acknowledged write means the
  bytes were accepted by the local socket stack, not that the client SDK
  received or parsed them. A write already in progress cannot be revoked:
  if it completes after the deadline, the client may see the marker (and the
  SDK may report success) while the outcome is `{:incomplete,
  {:terminal_unconfirmed, :deadline}}`. The reverse cannot happen: `{:done,
  _}` always follows a local write success reported before the deadline.

  ## Messages to attempts

      {:telelem_handoff, handoff_pid, {:cancel, reason}}

  An attempt should also monitor the coordinator; `attach/2` returns that
  monitor ref.

  ## Observer events

      {:elected, id} | {:committed, id} | {:upstream_complete, id} | {:outcome, outcome}
      {:attempt_closed, id, %{cleanup: cleanup, charge: :none | :possible}}
      {:closed, report}
      {:terminated, reason, unclosed_ids}    # coordinator stopped abnormally

  Events are at-most-once and carry no authority. `{:upstream_complete, _}`
  (`terminal_ack: true` only) is not a success: `{:outcome, {:done, _}}` is
  emitted only after the requester's confirmed write.

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
  # How long past the shared deadline finish/3 waits for the decision before
  # fencing the coordinator (scheduling slack, not policy).
  @decision_grace 1_000

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
    * `:admission_authority`. The pinned, live, local
      `Telelem.Admission.Authority` PID that grants terminal authorization
      (serving mode). Requires `:terminal_deadline`.
    * `:authority`. Legacy fence-only PID (any process, only monitored), or
      `nil`. Mutually exclusive with `:admission_authority`.
    * `:terminal_deadline`. Absolute `System.monotonic_time(:millisecond)`
      bound for terminal authorization and its confirmed write, or `nil`.
      Required with `:admission_authority` or `terminal_ack: true`.
    * `:observer`. Best-effort audit PID that receives events, or `nil`.
    * `:on_event`. A 1-arity function called in the coordinator for each event,
      instead of sending to `:observer`. Keep it non-blocking.
    * `:trace`. A PID that receives `{:telelem_handoff_trace, pid, event}` for
      each internal transition, including when a `:DOWN` is noticed. Use it
      for tests and evidence.
    * `:cleanup_timeout`. Default 5000 ms. The time from a cancel or finish
      until the coordinator force-kills an attempt that has not closed.
    * `:terminal_ack`. Default `false`. When `true` the requester must
      confirm the terminal write with `terminal_written/2` before `{:done, _}`.
      Requires `:admission_authority`.
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

  @doc """
  Winner only. **Terminal authorization**: the upstream is complete. `marker`
  (for example the exact `data: [DONE]` event) is the last downstream bytes,
  or `nil`. Commits if nothing was sent yet. See "Terminal authorization".

  Results:

    * `:ok`: authorized; `marker` was handed to the requester.
    * `{:error, :cancelled}`: refused (outcome already reached, winner
      cancelled, authority died before granting, deadline): `marker` was
      **not** handed to anyone and must never be forwarded.
    * `{:error, :invalid_marker}`: `nil`/empty marker in admission or
      `terminal_ack` mode; nothing changed.
    * `{:error, :handoff_down}`: the coordinator was already gone; this call
      handed nothing out.
    * `{:error, :unconfirmed}`: the coordinator was lost, or did not decide
      within the bound (then it was killed first, see "No late side effect").
      The marker may have been handed out. Never retry or replay.

  Blocks at most until the coordinator's deadline plus a grace period (or the
  call timeout when there is no deadline), and never returns while the
  coordinator could still release the marker afterwards.
  """
  def finish(h, terminal, marker \\ nil) when is_nil(marker) or is_binary(marker) do
    tag = make_ref()
    mref = Process.monitor(h)

    try do
      case GenServer.call(h, {:finish, terminal, marker, tag}, @call_timeout) do
        {:pending, deadline} -> await_decision(h, mref, tag, deadline)
        other -> other
      end
    catch
      :exit, {:noproc, _} -> {:error, :handoff_down}
      :exit, {:timeout, _} -> fence_coordinator(h, mref, tag)
      :exit, _ -> recover_decision(h, tag)
    after
      Process.demonitor(mref, [:flush])
    end
  end

  @doc """
  Requester only, `terminal_ack: true` only. The local result of writing the
  `{:terminal, marker, deadline}` downstream: `:ok` once the write returned
  success, or `{:error, reason}`. Ends the handoff `{:done, terminal}` (only
  before the deadline), `{:incomplete, {:downstream_write, reason}}`, or
  (after it) `{:incomplete, {:terminal_unconfirmed, :deadline}}`. The outcome
  message is authoritative; see "Requester terminal protocol". On a call
  timeout the coordinator is killed before `{:error, :unconfirmed}` returns,
  so no outcome can follow a timeout.
  """
  def terminal_written(h, result)
      when result == :ok or
             (is_tuple(result) and tuple_size(result) == 2 and elem(result, 0) == :error) do
    mref = Process.monitor(h)

    try do
      GenServer.call(h, {:terminal_written, result}, @call_timeout)
    catch
      :exit, {:noproc, _} -> {:error, :handoff_down}
      :exit, {:timeout, _} -> fence_coordinator(h, mref, nil)
      :exit, _ -> {:error, :unconfirmed}
    after
      Process.demonitor(mref, [:flush])
    end
  end

  # Client side of finish/3 in admission mode. The coordinator decides by its
  # deadline; after deadline + grace the caller fences it instead of guessing.
  defp await_decision(h, mref, tag, deadline) do
    wait =
      if is_integer(deadline),
        do: max(deadline - System.monotonic_time(:millisecond), 0) + @decision_grace,
        else: @call_timeout

    receive do
      {@tag, ^h, {:terminal_decision, ^tag, decision}} -> decision
      # A decision sent before the exit would have been matched first.
      {:DOWN, ^mref, :process, _, _} -> {:error, :unconfirmed}
    after
      wait -> fence_coordinator(h, mref, tag)
    end
  end

  # Kill, then wait for :DOWN (always delivered to a monitor), so the
  # coordinator can't act on our call after we return. A decision it sent
  # before dying precedes the :DOWN in our mailbox.
  defp fence_coordinator(h, mref, tag) do
    Process.exit(h, :kill)
    receive do: ({:DOWN, ^mref, :process, _, _} -> :ok)
    recover_decision(h, tag)
  end

  defp recover_decision(_h, nil), do: {:error, :unconfirmed}

  defp recover_decision(h, tag) do
    receive do
      {@tag, ^h, {:terminal_decision, ^tag, decision}} -> decision
    after
      0 -> {:error, :unconfirmed}
    end
  end

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
    fence_only = Keyword.get(opts, :authority)
    admission = Keyword.get(opts, :admission_authority)
    authority = admission || fence_only
    observer = Keyword.get(opts, :observer)
    deadline = Keyword.get(opts, :terminal_deadline)
    terminal_ack = Keyword.get(opts, :terminal_ack, false) == true
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

      not (is_nil(fence_only) or is_pid(fence_only)) or (fence_only != nil and admission != nil) ->
        {:stop, {:bad_option, :authority}}

      not (is_nil(admission) or admission_authority?(admission)) ->
        # Only a live local Telelem.Admission.Authority can grant terminals;
        # a generic PID must be passed as the fence-only :authority.
        {:stop, {:bad_option, :admission_authority}}

      terminal_ack and admission == nil ->
        {:stop, {:bad_option, {:terminal_ack, :requires_admission_authority}}}

      not (is_nil(deadline) or is_integer(deadline)) or
          (deadline == nil and (terminal_ack or admission != nil)) ->
        {:stop, {:bad_option, :terminal_deadline}}

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

        # One timer at the shared absolute deadline bounds both the pending
        # grant and the pending write result, independent of the requester.
        if deadline, do: Process.send_after(me, {:terminal_deadline, token}, deadline, abs: true)

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
           authority: authority,
           authority_mon: authority && Process.monitor(authority),
           authority_state: if(authority, do: :up, else: :none),
           authorization:
             cond do
               admission -> :admission
               fence_only -> :fence_only
               true -> :none
             end,
           terminal_deadline: deadline,
           # nil | %{id, caller, tag, nonce, terminal, marker}: grant requested
           pending: nil,
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
           terminal_ack: terminal_ack,
           # nil | {:authorized, terminal}: upstream complete, downstream unconfirmed
           terminal: nil,
           downstream: nil,
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

      {:ok, %{pid: ^pid}} when s.outcome != nil or s.terminal != nil ->
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

      {:ok, _} when s.outcome != nil or s.terminal != nil ->
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
          s.outcome != nil or s.terminal != nil -> {:reply, {:cancel, :ended}, s}
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

  def handle_call({:finish, terminal, marker, tag}, {pid, _}, s) do
    with {:ok, id} <- accepting_winner(s, pid),
         :ok <- valid_marker(s, marker) do
      cond do
        past_deadline?(s) ->
          s
          |> trace({:terminal_refused, id, :deadline})
          |> set_outcome(ended(s, :terminal_deadline))
          |> then(&reply_finalize({:error, :cancelled}, &1))

        s.authorization == :admission ->
          # Ask the pinned authority; decide only on its grant or its :DOWN.
          nonce = make_ref()
          :ok = Telelem.Admission.Authority.request_terminal_grant(s.authority, nonce)
          p = %{id: id, caller: pid, tag: tag, nonce: nonce, terminal: terminal, marker: marker}
          s = %{s | pending: p} |> trace({:terminal_requested, id})
          {:reply, {:pending, s.terminal_deadline}, s}

        true ->
          # Legacy: the coordinator's own decision, in mailbox order.
          s
          |> commit(id)
          |> authorize_terminal(id, terminal, marker)
          |> then(&reply_finalize(:ok, &1))
      end
    else
      e -> {:reply, e, s}
    end
  end

  def handle_call({:terminal_written, result}, {pid, _}, s) do
    cond do
      pid != s.requester ->
        {:reply, {:error, :not_requester}, s}

      not s.terminal_ack or s.terminal == nil ->
        {:reply, {:error, :no_terminal}, s}

      s.outcome != nil ->
        {:reply, {:error, :ended}, s}

      past_deadline?(s) ->
        # The timer is still queued; a late result can't make it done.
        s
        |> trace({:terminal_written_late, result})
        |> set_outcome({:incomplete, {:terminal_unconfirmed, :deadline}})
        |> then(&reply_finalize({:error, :deadline}, &1))

      true ->
        {:authorized, terminal} = s.terminal

        s =
          case result do
            :ok ->
              %{s | downstream: :written}
              |> trace({:terminal_written, :ok})
              |> set_outcome({:done, terminal})

            {:error, reason} ->
              %{s | downstream: {:error, reason}}
              |> trace({:terminal_written, {:error, reason}})
              |> set_outcome({:incomplete, {:downstream_write, reason}})
          end

        reply_finalize(:ok, s)
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
       terminal: terminal_state(s),
       authority: s.authority_state,
       authorization: s.authorization,
       terminal_pending: s.pending != nil,
       observer: s.observer_state,
       attempts:
         Map.new(s.attempts, fn {id, a} ->
           {id, Map.take(a, [:pid, :dispatched, :ready, :cancelled, :cleanup, :charge])}
         end)
     }, s}
  end

  @impl true
  # The grant was issued while the pinned authority was alive; it reaches us
  # before that authority's :DOWN. This is the authorization point.
  def handle_info(
        {:telelem_terminal_grant, auth, nonce, incarnation},
        %{authority: auth, pending: %{nonce: nonce} = p} = s
      ) do
    s = trace(s, {:terminal_granted, p.id, incarnation})

    if past_deadline?(s) do
      s |> set_outcome(ended(s, :terminal_deadline)) |> finalize()
    else
      s = %{s | pending: nil} |> commit(p.id) |> authorize_terminal(p.id, p.terminal, p.marker)
      # Only after the marker was handed over: a kill in between leaves the
      # winner with no decision, i.e. unconfirmed, never a false refusal.
      send(p.caller, {@tag, self(), {:terminal_decision, p.tag, :ok}})
      finalize(s)
    end
  end

  def handle_info({:telelem_terminal_grant, _auth, _nonce, _inc}, s),
    do: {:noreply, trace(s, :stale_terminal_grant)}

  def handle_info({:terminal_deadline, token}, %{register_token: token} = s) do
    cond do
      s.outcome != nil ->
        {:noreply, s}

      s.pending != nil ->
        s
        |> trace({:terminal_deadline, :pending_grant})
        |> then(&set_outcome(&1, ended(&1, :terminal_deadline)))
        |> finalize()

      s.terminal != nil and s.terminal_ack ->
        s
        |> trace({:terminal_deadline, :pending_write})
        |> set_outcome({:incomplete, {:terminal_unconfirmed, :deadline}})
        |> finalize()

      true ->
        # Still streaming: left to the upstream/requester; finish will refuse.
        {:noreply, trace(s, {:terminal_deadline, :no_terminal})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{requester_mon: ref} = s) do
    s = trace(s, {:requester_down, reason})
    finalize(set_outcome(s, ended(s, {:requester_down, reason})))
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{authority_mon: ref} = s) do
    finalize(authority_lost(s, reason, s.phase))
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

  # One step: record the upstream terminal and hand the marker over. Callers
  # reach it only on an admission grant, or (legacy) in mailbox order.
  defp authorize_terminal(s, id, terminal, marker) do
    s = %{s | terminal: {:authorized, terminal}} |> trace({:terminal_authorized, id, terminal})

    if s.terminal_ack do
      # Upstream complete only. {:done, _} waits for the local write result.
      send(s.requester, {@tag, self(), {:terminal, marker, s.terminal_deadline}})
      %{s | downstream: :unconfirmed} |> emit({:upstream_complete, id}) |> arm_deadline(id)
    else
      # M1 compatibility: marker and outcome are adjacent, nothing in between.
      if marker, do: send(s.requester, {@tag, self(), {:data, marker}})
      %{s | downstream: :not_tracked} |> set_outcome({:done, terminal})
    end
  end

  # The instance is fenced: end an open stream, never extend or reselect. An
  # authorized terminal (or any reached outcome) is never rewritten.
  defp authority_lost(s, reason, at) do
    s = trace(%{s | authority_state: :lost}, {:authority_down, at, reason})
    if s.terminal == nil, do: set_outcome(s, ended(s, {:authority_down, reason})), else: s
  end

  defp past_deadline?(%{terminal_deadline: nil}), do: false
  defp past_deadline?(%{terminal_deadline: d}), do: System.monotonic_time(:millisecond) >= d

  # Serving modes hand over real bytes; nil/empty would be a terminal with no
  # client-visible evidence.
  defp valid_marker(s, marker) do
    if (s.authorization == :admission or s.terminal_ack) and
         not (is_binary(marker) and byte_size(marker) > 0),
       do: {:error, :invalid_marker},
       else: :ok
  end

  # Only a live, local Telelem.Admission.Authority implements the grant.
  defp admission_authority?(pid) do
    is_pid(pid) and node(pid) == node() and
      :proc_lib.translate_initial_call(pid) == {Telelem.Admission.Authority, :init, 1}
  end

  defp terminal_state(%{terminal: nil}), do: nil

  defp terminal_state(%{terminal: {:authorized, _}, downstream: d}),
    do: %{upstream: :complete, downstream: d}

  defp set_outcome(%{outcome: o} = s, _) when o != nil, do: s

  defp set_outcome(s, outcome) do
    s = %{s | outcome: outcome}
    send(s.requester, {@tag, self(), outcome})
    # Any outcome reached while a grant is pending refuses it; a grant that
    # arrives later is stale.
    s =
      case s.pending do
        nil ->
          s

        p ->
          send(p.caller, {@tag, self(), {:terminal_decision, p.tag, {:error, :cancelled}}})
          trace(%{s | pending: nil}, {:terminal_refused, p.id, outcome})
      end

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
    # After terminal authorization the winner is expected to close; only the
    # requester's write result (or its loss) decides the outcome then.
    s =
      if id == s.winner and s.terminal == nil,
        do: set_outcome(s, ended(s, winner_reason)),
        else: s

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
      terminal: terminal_state(s),
      authority: s.authority_state,
      authorization: s.authorization,
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
      w == nil or w.pid != pid ->
        {:error, :not_winner}

      s.outcome != nil or s.terminal != nil or w.cancelled != nil or w.cleanup != nil ->
        {:error, :cancelled}

      s.pending != nil ->
        # Nothing may follow a requested terminal, not even another finish.
        {:error, :terminal_pending}

      true ->
        {:ok, s.winner}
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
