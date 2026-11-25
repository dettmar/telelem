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
      winner (`finish/4` takes it as `deadline:` before its first wait;
      legacy `finish/3` learns it from the coordinator) and the requester
      (`{:terminal, marker, deadline}`). The coordinator enforces it with one
      timer of its own, independent of the requester: a `finish/3` at or
      after it, or a grant still pending at it, is refused; a write result
      still pending at it ends `{:incomplete, {:terminal_unconfirmed,
      :deadline}}` with `downstream: :unconfirmed`. It does not end an open
      stream that has not finished yet (no finish can succeed after it).
    * **No late side effect after a caller gives up.** `finish/4` and
      `terminal_written/2` never return a local timeout while the coordinator
      can still act on the call: when their bound expires (`finish/4` with
      `deadline:`: one absolute `deadline + grace` across its initial reply
      and the decision; legacy `finish/3`: the call timeout for the reply,
      then the reported deadline plus a grace; otherwise the call timeout)
      they kill the coordinator, wait for its `:DOWN` and only then return.
      A decision
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

  ## Coordinator grant (opt-in: `coordinator_grant: {:one_primary, units}`)

  Off by default; without it nothing below applies and every shape above is
  unchanged. It requires `:admission_authority` (the pinned local
  authority), `:terminal_deadline` and `attempts: [:primary]` exactly (one
  attempt, the primary, no successor; any other id or list is refused). `units` is a trusted positive integer. The coordinator then
  owns the admission calls for that attempt, through
  `Telelem.Admission.Authority.send_reserve/4` / `send_authorize/2` to the
  pinned pid (never re-resolved), so it never blocks on the authority:

    1. `attach(h, id, deadline: d)` registers the caller, then reserves
       `units` with **exactly that pid** as owner (the authority monitors it).
       The reply is deferred: `{:ok, mref}` only on `{:ok, grant}`. The
       attempt connects only after that.
    2. After connect, `begin_dispatch(h, id, deadline: d)` sends one authorize
       for that grant. Its deferred reply `:ok` is the only send permission,
       given at most once and only for a fresh `{:ok, same_grant}` reply to
       that request. Anything else ends the request
       (`{:failed, :admission_refused}`): no retry, refund, settle or
       successor attempt.
    3. `ready/2` (and so `transfer/2`/`finish/3`) before that permission is
       refused with `{:error, :not_permitted}` and changes nothing; it does not
       imply the dispatch boundary in this mode.

  At most one request is outstanding and at most one reply is owed. Replies
  and the request monitor's `:DOWN` are matched with
  `:gen_server.check_response/2`; a request is abandoned with
  `:gen_server.receive_response(id, 0)`. Abandoning discards the reply only:
  the authority may still apply it, so the ledger (owner monitor, TTL) is what
  releases or encumbers the grant. Any outcome reached while a request is
  outstanding (requester cancel/death, attempt death, authority `:DOWN` seen
  by either monitor, the deadline) abandons it and answers the owed call with
  `{:cancel, reason}`. At the deadline, a request with no permission
  released yet (reserving, reserved/connecting or authorizing) ends
  `{:failed, :dispatch_deadline}`.

  Release order: the clock is checked (`now < deadline`) immediately before
  the permission reply and before the terminal marker; nothing, in
  particular no observer callback, runs between the check and the release.
  The attempt's own last check before sending is its own (see the test
  fake). Both deferred waits are bounded by `deadline + grace`; on expiry
  the caller kills the coordinator and waits for its `:DOWN`, so no
  permission can follow the caller's timeout.

  Status, report and trace carry only the phase (`:idle`, `:reserving`,
  `:reserved`, `:authorizing`, `:authorized`, `:refused`, `:abandoned`),
  never the grant token, op id or a raw authority reply.

  ## Receipts (opt-in: `receipts: [producer:, store_mode:, meta:]`)

  Off by default; without it nothing is published and every shape above is
  unchanged. It requires `coordinator_grant` and `terminal_ack: true`.

    * `producer`: a `Telelem.Receipts.Producer` resolved beforehand with
      `Telelem.Receipts.Store.producer/1`. The coordinator never calls the
      store. At start it checks, without messaging anyone, that the pending
      table exists, is a public `:set` owned by a live local
      `Telelem.Receipts.Store`, that the counters array has the size and sign
      the store creates, and that `max_pending <= 256` and
      `max_event_bytes <= 512` (the §2 ceilings). A handle failing any check
      (including one whose store has exited) is `{:bad_option, :receipts}`.
      These checks do not authenticate the handle: the store's own
      configured limits and its mode are not observable through the existing
      API.
    * `store_mode`: the trusted configuration's statement of the store's
      mode; it must equal `meta.mode`. It is not verified against the store.
    * `meta`: exactly `receipt_meta/1` of the trusted `Prepared` value
      (`tenant_id`, gateway `request_id`, `route_rev`, `deployment_id` of the
      primary, `mode`).

  The coordinator opens exactly one `Telelem.Receipts.Writer` (it is the only
  owner) at the end of `init/1` and keeps the advanced writer after every
  publication result, so a failed publication leaves a sequence gap that
  later records declare. Publication is one synchronous ETS insert that never
  waits on the store; its result never changes the request. Events, in
  sequence order (attempt `1` with the primary's deployment for `dispatch`
  and `commit`; attempt `0`, deployment `none` for the others):

    1. `ingress` (exposure `none`) at start.
    2. `dispatch` (`possible`) immediately after the one permission reply,
       before any trace or observer work; the deadline check stays right
       before the reply.
    3. `commit` (`possible`) once, when the stream commits: before the first
       chunk is forwarded, or, for a finish-first commit, after the marker and
       the winner's decision were sent.
    4. `authority_down` only while no outcome is reached, including an
       authorized terminal that still awaits its write acknowledgement.
    5. `terminal` once, immediately before the outcome is sent to the
       requester, with that outcome's kind. `done` therefore means only a
       local marker-write acknowledgement processed before the deadline.

  Exposure of `authority_down` and `terminal` is `possible` exactly when the
  dispatch permission was released, else `none`; it is not taken from the
  ledger or from what was published. No `connect_refused` is ever inferred,
  and usage stays unknown (`Telelem.Receipts.Receipt`). An exception raised
  while publishing disables this request's evidence for good (no later
  record, in particular no terminal), so its receipt can never read
  complete. `terminate/2` publishes nothing. Status and report carry only
  `receipts: :recording | :disabled`; the trace carries `{:receipt, kind,
  :ok | :dropped | :rejected | :fault}`.

  Known gap: a coordinator killed between a release (the permission reply,
  or the marker) and the record that follows it leaves that record missing
  and no terminal: the receipt is incomplete, never false-complete.

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
    * `:coordinator_grant`. Default `nil`. `{:one_primary, units}` opts in to
      "Coordinator grant". Requires `:admission_authority`,
      `:terminal_deadline` and `attempts: [:primary]` exactly.
    * `:receipts`. Default `nil`. `[producer:, store_mode:, meta:]` opts in
      to "Receipts". Requires `:coordinator_grant` and `terminal_ack: true`.
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

  `deadline:` (the shared absolute `:terminal_deadline`) bounds the wait by
  `deadline + grace` instead of the call timeout; when that bound expires the
  coordinator is killed and `{:error, :unconfirmed}` is returned. With
  `coordinator_grant` it is required, and `{:ok, _}` comes only after the
  authority reserved this attempt's grant (see "Coordinator grant").
  """
  def attach(h, id, opts \\ []) when is_list(opts) do
    mref = Process.monitor(h)

    result =
      case deadline_opt(opts, "attach/3") do
        nil -> call(h, {:attach, id})
        d -> bounded_call(h, {:attach, id, d}, d)
      end

    case result do
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

  With `coordinator_grant` (see "Coordinator grant"), call it after connect
  and with `deadline:`; `:ok` then also means the coordinator saw the
  authority dispatch this attempt's grant. `deadline:` bounds the wait as in
  `attach/3`.
  """
  def begin_dispatch(h, id, opts \\ []) when is_list(opts) do
    case deadline_opt(opts, "begin_dispatch/3") do
      nil -> call(h, {:begin_dispatch, id})
      d -> bounded_call(h, {:begin_dispatch, id, d}, d)
    end
  end

  defp deadline_opt(opts, fun) do
    case Keyword.get(opts, :deadline) do
      d when is_nil(d) or is_integer(d) -> d
      other -> raise ArgumentError, "#{fun} :deadline must be an integer, got: #{inspect(other)}"
    end
  end

  # One deferred reply, bounded by the shared absolute deadline plus the
  # grace. On expiry the coordinator is fenced (killed, :DOWN awaited) before
  # returning, so it cannot act on this call afterwards. A late reply can't
  # arrive either: GenServer.call deactivates its alias on timeout.
  defp bounded_call(h, msg, deadline) do
    bound = deadline + @decision_grace
    mref = Process.monitor(h)

    try do
      GenServer.call(h, msg, remaining(bound))
    catch
      :exit, {:noproc, _} -> {:error, :handoff_down}
      :exit, {:timeout, _} -> fence_coordinator(h, mref, nil)
      :exit, _ -> {:error, :unconfirmed}
    after
      Process.demonitor(mref, [:flush])
    end
  end

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

  Options:

    * `:deadline`. The shared absolute `:terminal_deadline`
      (`System.monotonic_time(:millisecond)`), when the caller knows it.
      Then one absolute bound, `deadline + grace`, covers both waits: the
      coordinator's initial reply and the decision that follows. Neither wait
      restarts a timeout or adds its own grace; if the coordinator reports a
      different deadline, the earlier bound wins. A caller that only arrives
      after `deadline + grace` fences the coordinator at once (it could not
      have released the marker anyway: a finish at or after the deadline is
      refused).
    * Without `:deadline` (legacy): the initial reply is bounded by the call
      timeout (5000 ms), then the decision by the deadline the coordinator
      reports plus the grace (or the call timeout if it reports none).

  Never returns while the coordinator could still release the marker
  afterwards.
  """
  def finish(h, terminal, marker \\ nil, opts \\ [])
      when (is_nil(marker) or is_binary(marker)) and is_list(opts) do
    bound =
      case Keyword.get(opts, :deadline) do
        nil ->
          nil

        d when is_integer(d) ->
          d + @decision_grace

        other ->
          raise ArgumentError, "finish/4 :deadline must be an integer, got: #{inspect(other)}"
      end

    tag = make_ref()
    mref = Process.monitor(h)

    try do
      first_wait = if bound, do: remaining(bound), else: @call_timeout

      case GenServer.call(h, {:finish, terminal, marker, tag}, first_wait) do
        {:pending, deadline} -> await_decision(h, mref, tag, decision_bound(bound, deadline))
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

  # The absolute instant finish/4 stops waiting for the decision. A bound the
  # caller already holds (its deadline + grace) is never extended; the
  # coordinator's reported deadline can only tighten it.
  # Legacy (no caller deadline), unchanged: the grace counts from the later of
  # the reported deadline and the pending reply's arrival.
  defp decision_bound(nil, deadline) when is_integer(deadline),
    do: max(deadline, System.monotonic_time(:millisecond)) + @decision_grace

  defp decision_bound(nil, _), do: System.monotonic_time(:millisecond) + @call_timeout

  defp decision_bound(bound, deadline) when is_integer(deadline),
    do: min(bound, deadline + @decision_grace)

  defp decision_bound(bound, _), do: bound

  defp remaining(bound), do: max(bound - System.monotonic_time(:millisecond), 0)

  # Client side of finish/4 in admission mode. The coordinator decides by its
  # deadline; at the absolute bound the caller fences it instead of guessing.
  defp await_decision(h, mref, tag, bound) do
    receive do
      {@tag, ^h, {:terminal_decision, ^tag, decision}} -> decision
      # A decision sent before the exit would have been matched first.
      {:DOWN, ^mref, :process, _, _} -> {:error, :unconfirmed}
    after
      remaining(bound) -> fence_coordinator(h, mref, tag)
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
    grant_opt = Keyword.get(opts, :coordinator_grant)
    # Parsed once: nil (off), :error, or the checked configuration.
    receipts_cfg =
      case Keyword.get(opts, :receipts) do
        nil -> nil
        r -> receipts_config(r)
      end

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

      not valid_grant_opt?(grant_opt, admission, ids, deadline) ->
        {:stop, {:bad_option, :coordinator_grant}}

      not (is_nil(receipts_cfg) or (grant_opt != nil and terminal_ack and receipts_cfg != :error)) ->
        {:stop, {:bad_option, :receipts}}

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

        s = %{
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
          # nil (default) | coordinator-owned admission grant, see new_grant/2
          grant: new_grant(grant_opt, ids),
          observer_mon: observer && Process.monitor(observer),
          observer_state: if(observer || Keyword.has_key?(opts, :on_event), do: :up, else: :none),
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
          unreported: [],
          # nil (default) | coordinator-owned receipt writer, see open_receipts/2
          receipts: nil
        }

        # Last step of a successful init: open the one writer, record ingress.
        {:ok, open_receipts(s, receipts_cfg)}
    end
  end

  @impl true
  def handle_call({:attach, id}, from, s), do: handle_call({:attach, id, nil}, from, s)

  def handle_call({:attach, id, deadline}, {pid, _} = from, s) do
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

      {:ok, %{pid: ^pid}} when s.grant != nil and s.grant.phase == :reserving ->
        # The first attach's reply is still owed; no second reservation.
        {:reply, {:error, :pending}, s}

      {:ok, %{pid: ^pid}} ->
        {:reply, :ok, s}

      {:ok, %{pid: p}} when is_pid(p) ->
        {:reply, {:error, :already_attached}, s}

      {:ok, a} when s.grant != nil ->
        grant_attach(s, id, a, from, deadline)

      {:ok, a} ->
        # Invariant: an unregistered, unclosed attempt exists only while the
        # selection is open (election and outcome close them all).
        s = put(s, id, %{a | pid: pid, mon: Process.monitor(pid)})
        {:reply, :ok, trace(s, {:attached, id})}
    end
  end

  def handle_call({:begin_dispatch, id}, from, s),
    do: handle_call({:begin_dispatch, id, nil}, from, s)

  def handle_call({:begin_dispatch, id, deadline}, {pid, _} = from, s) do
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

      {:ok, _} when s.grant != nil ->
        grant_dispatch(s, id, from, deadline)

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

      {:ok, a} when s.grant != nil and s.grant.phase != :authorized ->
        # No implied dispatch boundary: only the authority's dispatch of this
        # attempt's grant (begin_dispatch/3 == :ok) permits content.
        cond do
          a.cancelled != nil -> {:reply, {:cancel, a.cancelled}, s}
          s.outcome != nil -> {:reply, {:cancel, :ended}, s}
          true -> {:reply, {:error, :not_permitted}, trace(s, {:not_permitted, id, :ready})}
        end

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
     }
     |> with_grant_phase(s)
     |> with_receipts(s), s}
  end

  @impl true
  # With a coordinator grant request outstanding, its reply or its request
  # monitor's :DOWN is matched first, with the documented gen_server API.
  def handle_info(msg, %{grant: %{req: req}} = s) when req != nil do
    case :gen_server.check_response(msg, req) do
      :no_reply -> info(msg, s)
      {:reply, reply} -> grant_reply(reply, s)
      {:error, {reason, _server}} -> grant_authority_down(reason, s)
    end
  end

  def handle_info(msg, s), do: info(msg, s)

  # The grant was issued while the pinned authority was alive; it reaches us
  # before that authority's :DOWN. This is the authorization point.
  defp info(
         {:telelem_terminal_grant, auth, nonce, incarnation},
         %{authority: auth, pending: %{nonce: nonce} = p} = s
       ) do
    s = trace(s, {:terminal_granted, p.id, incarnation})

    cond do
      past_deadline?(s) ->
        s |> set_outcome(ended(s, :terminal_deadline)) |> finalize()

      s.grant != nil ->
        # Coordinator grant: nothing (no observer callback) between the clock
        # check and the marker.
        finalize(release_terminal(s, p))

      true ->
        s = %{s | pending: nil} |> commit(p.id) |> authorize_terminal(p.id, p.terminal, p.marker)
        # Only after the marker was handed over: a kill in between leaves the
        # winner with no decision, i.e. unconfirmed, never a false refusal.
        send(p.caller, {@tag, self(), {:terminal_decision, p.tag, :ok}})
        finalize(s)
    end
  end

  defp info({:telelem_terminal_grant, _auth, _nonce, _inc}, s),
    do: {:noreply, trace(s, :stale_terminal_grant)}

  defp info({:terminal_deadline, token}, %{register_token: token} = s) do
    cond do
      s.outcome != nil ->
        {:noreply, s}

      s.grant != nil and s.grant.phase != :authorized ->
        # Coordinator grant: no dispatch permission was released before the
        # deadline (reserving, reserved/connecting or authorizing), and none can
        # be now. Abandons an outstanding request; never refunds.
        s
        |> trace({:grant_deadline, s.grant.id, s.grant.phase})
        |> then(&set_outcome(&1, ended(&1, :dispatch_deadline)))
        |> finalize()

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

  defp info({:DOWN, ref, :process, _pid, reason}, %{requester_mon: ref} = s) do
    s = trace(s, {:requester_down, reason})
    finalize(set_outcome(s, ended(s, {:requester_down, reason})))
  end

  defp info({:DOWN, ref, :process, _pid, reason}, %{authority_mon: ref} = s) do
    finalize(authority_lost(s, reason, s.phase))
  end

  defp info({:DOWN, ref, :process, _pid, reason}, %{observer_mon: ref} = s) do
    {:noreply, trace(%{s | observer_state: :lost}, {:observer_down, s.phase, reason})}
  end

  defp info({:DOWN, ref, :process, _pid, reason}, s) do
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

  defp info({:cleanup_deadline, id, token}, s) do
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

  defp info({:register_deadline, token}, %{register_token: token} = s) do
    if s.phase == :open and s.outcome == nil do
      s |> close_unregistered() |> check_no_winner() |> finalize()
    else
      {:noreply, s}
    end
  end

  # Non-parent links (none are supported) are trapped and ignored; the
  # parent's EXIT is handled by gen_server and lands in terminate/2.
  defp info(_other, s), do: {:noreply, s}

  @impl true
  def terminate(reason, s) do
    # Nothing is published from here: an abnormal stop leaves no terminal, so
    # the receipt can never read complete.
    s = stop_receipts(s)

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

  # Commit evidence is recorded once, here, before observer work. For a
  # finish-first commit (coordinator grant) the marker and the winner's
  # decision were already sent by release_terminal/2.
  defp commit(%{phase: :elected} = s, id) do
    %{s | phase: :committed}
    |> receipt(:commit)
    |> emit({:committed, id})
    |> trace({:committed, id})
  end

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
    # Evidence only while no outcome is reached, including an authorized
    # terminal still awaiting its write acknowledgement.
    s = if s.outcome == nil, do: receipt(s, :authority_down), else: s
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
    # Terminal evidence before the requester can learn the outcome, so a
    # lookup made after the notification sees it (unless publication failed).
    s = receipt(s, :terminal, elem(outcome, 0))
    send(s.requester, {@tag, self(), outcome})
    record_outcome(s, outcome)
  end

  # set_outcome/2 without the requester message, for a release path that has
  # already sent it.
  defp record_outcome(s, outcome) do
    s = %{s | outcome: outcome}

    # An outstanding coordinator grant request is abandoned: its owed reply
    # refuses, and nothing for it is ever handled again.
    s = drop_grant_request(s, {:cancel, elem(outcome, 1)})

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
    s = drop_grant_request(s, {:cancel, :closed})
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
    |> with_grant_phase(s)
    |> with_receipts(s)
  end

  # Outward views carry only the grant phase: never the token, the op id or
  # a raw authority reply. Absent by default, so default shapes are unchanged.
  defp with_grant_phase(map, %{grant: nil}), do: map
  defp with_grant_phase(map, %{grant: g}), do: Map.put(map, :grant, g.phase)

  # ── Coordinator grant (opt-in, see moduledoc) ────────────────────────────

  defp valid_grant_opt?(nil, _admission, _ids, _deadline), do: true

  defp valid_grant_opt?({:one_primary, units}, admission, ids, deadline),
    do:
      is_integer(units) and units > 0 and admission != nil and is_integer(deadline) and
        ids == [:primary]

  defp valid_grant_opt?(_, _, _, _), do: false

  defp new_grant(nil, _ids), do: nil

  # gid is the private token; req/from the one outstanding request and the
  # one reply owed for it. Only `phase` is ever shown outside.
  defp new_grant({:one_primary, units}, [id]),
    do: %{id: id, units: units, phase: :idle, gid: nil, req: nil, from: nil}

  defp put_grant(s, kv), do: %{s | grant: Map.merge(s.grant, Map.new(kv))}

  # First attach of the primary: register it, then reserve with exactly its
  # pid as owner. The attach reply waits for the authority's reply.
  defp grant_attach(s, id, a, {pid, _} = from, deadline) do
    cond do
      deadline != s.terminal_deadline ->
        {:reply, {:error, :deadline_required}, s}

      past_deadline?(s) ->
        {:reply, {:cancel, :dispatch_deadline}, s}

      true ->
        s = put(s, id, %{a | pid: pid, mon: Process.monitor(pid)})
        op = {__MODULE__, make_ref()}
        req = Telelem.Admission.Authority.send_reserve(s.authority, op, pid, s.grant.units)
        s = put_grant(s, phase: :reserving, req: req, from: from)
        {:noreply, s |> trace({:attached, id}) |> trace({:grant, id, :reserving})}
    end
  end

  # After the attempt's connect: authorize the reserved grant, once.
  defp grant_dispatch(%{grant: g} = s, id, from, deadline) do
    case g.phase do
      :reserved when deadline != s.terminal_deadline ->
        {:reply, {:error, :deadline_required}, s}

      :reserved ->
        if past_deadline?(s) do
          s
          |> trace({:grant_deadline, id, :reserved})
          |> set_outcome(ended(s, :dispatch_deadline))
          |> then(&reply_finalize({:cancel, :dispatch_deadline}, &1))
        else
          req = Telelem.Admission.Authority.send_authorize(s.authority, g.gid)
          s = put_grant(s, phase: :authorizing, req: req, from: from)
          {:noreply, trace(s, {:grant, id, :authorizing})}
        end

      :authorized ->
        # At most one permission per request.
        {:reply, {:error, :duplicate}, s}

      phase when phase in [:reserving, :authorizing] ->
        {:reply, {:error, :pending}, s}

      _ ->
        {:reply, {:cancel, :ended}, s}
    end
  end

  # The fresh reply to the one outstanding request. Release is the reply
  # itself, sent right after the clock check; trace and events follow it.
  defp grant_reply(reply, %{grant: g} = s) do
    s = put_grant(s, req: nil, from: nil)
    a = s.attempts[g.id]
    live = s.outcome == nil and a.cancelled == nil and a.cleanup == nil

    case {g.phase, reply} do
      {:reserving, {:ok, gid}} when live ->
        if past_deadline?(s) do
          grant_refused(s, g, :dispatch_deadline)
        else
          GenServer.reply(g.from, :ok)

          {:noreply,
           s |> put_grant(phase: :reserved, gid: gid) |> trace({:grant, g.id, :reserved})}
        end

      {:authorizing, {:ok, gid}} when live and gid == g.gid ->
        if past_deadline?(s) do
          grant_refused(s, g, :dispatch_deadline)
        else
          GenServer.reply(g.from, :ok)
          # Dispatch evidence right after the one permission, before anything
          # else (the permission bit is set first; publication never gates it).
          s = s |> receipt_permitted() |> receipt(:dispatch)

          {:noreply,
           s
           |> put(g.id, %{a | dispatched: true})
           |> put_grant(phase: :authorized)
           |> trace({:dispatch_boundary, g.id})
           |> trace({:grant, g.id, :authorized})}
        end

      _ ->
        # Not a permission (refusal, duplicate, other token). No retry,
        # refund or successor: the request ends.
        grant_refused(s, g, :admission_refused)
    end
  end

  defp grant_refused(s, g, reason) do
    GenServer.reply(g.from, {:cancel, reason})

    s
    |> put_grant(phase: :refused)
    |> trace({:grant, g.id, {:refused, reason}})
    |> set_outcome(ended(s, reason))
    |> finalize()
  end

  # The request monitor saw the pinned authority exit with no reply. The
  # authority's own monitor is flushed so the loss is handled exactly once.
  defp grant_authority_down(reason, %{grant: g} = s) do
    Process.demonitor(s.authority_mon, [:flush])
    GenServer.reply(g.from, {:cancel, {:authority_down, reason}})

    s =
      %{s | authority_mon: nil}
      |> put_grant(req: nil, from: nil, phase: :abandoned)
      |> trace({:grant, g.id, {:abandoned, g.phase}})

    finalize(authority_lost(s, reason, s.phase))
  end

  # Documented abandonment: receive_response/2 with timeout 0 demonitors
  # (flushing a queued :DOWN, deactivating the reply alias) and takes a reply
  # already queued. Whatever it returns is discarded: the authority may still
  # apply the request, and the ledger, not this coordinator, says what it holds.
  defp drop_grant_request(%{grant: %{req: req} = g} = s, reply) when req != nil do
    _ = :gen_server.receive_response(req, 0)
    GenServer.reply(g.from, reply)

    s
    |> put_grant(req: nil, from: nil, phase: :abandoned)
    |> trace({:grant, g.id, {:abandoned, g.phase}})
  end

  defp drop_grant_request(s, _reply), do: s

  # Coordinator grant: hand the marker (and, by default, the outcome) to the
  # requester first, then answer the winner; events only after both.
  defp release_terminal(s, p) do
    me = self()

    if s.terminal_ack do
      send(s.requester, {@tag, me, {:terminal, p.marker, s.terminal_deadline}})
    else
      send(s.requester, {@tag, me, {:data, p.marker}})
      send(s.requester, {@tag, me, {:done, p.terminal}})
    end

    send(p.caller, {@tag, me, {:terminal_decision, p.tag, :ok}})

    s =
      %{s | pending: nil, terminal: {:authorized, p.terminal}}
      |> commit(p.id)
      |> trace({:terminal_authorized, p.id, p.terminal})

    if s.terminal_ack do
      %{s | downstream: :unconfirmed} |> emit({:upstream_complete, p.id}) |> arm_deadline(p.id)
    else
      %{s | downstream: :not_tracked} |> record_outcome({:done, p.terminal})
    end
  end

  # ── Receipts (opt-in, see moduledoc) ─────────────────────────────────────

  @doc """
  Projects the receipt metadata the coordinator may hold from one prepared
  request: `%{tenant_id, request_id, route_rev, deployment_id, mode}`, with
  `request_id` the gateway-generated id and `deployment_id` the plan's
  primary. The body, target, principal subject and deadlines are not part of
  it. Returns `{:error, :invalid_prepared}` if the plan does not belong to the
  pinned revision.
  """
  @spec receipt_meta(Telelem.Serve.Ingress.Prepared.t()) ::
          {:ok, map()} | {:error, :invalid_prepared}
  def receipt_meta(%Telelem.Serve.Ingress.Prepared{} = p) do
    with %Telelem.Route.Revision{} = rev <- p.revision,
         %Telelem.Route.Plan{} = plan <- p.plan,
         true <-
           plan.rev == rev.rev and plan.digest == rev.digest and p.route_id == rev.route_id and
             plan.primary in rev.deployments,
         meta = %{
           tenant_id: p.tenant_id,
           request_id: p.gateway_id,
           route_rev: rev.rev,
           deployment_id: plan.primary,
           mode: p.mode
         },
         true <- valid_meta?(meta) do
      {:ok, meta}
    else
      _ -> {:error, :invalid_prepared}
    end
  end

  def receipt_meta(_), do: {:error, :invalid_prepared}

  # Contract §2 initial ceilings. A handle claiming more is refused: its
  # fields alone cannot raise the bounds.
  @receipt_max_pending 256
  @receipt_max_event_bytes 512
  @int64_max 0x7FFFFFFFFFFFFFFF

  # `[producer:, store_mode:, meta:]` (keyword list or map, exactly these
  # keys). Every check is contained: a malformed handle is :error, never a
  # crash of init.
  defp receipts_config(opt) do
    opt = if is_list(opt) and Keyword.keyword?(opt), do: Map.new(opt), else: opt

    with %{} <- opt,
         false <- is_struct(opt),
         [:meta, :producer, :store_mode] <- opt |> Map.keys() |> Enum.sort(),
         %{producer: p, store_mode: mode, meta: meta} <- opt,
         true <- mode in [:sandbox, :serving] and valid_meta?(meta) and meta.mode == mode,
         true <- genuine_producer?(p) do
      %{producer: p, meta: meta}
    else
      _ -> :error
    end
  catch
    _, _ -> :error
  end

  defp valid_meta?(%{} = m) when not is_struct(m) do
    Enum.sort(Map.keys(m)) == [:deployment_id, :mode, :request_id, :route_rev, :tenant_id] and
      Telelem.Receipts.Event.valid_id?(m.tenant_id) and
      Telelem.Receipts.Event.valid_id?(m.request_id) and
      Telelem.Receipts.Event.valid_id?(m.deployment_id) and is_integer(m.route_rev) and
      m.route_rev > 0 and m.route_rev <= @int64_max and m.mode in [:sandbox, :serving]
  end

  defp valid_meta?(_), do: false

  # A handle resolved beforehand with Telelem.Receipts.Store.producer/1. The
  # coordinator never calls the store; it checks what the existing APIs show
  # without a message: the pending table exists, is a public :set owned by a
  # live local Telelem.Receipts.Store, the counters array has the size and
  # sign the store creates, and the ceilings are within the contract's. The
  # store's own configured limits and its mode are not observable this way.
  defp genuine_producer?(
         %Telelem.Receipts.Producer{
           table: t,
           ref: ref,
           max_pending: mp,
           max_event_bytes: mb,
           loss_slots: ls
         } = p
       )
       when map_size(p) == 6 and is_reference(t) and is_integer(mp) and is_integer(mb) and
              is_integer(ls) and ls > 0 do
    owner = :ets.info(t, :owner)

    mp in 1..@receipt_max_pending and mb in 1..@receipt_max_event_bytes and is_pid(owner) and
      node(owner) == node() and
      :proc_lib.translate_initial_call(owner) == {Telelem.Receipts.Store, :init, 1} and
      :ets.info(t, :protection) == :public and :ets.info(t, :type) == :set and
      :ets.info(t, :keypos) == 1 and counters_match?(:atomics.info(ref), ls)
  end

  defp genuine_producer?(_), do: false

  defp counters_match?(%{size: size, min: min}, loss_slots),
    do: min < 0 and size == Telelem.Receipts.Producer.counters_size(loss_slots)

  defp counters_match?(_, _), do: false

  defp open_receipts(s, nil), do: s

  # Exactly one writer per coordinator, owned by this process; ingress is
  # its sequence one.
  defp open_receipts(s, %{producer: p, meta: m}) do
    r =
      case Telelem.Receipts.Writer.open(p, m.tenant_id, m.request_id) do
        {:ok, w} ->
          %{writer: w, route_rev: m.route_rev, deployment_id: m.deployment_id, state: :recording}

        {:error, _} ->
          %{writer: nil, route_rev: nil, deployment_id: nil, state: :disabled}
      end

    %{s | receipts: Map.merge(r, %{permitted: false, terminal: false})}
    |> receipt(:ingress)
  end

  # The permission bit: set when the one dispatch permission is released.
  # It alone decides exposure of later request-level events.
  defp receipt_permitted(%{receipts: nil} = s), do: s
  defp receipt_permitted(%{receipts: r} = s), do: %{s | receipts: %{r | permitted: true}}

  defp stop_receipts(%{receipts: nil} = s), do: s

  defp stop_receipts(%{receipts: r} = s),
    do: %{s | receipts: %{r | state: :disabled, writer: nil}}

  defp receipt(s, kind, outcome \\ :none)

  defp receipt(%{receipts: %{state: :recording} = r} = s, kind, outcome) do
    if kind == :terminal and r.terminal do
      s
    else
      publish_receipt(s, r, kind, outcome)
    end
  end

  defp receipt(s, _kind, _outcome), do: s

  # One synchronous, non-waiting publication. Every normal result (ok,
  # dropped, rejected) keeps the advanced writer, so a lost record leaves a
  # sequence gap and is declared by later records. Anything raised disables
  # this request's evidence for good: no later record, in particular no
  # terminal, so the receipt cannot read complete. Never affects the request.
  defp publish_receipt(s, r, kind, outcome) do
    {tenant, request} = r.writer.key
    attempt? = kind in [:dispatch, :commit]

    exposure =
      cond do
        attempt? -> :possible
        kind == :ingress -> :none
        r.permitted -> :possible
        true -> :none
      end

    event = %{
      tenant_id: tenant,
      request_id: request,
      route_rev: r.route_rev,
      attempt: if(attempt?, do: 1, else: 0),
      deployment_id: if(attempt?, do: r.deployment_id, else: :none),
      at_ms: System.monotonic_time(:millisecond),
      kind: kind,
      outcome: outcome,
      exposure: exposure
    }

    r = if kind == :terminal, do: %{r | terminal: true}, else: r

    {r, result} =
      try do
        {result, w} = Telelem.Receipts.Writer.record(r.writer, event)
        {%{r | writer: w}, receipt_result(result)}
      catch
        _, _ -> {%{r | writer: nil, state: :disabled}, :fault}
      end

    trace(%{s | receipts: r}, {:receipt, kind, result})
  end

  # Fixed terms only; nothing from the event or the store. Any other result
  # raises inside publish_receipt/4's try and disables evidence as a fault.
  defp receipt_result(:ok), do: :ok
  defp receipt_result({:dropped, reason}) when reason in [:overflow, :store_down], do: :dropped
  defp receipt_result({:rejected, _}), do: :rejected

  defp with_receipts(map, %{receipts: nil}), do: map
  defp with_receipts(map, %{receipts: r}), do: Map.put(map, :receipts, r.state)

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

      s.grant != nil and s.grant.phase != :authorized ->
        # Unreachable through ready/2; kept so no path can skip the grant.
        {:error, :not_permitted}

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
