defmodule Telelem.Serve.Requester.Writer do
  @moduledoc """
  The downstream writer `Telelem.Serve.Requester` drives, in the requester
  process. An adapter (for example over `Plug.Conn`) implements it; this
  slice ships none.

    * `header/1`: write the response status and headers (for example
      `send_chunked(conn, 200)`). Called at most once, immediately before the
      first data chunk or the terminal marker.
    * `chunk/2`: write exactly the given non-empty binary as one downstream
      write (for example `Plug.Conn.chunk/2`). Called for each committed data
      chunk and once for the terminal marker. Never called with an empty
      binary (an empty chunk is the chunked-transfer terminator).

  Only an exact `{:ok, state}` is a local write success. `{:error, _}` is a
  local failure; any other return, raise, throw or exit is **unconfirmed**.
  Every failed or unconfirmed call is treated as possibly started: the
  requester writes nothing more and never replays it. Neither callback ends
  the response; ending it (terminator or not) is the caller's decision from
  the disposition `Telelem.Serve.Requester` returns.
  """

  @callback header(state :: term()) :: {:ok, term()} | {:error, term()}
  @callback chunk(state :: term(), bytes :: binary()) :: {:ok, term()} | {:error, term()}
end

defmodule Telelem.Serve.Requester do
  @moduledoc """
  W2d: listener-free requester side of one primary request. It runs in the
  **downstream response owner's process** (the process that will own the
  `Plug.Conn`), calls `Telelem.Serve.Primary.start/4` **once** and then runs
  the requester half of the `Telelem.Handoff` protocol against exactly the
  returned coordinator, through a `Telelem.Serve.Requester.Writer`. No
  listener, socket, HTTPS, Plug or Bandit code is here.

      run(cfg, auth_input, body_reader, {writer, state}, opts \\\\ []) -> result
      serve(%Primary.Handle{}, {writer, state}, opts \\\\ []) -> result

  `run/5` validates its own arguments first (nothing is started on a bad
  argument), then calls `Primary.start(cfg, auth_input, body_reader,
  controller: c, observer: o)` with both report destinations stated
  explicitly (`:controller` and `:observer`, default `self()`), and on
  `{:ok, handle}` calls `serve/3`. `serve/3` is public so the protocol can
  be driven against a real `Telelem.Handoff` without `Primary`.

  ## Results (dispositions)

      {:no_write, {:invalid, :writer | :opts | :handle}}   # nothing started or sent
      {:no_write, {:start, start_why}, start_evidence, %{start: Primary.reason()}}
      {:no_write, why, evidence, private}   # the writer was never called
      {:aborted, why, evidence, private}    # the writer was called; do not complete the response
      {:complete, evidence, private}        # see below

  Every disposition separates two things:

    * **Log-safe** parts (`why`, `start_why`, `evidence`, `start_evidence`):
      only closed atoms, booleans, integers and the gateway-generated
      `request_id` (a binary, else `nil`). Handoff outcomes, terminal-write
      replies and Primary start reasons are projected to fixed categories
      (below); an unknown detail becomes `:other`. No writer state, upstream
      terminal, attempt id, pid, raw reason, marker or chunk is copied.
    * **Private** parts (the last element): opaque, for the caller only,
      never meant for logs. `private` is `%{writer: state, handoff: pid,
      elected: attempt_id | nil, outcome: Handoff.outcome() | :not_observed,
      report: :not_sent | {:sent, reply}}` with the raw values; `writer` is
      the last state a writer callback returned with `{:ok, _}`, else the
      initial one. For a start error it is `%{start: reason}` with exactly
      Primary's typed error (its `failure` map, pid included).

  Meanings:

    * `{:start, start_why}`: `Primary.start/4` returned `{:error, reason}`.
      The requester sends nothing more: no cancel (Primary already submitted
      its one cancel where it applies), no error response (this slice has no
      standalone error writer), and it consumes nothing (Primary's and the
      coordinator's messages stay queued). `start_why` is `reason` with its
      `failure` map removed and every detail projected: `:invalid_config`,
      `{:invalid, :opts}`, `{:ingress, atom}` (`{:target_denied, _}` and
      `{:deadline_exceeded, _}` keep only their tag), `{:preflight, atom}`,
      `{:deadline_exceeded, :before_coordinator}`, `{:coordinator,
      {:not_sent, :expired | :start_refused | :start_unconfirmed |
      :handoff_down | :other}}`, `{:coordinator, {:refused, atom}}` (Handoff's
      activation refusals, else `:other`), `{:coordinator, :unconfirmed |
      :unrecognized}`, `{:launch, Primary.launch_why()}`, else `:other`.
      `start_evidence` is `%{request_id, activation, go, cancel}` from the
      `failure` map (closed values, unknown ones `:other`), or `%{}` when
      the reason has none.
    * `:no_write` (from `serve/3`): no writer callback was invoked by this
      call. It does not say whether, or what, the caller may still write.
    * `:aborted`: at least one writer callback was invoked (it may have
      written bytes, including a partial header or chunk) and the response
      must be ended without a success terminator. Nothing is replayed and no
      second response is attempted.
    * `:complete`: the marker write returned `{:ok, _}`,
      `Handoff.terminal_written(h, :ok)` was submitted once, and
      `{:done, _}` from exactly `h` was observed. Handoff sends `{:done, _}`
      only for a local `:ok` it processed before `D`, so it is accepted
      whenever it is observed (also after `D`, and whatever
      `terminal_written/2` returned, e.g. a lost or late reply).

  No disposition says anything about chunked framing, the peer's receipt of
  any byte, cleanup of the coordinator, guard, owner or witness, or whether
  the connection may be reused. `:complete` only permits the caller to end
  the response with its success terminator; ending it is the caller's.

  `evidence` is a map: `request_id`, `deadline_ms` (`D`), `elected` (an
  `{:elected, _}` was observed), `header`
  (`:not_called | :ok | :failed | :unconfirmed`), `chunks` (data chunks
  written with `{:ok, _}`), `credit` (bytes credited), `terminal`
  (`:none | :mismatch | :deadline | :header_failed | :header_unconfirmed |
  :written | :failed | :unconfirmed`), `written_late` (the marker write
  returned at or after `D`), `report` (`:not_sent`, or `{:sent, r}` for the
  one `terminal_written/2`, `r` one of `:ok`, `{:error, :ended | :deadline
  | :not_requester | :no_terminal | :handoff_down | :unconfirmed}` or
  `:other`), `outcome` and `cancel` (`:submitted | :not_submitted`).
  Writer error reasons are not kept or forwarded.

  `outcome` (and `why = {:outcome, outcome}`) is `:not_observed`, `:done`
  (the upstream terminal is only in `private`), or `{:incomplete |
  :failed, category}`: a Handoff atom reason (`:terminal_deadline`,
  `:dispatch_deadline`, `:no_winner`, `:winner_closed`,
  `:admission_refused`, `:launch_failed`) or one of this module's cancel
  reasons; `{:terminal_unconfirmed, :deadline}`; `{:downstream_write, r}`
  with `r` one of this module's terminal errors (else `:other`); the bare
  tag of `{:winner_down | :requester_down | :handoff_down |
  :authority_down, _}`; else `:other`.

  ## Mailbox discipline

  `serve/3` monitors `h` itself (its own monitor ref) and receives only
  `{:telelem_handoff, ^h, msg}` for the shapes below and that exact
  monitor's `:DOWN`. Everything else stays queued and untouched: Plug's
  sent sentinel, OwnerGuard witness and guard reports, owner
  `{:telelem_wire, ...}` messages, other coordinators' messages, and `h`'s
  own `{:closed, _}` (never awaited). On return its monitor is removed
  (flushing only its own `:DOWN`).

  ## Protocol (one request, `D = handle.deadline_ms`)

    * `{:elected, id}`: informational; recorded, nothing written.
    * `{:data, chunk}`: `chunk` must be a non-empty binary (else no write,
      cancel, `{:data, :invalid}`). Before the header (first write only) a
      clock check `now < D`, then `header/1`; then a separate clock check
      `now < D`, then `chunk/2`. Only on its `{:ok, _}` is exactly
      `{:telelem_wire_credit, h, byte_size(chunk)}` sent to `handle.owner`.
      No credit for the header, the marker, a failed or unconfirmed write,
      and none initially.
    * `{:terminal, marker, deadline}`: valid only for a non-empty binary
      marker and `deadline == D`; otherwise nothing is written
      (`{:terminal, :mismatch}`). Then the header (if not yet written) behind
      its own clock check, then a separate clock check `now < D`, then one
      `chunk/2` of exactly `marker`. Then exactly one
      `Handoff.terminal_written/2` with the actual local result: `:ok` for
      `{:ok, _}` (also when the write returned at or after `D`; Handoff then
      decides, and cannot give `{:done, _}`), else a fixed
      `{:error, :deadline | :terminal_mismatch | :header_failed |
      :header_unconfirmed | :write_failed | :write_unconfirmed}`. No cancel
      on this path.
    * `{:done | :incomplete | :failed, _}` before a terminal: the request
      ended; nothing more is written, no cancel.
    * The exact `:DOWN` before an outcome: `:handoff_down`, no cancel.

  The stream wait is bounded by the absolute `D + stream_grace_ms`
  (default 1_000): on expiry, `:receive_timeout`. Every `D` that
  `Telelem.Handoff.deadline_in_range?/1` accepts is served: no single
  `receive ... after` exceeds `2^32 - 1` ms (a farther bound is waited for in
  several clamped waits), and `:receive_timeout` is taken only after a clock
  read at or after the absolute bound (an earlier wake-up waits again).

  **The one cancel.** On a local header/data failure, an invalid chunk, a
  failed clock check before a data write, or the stream wait expiring, the
  requester submits exactly one `Handoff.submit_cancel(h, reason)` (one
  request, abandoned at once, never awaited) and returns. Reasons are fixed
  atoms (`:deadline`, `:header_failed`, `:header_unconfirmed`,
  `:write_failed`, `:write_unconfirmed`, `:invalid_chunk`,
  `:receive_timeout`). `cancel: :submitted` means only that it was sent.

  **After the terminal report** the outcome is observed with one explicit
  bounded wait of `observe_ms` (default 1_000) from the report's return:
  the first of `h`'s outcome message or the exact `:DOWN`. `{:closed, _}`
  is never awaited. `terminal_written/2` itself is Handoff's: a 5 s
  `GenServer.call` that may block that long, and on its timeout kills the
  coordinator and waits for its `:DOWN` before returning
  `{:error, :unconfirmed}`. `{:error, :unconfirmed}` from another exit
  is not proof that the coordinator is dead, and a reply does not prove the
  outcome; only the observed outcome message decides. Handoff sends the
  outcome message before it replies, and work in between (its `:on_event`
  callback, for example) can block or end the coordinator, so the reply can
  be lost (`{:error, :unconfirmed}`) after `{:done, _}` was sent: that
  `{:done, _}` is still observed and decides `:complete`.

  Bounds are monotonic clock checks and receive timeouts, not real-time
  guarantees: a writer call already started cannot be interrupted, so a
  blocking writer delays every bound here, and no overall wall-clock bound
  is claimed.

  ## Not covered

  A writer adapter, the response ending (terminator/close), any error
  response, the listener, real sockets, dialing, and cleanup confirmation.
  """

  alias Telelem.Handoff
  alias Telelem.Serve.Primary
  alias Telelem.Serve.Primary.Handle

  @tag :telelem_handoff
  @run_opt_keys [:controller, :observer, :stream_grace_ms, :observe_ms]
  @serve_opt_keys [:stream_grace_ms, :observe_ms]
  @default_grace 1_000
  @max_grace 60_000
  # The longest timeout `receive ... after` accepts.
  @max_wait 4_294_967_295

  # Closed vocabularies for the log-safe projections.
  @cancel_reasons [
    :deadline,
    :header_failed,
    :header_unconfirmed,
    :write_failed,
    :write_unconfirmed,
    :invalid_chunk,
    :receive_timeout
  ]
  @terminal_errors [
    :deadline,
    :terminal_mismatch,
    :header_failed,
    :header_unconfirmed,
    :write_failed,
    :write_unconfirmed
  ]
  @outcome_atoms [
                   :terminal_deadline,
                   :dispatch_deadline,
                   :no_winner,
                   :winner_closed,
                   :admission_refused,
                   :launch_failed
                 ] ++ @cancel_reasons
  @outcome_tags [:winner_down, :requester_down, :handoff_down, :authority_down]
  @reply_errors [:ended, :deadline, :not_requester, :no_terminal, :handoff_down, :unconfirmed]
  @ingress_atoms [
    :invalid_config,
    :clock_unavailable,
    :unauthenticated,
    :mode_mismatch,
    :permission_denied,
    :no_route,
    :store_unavailable,
    :store_integrity,
    :unsupported_attempt_policy,
    :no_eligible_deployment,
    :body_too_large,
    :body_timeout,
    :body_unavailable,
    :gateway_id_unavailable
  ]
  @preflight [:receipt_meta, :upstream, :guard]
  @not_sent [:expired, :start_unconfirmed, :handoff_down]
  @activation_refusals [
    :not_requester,
    :activation_limit_mismatch,
    :duplicate,
    :no_activation,
    :receipts_unbound,
    :activation_expired,
    :inactive
  ]
  @launch_whys [
    :deadline_exceeded,
    :expired,
    :start_failed,
    :guard_down,
    :witness_down,
    :unconfirmed
  ]

  @typedoc "Log-safe evidence: closed values only (see the module doc)."
  @type evidence :: %{
          request_id: String.t() | nil,
          deadline_ms: integer(),
          elected: boolean(),
          header: :not_called | :ok | :failed | :unconfirmed,
          chunks: non_neg_integer(),
          credit: non_neg_integer(),
          terminal: atom(),
          written_late: boolean(),
          report: :not_sent | {:sent, :ok | {:error, atom()} | :other},
          outcome: :not_observed | :done | {:incomplete | :failed, term()},
          cancel: :submitted | :not_submitted
        }

  @typedoc "Opaque caller-only handback; never log it."
  @type private :: %{
          writer: term(),
          handoff: pid(),
          elected: term(),
          outcome: Handoff.outcome() | :not_observed,
          report: :not_sent | {:sent, term()}
        }

  @type disposition ::
          {:no_write, {:invalid, :writer | :opts | :handle}}
          | {:no_write, {:start, term()}, map(), %{start: Primary.reason()}}
          | {:no_write, term(), evidence(), private()}
          | {:aborted, term(), evidence(), private()}
          | {:complete, evidence(), private()}

  @doc "One request from the calling (downstream owner) process. See the module doc."
  @spec run(Primary.Config.t(), term(), term(), {module(), term()}, keyword()) :: disposition()
  def run(cfg, auth_input, body_reader, writer, opts \\ []) do
    with :ok <- check_writer(writer),
         {:ok, o} <- options(opts, @run_opt_keys) do
      case Primary.start(cfg, auth_input, body_reader,
             controller: o.controller,
             observer: o.observer
           ) do
        {:ok, %Handle{} = handle} -> protocol(handle, writer, o)
        {:error, reason} -> start_error(reason)
      end
    end
  end

  @doc """
  The requester protocol for an already started request whose coordinator's
  requester is the calling process. See the module doc.
  """
  @spec serve(Handle.t(), {module(), term()}, keyword()) :: disposition()
  def serve(handle, writer, opts \\ []) do
    with :ok <- check_handle(handle),
         :ok <- check_writer(writer),
         {:ok, o} <- options(opts, @serve_opt_keys) do
      protocol(handle, writer, o)
    end
  end

  # ── Protocol ────────────────────────────────────────────────────────────

  defp protocol(%Handle{handoff: h, owner: owner, deadline_ms: d} = handle, {mod, ws}, o) do
    mon = Process.monitor(h)

    s = %{
      h: h,
      mon: mon,
      owner: owner,
      d: d,
      mod: mod,
      bound: d + o.stream_grace_ms,
      observe_ms: o.observe_ms,
      ev: %{
        request_id: if(is_binary(handle.request_id), do: handle.request_id),
        deadline_ms: d,
        elected: false,
        header: :not_called,
        chunks: 0,
        credit: 0,
        terminal: :none,
        written_late: false,
        report: :not_sent,
        outcome: :not_observed,
        cancel: :not_submitted
      },
      pv: %{writer: ws, handoff: h, elected: nil, outcome: :not_observed, report: :not_sent}
    }

    try do
      stream(s)
    after
      Process.demonitor(mon, [:flush])
    end
  end

  defp stream(%{h: h, mon: mon} = s) do
    receive do
      {@tag, ^h, {:elected, id}} ->
        stream(%{put_ev(s, :elected, true) | pv: %{s.pv | elected: id}})

      {@tag, ^h, {:data, chunk}} ->
        data(s, chunk)

      {@tag, ^h, {:terminal, marker, deadline}} ->
        terminal(s, marker, deadline)

      {@tag, ^h, {kind, _} = outcome} when kind in [:done, :incomplete, :failed] ->
        s = observed(s, outcome)
        ended(s, {:outcome, s.ev.outcome})

      {:DOWN, ^mon, :process, _, _} ->
        ended(s, :handoff_down)
    after
      wait(s.bound) ->
        # A clamped or early wake-up before the absolute bound waits again.
        if now() >= s.bound,
          do: cancel_end(s, :receive_timeout, :receive_timeout),
          else: stream(s)
    end
  end

  # ── Data ────────────────────────────────────────────────────────────────

  defp data(s, chunk) when is_binary(chunk) and byte_size(chunk) > 0 do
    with {:ok, s} <- data_header(s),
         :ok <- clock(s) do
      case write(s, chunk) do
        {:ok, s} ->
          n = byte_size(chunk)
          send(s.owner, {:telelem_wire_credit, s.h, n})
          stream(%{s | ev: %{s.ev | chunks: s.ev.chunks + 1, credit: s.ev.credit + n}})

        {fail, s} ->
          reason = write_failure(:write, fail)
          cancel_end(s, reason, {:data, fail})
      end
    else
      {:deadline, s} -> cancel_end(s, :deadline, {:data, :deadline})
      {:header, fail, s} -> cancel_end(s, header_failure(fail), {:header, fail})
    end
  end

  defp data(s, _chunk), do: cancel_end(s, :invalid_chunk, {:data, :invalid})

  # The header before the first data chunk, behind its own clock check.
  defp data_header(%{ev: %{header: :ok}} = s), do: {:ok, s}

  defp data_header(s) do
    case clock(s) do
      :ok -> header(s)
      {:deadline, s} -> {:deadline, s}
    end
  end

  # ── Terminal ────────────────────────────────────────────────────────────

  defp terminal(%{d: d} = s, marker, deadline)
       when is_binary(marker) and byte_size(marker) > 0 and deadline === d do
    with {:ok, s} <- terminal_header(s),
         :ok <- clock(s) do
      case write(s, marker) do
        {:ok, s} ->
          late = now() >= d
          s = %{s | ev: %{s.ev | terminal: :written, written_late: late}}
          report(s, :ok, nil)

        {fail, s} ->
          report(
            put_ev(s, :terminal, fail),
            {:error, write_failure(:write, fail)},
            {:terminal, fail}
          )
      end
    else
      {:deadline, s} ->
        report(put_ev(s, :terminal, :deadline), {:error, :deadline}, {:terminal, :deadline})

      {:header, fail, s} ->
        t = header_failure(fail)
        report(put_ev(s, :terminal, t), {:error, t}, {:header, fail})
    end
  end

  defp terminal(s, _marker, _deadline) do
    report(put_ev(s, :terminal, :mismatch), {:error, :terminal_mismatch}, {:terminal, :mismatch})
  end

  defp terminal_header(%{ev: %{header: :ok}} = s), do: {:ok, s}

  defp terminal_header(s) do
    case clock(s) do
      :ok -> header(s)
      {:deadline, s} -> {:deadline, s}
    end
  end

  # The one terminal report, then one bounded observation of the outcome.
  # `why` is nil only for a locally successful marker write.
  defp report(s, result, why) do
    reply = Handoff.terminal_written(s.h, result)

    s = %{
      put_ev(s, :report, {:sent, reply_category(reply)})
      | pv: %{s.pv | report: {:sent, reply}}
    }

    observe(s, why)
  end

  defp observe(%{h: h, mon: mon} = s, why) do
    receive do
      {@tag, ^h, {:done, _} = outcome} ->
        s = observed(s, outcome)

        if why == nil and s.ev.terminal == :written,
          do: {:complete, s.ev, s.pv},
          else: ended(s, {:outcome, s.ev.outcome})

      {@tag, ^h, {kind, _} = outcome} when kind in [:incomplete, :failed] ->
        s = observed(s, outcome)
        ended(s, why || {:outcome, s.ev.outcome})

      {:DOWN, ^mon, :process, _, _} ->
        ended(s, why || :handoff_down)
    after
      s.observe_ms -> ended(s, why || :outcome_not_observed)
    end
  end

  # ── Writer calls (contained) ────────────────────────────────────────────

  defp header(s) do
    case contained(fn -> s.mod.header(s.pv.writer) end) do
      {:ok, ws} -> {:ok, put_writer(put_ev(s, :header, :ok), ws)}
      fail -> {:header, fail, put_ev(s, :header, fail)}
    end
  end

  defp write(s, bytes) do
    case contained(fn -> s.mod.chunk(s.pv.writer, bytes) end) do
      {:ok, ws} -> {:ok, put_writer(s, ws)}
      fail -> {fail, s}
    end
  end

  # Only an exact {:ok, state} is a success; {:error, _} a local failure;
  # anything else (other shapes, raise, throw, exit) is unconfirmed.
  defp contained(fun) do
    case fun.() do
      {:ok, ws} -> {:ok, ws}
      {:error, _} -> :failed
      _ -> :unconfirmed
    end
  catch
    _kind, _reason -> :unconfirmed
  end

  defp header_failure(:failed), do: :header_failed
  defp header_failure(:unconfirmed), do: :header_unconfirmed
  defp write_failure(:write, :failed), do: :write_failed
  defp write_failure(:write, :unconfirmed), do: :write_unconfirmed

  # ── Endings ─────────────────────────────────────────────────────────────

  defp cancel_end(s, reason, why) do
    :submitted = Handoff.submit_cancel(s.h, reason)
    ended(put_ev(s, :cancel, :submitted), why)
  end

  defp ended(s, why) do
    if s.ev.header == :not_called,
      do: {:no_write, why, s.ev, s.pv},
      else: {:aborted, why, s.ev, s.pv}
  end

  defp start_error(reason) do
    {:no_write, {:start, start_why(reason)}, start_evidence(reason), %{start: reason}}
  end

  defp clock(s), do: if(now() < s.d, do: :ok, else: {:deadline, s})

  defp put_ev(s, k, v), do: %{s | ev: Map.put(s.ev, k, v)}
  defp put_writer(s, ws), do: %{s | pv: %{s.pv | writer: ws}}

  defp observed(s, outcome),
    do: %{put_ev(s, :outcome, outcome_category(outcome)) | pv: %{s.pv | outcome: outcome}}

  # One representable wait toward an absolute bound; the caller rechecks it.
  defp wait(bound), do: min(max(bound - now(), 0), @max_wait)
  defp now, do: System.monotonic_time(:millisecond)

  # ── Log-safe projections (closed vocabularies; anything else :other) ────

  defp outcome_category({:done, _}), do: :done

  defp outcome_category({kind, r}) when kind in [:incomplete, :failed],
    do: {kind, reason_category(r)}

  defp reason_category(r) when r in @outcome_atoms, do: r
  defp reason_category({:terminal_unconfirmed, :deadline} = r), do: r

  defp reason_category({:downstream_write, r}) when r in @terminal_errors,
    do: {:downstream_write, r}

  defp reason_category({:downstream_write, _}), do: {:downstream_write, :other}
  defp reason_category({tag, _}) when tag in @outcome_tags, do: tag
  defp reason_category(_), do: :other

  defp reply_category(:ok), do: :ok
  defp reply_category({:error, r}) when r in @reply_errors, do: {:error, r}
  defp reply_category(_), do: :other

  defp start_why(:invalid_config), do: :invalid_config
  defp start_why({:invalid, :opts}), do: {:invalid, :opts}
  defp start_why({:ingress, r}) when r in @ingress_atoms, do: {:ingress, r}
  defp start_why({:ingress, {:target_denied, _}}), do: {:ingress, :target_denied}
  defp start_why({:ingress, {:deadline_exceeded, _}}), do: {:ingress, :deadline_exceeded}
  defp start_why({:ingress, _}), do: {:ingress, :other}
  defp start_why({:preflight, r}) when r in @preflight, do: {:preflight, r}
  defp start_why({:preflight, _}), do: {:preflight, :other}
  defp start_why({:deadline_exceeded, :before_coordinator} = r), do: r
  defp start_why({:coordinator, c, _failure}), do: {:coordinator, coordinator_why(c)}
  defp start_why({:launch, w, _failure}) when w in @launch_whys, do: {:launch, w}
  defp start_why({:launch, _, _failure}), do: {:launch, :other}
  defp start_why(_), do: :other

  defp coordinator_why({:not_sent, n}) when n in @not_sent, do: {:not_sent, n}
  defp coordinator_why({:not_sent, {:start_refused, _}}), do: {:not_sent, :start_refused}
  defp coordinator_why({:not_sent, _}), do: {:not_sent, :other}
  defp coordinator_why({:refused, r}) when r in @activation_refusals, do: {:refused, r}
  defp coordinator_why({:refused, _}), do: {:refused, :other}
  defp coordinator_why(c) when c in [:unconfirmed, :unrecognized], do: c
  defp coordinator_why(_), do: :other

  defp start_evidence({tag, _, %{} = f}) when tag in [:coordinator, :launch] do
    %{
      request_id: if(is_binary(f[:request_id]), do: f[:request_id]),
      activation: closed(f[:activation], [:not_sent, :refused, :unconfirmed, :confirmed]),
      go: closed(f[:go], [:not_sent, :unconfirmed]),
      cancel: closed(f[:cancel], [:submitted, :not_submitted])
    }
  end

  defp start_evidence(_), do: %{}

  defp closed(v, allowed), do: if(v in allowed, do: v, else: :other)

  # ── Checks ──────────────────────────────────────────────────────────────

  defp check_handle(%Handle{handoff: h, owner: o, deadline_ms: d})
       when is_pid(h) and is_pid(o) and is_integer(d) do
    if node(h) == node() and node(o) == node() and Handoff.deadline_in_range?(d),
      do: :ok,
      else: {:no_write, {:invalid, :handle}}
  end

  defp check_handle(_), do: {:no_write, {:invalid, :handle}}

  defp check_writer({mod, _state}) when is_atom(mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :header, 1) and
         function_exported?(mod, :chunk, 2),
       do: :ok,
       else: {:no_write, {:invalid, :writer}}
  end

  defp check_writer(_), do: {:no_write, {:invalid, :writer}}

  defp options(opts, allowed) do
    me = self()

    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- allowed,
         true <- length(Enum.uniq(Keyword.keys(opts))) == length(opts),
         o = %{
           controller: Keyword.get(opts, :controller, me),
           observer: Keyword.get(opts, :observer, me),
           stream_grace_ms: Keyword.get(opts, :stream_grace_ms, @default_grace),
           observe_ms: Keyword.get(opts, :observe_ms, @default_grace)
         },
         true <- local_pid?(o.controller) and local_pid?(o.observer),
         true <- grace?(o.stream_grace_ms) and grace?(o.observe_ms) do
      {:ok, o}
    else
      _ -> {:no_write, {:invalid, :opts}}
    end
  end

  defp grace?(n), do: is_integer(n) and n >= 0 and n <= @max_grace
  defp local_pid?(p), do: is_pid(p) and node(p) == node()
end
