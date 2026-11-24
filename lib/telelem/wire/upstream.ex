defmodule Telelem.Wire.Upstream do
  @moduledoc """
  One upstream attempt for `Telelem.Handoff` over a real Mint TLS connection
  (TEL-005). One process owns the Mint connection and its TLS socket for its
  whole life; nothing else reads, writes or closes it.

  ## Reader: credit-driven, one delivery at a time

  (Bounds: `high_water` decides whether to arm. `max_event` and `hard_cap` are
  abort thresholds checked *after* a delivery has been appended and parsed.
  They are not a ceiling on memory already held: transiently the attempt
  holds up to its previous retained bytes plus one delivery, whose size the
  gateway does not control; see `Telelem.Wire.Limits`.)

  Mint runs in `:passive` mode, so `Mint.HTTP.stream/2` never re-arms the
  socket. The attempt itself arms it with `:ssl.setopts(sock, active: :once)`
  **only** while its retained bytes are below `Limits.high_water`, and it does
  so from a `receive` loop. So:

    * it is never blocked in `recv` and sees `{:cancel, _}`, credit and the
      coordinator's `:DOWN` as soon as they arrive;
    * at most one TLS delivery is in flight to its mailbox;
    * when it stops arming, it stops reading. TCP flow control then holds
      the upstream sender (outside the gateway's measurement).

  Downstream, it calls `Handoff.transfer/2` (one whole SSE event at a time)
  only while it holds byte credit. It starts with `Limits.window` and the
  requester returns credit with `{:telelem_wire_credit, handoff, n}` after a
  downstream write of `n` bytes succeeded.

  ## Dispatch boundary

  After `connect` succeeds the attempt calls `Handoff.begin_dispatch/2` and
  writes the request only on `:ok`. A refused `connect` is therefore provably
  pre-send (`charge: :none`); anything after the grant is `:possible`.

  ## Truthful termination (narrow M1 grammar, see `Telelem.Wire.SSE`)

  The pinned SDK ends iteration *cleanly* at the first event whose data starts
  with `[DONE]`, whatever follows on the wire. Whoever forwards that event
  therefore declares success to the client. So:

    * Only complete SSE events are forwarded, byte for byte. A partial frame
      left when the stream ends or breaks is discarded and counted.
    * The exact `data: [DONE]` event is **held**, not forwarded, until Mint
      reports the HTTP body complete (`{:done, ref}`: the chunked terminator
      arrived). It is forwarded, followed by `Handoff.finish/2`, only if:
      the attempt was released, every earlier event was transferred, a
      `finish_reason` in `#{inspect(["stop", "tool_calls"])}` was seen (and no other), no
      error event and no protocol violation. Otherwise the held `[DONE]` is
      dropped and the attempt ends without `finish`: the coordinator reports
      `incomplete` (after commit) or `failed`. The upstream's own finish
      chunk is forwarded unchanged when it arrives. The gateway never
      synthesizes or rewrites a terminal.
    * A `[DONE]`-prefixed but non-exact sentinel, any event outside the M1
      grammar, or any non-comment event after `[DONE]` is a protocol
      violation. It is not forwarded, reading stops, and earlier complete
      events are still flushed.
    * A top-level error event stops reading. If the attempt already holds
      content it is released and the error event is forwarded (the SDK raises
      `APIError`), then it ends `incomplete`. An error before any content
      fails the attempt before commit and nothing is forwarded.
    * A clean HTTP end without `[DONE]`, a reset or an overflow ends the
      attempt without `finish`.
    * No retry, no reconnect, no fallback inside an attempt.

  ## Classification for the controller

  The attempt sends `{:telelem_wire, self(), id, event}` to its controller:

    * `{:connect_failed, :refused, reason}`: `:econnrefused` from `connect`,
      before any request byte was written. The only failure a controller may
      treat as "provably not dispatched, safe to fail over".
    * `{:connect_failed, :ambiguous, reason}`: any other connect/TLS failure.
      Still no request bytes were written, but it is not a refusal (timeout,
      untrusted certificate, ...); the controller must not fail over.
    * `{:ended, reason, stats}`: final, after the socket is closed and
      `Handoff.cleaned/3` was acknowledged.

  A `{:telelem_wire_probe, from, ref}` message is answered with
  `{ref, snapshot}`: a live view of retained bytes, credit and arming, for
  evidence.
  """

  alias Telelem.Handoff
  alias Telelem.Wire.{Limits, Recorder, SSE}

  @tag :telelem_handoff

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :id)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @doc """
  Starts an attempt. Options: `:handoff`, `:id`, `:controller` (PID),
  `:lane` (`%{name:, host:, port:, cacerts: | cacertfile:}`), `:path`,
  `:body`, `:limits` (`Telelem.Wire.Limits`), `:recorder`, `:rid`.
  """
  def start_link(opts), do: {:ok, :proc_lib.spawn_link(__MODULE__, :init, [opts])}

  @doc false
  def init(opts) do
    %Limits{} = limits = Keyword.fetch!(opts, :limits)

    s = %{
      h: Keyword.fetch!(opts, :handoff),
      id: Keyword.fetch!(opts, :id),
      ctl: Keyword.fetch!(opts, :controller),
      lane: Keyword.fetch!(opts, :lane),
      path: Keyword.get(opts, :path, "/v1/chat/completions"),
      body: Keyword.fetch!(opts, :body),
      limits: limits,
      rec: Keyword.get(opts, :recorder),
      rid: Keyword.get(opts, :rid),
      hmon: nil,
      conn: nil,
      ref: nil,
      sock: nil,
      armed: false,
      dispatched: false,
      status: nil,
      phase: :awaiting_status,
      role: :candidate,
      err_body: "",
      rest: "",
      pending: :queue.new(),
      pending_bytes: 0,
      credit: limits.window,
      ended: nil,
      saw_done: false,
      held_done: nil,
      terminal: nil,
      paused: nil,
      starved: false,
      stats: %{
        deliveries: 0,
        delivery_bytes: 0,
        max_delivery: 0,
        arms: 0,
        peak_retained: 0,
        events_in: 0,
        events_forwarded: 0,
        bytes_forwarded: 0,
        comments_discarded: 0,
        partial_discarded: 0,
        events_dropped_after_end: 0,
        pauses: 0,
        credit_waits: 0
      }
    }

    case Handoff.attach(s.h, s.id) do
      {:ok, hmon} ->
        connect(%{s | hmon: hmon})

      other ->
        note(s, {:attach_refused, other})
        exit(:normal)
    end
  end

  # ── connect / dispatch ──────────────────────────────────────────────────

  defp connect(s) do
    %{host: host, port: port} = s.lane
    note(s, {:connect_attempt, s.lane.name, host, port})

    trust =
      cond do
        s.lane[:cacerts] -> [cacerts: s.lane.cacerts]
        s.lane[:cacertfile] -> [cacertfile: s.lane.cacertfile]
      end

    opts = [
      mode: :passive,
      protocols: [:http1],
      transport_opts:
        trust ++
          [
            verify: :verify_peer,
            versions: [:"tlsv1.3", :"tlsv1.2"],
            timeout: s.limits.connect_timeout_ms,
            recbuf: s.limits.upstream_recbuf,
            # user-level read size of the TCP driver: caps one TCP read, so
            # (measured) one TLS delivery; hard_cap still enforces the bound
            buffer: s.limits.upstream_read_buffer
          ]
    ]

    case Mint.HTTP.connect(:https, host, port, opts) do
      {:ok, conn} ->
        sock = Mint.HTTP.get_socket(conn)
        {:ok, info} = :ssl.connection_information(sock, [:protocol, :selected_cipher_suite])
        {:ok, {_, local_port}} = :ssl.sockname(sock)
        note(s, {:connected, s.lane.name, %{tls: info[:protocol], local_port: local_port}})
        dispatch(%{s | conn: conn, sock: sock})

      {:error, %Mint.TransportError{reason: reason}} ->
        connect_failed(s, reason)

      {:error, other} ->
        connect_failed(s, other)
    end
  end

  defp connect_failed(s, reason) do
    class = if reason == :econnrefused, do: :refused, else: :ambiguous
    note(s, {:connect_failed, s.lane.name, class, reason})
    # No request byte was written: the attempt's own claim is :none.
    _ = Handoff.cleaned(s.h, s.id, :none)
    send(s.ctl, {:telelem_wire, self(), s.id, {:connect_failed, class, reason}})
    exit(:normal)
  end

  defp dispatch(s) do
    # The may-have-dispatched boundary, granted by the coordinator immediately
    # before the first request byte. A cancel that arrived while connecting (in
    # the mailbox or already recorded by the coordinator) forbids dispatch.
    receive do
      {@tag, h, {:cancel, reason}} when h == s.h -> stop(s, {:cancelled_before_dispatch, reason})
    after
      0 -> :ok
    end

    case Handoff.begin_dispatch(s.h, s.id) do
      :ok -> :ok
      {:cancel, reason} -> stop(s, {:cancelled_before_dispatch, reason})
      {:error, reason} -> stop(s, {:dispatch_refused, reason})
    end

    headers = [
      {"content-type", "application/json"},
      {"accept", "text/event-stream"},
      {"user-agent", "telelem-wire-gateway/0 (TEL-005 test gateway)"}
    ]

    case Mint.HTTP.request(s.conn, "POST", s.path, headers, s.body) do
      {:ok, conn, ref} ->
        note(s, {:dispatched, s.lane.name, byte_size(s.body)})
        loop(%{s | conn: conn, ref: ref, dispatched: true})

      {:error, conn, reason} ->
        # Bytes may or may not have left: possible arrival, never fail over.
        stop(%{s | conn: conn, dispatched: true}, {:dispatch_error, err(reason)})
    end
  end

  # ── main loop ───────────────────────────────────────────────────────────

  defp loop(s) do
    s = s |> forward() |> maybe_end() |> maybe_arm()
    sock = s.sock
    h = s.h
    hmon = s.hmon

    receive do
      {:ssl, ^sock, data} = msg ->
        st = s.stats
        n = byte_size(data)

        st = %{
          st
          | deliveries: st.deliveries + 1,
            delivery_bytes: st.delivery_bytes + n,
            max_delivery: max(st.max_delivery, n)
        }

        on_socket(%{s | stats: st, armed: false}, msg)

      {:ssl_closed, ^sock} = msg ->
        on_socket(%{s | armed: false}, msg)

      {:ssl_error, ^sock, _} = msg ->
        on_socket(%{s | armed: false}, msg)

      {@tag, ^h, {:cancel, reason}} ->
        stop(s, {:cancelled, reason})

      {:telelem_wire_credit, ^h, n} when is_integer(n) and n > 0 ->
        loop(%{s | credit: s.credit + n})

      {:DOWN, ^hmon, :process, _, reason} ->
        stop(s, {:handoff_down, reason})

      {:telelem_wire_probe, from, ref} ->
        send(from, {ref, snapshot(s)})
        loop(s)
    after
      idle_timeout(s) -> stop(s, {:upstream_timeout, s.limits.response_timeout_ms})
    end
  end

  # Only time out while waiting for the upstream; waiting for credit is the
  # downstream's business (its send timeout ends it).
  defp idle_timeout(%{armed: true} = s), do: s.limits.response_timeout_ms
  defp idle_timeout(_), do: :infinity

  defp on_socket(s, msg) do
    case Mint.HTTP.stream(s.conn, msg) do
      {:ok, conn, responses} ->
        s = %{s | conn: conn} |> handle(responses) |> check_bounds()
        s |> maybe_ready() |> loop()

      {:error, conn, error, responses} ->
        s = %{s | conn: conn} |> handle(responses) |> check_bounds() |> maybe_ready()
        broken(s, err(error))

      :unknown ->
        loop(s)
    end
  end

  # ── responses ───────────────────────────────────────────────────────────

  defp handle(s, responses), do: Enum.reduce(responses, s, &response/2)

  defp response({:status, ref, code}, %{ref: ref} = s), do: %{s | status: code}

  defp response({:headers, ref, hs}, %{ref: ref, phase: :awaiting_status} = s) do
    ct = for({"content-type", v} <- hs, do: v) |> List.first()
    note(s, {:upstream_headers, s.status, ct})

    if s.status == 200 and is_binary(ct) and String.starts_with?(ct, "text/event-stream"),
      do: %{s | phase: :streaming},
      else: %{s | phase: :error_body}
  end

  defp response({:headers, ref, _trailers}, %{ref: ref} = s), do: s

  defp response({:data, ref, bin}, %{ref: ref, phase: :streaming} = s) do
    case SSE.feed(s.rest, bin, s.limits.max_event) do
      {:ok, events, rest} -> Enum.reduce(events, %{s | rest: rest}, &enqueue/2)
      {:overflow, n} -> throw_overflow(s, {:max_event, n})
    end
  end

  defp response({:data, ref, bin}, %{ref: ref} = s) do
    keep = binary_part(bin, 0, min(byte_size(bin), max(0, 4096 - byte_size(s.err_body))))
    %{s | err_body: s.err_body <> keep}
  end

  # A violation or error event already ended the read side; keep that reason.
  defp response({:done, ref}, %{ref: ref, ended: nil} = s), do: %{s | ended: :done}
  defp response(_other, s), do: s

  @supported_finish ["stop", "tool_calls"]

  defp enqueue({kind, ev}, s) do
    s = %{s | stats: %{s.stats | events_in: s.stats.events_in + 1}}

    cond do
      s.ended != nil ->
        # Read side already ended (violation / error event): never forwarded.
        bump(s, :events_dropped_after_end)

      kind == :comment and (s.role == :candidate or s.held_done != nil) ->
        # Pre-election and post-[DONE] heartbeats are discardable.
        bump(s, :comments_discarded)

      s.held_done != nil ->
        violation(s, {:event_after_done, kind})

      kind == :done ->
        note(s, {:done_held, %{terminal: s.terminal}})
        %{s | held_done: ev}

      kind in [:bad_sentinel, :unsupported] ->
        violation(s, {kind, byte_size(ev)})

      kind == :error_event ->
        note(s, {:upstream_error_event, byte_size(ev)})
        %{push(s, kind, ev) | ended: {:error_event, error_message(ev)}}

      kind == :semantic ->
        s |> track_finish(SSE.finish_reasons(ev)) |> push(kind, ev)

      true ->
        push(s, kind, ev)
    end
  end

  defp push(s, kind, ev) do
    %{
      s
      | pending: :queue.in({kind, ev}, s.pending),
        pending_bytes: s.pending_bytes + byte_size(ev)
    }
  end

  defp bump(s, key), do: put_in(s.stats[key], s.stats[key] + 1)

  defp violation(s, what) do
    note(s, {:protocol_violation, what})
    %{s | ended: {:violation, what}}
  end

  defp track_finish(s, []), do: s

  defp track_finish(s, reasons) do
    case {s.terminal, Enum.reject(reasons, &(&1 in @supported_finish))} do
      {{:bad, _}, _} -> s
      {_, []} -> %{s | terminal: {:ok, hd(reasons)}}
      {_, [bad | _]} -> %{s | terminal: {:bad, bad}}
    end
  end

  defp error_message(ev) do
    ev
    |> String.replace_prefix("data: ", "")
    |> String.trim()
    |> JSON.decode()
    |> case do
      {:ok, %{"error" => %{"message" => m}}} when is_binary(m) -> String.slice(m, 0, 200)
      _ -> :unparsed
    end
  end

  defp retained(s) do
    held = if is_binary(s.held_done), do: byte_size(s.held_done), else: 0
    mint_buffer(s.conn) + byte_size(s.rest) + s.pending_bytes + held
  end

  defp mint_buffer(%{buffer: b}) when is_binary(b), do: byte_size(b)
  defp mint_buffer(_), do: 0

  defp check_bounds(s) do
    r = retained(s)
    s = put_in(s.stats.peak_retained, max(s.stats.peak_retained, r))
    if r > s.limits.hard_cap, do: throw_overflow(s, {:hard_cap, r}), else: s
  end

  defp throw_overflow(s, what) do
    note(s, {:overflow, what, :abort})
    stop(s, {:overflow, what})
  end

  # ── election ────────────────────────────────────────────────────────────

  defp maybe_ready(%{role: :candidate} = s) do
    if Enum.any?(:queue.to_list(s.pending), fn {k, _} -> k == :semantic end) do
      case Handoff.ready(s.h, s.id) do
        :release ->
          note(s, {:released, s.lane.name})
          %{s | role: :winner}

        {:cancel, reason} ->
          stop(s, {:lost, reason})

        {:error, reason} ->
          stop(s, {:ready_error, reason})
      end
    else
      s
    end
  end

  defp maybe_ready(s), do: s

  # ── downstream transfer (credit) ────────────────────────────────────────

  defp forward(%{role: :winner} = s) do
    case :queue.peek(s.pending) do
      {:value, {kind, ev}} when byte_size(ev) <= s.credit ->
        case Handoff.transfer(s.h, ev) do
          :ok ->
            {_, q} = :queue.out(s.pending)
            st = s.stats

            s = %{
              s
              | pending: q,
                pending_bytes: s.pending_bytes - byte_size(ev),
                credit: s.credit - byte_size(ev),
                saw_done: s.saw_done or kind == :done,
                starved: false,
                stats: %{
                  st
                  | events_forwarded: st.events_forwarded + 1,
                    bytes_forwarded: st.bytes_forwarded + byte_size(ev)
                }
            }

            forward(s)

          {:error, reason} ->
            stop(s, {:transfer_refused, reason})
        end

      {:value, {_, ev}} ->
        if s.starved do
          s
        else
          note(s, {:credit_wait, %{credit: s.credit, head: byte_size(ev), retained: retained(s)}})
          %{s | starved: true, stats: %{s.stats | credit_waits: s.stats.credit_waits + 1}}
        end

      :empty ->
        s
    end
  end

  defp forward(s), do: s

  defp maybe_end(%{ended: nil} = s), do: s

  defp maybe_end(%{role: :winner} = s) do
    {:ok, reason} = if s.saw_done, do: s.terminal, else: {:ok, nil}

    cond do
      # Flush complete events already received (credit permitting) first.
      not :queue.is_empty(s.pending) ->
        s

      # The held [DONE] has been transferred: complete HTTP evidence is in.
      s.saw_done ->
        case Handoff.finish(s.h, {:upstream_done, reason}) do
          :ok -> stop(s, :finished)
          {:error, r} -> stop(s, {:finish_refused, r})
        end

      success?(s) ->
        note(s, {:done_released, %{terminal: s.terminal}})
        %{push(s, :done, s.held_done) | held_done: :released} |> forward() |> maybe_end()

      # End without finish: the coordinator reports incomplete.
      true ->
        stop(s, end_reason(s))
    end
  end

  defp maybe_end(%{phase: :error_body, ended: :done} = s),
    do: stop(s, {:upstream_status, s.status, s.err_body})

  defp maybe_end(s), do: stop(s, end_reason(s))

  defp success?(s),
    do: s.ended == :done and is_binary(s.held_done) and match?({:ok, _}, s.terminal)

  defp end_reason(%{ended: :done, held_done: nil}), do: :upstream_ended_without_done
  defp end_reason(%{ended: :done, terminal: nil}), do: {:done_without_finish_reason, :no_finish}
  defp end_reason(%{ended: :done, terminal: {:bad, r}}), do: {:unsupported_finish_reason, r}
  defp end_reason(%{ended: :done, role: :candidate}), do: :no_semantic_content

  defp end_reason(%{ended: {:broken, r}} = s),
    do: {:upstream_broken, r, %{done_held: is_binary(s.held_done)}}

  defp end_reason(%{ended: {:violation, what}}), do: {:protocol_violation, what}
  defp end_reason(%{ended: {:error_event, m}}), do: {:upstream_error_event, m}

  defp broken(s, reason) do
    note(
      s,
      {:upstream_error, reason, %{partial_frame: byte_size(s.rest), pending: s.pending_bytes}}
    )

    s = put_in(s.stats.partial_discarded, s.stats.partial_discarded + byte_size(s.rest))
    # An earlier end (complete body, violation, error event) keeps its reason.
    loop(%{s | rest: "", ended: s.ended || {:broken, reason}})
  end

  # ── arming ──────────────────────────────────────────────────────────────

  defp maybe_arm(%{armed: false, ended: nil, sock: sock} = s) when sock != nil do
    r = retained(s)

    if r < s.limits.high_water do
      case :ssl.setopts(sock, active: :once) do
        :ok ->
          s = if s.paused, do: note_resume(s), else: s
          %{s | armed: true, stats: %{s.stats | arms: s.stats.arms + 1}}

        {:error, reason} ->
          broken(s, {:setopts, reason})
      end
    else
      if s.paused do
        s
      else
        note(s, {:paused, %{retained: r, credit: s.credit, pending: s.pending_bytes}})
        %{s | paused: now(), stats: %{s.stats | pauses: s.stats.pauses + 1}}
      end
    end
  end

  defp maybe_arm(s), do: s

  defp note_resume(s) do
    note(s, {:resumed, %{paused_us: now() - s.paused, retained: retained(s)}})
    %{s | paused: nil}
  end

  # ── end ─────────────────────────────────────────────────────────────────

  defp stop(s, reason) do
    if s.conn, do: Mint.HTTP.close(s.conn)

    s =
      if s.rest != "",
        do: put_in(s.stats.partial_discarded, s.stats.partial_discarded + byte_size(s.rest)),
        else: s

    note(s, {:socket_closed, s.lane.name})
    charge = if s.dispatched, do: :possible, else: :none
    ack = Handoff.cleaned(s.h, s.id, charge)

    stats =
      Map.merge(s.stats, %{claim: charge, cleaned_ack: ack, unsent_pending: s.pending_bytes})

    note(s, {:ended, reason, stats})
    send(s.ctl, {:telelem_wire, self(), s.id, {:ended, reason, stats}})
    exit(:normal)
  end

  defp snapshot(s) do
    %{
      role: s.role,
      armed: s.armed,
      paused: s.paused != nil,
      retained: if(s.conn, do: retained(s), else: 0),
      pending: s.pending_bytes,
      partial: byte_size(s.rest),
      credit: s.credit,
      in_flight: s.limits.window - s.credit,
      ssl_process_memory: ssl_memory(s.sock),
      stats: s.stats
    }
  end

  # Sampled memory (bytes) of the OTP ssl processes behind this socket. Evidence
  # only: their internal buffers are not part of the declared application bound.
  defp ssl_memory(sock) do
    for p <- pids_in(sock), into: %{} do
      {inspect(p), with({:memory, m} <- Process.info(p, :memory), do: m)}
    end
  end

  defp pids_in(p) when is_pid(p), do: [p]
  defp pids_in(t) when is_tuple(t), do: t |> Tuple.to_list() |> Enum.flat_map(&pids_in/1)
  defp pids_in(l) when is_list(l), do: Enum.flat_map(l, &pids_in/1)
  defp pids_in(_), do: []

  defp err(%{reason: r}), do: r
  defp err(other), do: other

  defp note(s, ev) do
    if s.rec, do: Recorder.note(s.rec, s.rid, {:upstream, s.id, ev})
    s
  end

  defp now, do: System.monotonic_time(:microsecond)
end
