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
    * **Supported framing only.** A `200 text/event-stream` response is read
      only if its status line on the wire is `HTTP/1.1` and its body is
      framed by exactly `transfer-encoding: chunked` with no
      `content-length`. Mint reports `{:done, ref}` for a close-delimited (or
      length-delimited) body too; such a response, an HTTP/1.0 status line or
      any other coding is a protocol violation
      (`{:unsupported_framing, framing}`) before anything is forwarded.
    * The exact `data: [DONE]` event is **held**, not forwarded, until Mint
      reports the chunked body complete (`{:done, ref}` with chunked framing:
      the zero-length terminator chunk arrived). It is never `transfer/2`ed.
      It goes downstream only as the marker of `Handoff.finish/3`, or
      `Handoff.finish/4` with `deadline:` when the attempt was started with
      `:terminal_deadline` (terminal
      authorization; if it is refused the marker is dropped; if it is
      `{:error, :unconfirmed}` the attempt ends `:finish_unconfirmed` and
      never retries), and only if:
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

  ## Gated attempt (opt-in: `gate: [target:, deadline:]`)

  Off by default; without `:gate` (or with exactly one `gate: nil`) nothing
  below applies and every shape above is unchanged. More than one `:gate`
  entry (a leading `gate: nil` included) raises `ArgumentError` before either
  mode is chosen. It requires a coordinator started with `coordinator_grant`
  (see `Telelem.Handoff`): the attempt attaches with
  `Handoff.attach_grant/3`, which any other coordinator refuses with
  `{:error, :grant_required}` before registering it, so the attempt ends
  without dialing.

  **Interface, validated in the caller before the attempt is spawned
  (`ArgumentError`, naming only the offending field) and again in the
  attempt.** Exactly the keys `:handoff` (pid), `:id`, `:controller` (pid),
  `:body` (binary, at most `limits.max_request_body` bytes), `:limits` (a
  `Telelem.Wire.Limits` whose every numeric field is an integer in `1..max`,
  `max` being `2^32 - 1` for the `*_ms` timeouts and `2^31 - 1` for the byte
  counts and socket buffer sizes, and whose relations `Limits.new/1`
  accepts), `:gate`, and optionally `:terminal_deadline` equal to the gate's
  deadline; each once. Legacy attempts and the `Limits` defaults are not
  affected by these checks. `:lane`, `:recorder`, `:rid`, `:path` and
  any other key are refused: the destination, trust and transport options
  come only from the target and the limits, and no Recorder runs.
  `:gate` is exactly `[target: t, deadline: d]`:

    * `t` is a `Telelem.Sandbox.VerifiedTarget` that
      `VerifiedTarget.check(t, t.deployment_id)` accepts **and** whose
      `address` is IPv4 (`::1` is refused in this slice). The attempt dials
      `t.host` (proved equal to the canonical text of `t.address`, so no
      name resolution) on `t.port`, with `VerifiedTarget.tls_opts(t)` only
      (fixture CA, `verify_peer`; no OS trust store fallback).
    * `d` is the coordinator's absolute `:terminal_deadline`
      (`System.monotonic_time(:millisecond)`). The same integer is passed as
      `deadline:` to `Handoff.attach_grant/3`, `Handoff.begin_dispatch/3` and
      `Handoff.finish/4`. It may be past, but at most
      `Handoff.max_deadline_ahead_ms/0` (4_294_966_295 ms, `2^32 - 1` minus
      the coordinator's 1000 ms grace) ahead of now; a later one is refused
      before spawn, since no Handoff wait could cover `d + grace`.

  **Order.**

    1. `attach_grant(h, id, deadline: d)`. Its `:ok` comes only from a
       `coordinator_grant` coordinator, after the authority reserved the
       grant for this pid, so nothing is dialed before the reservation. A
       coordinator without `coordinator_grant` replies
       `{:error, :grant_required}` (controller:
       `{:ended, {:attach, :error, :grant_required}, stats}`).
    2. Check, without waiting: a coordinator `{:cancel, _}`, the
       coordinator's `:DOWN` (the attach monitor) and `now < d`. Any of them
       stops the attempt before dialing.
    3. Connect and TLS (clamped, see below), then the same check.
    4. `begin_dispatch(h, id, deadline: d)`. From its `:ok` the attempt's
       claim is `:possible`, whether or not it then sends.
    5. The same check, directly before the one `Mint.HTTP.request/5` call.
       There is exactly one request call site, reached at most once.
    6. While streaming: the clock is checked at the top of every loop turn
       and before every `transfer/2`, and every wait (for the upstream or
       for downstream credit) is bounded by the time left to `d`. Reaching
       `d` stops the attempt (`:deadline`); no `finish` is attempted.

  Every end closes the Mint connection (and so its socket) before the
  voluntary `Handoff.cleaned/3`. `cleaned/3` is sent after a refused or
  cancelled attach only (the coordinator then knows this pid), never after
  an `{:error, _}` attach: another process must not acknowledge the attempt
  id.

  **Controller messages are sanitized** to a fixed vocabulary: the controller
  gets `{:connect_failed, :refused | :ambiguous, kind}` or exactly one
  `{:ended, reason, stats}`, also when attach is refused (legacy sends
  nothing then). `kind`, `reason` and every nested term are atoms from fixed
  lists, an HTTP status integer, or `:other`. No upstream body, error
  message, TLS alert text, coordinator or requester reason term, socket,
  pid, host or certificate is copied. `stats` carry counters, `claim` and a
  `cleaned_ack` of `:ok | :error`.

  **Timeout clamps applied (the only ones this slice can apply):**

    * Connect: OTP `ssl` applies the one `timeout` given to the TCP connect
      and again to the TLS handshake, so the attempt passes
      `min(connect_timeout_ms, div(d - now, 2))` and does not dial when that
      is below 1 ms.
    * Waits for upstream data: `min(response_timeout_ms, d - now)`; for
      downstream credit (legacy: unbounded): `d - now`.

  **Limits, not solved here (no hard expiry).** The deadline is cooperative.
  Not clamped: the request write (`Mint.HTTP.request/5` -> `:ssl.send/2` has
  no per-call timeout; it can block on the kernel send buffer), `ready/2`,
  `transfer/2` and `cleaned/3` (the coordinator's 5 s call timeout),
  `attach_grant/3`, `begin_dispatch/3` and `finish/4` (`d` plus the coordinator's
  grace), `Mint.HTTP.close/1` (OTP's own close behaviour), parsing of one
  delivery, and scheduling. A blocked call delays the next check; nothing
  here proves socket teardown timing or slot quiescence.
  """

  alias Telelem.Handoff
  alias Telelem.Sandbox.VerifiedTarget
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
  `:body`, `:limits` (`Telelem.Wire.Limits`), `:recorder`, `:rid`, and
  optionally `:terminal_deadline`.

  `:terminal_deadline` is the coordinator's absolute `:terminal_deadline`
  (`System.monotonic_time(:millisecond)`), the same integer the coordinator
  was started with. When given, the attempt passes it to
  `Handoff.finish/4` as `deadline:`, so terminal authorization waits at most
  until that deadline plus the coordinator's grace across both of finish's
  waits. When absent or `nil` (legacy M1), the attempt calls
  `Handoff.finish/3` exactly as before. Any other value raises
  `ArgumentError` in the caller, before the attempt is spawned.

  `:gate` opts in to the gated attempt ("Gated attempt" above), with its own
  closed option set.
  """
  def start_link(opts) do
    case mode!(opts) do
      :legacy ->
        case Keyword.get(opts, :terminal_deadline) do
          d when is_nil(d) or is_integer(d) ->
            :ok

          other ->
            raise ArgumentError,
                  "Upstream :terminal_deadline must be an integer or nil, got: #{inspect(other)}"
        end

      :gated ->
        _ = gate!(opts)
    end

    {:ok, :proc_lib.spawn_link(__MODULE__, :init, [opts])}
  end

  # Mode selection. Every `:gate` entry is counted first, so a duplicate
  # (`[gate: nil, gate: [...]]` included) is refused before either mode is
  # chosen; no `:gate`, or exactly one `gate: nil`, is legacy.
  defp mode!(opts) do
    gates = if is_list(opts), do: for({:gate, g} <- opts, do: g), else: []

    case gates do
      [] -> :legacy
      [nil] -> :legacy
      [_] -> :gated
      _ -> gate_error(":gate must be given at most once")
    end
  end

  @gate_keys [:handoff, :id, :controller, :body, :limits, :gate, :terminal_deadline]
  @gate_required [:handoff, :id, :controller, :body, :limits, :gate]

  # Gated attempts only: every numeric field of `Limits`, an integer in
  # 1..max. Timeouts fit an Erlang `receive ... after` / timer (2^32 - 1 ms);
  # byte counts and socket buffer sizes fit a signed 32-bit int. Checked
  # before `Limits.new/1`, whose relations compare terms of any type.
  @max_timeout_ms 4_294_967_295
  @max_bytes 2_147_483_647
  @gate_limit_ranges [
    window: @max_bytes,
    max_event: @max_bytes,
    high_water: @max_bytes,
    hard_cap: @max_bytes,
    cleanup_timeout_ms: @max_timeout_ms,
    cleanup_deadline_ms: @max_timeout_ms,
    connect_timeout_ms: @max_timeout_ms,
    register_timeout_ms: @max_timeout_ms,
    response_timeout_ms: @max_timeout_ms,
    max_request_body: @max_bytes,
    upstream_recbuf: @max_bytes,
    upstream_read_buffer: @max_bytes,
    downstream_sndbuf: @max_bytes
  ]

  @doc false
  # The gated numeric limit ranges, `[{field, max}]` (min is always 1).
  def gate_limit_ranges, do: @gate_limit_ranges

  # The gated interface: a closed key set, each key once, and a target that
  # VerifiedTarget re-validates. Messages name the field, never its value.
  defp gate!(opts) do
    unless is_list(opts) and Enum.all?(opts, &match?({k, _} when is_atom(k), &1)),
      do: gate_error("options must be a keyword list")

    keys = Keyword.keys(opts)
    unless length(Enum.uniq(keys)) == length(keys), do: gate_error("each option at most once")

    case keys -- @gate_keys do
      [] -> :ok
      [k | _] -> gate_error("option #{inspect(k)} is not accepted")
    end

    case @gate_required -- keys do
      [] -> :ok
      [k | _] -> gate_error("option #{inspect(k)} is required")
    end

    {target, deadline} =
      case opts[:gate] do
        [target: t, deadline: d] when is_integer(d) -> {t, d}
        [deadline: d, target: t] when is_integer(d) -> {t, d}
        _ -> gate_error(":gate must be exactly [target: _, deadline: integer]")
      end

    # Handoff's shared bound: every Handoff wait on `d` is `d + grace`, which
    # must fit one wait. Checked before spawn and again in the attempt (later,
    # so never looser). A past deadline passes and stops the attempt as before.
    unless Handoff.deadline_in_range?(deadline),
      do: gate_error(":gate deadline must be at most #{Handoff.max_deadline_ahead_ms()} ms ahead")

    case target do
      %VerifiedTarget{deployment_id: id} = t ->
        case VerifiedTarget.check(t, id) do
          :ok -> :ok
          {:error, why} -> gate_error(":gate target rejected (#{inspect(why)})")
        end

        unless match?({_, _, _, _}, t.address), do: gate_error(":gate target must be IPv4")

      _ ->
        gate_error(":gate target must be a verified target")
    end

    case Keyword.fetch(opts, :terminal_deadline) do
      :error -> :ok
      {:ok, ^deadline} -> :ok
      {:ok, _} -> gate_error(":terminal_deadline must equal the gate deadline")
    end

    unless is_pid(opts[:handoff]), do: gate_error(":handoff must be a pid")
    unless is_pid(opts[:controller]), do: gate_error(":controller must be a pid")

    limits =
      case opts[:limits] do
        %Limits{} = l -> l
        _ -> gate_error(":limits must be a Telelem.Wire.Limits")
      end

    for {k, max} <- @gate_limit_ranges do
      v = Map.get(limits, k)

      unless is_integer(v) and v >= 1 and v <= max,
        do: gate_error(":limits #{k} must be an integer in 1..#{max}")
    end

    limits =
      try do
        Limits.new(limits |> Map.from_struct() |> Map.to_list())
      rescue
        _ -> gate_error(":limits are inconsistent")
      end

    body = opts[:body]

    unless is_binary(body) and byte_size(body) <= limits.max_request_body,
      do: gate_error(":body must be a binary of at most max_request_body bytes")

    %{target: target, deadline: deadline}
  end

  defp gate_error(msg), do: raise(ArgumentError, "Upstream gate: " <> msg)

  @doc false
  def init(opts) do
    gate = if mode!(opts) == :gated, do: gate!(opts)

    %Limits{} = limits = Keyword.fetch!(opts, :limits)

    s = %{
      h: Keyword.fetch!(opts, :handoff),
      id: Keyword.fetch!(opts, :id),
      ctl: Keyword.fetch!(opts, :controller),
      lane: if(gate, do: gate_lane(gate.target), else: Keyword.fetch!(opts, :lane)),
      path:
        if(gate,
          do: "/v1/chat/completions",
          else: Keyword.get(opts, :path, "/v1/chat/completions")
        ),
      body: Keyword.fetch!(opts, :body),
      limits: limits,
      rec: if(gate, do: nil, else: Keyword.get(opts, :recorder)),
      rid: if(gate, do: nil, else: Keyword.get(opts, :rid)),
      # Absolute, shared with the coordinator; nil = legacy finish/3.
      terminal_deadline: if(gate, do: gate.deadline, else: Keyword.get(opts, :terminal_deadline)),
      # nil = legacy; else %{target:, deadline:} (see "Gated attempt").
      gate: gate,
      hmon: nil,
      conn: nil,
      ref: nil,
      sock: nil,
      armed: false,
      dispatched: false,
      status: nil,
      phase: :awaiting_status,
      # {:pending, first_bytes} until the status line is seen on the wire,
      # then {:http, {major, minor}} or :unparsed.
      wire: {:pending, ""},
      framing: nil,
      role: :candidate,
      err_body: "",
      rest: "",
      pending: :queue.new(),
      pending_bytes: 0,
      credit: limits.window,
      ended: nil,
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

    if s.gate, do: gated_attach(s), else: legacy_attach(s)
  end

  defp legacy_attach(s) do
    case Handoff.attach(s.h, s.id) do
      {:ok, hmon} ->
        connect(%{s | hmon: hmon})

      other ->
        note(s, {:attach_refused, other})
        exit(:normal)
    end
  end

  # ── gated: attach, checks, clamps ───────────────────────────────────────

  defp gate_lane(%VerifiedTarget{} = t),
    do: %{name: t.deployment_id, host: t.host, port: t.port}

  # Grant-specific attach: a coordinator without `coordinator_grant` answers
  # {:error, :grant_required} before registering anything, so the attempt
  # ends here, before any dial, and does not acknowledge.
  defp gated_attach(s) do
    case Handoff.attach_grant(s.h, s.id, deadline: s.gate.deadline) do
      {:ok, hmon} ->
        s = %{s | hmon: hmon}

        case check(s) do
          :ok -> connect(s)
          {:stop, cause} -> stop(s, {:stopped, :after_attach, cause})
        end

      {:cancel, reason} ->
        # Registered (and maybe reserved) under this pid: acknowledge :none.
        stop(s, {:attach, :cancel, reason})

      {:error, reason} ->
        # Not ours to acknowledge: the id may belong to another process.
        report_end(s, {:attach, :error, reason}, :not_sent)
    end
  end

  # Non-blocking: a cancel or the coordinator's :DOWN already delivered, then
  # the clock. Consumed messages end the attempt, so nothing is lost.
  defp check(s) do
    h = s.h
    hmon = s.hmon

    receive do
      {@tag, ^h, {:cancel, reason}} -> {:stop, {:cancelled, reason}}
    after
      0 ->
        receive do
          {:DOWN, ^hmon, :process, _, _} -> {:stop, :handoff_down}
        after
          0 -> if expired?(s), do: {:stop, :deadline}, else: :ok
        end
    end
  end

  defp expired?(%{gate: nil}), do: false
  defp expired?(%{gate: %{deadline: d}}), do: System.monotonic_time(:millisecond) >= d

  @doc false
  # OTP ssl applies one timeout to the TCP connect and again to the
  # handshake: half the time left keeps both within the deadline.
  def connect_clamp(limit_ms, deadline, now_ms),
    do: min(limit_ms, div(max(deadline - now_ms, 0), 2))

  @doc false
  # A wait for upstream data (`base` ms) or for credit (`:infinity`).
  def wait_clamp(base, deadline, now_ms) do
    left = max(deadline - now_ms, 0)
    if base == :infinity, do: left, else: min(base, left)
  end

  # ── connect / dispatch ──────────────────────────────────────────────────

  defp connect(%{gate: %{target: t, deadline: d}} = s) do
    timeout = connect_clamp(s.limits.connect_timeout_ms, d, System.monotonic_time(:millisecond))

    if timeout < 1 do
      stop(s, {:stopped, :after_attach, :deadline})
    else
      opts = [
        mode: :passive,
        protocols: [:http1],
        transport_opts:
          VerifiedTarget.tls_opts(t) ++
            [
              timeout: timeout,
              recbuf: s.limits.upstream_recbuf,
              buffer: s.limits.upstream_read_buffer
            ]
      ]

      case Mint.HTTP.connect(:https, t.host, t.port, opts) do
        {:ok, conn} ->
          s = %{s | conn: conn, sock: Mint.HTTP.get_socket(conn)}

          case check(s) do
            :ok -> dispatch(s)
            {:stop, cause} -> stop(s, {:stopped, :after_connect, cause})
          end

        {:error, %Mint.TransportError{reason: reason}} ->
          connect_failed(s, reason)

        {:error, other} ->
          connect_failed(s, other)
      end
    end
  end

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
    reason = if s.gate, do: public_connect_reason(reason), else: reason
    send(s.ctl, {:telelem_wire, self(), s.id, {:connect_failed, class, reason}})
    exit(:normal)
  end

  defp dispatch(%{gate: %{deadline: d}} = s) do
    # The check right after TLS already consumed any earlier cancel/:DOWN.
    case Handoff.begin_dispatch(s.h, s.id, deadline: d) do
      :ok ->
        # Permission released: exposure is possible from here, sent or not.
        s = %{s | dispatched: true}

        case check(s) do
          :ok -> request(s)
          {:stop, cause} -> stop(s, {:stopped, :before_request, cause})
        end

      {:cancel, reason} ->
        stop(s, {:cancelled_before_dispatch, reason})

      {:error, reason} ->
        stop(s, {:dispatch_refused, reason})
    end
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

    request(s)
  end

  # The one request call site, reached at most once per attempt (`ref` is
  # set only here, and nothing returns to the dispatch path afterwards).
  defp request(%{ref: nil} = s) do
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
    # Gated: the deadline ends the attempt even while deliveries keep coming.
    if expired?(s), do: stop(s, :deadline)
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

        on_socket(%{s | stats: st, armed: false} |> sniff_status_line(data), msg)

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
      idle_timeout(s) ->
        if expired?(s),
          do: stop(s, :deadline),
          else: stop(s, {:upstream_timeout, s.limits.response_timeout_ms})
    end
  end

  # Only time out while waiting for the upstream; waiting for credit is the
  # downstream's business (its send timeout ends it). Gated: both waits are
  # also bounded by the time left to the deadline.
  defp idle_timeout(%{gate: %{deadline: d}} = s),
    do: wait_clamp(legacy_idle_timeout(s), d, System.monotonic_time(:millisecond))

  defp idle_timeout(s), do: legacy_idle_timeout(s)

  defp legacy_idle_timeout(%{armed: true} = s), do: s.limits.response_timeout_ms
  defp legacy_idle_timeout(_), do: :infinity

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
    framing = framing(s.wire, hs)
    note(s, {:upstream_headers, s.status, ct, framing})

    cond do
      not (s.status == 200 and is_binary(ct) and String.starts_with?(ct, "text/event-stream")) ->
        %{s | phase: :error_body, framing: framing}

      framing == :chunked ->
        %{s | phase: :streaming, framing: :chunked}

      true ->
        # Mint also reports {:done, ref} for a close-delimited (or
        # content-length) body; neither is the chunked terminator this slice
        # accepts as completion. Nothing of this response is forwarded.
        violation(
          %{s | phase: :unsupported_framing, framing: framing},
          {:unsupported_framing, framing}
        )
    end
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
    if expired?(s), do: stop(s, :deadline)

    case :queue.peek(s.pending) do
      {:value, {_kind, ev}} when byte_size(ev) <= s.credit ->
        case Handoff.transfer(s.h, ev) do
          :ok ->
            {_, q} = :queue.out(s.pending)
            st = s.stats

            s = %{
              s
              | pending: q,
                pending_bytes: s.pending_bytes - byte_size(ev),
                credit: s.credit - byte_size(ev),
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
    cond do
      # Flush complete events already received (credit permitting) first.
      not :queue.is_empty(s.pending) ->
        s

      success?(s) ->
        authorize_done(s)

      # End without finish: the coordinator reports incomplete.
      true ->
        stop(s, end_reason(s))
    end
  end

  defp maybe_end(%{phase: :error_body, ended: :done} = s),
    do: stop(s, {:upstream_status, s.status, s.err_body})

  defp maybe_end(s), do: stop(s, end_reason(s))

  # Complete HTTP evidence is in. The held [DONE] is never transferred as
  # ordinary data: it travels only inside the coordinator's terminal
  # authorization (Handoff.finish/3), which checks the pinned authority and
  # hands it downstream in the same serialized step. Refused => never sent.
  defp authorize_done(%{held_done: done, credit: credit} = s) when byte_size(done) > credit do
    if s.starved do
      s
    else
      note(s, {:credit_wait, %{credit: credit, head: byte_size(done), retained: retained(s)}})
      %{s | starved: true, stats: %{s.stats | credit_waits: s.stats.credit_waits + 1}}
    end
  end

  defp authorize_done(%{held_done: done, terminal: {:ok, reason}} = s) do
    note(s, {:done_released, %{terminal: s.terminal}})

    case finish(s, {:upstream_done, reason}, done) do
      :ok ->
        st = s.stats

        stop(
          %{
            s
            | held_done: :released,
              credit: s.credit - byte_size(done),
              stats: %{
                st
                | events_forwarded: st.events_forwarded + 1,
                  bytes_forwarded: st.bytes_forwarded + byte_size(done)
              }
          },
          :finished
        )

      {:error, :unconfirmed} ->
        # The coordinator is gone (or was fenced by us): the marker may have
        # been handed out. Not a refusal; never retried.
        stop(%{s | held_done: :unconfirmed}, :finish_unconfirmed)

      {:error, r} ->
        stop(%{s | held_done: :refused}, {:finish_refused, r})
    end
  end

  # Timed: one absolute bound (deadline + grace) across both of finish's
  # waits. Legacy (no deadline): finish/3, unchanged.
  defp finish(%{terminal_deadline: nil} = s, terminal, done),
    do: Handoff.finish(s.h, terminal, done)

  defp finish(%{terminal_deadline: d} = s, terminal, done),
    do: Handoff.finish(s.h, terminal, done, deadline: d)

  defp success?(s),
    do:
      s.ended == :done and s.framing == :chunked and is_binary(s.held_done) and
        match?({:ok, _}, s.terminal)

  # The response's own status line, read from the plaintext we hand to Mint
  # (Mint does not report the HTTP version). Only the first line matters: a
  # 1xx or anything else before the final response already fails closed.
  @max_status_line 256
  defp sniff_status_line(%{wire: {:pending, buf}} = s, data) do
    buf = buf <> data

    case :binary.match(buf, "\r\n") do
      {pos, _} ->
        %{s | wire: parse_status_line(binary_part(buf, 0, pos))}

      :nomatch when byte_size(buf) > @max_status_line ->
        %{s | wire: :unparsed}

      :nomatch ->
        %{s | wire: {:pending, buf}}
    end
  end

  defp sniff_status_line(s, _data), do: s

  defp parse_status_line(<<"HTTP/", ma, ".", mi, " ", _::binary>>)
       when ma in ?0..?9 and mi in ?0..?9,
       do: {:http, {ma - ?0, mi - ?0}}

  defp parse_status_line(_), do: :unparsed

  # Supported framing in this slice: HTTP/1.1, exactly `transfer-encoding:
  # chunked`, no content-length. Everything else is named, never accepted.
  defp framing(wire, hs) do
    te =
      for({"transfer-encoding", v} <- hs, t <- String.split(v, ","), do: t)
      |> Enum.map(&(&1 |> String.trim() |> String.downcase()))
      |> Enum.reject(&(&1 == ""))

    cond do
      wire != {:http, {1, 1}} -> {:http_version, wire}
      Enum.any?(hs, &match?({"content-length", _}, &1)) -> :content_length
      te == ["chunked"] -> :chunked
      te == [] -> :close_delimited
      true -> {:transfer_encoding, te}
    end
  end

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

  defp stop(%{gate: g} = s, reason) when g != nil do
    # Close first: the voluntary acknowledgement follows a closed transport.
    if s.conn, do: Mint.HTTP.close(s.conn)
    report_end(s, reason, Handoff.cleaned(s.h, s.id, claim(s)))
  end

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

  defp claim(s), do: if(s.dispatched, do: :possible, else: :none)

  # Gated end: exactly one sanitized {:ended, _, _} to the controller.
  defp report_end(s, reason, ack) do
    st = s.stats

    st =
      if s.rest != "",
        do: %{st | partial_discarded: st.partial_discarded + byte_size(s.rest)},
        else: st

    ack =
      case ack do
        :ok -> :ok
        :not_sent -> :not_sent
        _ -> :error
      end

    stats = Map.merge(st, %{claim: claim(s), cleaned_ack: ack, unsent_pending: s.pending_bytes})
    send(s.ctl, {:telelem_wire, self(), s.id, {:ended, public_reason(reason), stats}})
    exit(:normal)
  end

  # ── gated: the controller's fixed vocabulary ────────────────────────────

  @kinds [
    # coordinator / Handoff replies and cancel reasons
    :dispatch_deadline,
    :admission_refused,
    :ended,
    :closed,
    :cancelled,
    :unconfirmed,
    :handoff_down,
    :timeout,
    :pending,
    :duplicate,
    :deadline_required,
    :grant_required,
    :already_attached,
    :not_attached,
    :unknown_attempt,
    :linked_to_handoff,
    :not_permitted,
    :not_winner,
    :invalid_marker,
    :authority_down,
    :requester_down,
    :lost_to,
    :winner_down,
    :cleanup_timeout,
    :never_registered,
    # gated checks
    :deadline
  ]

  # An allowed atom, or the allowed atom heading a tuple; anything else is
  # :other. Nothing nested is copied.
  defp kind(k) when is_atom(k), do: if(k in @kinds, do: k, else: :other)
  defp kind(t) when is_tuple(t) and tuple_size(t) > 0, do: t |> elem(0) |> kind()
  defp kind(_), do: :other

  @connect_kinds [
    :econnrefused,
    :timeout,
    :closed,
    :econnreset,
    :econnaborted,
    :ehostunreach,
    :enetunreach,
    :eaddrnotavail,
    :eaddrinuse
  ]

  defp public_connect_reason(r) when is_atom(r), do: if(r in @connect_kinds, do: r, else: :other)
  defp public_connect_reason({:tls_alert, _}), do: :tls_alert
  defp public_connect_reason(_), do: :other

  @same [
    :finished,
    :finish_unconfirmed,
    :deadline,
    :upstream_ended_without_done,
    :no_semantic_content
  ]

  @first [
    :handoff_down,
    :dispatch_error,
    :upstream_timeout,
    :upstream_error_event,
    :upstream_broken,
    :done_without_finish_reason,
    :unsupported_finish_reason
  ]

  @with_kind [
    :finish_refused,
    :cancelled_before_dispatch,
    :dispatch_refused,
    :cancelled,
    :lost,
    :ready_error,
    :transfer_refused
  ]

  @violations [:unsupported_framing, :event_after_done, :bad_sentinel, :unsupported]
  @stages [:after_attach, :after_connect, :before_request]

  @doc false
  def public_reason(r) when r in @same, do: r
  def public_reason(t) when is_tuple(t) and elem(t, 0) in @first, do: elem(t, 0)
  def public_reason({tag, r}) when tag in @with_kind, do: {tag, kind(r)}

  def public_reason({:attach, how, r}) when how in [:cancel, :error], do: {:attach, how, kind(r)}

  def public_reason({:stopped, stage, cause}) when stage in @stages,
    do: {:stopped, stage, public_cause(cause)}

  def public_reason({:upstream_status, code, _}) when is_integer(code) and code in 100..599,
    do: {:upstream_status, code}

  def public_reason({:upstream_status, _, _}), do: {:upstream_status, :other}

  def public_reason({:protocol_violation, w}) do
    k = if is_tuple(w) and tuple_size(w) > 0, do: elem(w, 0), else: w
    {:protocol_violation, if(k in @violations, do: k, else: :other)}
  end

  def public_reason({:overflow, {k, _}}) when k in [:max_event, :hard_cap], do: {:overflow, k}
  def public_reason({:overflow, _}), do: {:overflow, :other}
  def public_reason(_), do: :other

  defp public_cause({:cancelled, r}), do: {:cancelled, kind(r)}
  defp public_cause(c) when c in [:handoff_down, :deadline], do: c
  defp public_cause(_), do: :other

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
