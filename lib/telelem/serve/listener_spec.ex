defmodule Telelem.Serve.ListenerSpec do
  @moduledoc """
  Fixed, secret-free Bandit listener *policy* options for the M2a serving
  listener, checked against the pinned `bandit` 1.14.1 and `thousand_island`
  1.6.0 option names.

  `options/0` returns one constant keyword list. It takes no input and has no
  override, merge or environment path. It is deliberately **not runnable**: it
  has no `:plug`, port, bind address (`:ip`, `:inet`, `:inet6`), TLS material
  or paths (`:certfile`, `:keyfile`, `:otp_app`, `:cipher_suite`,
  `transport_options`), handler or supervisor options, and it is not a child
  spec. `Bandit.start_link/1` raises on it for the missing plug before anything
  is bound. Whoever later owns listener startup adds those parts; this module
  only fixes the policy they must not change.

  ## Policy and its accurate limits (pinned versions)

    * `scheme: :https`, HTTP/1 only. `http_2_options: [enabled: false]`
      makes Bandit advertise only `"http/1.1"` by ALPN and never switch a
      connection to HTTP/2, with or without ALPN. TLS settings are not set
      here. These options do **not** reject HTTP/1.0: the pinned Bandit
      HTTP/1 handler parses and serves both `HTTP/1.1` and `HTTP/1.0`
      request lines (it rejects other version text itself). An HTTP/1.0
      request reaches the plug, and it is `Telelem.Serve.HTTP` that
      answers any protocol other than `:"HTTP/1.1"` with 505
      `version_not_supported`.
    * One request per connection: `http_1_options: [max_requests: 1]`. Bandit
      closes the connection after the first request's response. It is a
      per-connection cap; it does not bound the number of connections,
      acceptors or concurrent requests.
    * WebSocket upgrades disabled (`websocket_options: [enabled: false]`),
      with WebSocket compression and protocol-error logging also off.
    * No response compression: `compress: false`, `compress_chunked: false`.
    * Log options off: `startup_log: false`; `log_exceptions_with_status_codes:
      []` (no plug exception is logged by status); `log_protocol_errors:
      false` and `log_client_closures: false`; `log_unknown_messages: false`.
    * `thousand_island_options: [silent_terminate_on_error: true]`. In
      Thousand Island 1.6.0 this is read in one place: when a handler
      callback returns `{:error, reason, state}` with a `reason` other than
      `:timeout`, the connection stops with `{:shutdown,
      {:silent_termination, reason}}` instead of with `reason`. A
      `{:error, :timeout, state}` return is handled separately and stops
      with `{:shutdown, :read_timeout}` whatever this option is. On the
      HTTP/1 path this policy serves, the pinned Bandit HTTP/1 handler
      turns an `{:error, _}` result from `Bandit.Pipeline` into `{:close,
      state}`, so that connection ends as `{:shutdown, :local_closed}` and
      this option does not apply to it. The option does not change
      exceptions raised from a handler callback (Thousand Island re-raises
      them) or a `:tcp_error`/`:ssl_error` message (the connection stops
      with that reason).

  These options do not make the listener silent. The pinned Bandit still has
  log calls no option disables (for example the HTTP/1 request-trailer
  warning, the date-cache warning, the clear-channel TLS warning and the
  `:eaddrinuse` startup error), OTP `ssl` and the runtime log through
  `:logger`, and telemetry events are always executed. Use
  `Telelem.Serve.Diagnostics` to refuse while Logger or telemetry could
  observe them; that check is a snapshot, not a mute.
  """

  @options [
    scheme: :https,
    startup_log: false,
    http_options: [
      compress: false,
      compress_chunked: false,
      log_exceptions_with_status_codes: [],
      log_protocol_errors: false,
      log_client_closures: false
    ],
    http_1_options: [
      enabled: true,
      max_requests: 1,
      log_unknown_messages: false
    ],
    http_2_options: [enabled: false],
    websocket_options: [
      enabled: false,
      compress: false,
      log_protocol_errors: false
    ],
    thousand_island_options: [silent_terminate_on_error: true]
  ]

  @typedoc "The fixed policy keyword list; see the module documentation."
  @type t :: keyword()

  @doc """
  The fixed listener policy options. Always the same value; contains only
  atoms, booleans, integers and empty lists (no binaries, PIDs, references or
  functions).
  """
  @spec options() :: t()
  def options, do: @options
end
