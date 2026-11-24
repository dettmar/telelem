defmodule Telelem.Wire.Limits do
  @moduledoc """
  The declared TEL-005 budget. Build it (and print it) **before** a run.

  Application bytes the gateway holds for one request:

    * **attempt retained**: Mint's undecoded HTTP buffer + the partial SSE frame
      + complete events not yet transferred + a held `[DONE]`. The reader arms
      the TLS socket for one more delivery (`active: :once`) only while
      retained < `high_water`. That is the only *preventive* control.
    * `hard_cap` and `max_event` are **abort thresholds checked after** a
      delivery has been appended to Mint's buffer and split into events. They
      are not a ceiling on memory: before the check the attempt already holds
      its previous retained bytes (< `high_water`) plus the delivery, plus
      transient copies made while parsing. The gateway does not control the
      size of one TLS delivery (it is measured as `max_delivery`; in practice it
      tracked the pinned `upstream_recbuf`). If retained exceeds `hard_cap`
      after a delivery, or an event or partial frame exceeds `max_event`, the
      attempt aborts. Evidence reports `peak_retained` at those same check
      points, not a process-memory maximum.
    * **in flight**: bytes accepted by the coordinator but not yet written
      downstream. Bounded by `window` (byte credit). The requester returns
      credit only after `Plug.Conn.chunk/2` returned `:ok`.

  `overflow: :abort` is the only action: the upstream is closed and the
  downstream ends `failed` (before commit) or `incomplete` (after commit).
  Nothing is dropped silently and the stream never resumes.

  Kernel socket buffers are separate and not counted here; `upstream_recbuf`
  and `downstream_sndbuf` only pin the gateway's side of them (Linux doubles
  the value it is given). The client's receive buffer and the fixture's send
  buffer are outside the gateway's control.
  """

  @enforce_keys []
  defstruct window: 65_536,
            max_event: 65_536,
            high_water: 131_072,
            hard_cap: 262_144,
            overflow: :abort,
            cleanup_timeout_ms: 500,
            cleanup_deadline_ms: 1_000,
            connect_timeout_ms: 2_000,
            register_timeout_ms: 2_000,
            response_timeout_ms: 30_000,
            max_request_body: 1_048_576,
            upstream_recbuf: 16_384,
            upstream_read_buffer: 16_384,
            downstream_sndbuf: 65_536

  @type t :: %__MODULE__{}

  @doc "Validates the relations between limits. Raises on an inconsistent budget."
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    l = struct!(__MODULE__, opts)

    checks = [
      {l.overflow == :abort, "overflow must be :abort"},
      {l.max_event <= l.window, "max_event must fit in window (an event is transferred whole)"},
      {l.max_event < l.high_water,
       "max_event must be below high_water (else a split event deadlocks)"},
      {l.high_water < l.hard_cap, "high_water must be below hard_cap"},
      {l.cleanup_timeout_ms < l.cleanup_deadline_ms,
       "cleanup_timeout_ms (forced kill) must be below the declared cleanup_deadline_ms"}
    ]

    for {false, msg} <- checks, do: raise(ArgumentError, msg)
    l
  end

  @doc "Plain map for evidence files."
  def to_map(%__MODULE__{} = l), do: Map.from_struct(l)
end
