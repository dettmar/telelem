defmodule Telelem.Receipts.Writer do
  @moduledoc """
  Sequenced evidence writer for one request, held by its serving coordinator
  (M2a §2 "Producer death and read semantics").

      w = Writer.open(producer, tenant_id, request_id)   # owner = self()
      {:ok, w} = Writer.record(w, ingress_event)          # seq 1
      {result, w} = Writer.record(w, dispatch_event)      # seq 2, ...

  The coordinator that opens the writer is its only owner and the sole
  ordered writer of the request. Every `record/2` by the owner consumes the
  next consecutive sequence number (starting at one), **including when
  publication fails**, so a lost record always leaves a missing number before
  any later record. A failure also, before `record/2` returns:

    * sets the receipt's loss flag in the shared fixed-size array, and
    * increments the writer's `dropped` count, which is carried as private
      `dropped_before` metadata on every later published record, so a later
      terminal itself declares that evidence before it is missing.

  If the coordinator dies after a failure and before a later record, there is
  no later terminal, and a receipt without a terminal is never complete.

  The sequence number and `dropped_before` are internal publication metadata;
  they are not event fields and are not exposed by `Telelem.Receipts.Receipt`.

  `record/2` from any process other than the owner returns
  `{:rejected, :not_owner}`, publishes nothing and leaves the writer as is: no
  second process may continue a (possibly dead) coordinator's evidence. A
  second writer opened for the same request (request-ID reuse, or a takeover
  attempt) is detected by the store as `:writer_conflict`; reusing a stale
  copy of this struct shows up as `:duplicate_sequence`.
  """

  alias Telelem.Receipts.{Event, Producer}

  @enforce_keys [:producer, :key, :owner, :next_seq, :dropped]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          producer: Producer.t(),
          key: {Event.id(), Event.id()},
          owner: pid(),
          next_seq: pos_integer(),
          dropped: non_neg_integer()
        }
  @type result ::
          :ok
          | {:dropped, :overflow | :store_down}
          | {:rejected, Event.reason() | {:invalid, :request}}

  @doc """
  Open the writer for `{tenant_id, request_id}`, owned by the calling process.
  The ids must already be valid (the request id is gateway-generated).
  """
  @spec open(Producer.t(), Event.id(), Event.id()) :: {:ok, t()} | {:error, Event.reason()}
  def open(%Producer{} = p, tenant_id, request_id) do
    cond do
      not Event.valid_id?(tenant_id) ->
        {:error, {:invalid, :tenant_id}}

      not Event.valid_id?(request_id) ->
        {:error, {:invalid, :request_id}}

      true ->
        {:ok,
         %__MODULE__{
           producer: p,
           key: {tenant_id, request_id},
           owner: self(),
           next_seq: 1,
           dropped: 0
         }}
    end
  end

  @doc """
  Validate and publish one event as the next sequence number. Never waits on
  the store and never raises for bad input.

  The event must carry this writer's tenant and request ids, else it is
  rejected as `{:invalid, :request}`. Rejection reasons never echo input.
  """
  @spec record(t(), term()) :: {result() | {:rejected, :not_owner}, t()}
  def record(%__MODULE__{owner: owner} = w, _input) when owner != self(),
    do: {{:rejected, :not_owner}, w}

  def record(%__MODULE__{producer: p, key: key} = w, input) do
    seq = w.next_seq

    result =
      case validate(w, input) do
        {:ok, event} ->
          Producer.publish(p, {key, w.owner, seq, w.dropped}, event)

        {:error, reason} ->
          Producer.count(p, :rejected)
          Producer.mark_lost(p, key)
          {:rejected, reason}
      end

    dropped = if result == :ok, do: w.dropped, else: w.dropped + 1
    {result, %{w | next_seq: seq + 1, dropped: dropped}}
  end

  defp validate(%{producer: p, key: {tenant, request}}, input) do
    with {:ok, event} <- Event.new(input) do
      cond do
        event.tenant_id != tenant or event.request_id != request ->
          {:error, {:invalid, :request}}

        byte_size(Event.encode(event)) > p.max_event_bytes ->
          {:error, {:invalid, :encoded_size}}

        true ->
          {:ok, event}
      end
    end
  end
end
