defmodule Telelem.Receipts.Producer do
  @moduledoc """
  Non-blocking producer handle for a `Telelem.Receipts.Store`.

  Obtain one with `Telelem.Receipts.Store.producer/1` and call `record/2`
  from any process. `record/2` runs entirely in the caller and never waits
  on the store:

    1. validate and canonically encode the event (`Telelem.Receipts.Event`)
       and check the per-event byte ceiling; invalid input is rejected here
       and never reaches the store;
    2. **admit before enqueueing**: atomically increment the shared pending
       counter; if the result exceeds `max_pending`, undo and drop;
    3. only an admitted event is sent (`send/2`, asynchronous) to the store,
       which decrements pending once it has processed the message.

  ## Hard pending bound

  A message is sent only when `:atomics.add_get/3` returned `<= max_pending`.
  The counter is always `>=` the number of admitted, unprocessed messages
  (failed attempts only add transiently, then undo), so at most
  `max_pending` admitted event messages are ever outstanding, whatever the
  number of concurrent producers. Overflow drops evidence, never the
  caller's work: `record/2` returns `{:dropped, :overflow}` immediately.

  ## Fixed-size loss indicator

  All counters and the loss slots live in one fixed-size `:atomics` array
  allocated by the store, so loss stays visible through `stats/1` even if the
  store is down:

    * `dropped` – valid events refused by admission (overflow)
    * `rejected` – invalid events (attributable or not)
    * `unattributed` – rejected events without a valid tenant/request id
    * `evicted` – retained events removed by the retention limit
    * loss slots – `loss_slots` flags indexed by `phash2({tenant, request})`.
      A drop, an attributable rejection or an eviction sets the slot of its
      receipt. Slots are never cleared, so collisions can only make a receipt
      look *less* complete, never more.
  """

  alias Telelem.Receipts.Event

  @enforce_keys [:pid, :token, :ref, :max_pending, :max_event_bytes, :loss_slots]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          pid: pid(),
          token: reference(),
          ref: :atomics.atomics_ref(),
          max_pending: pos_integer(),
          max_event_bytes: pos_integer(),
          loss_slots: pos_integer()
        }

  # :atomics layout (1-based).
  @pending 1
  @dropped 2
  @rejected 3
  @unattributed 4
  @evicted 5
  @slot0 6

  @doc false
  def counters_size(loss_slots), do: @slot0 + loss_slots - 1

  @doc false
  def ix(:pending), do: @pending
  def ix(:dropped), do: @dropped
  def ix(:rejected), do: @rejected
  def ix(:unattributed), do: @unattributed
  def ix(:evicted), do: @evicted

  @doc false
  def slot_ix(%__MODULE__{loss_slots: n}, {tenant, request}),
    do: @slot0 + :erlang.phash2({tenant, request}, n)

  @doc false
  def mark_lost(%__MODULE__{ref: ref} = p, key), do: :atomics.put(ref, slot_ix(p, key), 1)

  @doc false
  def lost?(%__MODULE__{ref: ref} = p, key), do: :atomics.get(ref, slot_ix(p, key)) != 0

  @doc """
  Record one event without waiting on the store.

  Returns `:ok` (admitted and enqueued), `{:dropped, :overflow}` (valid,
  pending bound reached, counted as loss) or `{:rejected, reason}` (invalid,
  never retained; `reason` never contains input data). Producers may
  ignore the result.
  """
  @spec record(t(), term()) :: :ok | {:dropped, :overflow} | {:rejected, Event.reason()}
  def record(%__MODULE__{} = p, input) do
    case validate(p, input) do
      {:ok, event} -> admit(p, event)
      {:error, reason} -> reject(p, input, reason)
    end
  end

  defp validate(p, input) do
    with {:ok, event} <- Event.new(input) do
      if byte_size(Event.encode(event)) <= p.max_event_bytes,
        do: {:ok, event},
        else: {:error, {:invalid, :encoded_size}}
    end
  end

  defp admit(%__MODULE__{ref: ref} = p, event) do
    # Snapshot before admission: any unattributed rejection that happens
    # after this event was emitted is visible to the store as a newer value.
    epoch = :atomics.get(ref, @unattributed)

    if :atomics.add_get(ref, @pending, 1) <= p.max_pending do
      send(p.pid, {:telelem_receipt_event, p.token, epoch, event})
      :ok
    else
      :atomics.sub(ref, @pending, 1)
      :atomics.add(ref, @dropped, 1)
      mark_lost(p, {event.tenant_id, event.request_id})
      {:dropped, :overflow}
    end
  end

  defp reject(%__MODULE__{ref: ref} = p, input, reason) do
    :atomics.add(ref, @rejected, 1)

    case Event.key_of(input) do
      :unattributed -> :atomics.add(ref, @unattributed, 1)
      key -> mark_lost(p, key)
    end

    {:rejected, reason}
  end

  @doc """
  Loss counters from the shared array; does not contact the store.
  `loss?` is true once any event was dropped, rejected or evicted.
  """
  @spec stats(t()) :: %{
          pending: non_neg_integer(),
          dropped: non_neg_integer(),
          rejected: non_neg_integer(),
          unattributed: non_neg_integer(),
          evicted: non_neg_integer(),
          loss?: boolean()
        }
  def stats(%__MODULE__{ref: ref}) do
    s = %{
      pending: :atomics.get(ref, @pending),
      dropped: :atomics.get(ref, @dropped),
      rejected: :atomics.get(ref, @rejected),
      unattributed: :atomics.get(ref, @unattributed),
      evicted: :atomics.get(ref, @evicted)
    }

    Map.put(s, :loss?, s.dropped + s.rejected + s.evicted > 0)
  end
end
