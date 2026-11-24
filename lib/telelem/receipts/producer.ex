defmodule Telelem.Receipts.Producer do
  @moduledoc """
  Shared publication handle for one `Telelem.Receipts.Store` (internal).

  Coordinators do not call this module directly; they publish through a
  per-request `Telelem.Receipts.Writer`, which adds the private sequence
  metadata. `Store.producer/1` returns the handle.

  ## Atomic admission + publication

  Pending evidence lives in an ETS `:set` table owned by the store whose key
  space is fixed to the integers `1..max_pending`. Admission **is**
  publication: a record is accepted exactly when one `:ets.insert_new/2` of
  the whole record into a free slot succeeds. There is no counter to reserve
  first and no message to send afterwards, so a producer that dies at any
  point either published the whole record or left nothing behind; no slot
  can be stranded by a dead producer or by a failed admission. The store
  empties slots with `:ets.take/2` (periodic sweep and before every read).

  The table can never hold more than `max_pending` records, whatever the
  number of concurrent producers. A producer never waits on the store: when
  no slot is free after at most `max_pending` `insert_new` probes the record
  is dropped (`{:dropped, :overflow}`). If the store (and so its table) is
  gone, the failed ETS call is caught and reported as
  `{:dropped, :store_down}`. Dropping evidence never fails the caller's work.

  `:ok` means "published to the live store's pending table" only. Pending and
  retained evidence lives in the store process's memory and is lost with it;
  this is not a durable receipt log.

  ## Fixed-size loss indicator

  Counters and per-receipt loss flags live in one fixed-size `:atomics`
  array, readable through `stats/1` even after the store is gone:

    * `overflow` – records refused because every pending slot was full
    * `store_down` – records refused because the store table was gone
    * `rejected` – invalid events (never published), and malformed pending
      rows discarded by the store
    * `evicted` – retained records removed by the retention limit
    * loss slots – `loss_slots` flags indexed by `phash2({tenant, request})`,
      set **synchronously, before** the failing `record` returns, for every
      drop, rejection or eviction. Slots are never cleared, so collisions can
      only make a receipt look less complete, never more.
  """

  @enforce_keys [:table, :ref, :max_pending, :max_event_bytes, :loss_slots]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          table: :ets.tid(),
          ref: :atomics.atomics_ref(),
          max_pending: pos_integer(),
          max_event_bytes: pos_integer(),
          loss_slots: pos_integer()
        }

  # :atomics layout (1-based).
  @cursor 1
  @stamp 2
  @overflow 3
  @store_down 4
  @rejected 5
  @evicted 6
  @slot0 7

  @doc false
  def counters_size(loss_slots), do: @slot0 + loss_slots - 1

  @doc false
  def ix(:overflow), do: @overflow
  def ix(:store_down), do: @store_down
  def ix(:rejected), do: @rejected
  def ix(:evicted), do: @evicted

  @doc false
  def slot_ix(%__MODULE__{loss_slots: n}, {tenant, request}),
    do: @slot0 + :erlang.phash2({tenant, request}, n)

  @doc false
  def mark_lost(%__MODULE__{ref: ref} = p, key), do: :atomics.put(ref, slot_ix(p, key), 1)

  @doc false
  def lost?(%__MODULE__{ref: ref} = p, key), do: :atomics.get(ref, slot_ix(p, key)) != 0

  @doc false
  def count(%__MODULE__{ref: ref}, name), do: :atomics.add(ref, ix(name), 1)

  @doc false
  # Publish one already validated record with one atomic insert, or drop it.
  # `meta` is `{key, owner, seq, dropped_before}`; the row stored is
  # `{slot, stamp, key, owner, seq, dropped_before, event}`.
  @spec publish(t(), tuple(), Telelem.Receipts.Event.t()) ::
          :ok | {:dropped, :overflow | :store_down}
  def publish(%__MODULE__{} = p, {key, owner, seq, dropped_before}, event) do
    # Neither value reserves anything: the cursor only spreads probe starting
    # points and the stamp only orders retention; gaps in either are harmless.
    start = :atomics.add_get(p.ref, @cursor, 1)
    stamp = :atomics.add_get(p.ref, @stamp, 1)

    try do
      probe(p, start, 0, {stamp, key, owner, seq, dropped_before, event})
    rescue
      ArgumentError -> drop(p, key, :store_down)
    end
  end

  defp probe(%{max_pending: max} = p, _start, max, {_, key, _, _, _, _}),
    do: drop(p, key, :overflow)

  defp probe(%{max_pending: max} = p, start, i, row) do
    slot = rem(start + i, max) + 1

    if :ets.insert_new(p.table, Tuple.insert_at(row, 0, slot)),
      do: :ok,
      else: probe(p, start, i + 1, row)
  end

  @doc false
  def drop(p, key, reason) do
    count(p, reason)
    mark_lost(p, key)
    {:dropped, reason}
  end

  @doc """
  Loss counters from the shared array; does not contact the store.
  `loss?` is true once any record was dropped, rejected or evicted.
  """
  @spec stats(t()) :: %{
          overflow: non_neg_integer(),
          store_down: non_neg_integer(),
          rejected: non_neg_integer(),
          evicted: non_neg_integer(),
          loss?: boolean()
        }
  def stats(%__MODULE__{ref: ref}) do
    s = %{
      overflow: :atomics.get(ref, @overflow),
      store_down: :atomics.get(ref, @store_down),
      rejected: :atomics.get(ref, @rejected),
      evicted: :atomics.get(ref, @evicted)
    }

    Map.put(s, :loss?, s.overflow + s.store_down + s.rejected + s.evicted > 0)
  end
end
