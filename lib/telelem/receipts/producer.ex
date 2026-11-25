defmodule Telelem.Receipts.Producer do
  @moduledoc """
  Shared publication handle for one `Telelem.Receipts.Store` (internal).

  Coordinators do not call this module directly; they publish through a
  per-request `Telelem.Receipts.Writer`, which adds the private sequence
  metadata. `Store.producer/1` returns the handle; `binding/2` checks it
  against a separately pinned store pid and reads that store's actual mode
  and limits from the store's binding descriptor.

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
    * `evicted` – retained records removed by the ordinary retention limit
      (a counter only; see below)
    * loss slots – `loss_slots` flags indexed by `phash2({tenant, request})`,
      set **synchronously, before** the failing `record` returns, for every
      drop (overflow, store down) or rejection. Slots are never cleared, so
      collisions can only make a receipt look less complete, never more.

  Ordinary retention eviction does **not** set a loss slot: scheduled
  eviction is routine, and marking shared never-cleared slots for it would
  eventually mark every future receipt incomplete. The store records it on
  the receipt's own entry instead (`Telelem.Receipts.Store`).
  """

  @enforce_keys [:table, :descriptor, :ref, :max_pending, :max_event_bytes, :loss_slots]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          table: :ets.tid(),
          descriptor: :ets.tid(),
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

  @limit_keys [:loss_slots, :max_event_bytes, :max_pending, :max_retained]

  @doc """
  The actual mode and limits of `store`, if `producer` is exactly the
  canonical handle of that store; otherwise `:error`.

  `store` must be the store's pid as pinned by trusted setup (for example
  from `start_link/1` or the supervisor), obtained independently of
  `producer`: this function only confirms that the two belong together and
  never derives the expected store from the handle. ETS reads only: no
  message, no call, no `process_info`, so a busy or suspended store cannot
  delay it. Every check fails closed (`:error`, never a raise):

    * the handle's descriptor table exists, is unnamed, `:protected`, a
      `:set` with no heir and exactly one row, owned by `store`;
    * the handle's pending table exists, is a `:public` `:set` (key
      position 1) owned by the same `store`;
    * the descriptor's row is `{:binding, canonical, mode, limits}` with
      `canonical` exactly equal to `producer` (every field), `mode` in
      `:sandbox | :serving` and `limits` exactly
      `max_pending`/`max_retained`/`max_event_bytes`/`loss_slots`, positive
      integers consistent with the handle.

  A handle of a store that has exited is `:error` (its tables are gone). The
  result is a snapshot: if the store exits afterwards, publication through
  the handle drops as `{:dropped, :store_down}`, as for any handle.

  Trust limit: `store` is trusted. A non-store process that owns tables of
  this exact shape would be accepted if it were pinned; the check does not
  identify the pinned process's code.
  """
  @spec binding(t(), pid()) ::
          {:ok,
           %{
             mode: :sandbox | :serving,
             limits: %{
               max_pending: pos_integer(),
               max_retained: pos_integer(),
               max_event_bytes: pos_integer(),
               loss_slots: pos_integer()
             }
           }}
          | :error
  def binding(%__MODULE__{table: t, descriptor: d} = p, store)
      when is_reference(t) and is_reference(d) and is_pid(store) do
    with true <- descriptor?(d, store) and pending?(t, store),
         [{:binding, ^p, mode, limits}] when mode in [:sandbox, :serving] <-
           :ets.lookup(d, :binding),
         true <- limits?(limits, p) do
      {:ok, %{mode: mode, limits: limits}}
    else
      _ -> :error
    end
  rescue
    # A table that is gone (or was never a table) between the checks.
    ArgumentError -> :error
  end

  def binding(_, _), do: :error

  defp descriptor?(d, store) do
    :ets.info(d, :owner) == store and :ets.info(d, :protection) == :protected and
      :ets.info(d, :named_table) == false and :ets.info(d, :heir) == :none and
      :ets.info(d, :type) == :set and :ets.info(d, :keypos) == 1 and :ets.info(d, :size) == 1
  end

  defp pending?(t, store) do
    :ets.info(t, :owner) == store and :ets.info(t, :protection) == :public and
      :ets.info(t, :type) == :set and :ets.info(t, :keypos) == 1
  end

  defp limits?(%{} = l, p) when not is_struct(l) do
    l |> Map.keys() |> Enum.sort() == @limit_keys and
      Enum.all?(l, fn {_, v} -> is_integer(v) and v > 0 end) and
      l.max_pending == p.max_pending and l.max_event_bytes == p.max_event_bytes and
      l.loss_slots == p.loss_slots
  end

  defp limits?(_, _), do: false

  @doc """
  Loss counters from the shared array; does not contact the store.
  `loss?` is true once any record was dropped or rejected; ordinary
  retention eviction (`evicted`) is not loss and does not set it.
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

    Map.put(s, :loss?, s.overflow + s.store_down + s.rejected > 0)
  end
end
