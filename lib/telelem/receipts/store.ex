defmodule Telelem.Receipts.Store do
  @moduledoc """
  Standalone, bounded, payload-free evidence store (M2a contract §2).

  Not wired into serving. A coordinator gets the shared handle with
  `producer/1` and opens one `Telelem.Receipts.Writer` per request; each
  accepted record is published by **one** atomic `:ets.insert_new/2` into a
  fixed-key pending table owned by this process (see
  `Telelem.Receipts.Producer`). There is no event mailbox: the store empties
  pending slots with `:ets.take/2` on a periodic sweep (`sweep_ms`) and before
  every `lookup/3` and `stats/1`, so a read sees every record whose
  publication completed before the read was issued.

  ## Limits (options, contract defaults)

    * `max_pending: 256` – pending slots (hard bound on unswept records)
    * `max_retained: 1024` – retained records; the oldest published record is
      evicted first. Eviction is counted (`evicted`) and recorded only on the
      receipt's own entry: a partly evicted receipt reports `:evicted` (plus
      the resulting sequence gaps), a fully evicted one is not found. It never
      sets a shared loss slot, so routine retention cannot make other
      receipts incomplete. If the same live writer publishes again after its
      receipt was fully evicted, the new entry starts above sequence one and
      reads `:missing_sequence`/`:no_ingress`. Request-ID reuse by another
      writer after full eviction is forbidden by the contract and is not
      detected (the earlier owner is forgotten with the entry).
    * `max_event_bytes: 512` – canonical encoding per event
    * `loss_slots: 1024` – fixed number of per-receipt loss flags
    * `sweep_ms: 10` – pending sweep interval

  These bound event storage, not VM or process memory (records, the per-
  request index and the receipt built for a reply are ordinary heap data).

  The required option `mode: :sandbox | :serving` is the store's binding
  mode; readers must have the same mode.

  ## Reads

  `lookup/3` takes an **already authenticated** principal (§3 shape:
  `tenant_id`, `subject_id`, `mode`, `permissions`); authentication itself
  happens outside the store, which only checks the shape it is handed. It
  requires valid ids, `mode` equal to the store mode, and `:receipt_read` in
  `permissions` (a `MapSet` or list); otherwise
  `{:error, {:denied, :unauthenticated | :mode_mismatch | :missing_permission}}`,
  decided before any data is touched. Only the exact
  `{principal.tenant_id, request_id}` key is read: another tenant's request
  and a request with no retained evidence both return `{:error, :not_found}`,
  which is never evidence of success. Completeness rules are in
  `Telelem.Receipts.Receipt`.

  ## Not durable

  Pending and retained evidence live in this process (its ETS table and
  state) and are lost when it exits. After that, writers get
  `{:dropped, :store_down}` and the loss counters in the shared `:atomics`
  array remain readable through `Producer.stats/1`. A restarted store is a new
  store with new handles and no memory of earlier receipts.

  The pending table is `:public` so writers can insert without a message;
  code holding the handle is trusted in-VM code. The store still re-validates
  every row it takes and discards (and counts as `rejected`) malformed ones.

  The store does not log. `format_status/1` reduces the state to counts.
  """
  use GenServer

  alias Telelem.Receipts.{Event, Producer, Receipt}

  @defaults [
    max_pending: 256,
    max_retained: 1024,
    max_event_bytes: 512,
    loss_slots: 1024,
    sweep_ms: 10
  ]
  @modes [:sandbox, :serving]

  # -- API -------------------------------------------------------------------

  def start_link(opts \\ []) do
    {gen_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Shared publication handle; pass it to `Telelem.Receipts.Writer.open/3`."
  @spec producer(GenServer.server()) :: Producer.t()
  def producer(store), do: GenServer.call(store, :producer)

  @type denied :: {:denied, :unauthenticated | :mode_mismatch | :missing_permission}

  @spec lookup(GenServer.server(), term(), term()) ::
          {:ok, Receipt.t()} | {:error, :not_found | denied()}
  def lookup(store, principal, request_id),
    do: GenServer.call(store, {:lookup, principal, request_id})

  @doc "Loss counters plus pending/retained counts and limits."
  @spec stats(GenServer.server()) :: map()
  def stats(store), do: GenServer.call(store, :stats)

  # -- Server ----------------------------------------------------------------

  @impl true
  def init(opts) do
    {mode, opts} = Keyword.pop(opts, :mode)

    unless mode in @modes,
      do: raise(ArgumentError, "receipts store requires mode: :sandbox | :serving")

    opts = Keyword.merge(@defaults, opts)

    for {k, v} <- opts do
      unless k in Keyword.keys(@defaults) and is_integer(v) and v > 0,
        do: raise(ArgumentError, "invalid receipts store option #{inspect(k)}")
    end

    table = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])

    producer = %Producer{
      table: table,
      ref: :atomics.new(Producer.counters_size(opts[:loss_slots]), signed: true),
      max_pending: opts[:max_pending],
      max_event_bytes: opts[:max_event_bytes],
      loss_slots: opts[:loss_slots]
    }

    schedule(opts[:sweep_ms])

    {:ok,
     %{
       mode: mode,
       producer: producer,
       sweep_ms: opts[:sweep_ms],
       max_retained: opts[:max_retained],
       retained: 0,
       # {stamp, key} per retained record, oldest published first
       order: :gb_sets.new(),
       # key => %{records: %{stamp => {seq, dropped_before, event}},
       #          owner: pid, writer_conflict?: boolean, evicted?: boolean}
       by_key: %{}
     }}
  end

  @impl true
  def handle_call(:producer, _from, s), do: {:reply, s.producer, s}

  def handle_call(:stats, _from, s) do
    s = sweep(s)
    p = s.producer

    stats =
      p
      |> Producer.stats()
      |> Map.merge(%{
        pending: :ets.info(p.table, :size),
        retained: s.retained,
        receipts: map_size(s.by_key),
        limits: %{
          max_pending: p.max_pending,
          max_retained: s.max_retained,
          max_event_bytes: p.max_event_bytes,
          loss_slots: p.loss_slots
        }
      })

    {:reply, stats, s}
  end

  def handle_call({:lookup, principal, request_id}, _from, s) do
    case authorize(principal, s.mode) do
      {:ok, tenant} ->
        s = sweep(s)
        {:reply, read(s, {tenant, request_id}), s}

      denied ->
        {:reply, {:error, denied}, s}
    end
  end

  @impl true
  def handle_info(:sweep, s) do
    schedule(s.sweep_ms)
    {:noreply, sweep(s)}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def format_status(status) do
    Map.update(status, :state, nil, fn
      %{retained: n, by_key: m} -> %{retained: n, receipts: map_size(m)}
      _ -> :redacted
    end)
  end

  # -- Internals -------------------------------------------------------------

  defp schedule(ms), do: Process.send_after(self(), :sweep, ms)

  defp authorize(
         %{tenant_id: tenant, subject_id: subject, mode: mode, permissions: perms},
         store_mode
       ) do
    cond do
      not (Event.valid_id?(tenant) and Event.valid_id?(subject) and mode in @modes) ->
        {:denied, :unauthenticated}

      mode != store_mode ->
        {:denied, :mode_mismatch}

      not permitted?(perms) ->
        {:denied, :missing_permission}

      true ->
        {:ok, tenant}
    end
  end

  defp authorize(_, _), do: {:denied, :unauthenticated}

  defp permitted?(%MapSet{} = perms), do: MapSet.member?(perms, :receipt_read)
  defp permitted?(perms) when is_list(perms), do: :receipt_read in perms
  defp permitted?(_), do: false

  # Take every occupied slot, then ingest in publication-stamp order (slot
  # order is meaningless). Reads order by sequence, not by this order.
  defp sweep(%{producer: p} = s) do
    if :ets.info(p.table, :size) == 0 do
      s
    else
      1..p.max_pending
      |> Enum.flat_map(&:ets.take(p.table, &1))
      |> Enum.sort_by(&elem(&1, 1))
      |> Enum.reduce(s, &ingest(&2, &1))
      |> evict()
    end
  end

  defp ingest(s, {_slot, stamp, {_, _} = key, owner, seq, dropped, event} = _row)
       when is_integer(stamp) and is_pid(owner) and is_integer(seq) and seq > 0 and
              is_integer(dropped) and dropped >= 0 do
    case Event.new(event) do
      {:ok, %Event{tenant_id: t, request_id: r} = e} when {t, r} == key ->
        if :gb_sets.is_member({stamp, key}, s.order),
          do: discard(s, key),
          else: retain(s, stamp, key, owner, {seq, dropped, e})

      _ ->
        discard(s, key)
    end
  end

  # Only reachable by writing to the public table without a Writer.
  defp ingest(s, row), do: discard(s, if(tuple_size(row) >= 3, do: elem(row, 2)))

  defp discard(%{producer: p} = s, key) do
    Producer.count(p, :rejected)

    case key do
      {t, r} -> if Event.valid_id?(t) and Event.valid_id?(r), do: Producer.mark_lost(p, key)
      _ -> :ok
    end

    s
  end

  defp retain(s, stamp, key, owner, record) do
    entry =
      case Map.fetch(s.by_key, key) do
        {:ok, entry} ->
          %{entry | writer_conflict?: entry.writer_conflict? or entry.owner != owner}

        :error ->
          %{records: %{}, owner: owner, writer_conflict?: false, evicted?: false}
      end

    entry = %{entry | records: Map.put(entry.records, stamp, record)}

    %{
      s
      | by_key: Map.put(s.by_key, key, entry),
        order: :gb_sets.add({stamp, key}, s.order),
        retained: s.retained + 1
    }
  end

  defp evict(%{retained: n, max_retained: max} = s) when n <= max, do: s

  defp evict(%{producer: p} = s) do
    {{stamp, key}, order} = :gb_sets.take_smallest(s.order)
    entry = Map.fetch!(s.by_key, key)
    records = Map.delete(entry.records, stamp)

    # Counted, but no shared loss slot (see moduledoc): entry-local only.
    Producer.count(p, :evicted)

    by_key =
      if records == %{},
        do: Map.delete(s.by_key, key),
        else: Map.put(s.by_key, key, %{entry | records: records, evicted?: true})

    evict(%{s | order: order, by_key: by_key, retained: s.retained - 1})
  end

  defp read(s, key) do
    case Map.fetch(s.by_key, key) do
      {:ok, entry} ->
        ctx = %{
          lost?: Producer.lost?(s.producer, key),
          evicted?: entry.evicted?,
          writer_conflict?: entry.writer_conflict?
        }

        # Publication-stamp order first, so equal sequence numbers keep a
        # deterministic order; `derive/3` then orders by sequence.
        records = entry.records |> Enum.sort() |> Enum.map(&elem(&1, 1))
        {:ok, Receipt.derive(key, records, ctx)}

      :error ->
        {:error, :not_found}
    end
  end
end
