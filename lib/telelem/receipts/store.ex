defmodule Telelem.Receipts.Store do
  @moduledoc """
  Standalone, bounded, payload-free evidence store (M2a contract §2).

  Not wired into serving. Producers get a `Telelem.Receipts.Producer` via
  `producer/1` and call `Producer.record/2`, which validates, admits against
  the shared pending bound **before** the asynchronous `send/2`, and never
  waits on this process (see `Telelem.Receipts.Producer`).

  ## Limits (options, contract defaults)

    * `max_pending: 256` – admitted, not yet processed event messages
    * `max_retained: 1024` – retained events; the oldest event (by arrival)
      is evicted first and counted as loss for its receipt
    * `max_event_bytes: 512` – canonical encoding per event
    * `loss_slots: 1024` – fixed number of per-receipt loss flags

  These bound event storage, not VM allocation.

  ## Reads

  `lookup/3` takes the **authenticated principal** (any map or struct with
  a valid `tenant_id`, e.g. a §3 `Principal`) and a request id. Only the
  exact `{principal.tenant_id, request_id}` key is read. Another tenant's
  request and a request with no retained evidence both return
  `{:error, :not_found}`; that result is never evidence of success. A
  principal without a valid tenant id gets `{:error, :denied}`. The tenant is
  never taken from event or request data. Before answering, the store
  processes up to `max_pending` already-delivered event messages so a read
  sees events that were admitted before it.

  Completeness rules are in `Telelem.Receipts.Receipt`. They assume every
  event of a request is emitted after that request's ingress event.

  The store does not log. `format_status/1` reduces the state to counts.
  """
  use GenServer

  alias Telelem.Receipts.{Event, Producer, Receipt}

  @defaults [max_pending: 256, max_retained: 1024, max_event_bytes: 512, loss_slots: 1024]

  # -- API -------------------------------------------------------------------

  def start_link(opts \\ []) do
    {gen_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Producer handle for non-blocking `Producer.record/2`."
  @spec producer(GenServer.server()) :: Producer.t()
  def producer(store), do: GenServer.call(store, :producer)

  @spec lookup(GenServer.server(), term(), term()) ::
          {:ok, Receipt.t()} | {:error, :not_found | :denied}
  def lookup(store, principal, request_id),
    do: GenServer.call(store, {:lookup, principal, request_id})

  @doc "Loss counters plus retained counts and limits."
  @spec stats(GenServer.server()) :: map()
  def stats(store), do: GenServer.call(store, :stats)

  # -- Server ----------------------------------------------------------------

  @impl true
  def init(opts) do
    opts = Keyword.merge(@defaults, opts)

    for {k, v} <- opts do
      unless k in Keyword.keys(@defaults) and is_integer(v) and v > 0,
        do: raise(ArgumentError, "invalid receipts store option #{inspect(k)}")
    end

    producer = %Producer{
      pid: self(),
      token: make_ref(),
      ref: :atomics.new(Producer.counters_size(opts[:loss_slots]), signed: true),
      max_pending: opts[:max_pending],
      max_event_bytes: opts[:max_event_bytes],
      loss_slots: opts[:loss_slots]
    }

    {:ok,
     %{
       producer: producer,
       max_retained: opts[:max_retained],
       retained: 0,
       # one key per retained event, oldest first
       order: :queue.new(),
       # key => %{events: :queue, ingress_epoch: integer | nil, evicted?: boolean}
       by_key: %{}
     }}
  end

  @impl true
  def handle_call(:producer, _from, s), do: {:reply, s.producer, s}

  def handle_call(:stats, _from, s) do
    s = drain(s)

    stats =
      s.producer
      |> Producer.stats()
      |> Map.merge(%{
        retained: s.retained,
        receipts: map_size(s.by_key),
        limits: %{
          max_pending: s.producer.max_pending,
          max_retained: s.max_retained,
          max_event_bytes: s.producer.max_event_bytes,
          loss_slots: s.producer.loss_slots
        }
      })

    {:reply, stats, s}
  end

  def handle_call({:lookup, principal, request_id}, _from, s) do
    case principal do
      %{tenant_id: tenant} when is_binary(tenant) ->
        if Event.valid_id?(tenant) do
          s = drain(s)
          {:reply, read(s, {tenant, request_id}), s}
        else
          {:reply, {:error, :denied}, s}
        end

      _ ->
        {:reply, {:error, :denied}, s}
    end
  end

  @impl true
  def handle_info(
        {:telelem_receipt_event, token, epoch, event},
        %{producer: %{token: token}} = s
      ),
      do: {:noreply, ingest(s, epoch, event)}

  # Anything else (including forged event messages without our token) is
  # ignored without touching the pending counter.
  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def format_status(status) do
    Map.update(status, :state, nil, fn
      %{retained: n, by_key: m} -> %{retained: n, receipts: map_size(m)}
      _ -> :redacted
    end)
  end

  # -- Internals -------------------------------------------------------------

  defp drain(s), do: drain(s, s.producer.max_pending)

  defp drain(s, 0), do: s

  defp drain(%{producer: %{token: token}} = s, n) do
    receive do
      {:telelem_receipt_event, ^token, epoch, event} -> s |> ingest(epoch, event) |> drain(n - 1)
    after
      0 -> s
    end
  end

  defp ingest(%{producer: p} = s, epoch, event) do
    s =
      case Event.new(event) do
        {:ok, event} ->
          s |> retain(epoch, event) |> evict()

        {:error, _} ->
          # Unreachable through Producer.record/2; never retain regardless.
          :atomics.add(p.ref, Producer.ix(:rejected), 1)
          :atomics.add(p.ref, Producer.ix(:unattributed), 1)
          s
      end

    # Release the admission slot only after the event is fully processed.
    :atomics.sub(p.ref, Producer.ix(:pending), 1)
    s
  end

  defp retain(s, epoch, %Event{} = e) do
    key = {e.tenant_id, e.request_id}
    entry = Map.get(s.by_key, key, %{events: :queue.new(), ingress_epoch: nil, evicted?: false})

    entry = %{
      entry
      | events: :queue.in(e, entry.events),
        ingress_epoch:
          if(e.kind == :ingress and entry.ingress_epoch == nil,
            do: epoch,
            else: entry.ingress_epoch
          )
    }

    %{
      s
      | by_key: Map.put(s.by_key, key, entry),
        order: :queue.in(key, s.order),
        retained: s.retained + 1
    }
  end

  defp evict(%{retained: n, max_retained: max} = s) when n <= max, do: s

  defp evict(%{producer: p} = s) do
    {{:value, key}, order} = :queue.out(s.order)
    entry = Map.fetch!(s.by_key, key)
    {{:value, _oldest}, events} = :queue.out(entry.events)

    :atomics.add(p.ref, Producer.ix(:evicted), 1)
    Producer.mark_lost(p, key)

    by_key =
      if :queue.is_empty(events),
        do: Map.delete(s.by_key, key),
        else: Map.put(s.by_key, key, %{entry | events: events, evicted?: true})

    evict(%{s | order: order, by_key: by_key, retained: s.retained - 1})
  end

  defp read(s, key) do
    case Map.fetch(s.by_key, key) do
      {:ok, entry} ->
        ctx = %{
          lost?: Producer.lost?(s.producer, key),
          evicted?: entry.evicted?,
          ingress_epoch: entry.ingress_epoch,
          unattributed_now: :atomics.get(s.producer.ref, Producer.ix(:unattributed))
        }

        {:ok, Receipt.derive(key, :queue.to_list(entry.events), ctx)}

      :error ->
        {:error, :not_found}
    end
  end
end
