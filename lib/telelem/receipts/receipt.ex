defmodule Telelem.Receipts.Receipt do
  @moduledoc """
  Read model for one `{tenant_id, request_id}`, derived from retained,
  sequenced records (M2a §2). Records are read in **sequence order**, never
  storage or arrival order. Sequence numbers stay internal; `events` are the
  plain §2 events in sequence order (duplicates and post-terminal records
  included, so they are visible rather than hidden).

  `completeness` and `observed_outcome` are separate facts. Evidence is
  `:complete` only when `gaps == []`. An observed `:done` in incomplete
  evidence is not a verified success.

  Gaps (fixed atoms, fixed order):

    * `:missing_sequence` – a number in `1..max retained sequence` is absent
      (a failed or evicted publication, or a lost ingress)
    * `:duplicate_sequence` – a sequence number is retained more than once
    * `:no_ingress` – sequence one is not a retained ingress
    * `:duplicate_ingress` – more than one ingress
    * `:no_terminal` – no retained terminal (never complete without one)
    * `:duplicate_terminal` – more than one terminal
    * `:after_terminal` – a record with a higher sequence than the first
      terminal
    * `:reported_drop` – a retained record declares an earlier failed
      publication by its writer
    * `:writer_conflict` – records from more than one writer process
    * `:loss` – the receipt's loss flag is set (drop, rejection or eviction
      that may belong to it; collisions only add false `:loss`)
    * `:evicted` – one of the receipt's own records was evicted

  Derived fields:

    * `observed_outcome` – outcome of the first terminal in sequence order
      (`:done | :incomplete | :failed`), else `:not_observed`; never inferred
      from absence
    * `exposure` – maximum (`none < possible < unknown`) over the events when
      complete, else `:unknown`
    * `usage` – always `:unknown` in M2a
  """

  alias Telelem.Receipts.Event

  @enforce_keys [
    :tenant_id,
    :request_id,
    :events,
    :completeness,
    :gaps,
    :observed_outcome,
    :exposure,
    :usage
  ]
  defstruct @enforce_keys

  @gaps [
    :missing_sequence,
    :duplicate_sequence,
    :no_ingress,
    :duplicate_ingress,
    :no_terminal,
    :duplicate_terminal,
    :after_terminal,
    :reported_drop,
    :writer_conflict,
    :loss,
    :evicted
  ]

  @type gap ::
          :missing_sequence
          | :duplicate_sequence
          | :no_ingress
          | :duplicate_ingress
          | :no_terminal
          | :duplicate_terminal
          | :after_terminal
          | :reported_drop
          | :writer_conflict
          | :loss
          | :evicted
  @type t :: %__MODULE__{
          tenant_id: Event.id(),
          request_id: Event.id(),
          events: [Event.t()],
          completeness: :complete | :incomplete,
          gaps: [gap()],
          observed_outcome: :done | :incomplete | :failed | :not_observed,
          exposure: Event.exposure(),
          usage: :unknown
        }

  @doc "All gap atoms in report order."
  def gaps, do: @gaps

  @doc false
  # `records` are `{seq, dropped_before, event}` in any order.
  # `ctx` has `lost?`, `evicted?` and `writer_conflict?` booleans.
  @spec derive({Event.id(), Event.id()}, [{pos_integer(), non_neg_integer(), Event.t()}], map()) ::
          t()
  def derive({tenant, request}, records, %{} = ctx) do
    sorted = Enum.sort_by(records, &elem(&1, 0))
    seqs = Enum.map(sorted, &elem(&1, 0))
    distinct = Enum.uniq(seqs)
    max_seq = List.last(seqs) || 0
    events = Enum.map(sorted, &elem(&1, 2))

    terminals = Enum.filter(sorted, fn {_, _, e} -> e.kind == :terminal end)
    first_terminal = List.first(terminals)

    found = %{
      missing_sequence: length(distinct) != max_seq,
      duplicate_sequence: length(distinct) != length(seqs),
      no_ingress: not match?([{1, _, %{kind: :ingress}} | _], sorted),
      duplicate_ingress: Enum.count(events, &(&1.kind == :ingress)) > 1,
      no_terminal: terminals == [],
      duplicate_terminal: length(terminals) > 1,
      after_terminal: first_terminal != nil and max_seq > elem(first_terminal, 0),
      reported_drop: Enum.any?(sorted, fn {_, d, _} -> d > 0 end),
      writer_conflict: ctx.writer_conflict?,
      loss: ctx.lost?,
      evicted: ctx.evicted?
    }

    gaps = Enum.filter(@gaps, &found[&1])
    complete? = gaps == []

    %__MODULE__{
      tenant_id: tenant,
      request_id: request,
      events: events,
      completeness: if(complete?, do: :complete, else: :incomplete),
      gaps: gaps,
      observed_outcome:
        case first_terminal do
          {_, _, e} -> e.outcome
          nil -> :not_observed
        end,
      exposure: if(complete?, do: max_exposure(events), else: :unknown),
      usage: :unknown
    }
  end

  defp max_exposure(events) do
    Enum.reduce(events, :none, fn
      _, :unknown -> :unknown
      %{exposure: :unknown}, _ -> :unknown
      %{exposure: :possible}, _ -> :possible
      _, acc -> acc
    end)
  end
end
