defmodule Telelem.Receipts.Receipt do
  @moduledoc """
  Read model for one `{tenant_id, request_id}`, derived from retained
  events. Completeness is conservative: a receipt is `:complete` only when
  `gaps == []`, and every unknown stays unknown.

  Gaps (fixed atoms):

    * `:no_ingress` / `:duplicate_ingress` – not exactly one ingress retained
    * `:no_terminal` / `:duplicate_terminal` – not exactly one terminal retained
    * `:loss` – this receipt's loss slot is set: an event that may belong to
      it was dropped at admission, rejected as invalid, or evicted
      (slot collisions only add false `:loss`, never hide one)
    * `:evicted` – one of this receipt's own events was evicted
    * `:unattributed_loss` – an invalid event without usable ids was
      rejected after this receipt's ingress was emitted

  Derived fields:

    * `outcome` – the **first** retained terminal's outcome (a recorded
      terminal stays terminal), else `:unknown`; never inferred from absence
    * `exposure` – the maximum (`none < possible < unknown`) over retained
      events when complete, else `:unknown`
    * `usage` – always `:unknown`: the §2 event carries no usage field yet

  `events` are in store arrival order; each carries its own `at_ms`.
  """

  alias Telelem.Receipts.Event

  @enforce_keys [
    :tenant_id,
    :request_id,
    :events,
    :completeness,
    :gaps,
    :outcome,
    :exposure,
    :usage
  ]
  defstruct @enforce_keys

  @type gap ::
          :no_ingress
          | :duplicate_ingress
          | :no_terminal
          | :duplicate_terminal
          | :loss
          | :evicted
          | :unattributed_loss
  @type t :: %__MODULE__{
          tenant_id: Event.id(),
          request_id: Event.id(),
          events: [Event.t()],
          completeness: :complete | :incomplete,
          gaps: [gap()],
          outcome: :done | :incomplete | :failed | :unknown,
          exposure: Event.exposure(),
          usage: :unknown
        }

  @doc false
  @spec derive({Event.id(), Event.id()}, [Event.t()], map()) :: t()
  def derive({tenant, request}, events, %{} = ctx) do
    ingresses = Enum.count(events, &(&1.kind == :ingress))
    terminals = Enum.filter(events, &(&1.kind == :terminal))

    gaps =
      [
        ingresses == 0 && :no_ingress,
        ingresses > 1 && :duplicate_ingress,
        terminals == [] && :no_terminal,
        length(terminals) > 1 && :duplicate_terminal,
        ctx.lost? && :loss,
        ctx.evicted? && :evicted,
        (ctx.ingress_epoch != nil and ctx.ingress_epoch != ctx.unattributed_now) &&
          :unattributed_loss
      ]
      |> Enum.filter(& &1)

    complete? = gaps == []

    %__MODULE__{
      tenant_id: tenant,
      request_id: request,
      events: events,
      completeness: if(complete?, do: :complete, else: :incomplete),
      gaps: gaps,
      outcome:
        case terminals do
          [first | _] -> first.outcome
          [] -> :unknown
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
