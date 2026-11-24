defmodule Telelem.Admission.Ledger do
  @moduledoc """
  Pure, single-owner grant ledger for one local admission authority (TEL-006, M1).

  This module holds no processes and performs no I/O. `Telelem.Admission.Authority`
  is its only production caller and applies exactly one ledger transition per
  message, so every transition is atomic with respect to the instance.

  ## Grant states

      reserve           authorize          settle
      ───────▶ reserved ─────────▶ dispatched ──────▶ settled
                 │                   │                  ▲
         cancel/ │           cancel/ │                  │ settle (late usage)
      expire/    │        owner down │                  │ or reconcile
      owner down ▼                   ▼                  │
              released           unresolved ────────────┘

  * `:reserved` — holds a slot and refundable units. Never dispatched, so it
    can be refunded.
  * `:dispatched` — holds a slot; its units are *encumbered*, because the
    upstream may already bill for this work.
  * `:unresolved` — no slot (no live worker), but its units stay encumbered.
    Losing the worker, or a client cancel after dispatch, never refunds the
    spend that might be billed.
  * `:released` / `:settled` — terminal states.

  ## Observations

  * **occupancy** (`active`): grants that are `:reserved` or `:dispatched`
  * **unresolved exposure** (`encumbered_units`): units of grants that are
    `:dispatched` or `:unresolved`
  * `available_units = limit - reserved - encumbered - spent`. If a
    settlement reports more than the reservation, this goes negative
    (truthful overspend).

  ## Fencing

  Grant ids are `{incarnation, seq}`. Calls that carry a token (authorize,
  cancel, settle, expire) are rejected with `:stale_incarnation` unless the
  token's incarnation is the ledger's current one. `owner_down/2` and
  `reconcile/3` come from the authority itself, so they accept any
  incarnation. `seq` survives `next_incarnation/1`, so ids are never reused.

  All transitions return `{:ok, grant_id, ledger}`, `{:duplicate, grant_id, ledger}`
  (no change), or `{:error, reason, ledger}` (no change). A duplicate or late
  event therefore cannot apply a transition twice.
  """

  @enforce_keys [:limits]
  defstruct limits: nil,
            incarnation: 0,
            seq: 0,
            grants: %{},
            ops: %{},
            active: 0,
            reserved_units: 0,
            encumbered_units: 0,
            spent_units: 0

  @type grant_id :: {non_neg_integer(), pos_integer()}
  @type state :: :reserved | :dispatched | :unresolved | :released | :settled
  @type grant :: %{
          id: grant_id(),
          op_id: term(),
          owner: pid() | term(),
          units: pos_integer(),
          state: state(),
          actual: non_neg_integer() | nil
        }
  @type limits :: %{slots: non_neg_integer(), units: non_neg_integer()}
  @type t :: %__MODULE__{}
  @type result :: {:ok | :duplicate, grant_id(), t()} | {:error, term(), t()}

  @live [:reserved, :dispatched]

  @spec new(limits()) :: t()
  def new(%{slots: slots, units: units} = limits)
      when is_integer(slots) and slots >= 0 and is_integer(units) and units >= 0 do
    %__MODULE__{limits: Map.take(limits, [:slots, :units])}
  end

  @doc "Advance to a new incarnation. Every older token becomes stale."
  @spec next_incarnation(t()) :: t()
  def next_incarnation(%__MODULE__{} = l), do: %{l | incarnation: l.incarnation + 1}

  @doc "All-or-none reservation of one slot plus `units`, idempotent by `op_id`."
  @spec reserve(t(), term(), term(), term()) :: result()
  def reserve(%__MODULE__{} = l, op_id, owner, units) do
    case Map.fetch(l.ops, op_id) do
      {:ok, gid} ->
        case Map.fetch!(l.grants, gid) do
          %{owner: ^owner, units: ^units} -> {:duplicate, gid, l}
          _ -> {:error, :op_conflict, l}
        end

      :error ->
        cond do
          not (is_integer(units) and units > 0) ->
            {:error, :invalid_units, l}

          l.active >= l.limits.slots ->
            {:error, :no_slot, l}

          units > available_units(l) ->
            {:error, :insufficient_units, l}

          true ->
            seq = l.seq + 1
            gid = {l.incarnation, seq}

            grant = %{
              id: gid,
              op_id: op_id,
              owner: owner,
              units: units,
              state: :reserved,
              actual: nil
            }

            {:ok, gid,
             %{
               l
               | seq: seq,
                 grants: Map.put(l.grants, gid, grant),
                 ops: Map.put(l.ops, op_id, gid),
                 active: l.active + 1,
                 reserved_units: l.reserved_units + units
             }}
        end
    end
  end

  @doc "reserved → dispatched. The units become encumbered (possibly billed)."
  @spec authorize(t(), term()) :: result()
  def authorize(l, gid) do
    with_current(l, gid, fn
      %{state: :reserved} = g -> {:ok, move(l, g, :dispatched)}
      %{state: :dispatched} -> :duplicate
      %{state: s} -> {:error, {:not_reserved, s}}
    end)
  end

  @doc "Client/worker cancel: reserved → released (refund), dispatched → unresolved (no refund)."
  @spec cancel(t(), term()) :: result()
  def cancel(l, gid) do
    with_current(l, gid, fn
      %{state: :reserved} = g -> {:ok, move(l, g, :released)}
      %{state: :dispatched} = g -> {:ok, move(l, g, :unresolved)}
      _ -> :duplicate
    end)
  end

  @doc "Reservation TTL timer. Only a still-`:reserved` grant is released."
  @spec expire(t(), term()) :: result()
  def expire(l, gid) do
    with_current(l, gid, fn
      %{state: :reserved} = g -> {:ok, move(l, g, :released)}
      _ -> :duplicate
    end)
  end

  @doc "Worker usage report for a dispatched or unresolved grant."
  @spec settle(t(), term(), term()) :: result()
  def settle(l, gid, actual) do
    with_current(l, gid, fn g -> settle_grant(l, g, actual, [:dispatched, :unresolved]) end)
  end

  @doc """
  Reconcile an `:unresolved` grant from an out-of-band usage source.
  Accepts any incarnation, because a fenced grant can only be resolved this way.
  """
  @spec reconcile(t(), term(), term()) :: result()
  def reconcile(l, gid, actual) do
    with_known(l, gid, fn g -> settle_grant(l, g, actual, [:unresolved]) end)
  end

  @doc "The authority's monitor saw the owner die. Accepts any incarnation."
  @spec owner_down(t(), term()) :: result()
  def owner_down(l, gid) do
    with_known(l, gid, fn
      %{state: :reserved} = g -> {:ok, move(l, g, :released)}
      %{state: :dispatched} = g -> {:ok, move(l, g, :unresolved)}
      _ -> :duplicate
    end)
  end

  @spec available_units(t()) :: integer()
  def available_units(l),
    do: l.limits.units - l.reserved_units - l.encumbered_units - l.spent_units

  @spec live?(grant()) :: boolean()
  def live?(%{state: s}), do: s in @live

  @doc "`[{owner, grant_id}]` for grants that still hold a slot (the authority monitors these owners)."
  @spec live_owners(t()) :: [{term(), grant_id()}]
  def live_owners(l) do
    for {gid, g} <- l.grants, live?(g), do: {g.owner, gid}
  end

  @spec grant(t(), grant_id()) :: grant() | nil
  def grant(l, gid), do: Map.get(l.grants, gid)

  @spec snapshot(t()) :: map()
  def snapshot(l) do
    %{
      incarnation: l.incarnation,
      limits: l.limits,
      active: l.active,
      available_slots: l.limits.slots - l.active,
      reserved_units: l.reserved_units,
      encumbered_units: l.encumbered_units,
      spent_units: l.spent_units,
      available_units: available_units(l),
      grants: l.grants
    }
  end

  @doc "Recompute the cached counters from the grants. Self-check only; the test model does not use it."
  @spec consistent?(t()) :: boolean()
  def consistent?(l) do
    gs = Map.values(l.grants)

    sum = fn states, f ->
      gs |> Enum.filter(&(&1.state in states)) |> Enum.map(f) |> Enum.sum()
    end

    l.active == Enum.count(gs, &live?/1) and
      l.reserved_units == sum.([:reserved], & &1.units) and
      l.encumbered_units == sum.([:dispatched, :unresolved], & &1.units) and
      l.spent_units == sum.([:settled], & &1.actual) and
      map_size(l.ops) == map_size(l.grants)
  end

  # -- internals -------------------------------------------------------------

  defp with_current(l, {inc, seq} = gid, fun) when is_integer(inc) and is_integer(seq) do
    if inc != l.incarnation, do: {:error, :stale_incarnation, l}, else: with_known(l, gid, fun)
  end

  defp with_current(l, _gid, _fun), do: {:error, :invalid_token, l}

  defp with_known(l, gid, fun) do
    case Map.fetch(l.grants, gid) do
      :error ->
        {:error, :unknown_grant, l}

      {:ok, g} ->
        case fun.(g) do
          {:ok, l2} -> {:ok, gid, l2}
          :duplicate -> {:duplicate, gid, l}
          {:error, reason} -> {:error, reason, l}
        end
    end
  end

  defp settle_grant(_l, _g, actual, _from) when not (is_integer(actual) and actual >= 0),
    do: {:error, :invalid_amount}

  defp settle_grant(l, g, actual, from) do
    cond do
      g.state in from ->
        {:ok, move(%{l | spent_units: l.spent_units + actual}, %{g | actual: actual}, :settled)}

      g.state == :settled and g.actual == actual ->
        :duplicate

      g.state == :settled ->
        {:error, :settle_conflict}

      :unresolved in from and :dispatched not in from ->
        {:error, {:not_unresolved, g.state}}

      true ->
        {:error, :not_dispatched}
    end
  end

  # Moves one grant and updates the cached counters by the delta of its contribution.
  defp move(l, g, to) do
    from = g.state
    u = g.units

    l = %{
      l
      | active: l.active - live(from) + live(to),
        reserved_units: l.reserved_units - held(from, u) + held(to, u),
        encumbered_units: l.encumbered_units - enc(from, u) + enc(to, u)
    }

    %{l | grants: Map.put(l.grants, g.id, %{g | state: to})}
  end

  defp live(s) when s in @live, do: 1
  defp live(_), do: 0
  defp held(:reserved, u), do: u
  defp held(_, _), do: 0
  defp enc(s, u) when s in [:dispatched, :unresolved], do: u
  defp enc(_, _), do: 0
end
