defmodule Telelem.Test.Admission.Model do
  @moduledoc """
  Independent reference model of local admission (TEL-006).

  It shares **no** code with `Telelem.Admission.Ledger`: no aliases, no
  helpers, no cached counters. Every observation is recomputed from the
  per-op grant list. Grants are keyed by op id (an op has at most one grant,
  for ever), and owners are symbolic `{index, generation}`.

  Commands (symbolic; the driver maps ops and owners to real ids and pids):

      {:reserve, op, owner_ix, units}
      {:authorize, op} | {:cancel, op}
      {:settle, op, actual} | {:reconcile, op, actual}
      {:kill_owner, owner_ix}          # owner death
      :crash_authority                 # kill the authority; the successor fences
      {:advance, ms}                   # manual clock; due TTL timers fire in order

  Rules the model encodes:

    * reserve is all-or-none (one slot and the units) and idempotent per op
      for the same owner and units; any other reuse of the op id conflicts
    * a token is only valid in the incarnation that issued it
    * owner death releases a live slot once and refunds only never-dispatched
      units; dispatched units stay exposed until a settle/reconcile
    * a TTL timer only releases a grant that is still reserved, in its own
      incarnation
  """

  def new(%{slots: slots, units: units, ttl: ttl}) do
    %{
      slots: slots,
      units: units,
      ttl: ttl,
      inc: 1,
      now: 0,
      tseq: 0,
      grants: %{},
      timers: [],
      gens: %{}
    }
  end

  def owner(m, ix), do: {ix, Map.get(m.gens, ix, 0)}

  # -- transitions -------------------------------------------------------------

  def step(m, {:reserve, op, ix, units}) do
    me = owner(m, ix)

    case m.grants[op] do
      %{owner: ^me, units: ^units} ->
        {:duplicate, m}

      %{} ->
        {{:error, :op_conflict}, m}

      nil ->
        live = Enum.count(Map.values(m.grants), &(&1.st == :reserved or &1.st == :dispatched))

        committed =
          Enum.reduce(Map.values(m.grants), 0, fn g, acc ->
            case g.st do
              :settled -> acc + g.actual
              :released -> acc
              _ -> acc + g.units
            end
          end)

        cond do
          live >= m.slots ->
            {{:error, :no_slot}, m}

          units > m.units - committed ->
            {{:error, :insufficient_units}, m}

          true ->
            g = %{owner: me, units: units, st: :reserved, inc: m.inc, actual: nil}
            timer = {m.now + m.ttl, m.tseq, op, m.inc}

            {:ok,
             %{
               m
               | grants: Map.put(m.grants, op, g),
                 timers: m.timers ++ [timer],
                 tseq: m.tseq + 1
             }}
        end
    end
  end

  def step(m, {:authorize, op}) do
    tokened(m, op, fn
      %{st: :reserved} -> {:ok, :dispatched}
      %{st: :dispatched} -> :duplicate
      %{st: st} -> {:error, {:not_reserved, st}}
    end)
  end

  def step(m, {:cancel, op}) do
    tokened(m, op, fn
      %{st: :reserved} -> {:ok, :released}
      %{st: :dispatched} -> {:ok, :unresolved}
      _ -> :duplicate
    end)
  end

  def step(m, {:settle, op, actual}) do
    tokened(m, op, fn
      %{st: st} when st in [:dispatched, :unresolved] -> {:ok, {:settled, actual}}
      %{st: :settled, actual: ^actual} -> :duplicate
      %{st: :settled} -> {:error, :settle_conflict}
      _ -> {:error, :not_dispatched}
    end)
  end

  def step(m, {:reconcile, op, actual}) do
    case m.grants[op] do
      nil ->
        {{:error, :unknown_grant}, m}

      g ->
        case g do
          %{st: :unresolved} -> apply_change(m, op, g, {:settled, actual})
          %{st: :settled, actual: ^actual} -> {:duplicate, m}
          %{st: :settled} -> {{:error, :settle_conflict}, m}
          %{st: st} -> {{:error, {:not_unresolved, st}}, m}
        end
    end
  end

  def step(m, {:kill_owner, ix}) do
    me = owner(m, ix)

    live? =
      Enum.any?(m.grants, fn {_, g} -> g.owner == me and g.st in [:reserved, :dispatched] end)

    grants =
      Map.new(m.grants, fn
        {op, %{owner: ^me, st: :reserved} = g} -> {op, %{g | st: :released}}
        {op, %{owner: ^me, st: :dispatched} = g} -> {op, %{g | st: :unresolved}}
        other -> other
      end)

    {{:killed, live?}, %{m | grants: grants, gens: Map.put(m.gens, ix, elem(me, 1) + 1)}}
  end

  def step(m, :crash_authority), do: {:crashed, %{m | inc: m.inc + 1}}

  def step(m, {:advance, ms}) do
    now = m.now + ms
    {due, rest} = Enum.split_with(m.timers, fn {d, _, _, _} -> d <= now end)
    due = Enum.sort(due)

    {results, m} =
      Enum.map_reduce(due, %{m | now: now, timers: rest}, fn {_, _, op, inc}, m ->
        g = m.grants[op]

        cond do
          inc != m.inc -> {{:error, :stale_incarnation}, m}
          g.st == :reserved -> {:ok, put_in(m.grants[op].st, :released)}
          true -> {:duplicate, m}
        end
      end)

    {{:timers, results}, m}
  end

  defp tokened(m, op, rule) do
    case m.grants[op] do
      nil ->
        {{:error, :unknown_grant}, m}

      %{inc: inc} when inc != m.inc ->
        {{:error, :stale_incarnation}, m}

      g ->
        case rule.(g) do
          {:ok, change} -> apply_change(m, op, g, change)
          :duplicate -> {:duplicate, m}
          {:error, r} -> {{:error, r}, m}
        end
    end
  end

  defp apply_change(m, op, g, {:settled, actual}),
    do: {:ok, %{m | grants: Map.put(m.grants, op, %{g | st: :settled, actual: actual})}}

  defp apply_change(m, op, g, st), do: {:ok, %{m | grants: Map.put(m.grants, op, %{g | st: st})}}

  # -- observations ------------------------------------------------------------

  @doc "What the authority must report after every transition."
  def observe(m) do
    gs = Map.values(m.grants)
    units_in = fn sts -> for(g <- gs, g.st in sts, do: g.units) |> Enum.sum() end
    spent = for(g <- gs, g.st == :settled, do: g.actual) |> Enum.sum()
    active = Enum.count(gs, &(&1.st in [:reserved, :dispatched]))
    reserved = units_in.([:reserved])
    exposure = units_in.([:dispatched, :unresolved])

    %{
      incarnation: m.inc,
      active: active,
      available_slots: m.slots - active,
      reserved_units: reserved,
      encumbered_units: exposure,
      spent_units: spent,
      available_units: m.units - reserved - exposure - spent,
      states: Map.new(m.grants, fn {op, g} -> {op, g.st} end)
    }
  end
end
