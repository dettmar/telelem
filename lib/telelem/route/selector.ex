defmodule Telelem.Route.Selector do
  @moduledoc """
  Pure route selector (M2a contract §1).

      select(revision, eligible: set<Id>, monotonic_now_ms)
        -> Plan | NoEligibleDeployment

  Called once per ingress. Independent of transport, store and global clock:
  the caller supplies eligibility and its own monotonic time. No reselection
  happens here after an ambiguous send or commit; the runtime rechecks
  deadline, attempt budget and pinned authority before each dispatch.

  Rules (deployments `[p, o]` where `p` is preferred):

    * `p` eligible → `primary = p`; `backup = o` iff `o` is eligible,
      `fallback_on_refusal` is true and `max_attempts == 2`; else `:none`.
    * `p` not eligible, `o` eligible → `primary = o`, `backup = :none`.
    * neither eligible → `{:error, :no_eligible_deployment}`.

  IDs in `eligible` that are not deployments of the revision are ignored.
  """

  alias Telelem.Route.{Plan, Revision}

  @type eligible :: MapSet.t(Revision.id()) | [Revision.id()]

  @spec select(Revision.t(), eligible(), integer()) ::
          {:ok, Plan.t()} | {:error, :no_eligible_deployment}
  def select(%Revision{} = r, eligible, monotonic_now_ms)
      when is_integer(monotonic_now_ms) do
    eligible = to_set(eligible)
    [other] = r.deployments -- [r.preferred]
    pref? = MapSet.member?(eligible, r.preferred)
    other? = MapSet.member?(eligible, other)

    case {pref?, other?} do
      {false, false} ->
        {:error, :no_eligible_deployment}

      {true, _} ->
        backup =
          if other? and r.fallback_on_refusal and r.max_attempts == 2, do: other, else: :none

        {:ok, plan(r, r.preferred, backup, monotonic_now_ms)}

      {false, true} ->
        {:ok, plan(r, other, :none, monotonic_now_ms)}
    end
  end

  defp plan(r, primary, backup, now) do
    %Plan{
      rev: r.rev,
      digest: r.digest,
      primary: primary,
      backup: backup,
      absolute_deadline_ms: now + r.deadline_ms,
      max_attempts: if(backup == :none, do: 1, else: 2)
    }
  end

  defp to_set(%MapSet{} = s), do: s
  defp to_set(list) when is_list(list), do: MapSet.new(list)
end
