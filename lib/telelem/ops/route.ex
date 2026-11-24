defmodule Telelem.Ops.Route do
  @moduledoc """
  Route operation handlers (m2a-contracts §4): `plan/5`, `apply/6`, `read/3`.

  Every entry point (the JSON CLI adapter today; MCP and UI after day two)
  calls these functions. The only editable field is `preferred`.

  ## Authority

  The principal comes from the caller's authenticated context, never from
  request data. The tenant used for every store access is
  `principal.tenant_id`; a route of another tenant is indistinguishable from a
  missing one (`:not_found`). The principal's `mode` must equal the context's
  mode (sandbox principals cannot act on a serving context, and vice versa).
  Permissions: `:route_read` for `read/3`, `:route_write` for `plan/5` and
  `apply/6`.

  ## Apply

  `apply/6` re-reads the effective revision, rejects a different `rev` as a
  conflict, re-derives the proposal through the injected `Revisions` module,
  recomputes the plan digest and compares it with the caller's, and then asks
  the store for an atomic compare-and-write conditioned on the read `rev`
  **and** revision digest. A concurrent writer between the read and the write
  therefore produces `{:conflict, _}` and no write.
  """

  alias Telelem.Ops.{Context, PlanDigest}

  @fixed_fields [:route_id, :deployments, :fallback_on_refusal, :deadline_ms, :max_attempts]

  @type principal :: %{
          required(:tenant_id) => String.t(),
          required(:subject_id) => String.t(),
          required(:mode) => :sandbox | :serving,
          required(:permissions) => MapSet.t(atom()) | [atom()],
          optional(atom()) => term()
        }

  @type denied :: {:denied, :unauthenticated | :mode_mismatch | :missing_permission}
  @type invalid ::
          {:invalid, :route_id | :expected_rev | :preferred | :plan_digest}
          | {:invalid, {:revision, term()}}
  @type conflict ::
          {:conflict,
           %{
             reason: :stale_revision | :plan_digest_mismatch,
             expected_rev: pos_integer(),
             current_rev: pos_integer() | nil,
             last_plan_digest: String.t() | nil
           }}
  @type fault :: {:error, {:revision_invariant, atom()}}

  @spec plan(principal(), Context.t(), String.t(), pos_integer(), String.t()) ::
          {:ok, %{expected_rev: pos_integer(), plan_digest: String.t(), proposed: map()}}
          | denied()
          | invalid()
          | conflict()
          | :not_found
          | fault()
  def plan(principal, %Context{} = ctx, route_id, expected_rev, preferred) do
    with {:ok, tenant} <- authorize(principal, ctx, :route_write),
         :ok <- validate_edit(route_id, expected_rev, preferred),
         {:ok, record} <- fetch(ctx, tenant, route_id),
         :ok <- check_rev(record, expected_rev),
         {:ok, proposed} <- propose(ctx, record.revision, preferred) do
      digest = PlanDigest.compute(tenant, route_id, expected_rev, preferred, proposed.digest)
      {:ok, %{expected_rev: expected_rev, plan_digest: digest, proposed: proposed}}
    end
  end

  @spec apply(principal(), Context.t(), String.t(), pos_integer(), String.t(), String.t()) ::
          {:ok, %{effective_revision: map(), plan_digest: String.t()}}
          | denied()
          | invalid()
          | conflict()
          | :not_found
          | fault()
  def apply(principal, %Context{} = ctx, route_id, expected_rev, preferred, plan_digest) do
    with {:ok, tenant} <- authorize(principal, ctx, :route_write),
         :ok <- validate_edit(route_id, expected_rev, preferred),
         :ok <- validate_digest(plan_digest),
         {:ok, record} <- fetch(ctx, tenant, route_id),
         :ok <- check_rev(record, expected_rev),
         {:ok, proposed} <- propose(ctx, record.revision, preferred),
         recomputed =
           PlanDigest.compute(tenant, route_id, expected_rev, preferred, proposed.digest),
         :ok <- check_plan(recomputed, plan_digest, record, expected_rev) do
      {mod, arg} = ctx.store
      expected = %{rev: record.revision.rev, digest: record.revision.digest}

      case mod.compare_and_write(arg, tenant, route_id, expected, proposed, recomputed) do
        {:ok, written} ->
          {:ok, %{effective_revision: written.revision, plan_digest: written.last_plan_digest}}

        {:conflict, :not_found} ->
          conflict(:stale_revision, expected_rev, nil)

        {:conflict, current} ->
          conflict(:stale_revision, expected_rev, current)
      end
    end
  end

  @spec read(principal(), Context.t(), String.t()) ::
          {:ok, %{effective_revision: map(), last_plan_digest: String.t() | nil}}
          | denied()
          | invalid()
          | :not_found
  def read(principal, %Context{} = ctx, route_id) do
    with {:ok, tenant} <- authorize(principal, ctx, :route_read),
         :ok <- validate_id(route_id, :route_id),
         {:ok, record} <- fetch(ctx, tenant, route_id) do
      {:ok, %{effective_revision: record.revision, last_plan_digest: record.last_plan_digest}}
    end
  end

  ## Authority

  defp authorize(
         %{tenant_id: tenant, subject_id: subject, mode: mode, permissions: perms},
         %Context{mode: ctx_mode},
         needed
       ) do
    cond do
      not (id?(tenant) and id?(subject) and mode in [:sandbox, :serving]) ->
        {:denied, :unauthenticated}

      mode != ctx_mode ->
        {:denied, :mode_mismatch}

      not has_permission?(perms, needed) ->
        {:denied, :missing_permission}

      true ->
        {:ok, tenant}
    end
  end

  defp authorize(_principal, _ctx, _needed), do: {:denied, :unauthenticated}

  defp has_permission?(%MapSet{} = perms, needed), do: MapSet.member?(perms, needed)
  defp has_permission?(perms, needed) when is_list(perms), do: needed in perms
  defp has_permission?(_, _), do: false

  ## Validation

  defp validate_edit(route_id, expected_rev, preferred) do
    with :ok <- validate_id(route_id, :route_id),
         :ok <- validate_rev(expected_rev) do
      validate_id(preferred, :preferred)
    end
  end

  defp validate_rev(rev) when is_integer(rev) and rev > 0, do: :ok
  defp validate_rev(_), do: {:invalid, :expected_rev}

  defp validate_id(id, field), do: if(id?(id), do: :ok, else: {:invalid, field})

  defp validate_digest(digest) do
    if hex_digest?(digest), do: :ok, else: {:invalid, :plan_digest}
  end

  defp hex_digest?(d) when is_binary(d) and byte_size(d) == 64, do: hex?(d)
  defp hex_digest?(_), do: false

  @doc false
  @spec id?(term()) :: boolean()
  def id?(id) when is_binary(id) and byte_size(id) in 1..64, do: id_chars?(id)
  def id?(_), do: false

  defp id_chars?(<<>>), do: true

  defp id_chars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?-,
       do: id_chars?(rest)

  defp id_chars?(_), do: false

  defp hex?(<<>>), do: true
  defp hex?(<<c, rest::binary>>) when c in ?0..?9 or c in ?a..?f, do: hex?(rest)
  defp hex?(_), do: false

  ## Store and revision boundary

  defp fetch(%Context{store: {mod, arg}}, tenant, route_id) do
    case mod.fetch(arg, tenant, route_id) do
      {:ok, %{revision: %{rev: _, digest: _}} = record} -> {:ok, record}
      :not_found -> :not_found
    end
  end

  defp check_rev(%{revision: %{rev: rev}}, rev), do: :ok
  defp check_rev(record, expected_rev), do: conflict(:stale_revision, expected_rev, record)

  defp check_plan(recomputed, given, record, expected_rev) do
    if PlanDigest.equal?(recomputed, given),
      do: :ok,
      else: conflict(:plan_digest_mismatch, expected_rev, record)
  end

  defp propose(%Context{revisions: revisions}, current, preferred) do
    # Re-preferring the current deployment is allowed and advances `rev`
    # (orchestrator decision, day zero).
    case revisions.with_preferred(current, preferred) do
      {:ok, proposed} -> check_invariants(current, proposed, preferred)
      {:error, reason} -> {:invalid, {:revision, reason}}
    end
  end

  # Defensive check of the §4 "one editable field" rule on the injected
  # module's output. Ops does not recompute the §1 digest.
  defp check_invariants(current, proposed, preferred) do
    cond do
      not is_map(proposed) ->
        fault(:shape)

      Enum.any?(@fixed_fields, &(Map.get(proposed, &1) != Map.fetch!(current, &1))) ->
        fault(Enum.find(@fixed_fields, &(Map.get(proposed, &1) != Map.fetch!(current, &1))))

      Map.get(proposed, :rev) != current.rev + 1 ->
        fault(:rev)

      Map.get(proposed, :preferred) != preferred ->
        fault(:preferred)

      not hex_digest?(Map.get(proposed, :digest)) or proposed.digest == current.digest ->
        fault(:digest)

      true ->
        {:ok, proposed}
    end
  end

  defp fault(field), do: {:error, {:revision_invariant, field}}

  defp conflict(reason, expected_rev, record) do
    {current_rev, last} =
      case record do
        %{revision: %{rev: rev}, last_plan_digest: last} -> {rev, last}
        _ -> {nil, nil}
      end

    {:conflict,
     %{
       reason: reason,
       expected_rev: expected_rev,
       current_rev: current_rev,
       last_plan_digest: last
     }}
  end
end
