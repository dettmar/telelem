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

  ## Store failures and integrity

  Store calls that exit, raise or throw never escape these handlers, and their
  reasons are never returned:

    * a failed `fetch/3` gives `{:error, :store_unavailable}`. Nothing was
      written by this call (apply fetches before it writes).
    * a failed or malformed `compare_and_write/6` reply gives
      `{:error, {:apply_outcome_unknown, %{cause: cause, expected_rev: e,
      plan_digest: d}}}` with `cause` `:store_unavailable` (call exited,
      raised or threw, e.g. a timeout) or `:store_integrity` (reply not
      well formed, or inconsistent with the request). A `{:conflict, record}`
      reply is usable only if `record` is well formed, has a `rev` greater
      than the base apply read, and is accepted by the injected `Revisions`
      module as a base revision (as `read/3` probes it); otherwise it is
      `:store_integrity` and none of its data is returned. The write may or
      may not have committed. Ops does not retry, and must not report this as a
      failure: the caller reads back and compares `last_plan_digest` with `d`
      (§4). A match confirms the plan is applied; a mismatch does not prove it
      never landed, since a later write may have superseded it.
    * a fetched record that is malformed (wrong shape or types, another
      route's ID, bad `last_plan_digest`) or that the injected `Revisions`
      module refuses as a base revision (for example a digest mismatch) gives
      `{:error, :store_integrity}`. `read/3` probes the stored revision the
      same way, so it never returns a revision that plan would refuse.

  Only `{:error, :unknown_deployment}` from `Revisions.with_preferred/2` is
  attributed to the caller's `preferred`; any other refusal concerns the
  stored revision (the requested `preferred` is already a valid ID).
  """

  alias Telelem.Ops.{Context, PlanDigest}

  @fixed_fields [:route_id, :deployments, :fallback_on_refusal, :deadline_ms, :max_attempts]
  @revision_fields [:rev, :preferred, :digest | @fixed_fields]

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
          | {:invalid, {:revision, :unknown_deployment}}
  @type conflict ::
          {:conflict,
           %{
             reason: :stale_revision | :plan_digest_mismatch,
             expected_rev: pos_integer(),
             current_rev: pos_integer() | nil,
             last_plan_digest: String.t() | nil
           }}
  @type fault ::
          {:error, {:revision_invariant, atom()}}
          | {:error, :store_unavailable}
          | {:error, :store_integrity}
  @type outcome_unknown ::
          {:error,
           {:apply_outcome_unknown,
            %{
              cause: :store_unavailable | :store_integrity,
              expected_rev: pos_integer(),
              plan_digest: String.t()
            }}}

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
          | outcome_unknown()
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

      sent = %{
        ctx: ctx,
        route_id: route_id,
        expected: expected,
        proposed: proposed,
        plan_digest: recomputed,
        expected_rev: expected_rev
      }

      contained(fn ->
        mod.compare_and_write(arg, tenant, route_id, expected, proposed, recomputed)
      end)
      |> written(sent)
    end
  end

  # Interpret a compare-and-write reply. Only a well-formed success for
  # exactly this proposal, or a usable conflict (`stale_conflict?/2`), is
  # reported as such; anything else is outcome-unknown. Ops never retries.
  defp written({:returned, {:ok, record}}, s) do
    if record?(record, s.route_id) and same_revision?(record.revision, s.proposed) and
         record.last_plan_digest == s.plan_digest,
       do: {:ok, %{effective_revision: record.revision, plan_digest: record.last_plan_digest}},
       else: outcome_unknown(:store_integrity, s)
  end

  defp written({:returned, {:conflict, :not_found}}, s),
    do: conflict(:stale_revision, s.expected_rev, nil)

  defp written({:returned, {:conflict, record}}, s) do
    if stale_conflict?(record, s),
      do: conflict(:stale_revision, s.expected_rev, record),
      else: outcome_unknown(:store_integrity, s)
  end

  defp written({:returned, _malformed}, s), do: outcome_unknown(:store_integrity, s)
  defp written(:failed, s), do: outcome_unknown(:store_unavailable, s)

  # A conflict record is reported (its `rev` and `last_plan_digest` reach the
  # caller) only if it is well formed, strictly newer than the base this apply
  # read (revisions only advance, so an equal or older `rev` contradicts the
  # fetch), and accepted by the injected `Revisions` module as a base
  # revision, the same check `read/3` applies. The check is contained: a
  # revision module that raises here yields outcome-unknown, not a crash
  # after a write that may have committed.
  defp stale_conflict?(record, s) do
    record?(record, s.route_id) and record.revision.rev > s.expected.rev and
      contained(fn -> probe(s.ctx, record.revision) end) == {:returned, :ok}
  end

  # Field-wise, so a store may return an equal plain map instead of a struct.
  defp same_revision?(written, proposed),
    do: Enum.all?(@revision_fields, &(Map.get(written, &1) == Map.get(proposed, &1)))

  defp outcome_unknown(cause, s),
    do:
      {:error,
       {:apply_outcome_unknown,
        %{cause: cause, expected_rev: s.expected_rev, plan_digest: s.plan_digest}}}

  @spec read(principal(), Context.t(), String.t()) ::
          {:ok, %{effective_revision: map(), last_plan_digest: String.t() | nil}}
          | denied()
          | invalid()
          | :not_found
          | fault()
  def read(principal, %Context{} = ctx, route_id) do
    with {:ok, tenant} <- authorize(principal, ctx, :route_read),
         :ok <- validate_id(route_id, :route_id),
         {:ok, record} <- fetch(ctx, tenant, route_id),
         :ok <- probe(ctx, record.revision) do
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
    case contained(fn -> mod.fetch(arg, tenant, route_id) end) do
      {:returned, :not_found} ->
        :not_found

      {:returned, {:ok, record}} ->
        if record?(record, route_id), do: {:ok, record}, else: integrity()

      {:returned, _malformed} ->
        integrity()

      :failed ->
        {:error, :store_unavailable}
    end
  end

  # Store adapters may exit (GenServer timeout, noproc), raise (driver errors)
  # or throw. None of that, nor its reason, crosses the handler boundary. Also
  # used for the post-CAS conflict check, which must not crash either.
  defp contained(fun) do
    {:returned, fun.()}
  catch
    _kind, _reason -> :failed
  end

  defp integrity, do: {:error, :store_integrity}

  # Structural check of a store record. Semantic (§1) validity, including the
  # revision digest, is left to the injected `Revisions` module.
  defp record?(%{revision: revision, last_plan_digest: last}, route_id),
    do: revision?(revision, route_id) and (is_nil(last) or hex_digest?(last))

  defp record?(_, _), do: false

  defp revision?(
         %{
           route_id: route_id,
           rev: rev,
           deployments: deployments,
           preferred: preferred,
           fallback_on_refusal: fallback,
           deadline_ms: deadline_ms,
           max_attempts: max_attempts,
           digest: digest
         },
         route_id
       ) do
    pos_int?(rev) and deployments != [] and ids?(deployments) and id?(preferred) and
      is_boolean(fallback) and pos_int?(deadline_ms) and max_attempts in [1, 2] and
      hex_digest?(digest)
  end

  defp revision?(_, _), do: false

  defp pos_int?(n), do: is_integer(n) and n > 0

  defp ids?([]), do: true
  defp ids?([id | rest]), do: id?(id) and ids?(rest)
  defp ids?(_), do: false

  # Ask the revision module whether it accepts the stored revision as a base.
  defp probe(%Context{revisions: revisions}, current) do
    case revisions.with_preferred(current, current.preferred) do
      {:ok, _} -> :ok
      _ -> integrity()
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
      {:error, :unknown_deployment} -> {:invalid, {:revision, :unknown_deployment}}
      _refused_base -> integrity()
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
