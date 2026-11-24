defmodule Telelem.Ops.CLI do
  @moduledoc """
  Thin JSON CLI adapter over `Telelem.Ops.Route`. It parses, calls the same
  handler as every other entry point, and renders. It holds no route logic.

      run(["plan",  ~s({"route_id":"chat","expected_rev":1,"preferred":"b"})], principal, ctx)
      run(["apply", ~s({"route_id":"chat","expected_rev":1,"preferred":"b","plan_digest":"…"})], principal, ctx)
      run(["read",  ~s({"route_id":"chat"})], principal, ctx)

  Returns `{exit_status, json}`: `json` is always exactly one compact JSON
  object (the would-be stdout), with `"ok"` true or false and, when false, a
  fixed `"error"` string. The principal is supplied by the caller's
  authenticated context; request JSON carrying any field other than the ones
  listed per verb (for example `tenant_id`) is rejected as invalid.

  | Exit | `error` | Meaning |
  | --- | --- | --- |
  | 0 | — | ok |
  | 1 | `conflict` | stale revision or plan digest mismatch; nothing written |
  | 2 | `invalid` / `usage` | bad request; `field` names the request field |
  | 3 | `denied` | principal lacks authority |
  | 4 | `not_found` | no such route for this tenant |
  | 5 | `apply_outcome_unknown` | apply only: the compare-and-write was sent but no well-formed reply came back (`cause`: `store_unavailable` or `store_integrity`). The write **may have committed**. Do not reissue blindly: `read` and compare `last_plan_digest` with the returned `plan_digest` (§4). |
  | 69 | `unavailable` | `reason: store_unavailable`; the store could not be read, nothing written by this call; safe to retry |
  | 70 | `internal` | `reason: store_integrity` (stored record or store reply malformed or corrupt) or `revision_invariant`; nothing written by this call |

  Failure bodies carry only fixed tokens, the caller's own request values and,
  for conflicts, the current `rev` and `last_plan_digest`. They never carry
  store error reasons or stored data from a record that failed checks.

  Not wired into a main CLI, escript or Mix task in this slice.
  """

  alias Telelem.Ops.{Context, Route}

  @max_request_bytes 4096

  @fields %{
    "plan" => ~w(route_id expected_rev preferred),
    "apply" => ~w(route_id expected_rev preferred plan_digest),
    "read" => ~w(route_id)
  }

  @revision_fields ~w(route_id rev deployments preferred fallback_on_refusal deadline_ms max_attempts digest)a

  @spec run([String.t()], Route.principal(), Context.t()) :: {non_neg_integer(), String.t()}
  def run([verb, json], principal, %Context{} = ctx)
      when is_map_key(@fields, verb) and is_binary(json) do
    case decode(verb, json) do
      {:ok, req} -> verb |> dispatch(req, principal, ctx) |> render(verb)
      {:usage, field} -> render({:invalid, field}, verb)
    end
  end

  def run(_argv, _principal, %Context{}) do
    emit(2, %{ok: false, error: "usage", usage: "plan|apply|read <json-object>"})
  end

  defp decode(verb, json) when byte_size(json) <= @max_request_bytes do
    allowed = Map.fetch!(@fields, verb)

    case JSON.decode(json) do
      {:ok, %{} = req} ->
        cond do
          (extra = Map.keys(req) -- allowed) != [] ->
            {:usage, {:unknown_field, hd(Enum.sort(extra))}}

          (missing = allowed -- Map.keys(req)) != [] ->
            {:usage, {:missing_field, hd(missing)}}

          true ->
            {:ok, req}
        end

      _ ->
        {:usage, :json}
    end
  end

  defp decode(_verb, _json), do: {:usage, :too_large}

  defp dispatch("plan", r, p, ctx),
    do: Route.plan(p, ctx, r["route_id"], r["expected_rev"], r["preferred"])

  defp dispatch("apply", r, p, ctx),
    do: Route.apply(p, ctx, r["route_id"], r["expected_rev"], r["preferred"], r["plan_digest"])

  defp dispatch("read", r, p, ctx), do: Route.read(p, ctx, r["route_id"])

  defp render({:ok, %{plan_digest: d, proposed: rev, expected_rev: e}}, "plan"),
    do: emit(0, %{ok: true, op: "plan", expected_rev: e, plan_digest: d, proposed: revision(rev)})

  defp render({:ok, %{effective_revision: rev, plan_digest: d}}, "apply"),
    do: emit(0, %{ok: true, op: "apply", effective_revision: revision(rev), plan_digest: d})

  defp render({:ok, %{effective_revision: rev, last_plan_digest: d}}, "read"),
    do: emit(0, %{ok: true, op: "read", effective_revision: revision(rev), last_plan_digest: d})

  defp render({:conflict, c}, op),
    do: emit(1, Map.merge(%{ok: false, op: op, error: "conflict"}, c))

  defp render({:invalid, field}, op),
    do: emit(2, Map.merge(%{ok: false, op: op, error: "invalid"}, invalid(field)))

  defp render({:denied, reason}, op),
    do: emit(3, %{ok: false, op: op, error: "denied", reason: reason})

  defp render(:not_found, op), do: emit(4, %{ok: false, op: op, error: "not_found"})

  defp render({:error, {:apply_outcome_unknown, %{cause: cause} = u}}, "apply"),
    do:
      emit(5, %{
        ok: false,
        op: "apply",
        error: "apply_outcome_unknown",
        cause: cause,
        expected_rev: u.expected_rev,
        plan_digest: u.plan_digest,
        resolve: "read_and_compare_last_plan_digest"
      })

  defp render({:error, :store_unavailable}, op),
    do: emit(69, %{ok: false, op: op, error: "unavailable", reason: "store_unavailable"})

  defp render({:error, :store_integrity}, op),
    do: emit(70, %{ok: false, op: op, error: "internal", reason: "store_integrity"})

  defp render({:error, {:revision_invariant, field}}, op),
    do:
      emit(70, %{ok: false, op: op, error: "internal", reason: "revision_invariant", field: field})

  defp invalid({:revision, :unknown_deployment}),
    do: %{field: "preferred", reason: "unknown_deployment"}

  defp invalid({kind, field}) when kind in [:unknown_field, :missing_field],
    do: %{field: field, reason: kind}

  defp invalid(field) when is_atom(field), do: %{field: field}

  defp revision(rev), do: Map.new(@revision_fields, &{&1, Map.fetch!(rev, &1)})

  defp emit(status, body), do: {status, JSON.encode!(body)}
end
