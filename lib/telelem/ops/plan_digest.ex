defmodule Telelem.Ops.PlanDigest do
  @moduledoc """
  Plan digest for the route operation envelope (m2a-contracts §4).

  Input is a version-prefixed, ordered, compact UTF-8 JSON array:

      ["telelem.ops.route_plan/1", tenant_id, route_id, expected_rev,
       preferred, proposed_revision_digest]

  hashed with SHA-256 and rendered as 64 lowercase hex characters. All IDs are
  pre-validated ASCII `[a-zA-Z0-9_-]{1,64}` and the proposed digest is 64 hex
  characters, so the encoding needs no escaping and is byte-stable.

  This digest binds *what the operator planned*. It does not compute the
  route revision digest (§1); that belongs to the route revision module and
  reaches this module only as an opaque hex string.
  """

  @version "telelem.ops.route_plan/1"

  @spec version() :: String.t()
  def version, do: @version

  @doc "Canonical bytes hashed by `compute/5`. Exposed for golden-vector tests."
  @spec canonical(String.t(), String.t(), pos_integer(), String.t(), String.t()) :: binary()
  def canonical(tenant_id, route_id, expected_rev, preferred, proposed_digest) do
    JSON.encode!([@version, tenant_id, route_id, expected_rev, preferred, proposed_digest])
  end

  @spec compute(String.t(), String.t(), pos_integer(), String.t(), String.t()) :: String.t()
  def compute(tenant_id, route_id, expected_rev, preferred, proposed_digest) do
    :crypto.hash(
      :sha256,
      canonical(tenant_id, route_id, expected_rev, preferred, proposed_digest)
    )
    |> Base.encode16(case: :lower)
  end

  @doc "Constant-time equality of two well-formed hex digests."
  @spec equal?(String.t(), String.t()) :: boolean()
  def equal?(a, b) when byte_size(a) == 64 and byte_size(b) == 64, do: :crypto.hash_equals(a, b)
  def equal?(_, _), do: false
end
