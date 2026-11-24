defmodule Telelem.Route.Revision do
  @moduledoc """
  Immutable, digested route revision (M2a contract §1).

      RouteRevision {route_id, rev, deployments: [Id, Id], preferred,
                     fallback_on_refusal, deadline_ms, max_attempts: 1 | 2,
                     digest: hex SHA-256}

  Pure data: no processes, sockets, store or clock. Build values only through
  `new/1`, which validates every field and computes (or checks) the digest.

  ## Digest

  The digest is the lowercase hex SHA-256 of `digest_input/1`: the ordered,
  version-prefixed list of every field except the digest, encoded as compact
  UTF-8 JSON:

      ["telelem.route_revision.v1",route_id,rev,[dep_a,dep_b],preferred,
       fallback_on_refusal,deadline_ms,max_attempts]

  Deployment order is significant. IDs are restricted to ASCII
  `[a-zA-Z0-9_-]{1,64}`, so the encoding never needs escapes.

  Deployment IDs name destinations, not model identity.
  """

  @version "telelem.route_revision.v1"
  @id_re ~r/\A[a-zA-Z0-9_-]{1,64}\z/
  @hex_re ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [
    :route_id,
    :rev,
    :deployments,
    :preferred,
    :fallback_on_refusal,
    :deadline_ms,
    :max_attempts,
    :digest
  ]
  defstruct @enforce_keys

  @type id :: String.t()
  @type t :: %__MODULE__{
          route_id: id(),
          rev: pos_integer(),
          deployments: [id()],
          preferred: id(),
          fallback_on_refusal: boolean(),
          deadline_ms: pos_integer(),
          max_attempts: 1 | 2,
          digest: String.t()
        }

  @type error ::
          {:invalid, field :: atom()}
          | :duplicate_deployment
          | :preferred_not_in_deployments
          | :digest_mismatch

  @fields [
    :route_id,
    :rev,
    :deployments,
    :preferred,
    :fallback_on_refusal,
    :deadline_ms,
    :max_attempts
  ]

  @doc "Digest input version prefix."
  @spec version() :: String.t()
  def version, do: @version

  @doc """
  Validates `attrs` (map or keyword list with atom keys) and returns a revision.

  If `attrs` carries a `:digest`, it must equal the computed digest
  (`{:error, :digest_mismatch}` otherwise); if absent, it is computed.
  Unknown keys are rejected as `{:invalid, key}`.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, error()}
  def new(attrs) when is_list(attrs), do: new(Map.new(attrs))

  def new(attrs) when is_map(attrs) do
    with :ok <- no_unknown_keys(attrs),
         {:ok, route_id} <- fetch(attrs, :route_id, &id?/1),
         {:ok, rev} <- fetch(attrs, :rev, &pos_int?/1),
         {:ok, deployments} <- fetch(attrs, :deployments, &two_ids?/1),
         :ok <- distinct(deployments),
         {:ok, preferred} <- fetch(attrs, :preferred, &id?/1),
         :ok <- member(preferred, deployments),
         {:ok, fallback} <- fetch(attrs, :fallback_on_refusal, &is_boolean/1),
         {:ok, deadline_ms} <- fetch(attrs, :deadline_ms, &pos_int?/1),
         {:ok, max_attempts} <- fetch(attrs, :max_attempts, &(&1 in [1, 2])) do
      revision = %__MODULE__{
        route_id: route_id,
        rev: rev,
        deployments: deployments,
        preferred: preferred,
        fallback_on_refusal: fallback,
        deadline_ms: deadline_ms,
        max_attempts: max_attempts,
        digest: nil
      }

      computed = compute_digest(revision)

      case Map.fetch(attrs, :digest) do
        :error -> {:ok, %{revision | digest: computed}}
        {:ok, ^computed} -> {:ok, %{revision | digest: computed}}
        {:ok, given} when is_binary(given) -> digest_error(given)
        {:ok, _} -> {:error, {:invalid, :digest}}
      end
    end
  end

  def new(_), do: {:error, {:invalid, :attrs}}

  @doc "Like `new/1` but raises `ArgumentError`."
  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, revision} -> revision
      {:error, reason} -> raise ArgumentError, "invalid route revision: #{inspect(reason)}"
    end
  end

  @doc "Compact UTF-8 JSON digest input (every field except the digest)."
  @spec digest_input(t()) :: String.t()
  def digest_input(%__MODULE__{} = r) do
    JSON.encode!([
      @version,
      r.route_id,
      r.rev,
      r.deployments,
      r.preferred,
      r.fallback_on_refusal,
      r.deadline_ms,
      r.max_attempts
    ])
  end

  @doc """
  Successor of `current` with `preferred` as the preferred deployment.

  Returns a new revision with `rev + 1`, the given `preferred` and a fresh
  digest; every other field is unchanged. Applying the current `preferred`
  still yields an explicit new revision (`rev + 1`), not a no-op.

  `current` must be a `%Revision{}` or a plain map with exactly the §1 fields
  including `:digest`; it is fully re-validated per §1 and its digest checked
  first, so invalid or tampered input returns the `new/1` error
  (`{:invalid, field}`, `:digest_mismatch`, ...). Then a `preferred` that is
  not a valid ID gives `{:error, {:invalid, :preferred}}`, and a valid ID that
  is not one of the configured deployments gives
  `{:error, :unknown_deployment}`.
  """
  @spec with_preferred(t() | map(), id()) ::
          {:ok, t()} | {:error, error() | :unknown_deployment}
  def with_preferred(current, preferred) do
    with {:ok, attrs} <- current_attrs(current),
         {:ok, current} <- new(attrs),
         :ok <- valid_preferred(preferred, current.deployments) do
      attrs
      |> Map.delete(:digest)
      |> Map.merge(%{rev: current.rev + 1, preferred: preferred})
      |> new()
    end
  end

  defp current_attrs(%__MODULE__{} = r), do: {:ok, Map.from_struct(r)}

  defp current_attrs(%{} = map) when not is_struct(map) do
    if Map.has_key?(map, :digest), do: {:ok, map}, else: {:error, {:invalid, :digest}}
  end

  defp current_attrs(_), do: {:error, {:invalid, :revision}}

  defp valid_preferred(preferred, deployments) do
    cond do
      not id?(preferred) -> {:error, {:invalid, :preferred}}
      preferred in deployments -> :ok
      true -> {:error, :unknown_deployment}
    end
  end

  @doc "Re-checks a revision's stored digest against its fields."
  @spec verify(t()) :: :ok | {:error, :digest_mismatch}
  def verify(%__MODULE__{digest: digest} = r) do
    if compute_digest(r) == digest, do: :ok, else: {:error, :digest_mismatch}
  end

  @doc "True for a valid deployment/route ID."
  @spec id?(term()) :: boolean()
  def id?(id) when is_binary(id), do: Regex.match?(@id_re, id)
  def id?(_), do: false

  defp compute_digest(r),
    do: :crypto.hash(:sha256, digest_input(r)) |> Base.encode16(case: :lower)

  defp digest_error(given) do
    if Regex.match?(@hex_re, given),
      do: {:error, :digest_mismatch},
      else: {:error, {:invalid, :digest}}
  end

  defp no_unknown_keys(attrs) do
    case Map.keys(attrs) -- [:digest | @fields] do
      [] -> :ok
      [key | _] -> {:error, {:invalid, key}}
    end
  end

  defp fetch(attrs, key, valid?) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> if valid?.(value), do: {:ok, value}, else: {:error, {:invalid, key}}
      :error -> {:error, {:invalid, key}}
    end
  end

  defp pos_int?(n), do: is_integer(n) and n > 0
  defp two_ids?([a, b]), do: id?(a) and id?(b)
  defp two_ids?(_), do: false
  defp distinct([a, a]), do: {:error, :duplicate_deployment}
  defp distinct([_, _]), do: :ok

  defp member(p, deployments),
    do: if(p in deployments, do: :ok, else: {:error, :preferred_not_in_deployments})
end
