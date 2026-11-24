defmodule Telelem.Sandbox.Bindings do
  @moduledoc """
  Contract §3 `Bindings {mode, fixture_targets, sandbox_store_root,
  sandbox_signer_ref}`, extended with `tenant_id` (day-two proposal). Built
  by `Telelem.Sandbox.Bootstrap`; `check/1` re-validates the whole envelope,
  every target included, on each resolve. `check/1` is pure: it repeats the
  string policy but not the filesystem checks Bootstrap made.

  * `tenant_id` scopes the bindings: `Telelem.Sandbox.resolve/3` resolves
    only for a principal of this tenant. Another tenant's principal sees
    `:no_binding`, indistinguishable from a deployment that is not bound.
  * `mode` is `:sandbox` in this slice. There is no serving bootstrap here and
    no field that could hold a production resolver, secret store or signer.
  * `fixture_targets` maps 1..16 deployment IDs to `VerifiedTarget`s bound
    under that ID, with pairwise distinct `{address, port}` endpoints.
  * `sandbox_store_root` is a normalized absolute path outside
    `Telelem.Sandbox.PathPolicy.default_denied_roots/0`. Nothing here opens it.
  * `sandbox_signer_ref` is a reference, never key material:
    `"sandbox:" <> Id`. Other schemes (`file:`, `env:`, `vault:` …) are rejected.
  """

  alias Telelem.Sandbox.{Id, PathPolicy, VerifiedTarget}

  @enforce_keys [:tenant_id, :mode, :fixture_targets, :sandbox_store_root, :sandbox_signer_ref]
  defstruct @enforce_keys
  @all_keys Enum.sort([:__struct__ | @enforce_keys])

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          mode: :sandbox,
          fixture_targets: %{String.t() => VerifiedTarget.t()},
          sandbox_store_root: String.t(),
          sandbox_signer_ref: String.t()
        }

  @spec check(term()) :: :ok | {:error, atom()}
  def check(%__MODULE__{} = b) do
    cond do
      Enum.sort(Map.keys(b)) != @all_keys ->
        {:error, :invalid_bindings}

      not Id.valid?(b.tenant_id) ->
        {:error, :invalid_tenant}

      b.mode != :sandbox ->
        {:error, :bindings_mode_not_sandbox}

      not targets_ok?(b.fixture_targets) ->
        {:error, :invalid_fixture_targets}

      not store_root_ok?(b.sandbox_store_root, PathPolicy.default_denied_roots()) ->
        {:error, :invalid_store_root}

      not signer_ref_ok?(b.sandbox_signer_ref) ->
        {:error, :invalid_signer_ref}

      true ->
        :ok
    end
  end

  def check(_), do: {:error, :invalid_bindings}

  @doc false
  def targets_ok?(ts) when is_map(ts) and map_size(ts) in 1..16 do
    Enum.all?(ts, fn {id, t} -> VerifiedTarget.check(t, id) == :ok end) and
      distinct_endpoints?(Map.values(ts))
  end

  def targets_ok?(_), do: false

  @doc false
  def distinct_endpoints?(ts) do
    eps = Enum.map(ts, &{&1.address, &1.port})
    length(Enum.uniq(eps)) == length(eps)
  end

  @doc false
  def store_root_ok?(p, denied),
    do: PathPolicy.normalized_absolute?(p) and not PathPolicy.denied?(p, denied)

  @doc false
  def signer_ref_ok?("sandbox:" <> id), do: Id.valid?(id)
  def signer_ref_ok?(_), do: false
end
