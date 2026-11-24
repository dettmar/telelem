defmodule Telelem.Sandbox.Bindings do
  @moduledoc """
  Contract §3 `Bindings {mode, fixture_targets, sandbox_store_root,
  sandbox_signer_ref}`. Built by `Telelem.Sandbox.Bootstrap`; `check/1`
  re-validates the envelope (not every target) on each resolve.

  * `mode` is `:sandbox` in this slice. There is no serving bootstrap here and
    no field that could hold a production resolver, secret store or signer.
  * `sandbox_store_root` is a normalized absolute path outside
    `Telelem.Sandbox.PathPolicy.default_denied_roots/0`. Nothing here opens it.
  * `sandbox_signer_ref` is a reference, never key material:
    `"sandbox:" <> Id`. Other schemes (`file:`, `env:`, `vault:` …) are rejected.
  """

  alias Telelem.Sandbox.{Id, PathPolicy, VerifiedTarget}

  @enforce_keys [:mode, :fixture_targets, :sandbox_store_root, :sandbox_signer_ref]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          mode: :sandbox,
          fixture_targets: %{String.t() => VerifiedTarget.t()},
          sandbox_store_root: String.t(),
          sandbox_signer_ref: String.t()
        }

  @spec check(term()) :: :ok | {:error, atom()}
  def check(%__MODULE__{} = b) do
    cond do
      b.mode != :sandbox ->
        {:error, :bindings_mode_not_sandbox}

      not (is_map(b.fixture_targets) and map_size(b.fixture_targets) in 1..16) ->
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
  def store_root_ok?(p, denied),
    do: PathPolicy.normalized_absolute?(p) and not PathPolicy.denied?(p, denied)

  @doc false
  def signer_ref_ok?("sandbox:" <> id), do: Id.valid?(id)
  def signer_ref_ok?(_), do: false
end
