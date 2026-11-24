defmodule Telelem.Sandbox do
  @moduledoc """
  Contract §3: `resolve(principal, bindings, deployment_id) -> VerifiedTarget | Denied`.

  Pure and total: no I/O, no DNS, no process state, no fallback. Every input
  struct is re-validated, so anything not produced by `Principal.new/4` and
  `Bootstrap` in valid shape is denied. Denial reasons are bounded atoms.

  | Condition                                    | Result                          |
  | -------------------------------------------- | ------------------------------- |
  | principal not a valid `Principal`            | `{:denied, :invalid_principal}` |
  | bindings not valid `Bindings`                | `{:denied, :invalid_bindings}`  |
  | principal is `:serving`                      | `{:denied, :no_serving_resolver}` (no serving path in this slice) |
  | principal mode ≠ bindings mode               | `{:denied, :mode_mismatch}`     |
  | principal lacks `:dispatch`                  | `{:denied, :permission_denied}` |
  | malformed deployment ID                      | `{:denied, :invalid_deployment_id}` |
  | no binding for the ID                        | `{:denied, :no_binding}`        |
  | bound target fails re-verification           | `{:denied, :target_unverified}` |

  Tenant is taken only from the authenticated principal; `Bindings` carries no
  tenant in the §3 shape, so tenant scoping of bindings is a day-two question.
  """

  alias Telelem.Sandbox.{Bindings, Id, Principal, VerifiedTarget}

  @type denial ::
          :invalid_principal
          | :invalid_bindings
          | :no_serving_resolver
          | :mode_mismatch
          | :permission_denied
          | :invalid_deployment_id
          | :no_binding
          | :target_unverified

  @spec resolve(Principal.t(), Bindings.t(), String.t()) ::
          {:ok, VerifiedTarget.t()} | {:denied, denial()}
  def resolve(principal, bindings, deployment_id) do
    with :ok <- deny(Principal.check(principal) == :ok, :invalid_principal),
         :ok <- deny(Bindings.check(bindings) == :ok, :invalid_bindings),
         :ok <- deny(principal.mode != :serving, :no_serving_resolver),
         :ok <- deny(principal.mode == bindings.mode, :mode_mismatch),
         :ok <- deny(Principal.permitted?(principal, :dispatch), :permission_denied),
         :ok <- deny(Id.valid?(deployment_id), :invalid_deployment_id),
         {:ok, t} <- fetch(bindings.fixture_targets, deployment_id),
         :ok <- deny(VerifiedTarget.check(t, deployment_id) == :ok, :target_unverified) do
      {:ok, t}
    end
  end

  defp fetch(targets, id) do
    case Map.fetch(targets, id) do
      {:ok, t} -> {:ok, t}
      :error -> {:denied, :no_binding}
    end
  end

  defp deny(true, _), do: :ok
  defp deny(false, reason), do: {:denied, reason}
end
