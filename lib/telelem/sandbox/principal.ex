defmodule Telelem.Sandbox.Principal do
  @moduledoc """
  Contract §3 `Principal {tenant_id, subject_id, mode, permissions}`.

  A principal is produced by the authenticator (not by this module) and is
  never derived from request data. `new/4` only validates shape; resolution
  re-validates every struct it is handed, so a hand-built or mutated struct
  fails closed rather than being trusted.

  ## Permission enum (closed; day-two proposal)

  One atom per consumer check, so the same principal works unchanged at every
  boundary:

  | Permission      | Required by                                            |
  | --------------- | ------------------------------------------------------ |
  | `:dispatch`     | `Telelem.Sandbox.resolve/3`                            |
  | `:route_read`   | `Telelem.Ops.Route.read/3` (§4 `read_route`)           |
  | `:route_write`  | `Telelem.Ops.Route.plan/5` and `apply/6` (§4)          |
  | `:receipt_read` | `Telelem.Receipts.Store.lookup/3` (§2)                 |

  Any other atom (including the earlier `:route_plan` / `:route_apply`
  proposal, which §4 replaced with `:route_write`) makes the principal
  invalid. Permissions never carry tenant: tenant is `tenant_id` only.
  """

  alias Telelem.Sandbox.Id

  @modes [:sandbox, :serving]
  @permissions [:dispatch, :route_read, :route_write, :receipt_read]

  @enforce_keys [:tenant_id, :subject_id, :mode, :permissions]
  defstruct @enforce_keys

  @type mode :: :sandbox | :serving
  @type permission :: :dispatch | :route_read | :route_write | :receipt_read
  @type t :: %__MODULE__{
          tenant_id: String.t(),
          subject_id: String.t(),
          mode: mode(),
          permissions: MapSet.t(permission())
        }

  @spec permissions() :: [permission()]
  def permissions, do: @permissions

  @spec new(String.t(), String.t(), mode(), Enumerable.t()) :: {:ok, t()} | {:error, atom()}
  def new(tenant_id, subject_id, mode, permissions) do
    p = %__MODULE__{
      tenant_id: tenant_id,
      subject_id: subject_id,
      mode: mode,
      permissions: if(is_list(permissions), do: MapSet.new(permissions), else: permissions)
    }

    case check(p) do
      :ok -> {:ok, p}
      err -> err
    end
  end

  @doc "Re-validate a principal struct. Anything else is `{:error, :invalid_principal}`."
  @spec check(term()) :: :ok | {:error, :invalid_principal}
  def check(%__MODULE__{tenant_id: t, subject_id: s, mode: m, permissions: %MapSet{} = ps}) do
    if Id.valid?(t) and Id.valid?(s) and m in @modes and
         Enum.all?(ps, &(&1 in @permissions)),
       do: :ok,
       else: {:error, :invalid_principal}
  end

  def check(_), do: {:error, :invalid_principal}

  @spec permitted?(t(), permission()) :: boolean()
  def permitted?(%__MODULE__{permissions: %MapSet{} = ps}, perm), do: MapSet.member?(ps, perm)
  def permitted?(_, _), do: false
end
