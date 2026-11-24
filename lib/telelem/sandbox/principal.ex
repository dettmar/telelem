defmodule Telelem.Sandbox.Principal do
  @moduledoc """
  Contract §3 `Principal {tenant_id, subject_id, mode, permissions}`.

  A principal is produced by the authenticator (not by this module) and is
  never derived from request data. `new/4` only validates shape; resolution
  re-validates every struct it is handed, so a hand-built or mutated struct
  fails closed rather than being trusted.

  Permission enum (proposal for day-two review; §3 leaves it open):
  `:route_read`, `:route_plan`, `:route_apply`, `:receipt_read`, `:dispatch`.
  `Telelem.Sandbox.resolve/3` requires `:dispatch`.
  """

  alias Telelem.Sandbox.Id

  @modes [:sandbox, :serving]
  @permissions [:route_read, :route_plan, :route_apply, :receipt_read, :dispatch]

  @enforce_keys [:tenant_id, :subject_id, :mode, :permissions]
  defstruct @enforce_keys

  @type mode :: :sandbox | :serving
  @type permission :: :route_read | :route_plan | :route_apply | :receipt_read | :dispatch
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
  def permitted?(%__MODULE__{permissions: ps}, perm), do: MapSet.member?(ps, perm)
end
