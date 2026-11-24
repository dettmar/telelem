defmodule Telelem.Ops.Context do
  @moduledoc """
  Injected dependencies for the route operation handlers.

    * `mode` — `:sandbox | :serving`; must equal the principal's mode.
    * `store` — `{module, arg}` implementing `Telelem.Ops.RevisionStore`.
    * `revisions` — module implementing `Telelem.Ops.Revisions` (A1's
      route revision logic, adapted; ops never hashes a revision itself).
  """

  @enforce_keys [:mode, :store, :revisions]
  defstruct [:mode, :store, :revisions]

  @type t :: %__MODULE__{
          mode: :sandbox | :serving,
          store: {module(), term()},
          revisions: module()
        }
end
