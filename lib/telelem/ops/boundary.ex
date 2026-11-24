defmodule Telelem.Ops.Revisions do
  @moduledoc """
  Injected route-revision logic (owned by A1 under `lib/telelem/route/`).

  Ops never builds or hashes a `RouteRevision` itself. It asks this module for
  the successor of `current` with a new preferred deployment and then checks
  only the §4 edit invariants on the result (same route and fixed controls,
  `rev + 1`, requested `preferred`, well-formed digest). Membership of
  `preferred` in `deployments` and the §1 digest are the implementation's job.

  A revision is any map (or struct) carrying the §1 fields: `route_id`, `rev`,
  `deployments`, `preferred`, `fallback_on_refusal`, `deadline_ms`,
  `max_attempts`, `digest`.
  """

  @type revision :: %{
          required(:route_id) => String.t(),
          required(:rev) => pos_integer(),
          required(:deployments) => [String.t()],
          required(:preferred) => String.t(),
          required(:fallback_on_refusal) => boolean(),
          required(:deadline_ms) => pos_integer(),
          required(:max_attempts) => 1 | 2,
          required(:digest) => String.t(),
          optional(atom()) => term()
        }

  @callback with_preferred(current :: revision(), preferred :: String.t()) ::
              {:ok, revision()} | {:error, :unknown_deployment | term()}
end

defmodule Telelem.Ops.RevisionStore do
  @moduledoc """
  Injected, tenant-scoped storage of the effective route revision.

  `compare_and_write/6` must be atomic: it writes only if the stored revision
  still has exactly `expected.rev` **and** `expected.digest`, and otherwise
  returns the current record unchanged. `Telelem.Ops.MemoryStore` is the
  reference implementation.
  """

  alias Telelem.Ops.Revisions

  @type route_record :: %{revision: Revisions.revision(), last_plan_digest: String.t() | nil}
  @type expected :: %{rev: pos_integer(), digest: String.t()}

  @callback fetch(arg :: term(), tenant_id :: String.t(), route_id :: String.t()) ::
              {:ok, route_record()} | :not_found

  @callback compare_and_write(
              arg :: term(),
              tenant_id :: String.t(),
              route_id :: String.t(),
              expected(),
              new_revision :: Revisions.revision(),
              plan_digest :: String.t()
            ) :: {:ok, route_record()} | {:conflict, route_record() | :not_found}
end
