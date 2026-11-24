defmodule Telelem.Route.Plan do
  @moduledoc """
  Result of one `Telelem.Route.Selector.select/3` call (M2a contract §1).

      Plan {rev, digest, primary: Id, backup: Id | none,
            absolute_deadline_ms, max_attempts}

  `absolute_deadline_ms` is in the caller's monotonic millisecond timebase
  (the `monotonic_now_ms` passed to `select/3`); it may be negative.

  `backup` is usable only after a positively proved pre-send refusal of
  `primary`. `max_attempts` is the effective budget: `2` only when a backup
  exists, otherwise `1`. Holds no PIDs or sockets; the coordinator binds a
  plan to grants and attempts.
  """

  @enforce_keys [:rev, :digest, :primary, :backup, :absolute_deadline_ms, :max_attempts]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          rev: pos_integer(),
          digest: String.t(),
          primary: Telelem.Route.Revision.id(),
          backup: Telelem.Route.Revision.id() | :none,
          absolute_deadline_ms: integer(),
          max_attempts: 1 | 2
        }
end
