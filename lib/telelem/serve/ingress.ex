defmodule Telelem.Serve.Ingress.Config do
  @moduledoc """
  Trusted, validated ingress configuration. Build only with
  `Telelem.Serve.Ingress.config/1`; `prepare/3` re-checks the struct.
  `Inspect` hides the callbacks and the store argument.
  """

  @enforce_keys [
    :authenticate,
    :context,
    :bindings,
    :route_id,
    :max_attempts,
    :deadline_ceiling_ms,
    :max_body_bytes,
    :clock,
    :new_gateway_id
  ]
  @derive {Inspect, only: [:route_id, :max_attempts, :deadline_ceiling_ms, :max_body_bytes]}
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          authenticate: (term() -> term()),
          context: Telelem.Ops.Context.t(),
          bindings: Telelem.Sandbox.Bindings.t(),
          route_id: String.t(),
          max_attempts: 1,
          deadline_ceiling_ms: pos_integer(),
          max_body_bytes: pos_integer(),
          clock: {module(), term()},
          new_gateway_id: (-> String.t())
        }
end

defmodule Telelem.Serve.Ingress.Prepared do
  @moduledoc """
  One prepared request: plain immutable data, no PIDs, sockets or callbacks.

    * `gateway_id` — opaque, gateway-generated (§2); never caller-supplied.
    * `tenant_id`, `subject_id`, `mode` — from the authenticated principal.
    * `route_id`, `revision` — the configured route and the one revision
      fetched for this request, re-validated (§1 digest) and pinned.
    * `plan` — the one selection for this ingress (`backup: :none`,
      `max_attempts: 1`); `plan.absolute_deadline_ms` is the selector's value.
    * `target` — the verified sandbox target of `plan.primary`.
    * `max_attempts` — always `1` in this slice.
    * `accepted_at_ms` — `t0`, the clock reading the deadline is anchored at.
    * `deadline_ms` — the single pinned monotonic deadline:
      `min(t0 + ceiling, plan.absolute_deadline_ms)`. The runtime must recheck
      it (and the pinned authority) before dispatch.
    * `body` — the bounded request body, uninspected.

  `Inspect` never shows `body`. Nothing here is a receipt, an admission grant
  or a dispatch.
  """

  @enforce_keys [
    :gateway_id,
    :tenant_id,
    :subject_id,
    :mode,
    :route_id,
    :revision,
    :plan,
    :target,
    :max_attempts,
    :accepted_at_ms,
    :deadline_ms,
    :body
  ]
  @derive {Inspect, except: [:body]}
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          gateway_id: String.t(),
          tenant_id: String.t(),
          subject_id: String.t(),
          mode: :sandbox,
          route_id: String.t(),
          revision: Telelem.Route.Revision.t(),
          plan: Telelem.Route.Plan.t(),
          target: Telelem.Sandbox.VerifiedTarget.t(),
          max_attempts: 1,
          accepted_at_ms: integer(),
          deadline_ms: integer(),
          body: binary()
        }
end

defmodule Telelem.Serve.Ingress do
  @moduledoc """
  Listener-free ingress preparation core (M2a: TEL-010 selection, TEL-014
  sandbox binding, TEL-015 shared route store). It turns one accepted request
  into an immutable `Telelem.Serve.Ingress.Prepared` value, or into a bounded
  error, and does nothing else: it opens no socket, admits nothing, writes no
  receipt and logs nothing.

      prepare(config, auth_input, body_reader) -> {:ok, Prepared} | {:error, reason}

  ## Trusted inputs

  Everything that decides *where* a request goes is trusted configuration in
  `Telelem.Serve.Ingress.Config` (built by `config/1`): the authentication
  callback, the `Telelem.Ops.Context` (the same store/context the route
  operations use), the sandbox `Bindings`, the one configured `route_id`, the
  attempt policy, the deadline ceiling, the body bound, the clock and the
  gateway ID source. The request contributes only:

    * `auth_input`: opaque, handed unchanged to the authentication callback
      and never inspected here;
    * `body_reader`: a function that reads the bounded body, called at most
      once and only after authorization and route preparation succeeded.

  The body is never parsed or inspected, so no caller-supplied tenant, model,
  route or deployment field can influence routing. Tenant comes only from the
  `Telelem.Sandbox.Principal` the authentication callback returns.

  ## Order (each step runs only if every earlier step succeeded)

   1. `t0` = one clock reading. The ingress deadline is `t0 + deadline_ceiling_ms`.
   2. Authentication callback with `auth_input`; result must be `{:ok,
      %Principal{}}` that passes `Principal.check/1`. Anything else, a raise,
      throw or exit included, is `:unauthenticated`.
   3. Principal mode must equal the configured mode; the principal must hold
      `:dispatch`; its tenant must be the bindings' tenant. A foreign tenant
      gets `:no_route`, the same reply as a missing route, and the store is
      not touched for it.
   4. One `fetch/3` on the context store, for `principal.tenant_id` and the
      configured `route_id`. The record must hold a revision of that route
      that `Telelem.Route.Revision.new/1` accepts with its §1 digest. That
      revision is pinned: it is never fetched again for this request.
   5. Attempt policy: the configuration must say `max_attempts: 1` and the
      pinned revision must have `max_attempts == 1` and `fallback_on_refusal
      == false`. A two-attempt or fallback revision is refused
      (`:unsupported_attempt_policy`), never silently downgraded.
   6. Eligibility = the revision's deployments that `Telelem.Sandbox.resolve/3`
      binds for this principal. `Telelem.Route.Selector.select/3` runs once,
      with `t0`. The primary target is resolved again (it must verify) and
      pinned. The plan must have `backup: :none` and `max_attempts: 1`.
   7. The bounded body reader runs once with `(max_body_bytes, remaining_ms)`.
   8. The gateway ID is generated, and `Prepared` is returned.

  The deadline is rechecked after steps 2, 4, 6 and 7 and before 8, against
  the clock. Crossing it gives `{:error, {:deadline_exceeded, stage}}` with
  `stage` in `:authenticate | :route | :select | :body`.

  ## Deadline semantics (day-two review point)

  There is exactly one pinned deadline per request:

      deadline_ms = min(t0 + deadline_ceiling_ms, plan.absolute_deadline_ms)

  `plan.absolute_deadline_ms` is exactly what the selector returns for
  `monotonic_now_ms = t0`, i.e. `t0 + revision.deadline_ms`. §1 does not say
  which instant `monotonic_now_ms` is; this module anchors it at ingress
  acceptance (`t0`, before authentication), the conservative choice: time
  spent authenticating, reading the store and reading the body counts
  against the route deadline. It never extends a route deadline and never
  lets one exceed the ceiling. If the contract wants the anchor at selection
  time instead, that is a one-argument change in `select/3`'s call, and it
  must be decided by the contract owner, not here. Before step 6 only the
  ceiling deadline is known and applies.

  ## Cancellation limits

  Nothing here hard-cancels a blocking call. The authentication callback,
  the store `fetch/3` (`GenServer.call` with the store's own 5 s default for
  `Telelem.Ops.FileStore`; the `RevisionStore` interface has no timeout
  argument) and the body reader all run in the caller's process; the deadline
  is only checked when they return. The body reader receives the remaining
  budget as an argument and must enforce it itself. A production listener
  therefore needs, before it can claim the deadline holds end to end: an
  authentication callback with its own bound, a store `fetch` bounded by the
  remaining budget, and a body reader with a total (not per-chunk) timeout
  and byte bound.

  ## Errors

  Every error is a bounded atom or `{atom, atom}`. No error carries the body,
  `auth_input`, callback reasons, store reasons or record data.

  | Reason                            | Meaning                                      |
  | --------------------------------- | -------------------------------------------- |
  | `:invalid_config`                 | not a `Config` from `config/1`               |
  | `:unauthenticated`                | callback did not return a valid principal    |
  | `:mode_mismatch`                  | principal mode ≠ configured mode             |
  | `:permission_denied`              | principal lacks `:dispatch`                  |
  | `:no_route`                       | route missing, or principal's tenant not bound |
  | `:store_unavailable`              | `fetch/3` raised, threw or exited            |
  | `:store_integrity`                | malformed record or revision refused by §1   |
  | `:unsupported_attempt_policy`     | pinned revision allows two attempts/fallback |
  | `:no_eligible_deployment`         | no deployment of the revision is bound       |
  | `{:target_denied, denial}`        | sandbox resolve denied the request outright  |
  | `{:deadline_exceeded, stage}`     | deadline passed at a boundary                |
  | `:body_too_large`                 | reader said so, or returned > bound          |
  | `:body_timeout`                   | reader reported its own timeout              |
  | `:body_unavailable`               | reader failed in any other way               |
  | `:gateway_id_unavailable`         | ID source failed or returned an invalid ID   |
  """

  alias Telelem.Ops.Context
  alias Telelem.Route.{Plan, Revision, Selector}
  alias Telelem.Sandbox
  alias Telelem.Sandbox.{Bindings, Id, Principal}
  alias Telelem.Serve.Ingress.{Config, Prepared}

  @revision_fields [
    :route_id,
    :rev,
    :deployments,
    :preferred,
    :fallback_on_refusal,
    :deadline_ms,
    :max_attempts,
    :digest
  ]

  # Hard bounds on trusted configuration, so a typo cannot disable a bound.
  @max_ceiling_ms 600_000
  @max_body_limit 16 * 1024 * 1024

  @type stage :: :authenticate | :route | :select | :body
  @type reason ::
          :invalid_config
          | :unauthenticated
          | :mode_mismatch
          | :permission_denied
          | :no_route
          | :store_unavailable
          | :store_integrity
          | :unsupported_attempt_policy
          | :no_eligible_deployment
          | {:target_denied, Sandbox.denial()}
          | {:deadline_exceeded, stage()}
          | :body_too_large
          | :body_timeout
          | :body_unavailable
          | :gateway_id_unavailable

  @typedoc """
  Called once, as `reader.(max_body_bytes, remaining_ms)`. Must return
  `{:ok, binary}` of at most `max_body_bytes`, `{:error, :too_large}`,
  `{:error, :timeout}` or any other failure. It must enforce `remaining_ms`
  as a total budget itself; this module cannot interrupt it.
  """
  @type body_reader :: (pos_integer(), pos_integer() -> term())

  @doc "Upper bound accepted for `deadline_ceiling_ms`."
  def max_ceiling_ms, do: @max_ceiling_ms
  @doc "Upper bound accepted for `max_body_bytes`."
  def max_body_limit, do: @max_body_limit

  @doc """
  Validates trusted configuration and returns a `Config`.

  Required keys: `:authenticate` (1-arity fun), `:context` (`%Telelem.Ops.Context{}`
  with `mode: :sandbox` and `revisions: Telelem.Route.Revision`), `:bindings`
  (valid `%Telelem.Sandbox.Bindings{}`), `:route_id`, `:max_attempts` (must be
  `1`; anything else is `{:error, {:unsupported, :max_attempts}}`),
  `:deadline_ceiling_ms` (`1..max_ceiling_ms/0`), `:max_body_bytes`
  (`1..max_body_limit/0`), `:clock` (`{module, arg}` implementing
  `Telelem.Instance.Clock.now/1`, monotonic milliseconds).

  Optional: `:new_gateway_id` (0-arity fun returning an ID; default 16 random
  bytes, URL-safe base64 without padding). It is trusted gateway code, never
  request data.

  A `:serving` context is `{:error, {:unsupported, :serving}}`: this slice has
  no serving resolver (`Telelem.Sandbox.resolve/3` denies serving principals).
  """
  @spec config(keyword() | map()) :: {:ok, Config.t()} | {:error, term()}
  def config(opts) when is_list(opts), do: config(Map.new(opts))

  def config(%{} = opts) when not is_struct(opts) do
    known = [
      :authenticate,
      :context,
      :bindings,
      :route_id,
      :max_attempts,
      :deadline_ceiling_ms,
      :max_body_bytes,
      :clock,
      :new_gateway_id
    ]

    with :ok <- only_keys(opts, known),
         {:ok, auth} <- req(opts, :authenticate, &is_function(&1, 1)),
         {:ok, ctx} <- req(opts, :context, &match?(%Context{}, &1)),
         :ok <- context_ok(ctx),
         {:ok, bindings} <- req(opts, :bindings, &(Bindings.check(&1) == :ok)),
         {:ok, route_id} <- req(opts, :route_id, &Revision.id?/1),
         :ok <- attempts_ok(Map.get(opts, :max_attempts)),
         {:ok, ceiling} <- req(opts, :deadline_ceiling_ms, &in_range?(&1, @max_ceiling_ms)),
         {:ok, max_body} <- req(opts, :max_body_bytes, &in_range?(&1, @max_body_limit)),
         {:ok, clock} <- req(opts, :clock, &clock?/1),
         {:ok, new_id} <- opt(opts, :new_gateway_id, &is_function(&1, 0), &random_id/0) do
      {:ok,
       %Config{
         authenticate: auth,
         context: ctx,
         bindings: bindings,
         route_id: route_id,
         max_attempts: 1,
         deadline_ceiling_ms: ceiling,
         max_body_bytes: max_body,
         clock: clock,
         new_gateway_id: new_id
       }}
    end
  end

  def config(_), do: {:error, {:invalid, :config}}

  @doc "Prepares one request. See the moduledoc for order, deadline and errors."
  @spec prepare(Config.t(), term(), body_reader()) :: {:ok, Prepared.t()} | {:error, reason()}
  def prepare(%Config{} = cfg, auth_input, body_reader) when is_function(body_reader, 2) do
    if config_intact?(cfg), do: run(cfg, auth_input, body_reader), else: {:error, :invalid_config}
  end

  def prepare(_cfg, _auth_input, _body_reader), do: {:error, :invalid_config}

  defp run(cfg, auth_input, body_reader) do
    t0 = now(cfg)
    ceiling_deadline = t0 + cfg.deadline_ceiling_ms

    with {:ok, principal} <- authenticate(cfg, auth_input),
         :ok <- in_time(cfg, ceiling_deadline, :authenticate),
         :ok <- authorize(cfg, principal),
         {:ok, revision} <- fetch_revision(cfg, principal),
         :ok <- in_time(cfg, ceiling_deadline, :route),
         :ok <- attempt_policy(revision),
         {:ok, plan, target} <- select(cfg, principal, revision, t0),
         deadline = min(ceiling_deadline, plan.absolute_deadline_ms),
         :ok <- in_time(cfg, deadline, :select),
         {:ok, body} <- read_body(cfg, body_reader, deadline),
         :ok <- in_time(cfg, deadline, :body),
         {:ok, gateway_id} <- gateway_id(cfg) do
      {:ok,
       %Prepared{
         gateway_id: gateway_id,
         tenant_id: principal.tenant_id,
         subject_id: principal.subject_id,
         mode: principal.mode,
         route_id: revision.route_id,
         revision: revision,
         plan: plan,
         target: target,
         max_attempts: 1,
         accepted_at_ms: t0,
         deadline_ms: deadline,
         body: body
       }}
    end
  end

  ## Steps

  defp authenticate(cfg, auth_input) do
    case contained(fn -> cfg.authenticate.(auth_input) end) do
      {:returned, {:ok, %Principal{} = p}} ->
        if Principal.check(p) == :ok, do: {:ok, p}, else: {:error, :unauthenticated}

      _ ->
        {:error, :unauthenticated}
    end
  end

  defp authorize(cfg, principal) do
    cond do
      principal.mode != cfg.context.mode -> {:error, :mode_mismatch}
      not Principal.permitted?(principal, :dispatch) -> {:error, :permission_denied}
      # Same reply as a missing route: a foreign tenant learns nothing, and
      # the store is never asked about a tenant the bindings do not cover.
      principal.tenant_id != cfg.bindings.tenant_id -> {:error, :no_route}
      true -> :ok
    end
  end

  defp fetch_revision(cfg, principal) do
    {mod, arg} = cfg.context.store

    case contained(fn -> mod.fetch(arg, principal.tenant_id, cfg.route_id) end) do
      {:returned, :not_found} -> {:error, :no_route}
      {:returned, {:ok, %{revision: revision}}} -> pin_revision(revision, cfg.route_id)
      {:returned, _malformed} -> {:error, :store_integrity}
      :failed -> {:error, :store_unavailable}
    end
  end

  # Rebuild through `Revision.new/1` so the pinned value is a fresh, fully
  # re-validated `%Revision{}` (digest included) of exactly the §1 fields,
  # whatever struct or map the store handed back.
  defp pin_revision(%{} = stored, route_id) do
    extra = Map.keys(stored) -- [:__struct__ | @revision_fields]
    struct_ok = Map.get(stored, :__struct__, Revision) == Revision

    with true <- extra == [] and struct_ok,
         {:ok, %Revision{route_id: ^route_id} = r} <-
           Revision.new(Map.take(stored, @revision_fields)) do
      {:ok, r}
    else
      _ -> {:error, :store_integrity}
    end
  end

  defp pin_revision(_, _), do: {:error, :store_integrity}

  defp attempt_policy(%Revision{max_attempts: 1, fallback_on_refusal: false}), do: :ok
  defp attempt_policy(%Revision{}), do: {:error, :unsupported_attempt_policy}

  defp select(cfg, principal, revision, t0) do
    with {:ok, eligible} <- eligible(cfg, principal, revision),
         {:ok, %Plan{backup: :none, max_attempts: 1} = plan} <-
           selected(Selector.select(revision, eligible, t0)),
         {:ok, target} <- Sandbox.resolve(principal, cfg.bindings, plan.primary) do
      {:ok, plan, target}
    else
      {:denied, denial} -> {:error, {:target_denied, denial}}
      {:ok, %Plan{}} -> {:error, :unsupported_attempt_policy}
      {:error, _} = err -> err
    end
  end

  defp selected({:ok, plan}), do: {:ok, plan}
  defp selected({:error, :no_eligible_deployment}), do: {:error, :no_eligible_deployment}

  # A lane is eligible iff the sandbox binds it for this principal. An
  # unbound or unverifiable lane is merely ineligible; any other denial
  # (invalid principal/bindings, mode, permission) refuses the whole request.
  defp eligible(cfg, principal, revision) do
    Enum.reduce_while(revision.deployments, {:ok, MapSet.new()}, fn id, {:ok, acc} ->
      case Sandbox.resolve(principal, cfg.bindings, id) do
        {:ok, _target} -> {:cont, {:ok, MapSet.put(acc, id)}}
        {:denied, d} when d in [:no_binding, :target_unverified] -> {:cont, {:ok, acc}}
        {:denied, d} -> {:halt, {:denied, d}}
      end
    end)
  end

  defp read_body(cfg, reader, deadline) do
    remaining = deadline - now(cfg)
    max = cfg.max_body_bytes

    if remaining <= 0 do
      {:error, {:deadline_exceeded, :select}}
    else
      case contained(fn -> reader.(max, remaining) end) do
        {:returned, {:ok, body}} when is_binary(body) and byte_size(body) <= max -> {:ok, body}
        {:returned, {:ok, body}} when is_binary(body) -> {:error, :body_too_large}
        {:returned, {:error, :too_large}} -> {:error, :body_too_large}
        {:returned, {:error, :timeout}} -> {:error, :body_timeout}
        _ -> {:error, :body_unavailable}
      end
    end
  end

  defp gateway_id(cfg) do
    case contained(cfg.new_gateway_id) do
      {:returned, id} when is_binary(id) -> if Id.valid?(id), do: {:ok, id}, else: no_id()
      _ -> no_id()
    end
  end

  defp no_id, do: {:error, :gateway_id_unavailable}

  @doc false
  # Default gateway ID: 128 random bits, 22 URL-safe characters, a valid §2 Id.
  def random_id, do: :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)

  ## Deadline

  defp in_time(cfg, deadline, stage) do
    if now(cfg) < deadline, do: :ok, else: {:error, {:deadline_exceeded, stage}}
  end

  defp now(%Config{clock: {mod, arg}}) do
    case mod.now(arg) do
      n when is_integer(n) -> n
      _ -> raise ArgumentError, "ingress clock returned a non-integer"
    end
  end

  ## Helpers

  # Callbacks may raise, throw or exit; none of that, nor its reason, leaves
  # this module.
  defp contained(fun) do
    {:returned, fun.()}
  catch
    _kind, _reason -> :failed
  end

  # A `Config` is only ever built by `config/1`; re-check the parts a
  # hand-built or modified struct could get wrong before trusting it.
  defp config_intact?(%Config{} = c) do
    is_function(c.authenticate, 1) and match?(%Context{}, c.context) and
      context_ok(c.context) == :ok and Bindings.check(c.bindings) == :ok and
      Revision.id?(c.route_id) and c.max_attempts == 1 and
      in_range?(c.deadline_ceiling_ms, @max_ceiling_ms) and
      in_range?(c.max_body_bytes, @max_body_limit) and clock?(c.clock) and
      is_function(c.new_gateway_id, 0)
  end

  defp context_ok(%Context{mode: :sandbox, store: {mod, _arg}, revisions: Revision})
       when is_atom(mod),
       do: :ok

  defp context_ok(%Context{mode: :serving}), do: {:error, {:unsupported, :serving}}
  defp context_ok(_), do: {:error, {:invalid, :context}}

  defp attempts_ok(1), do: :ok
  defp attempts_ok(nil), do: {:error, {:invalid, :max_attempts}}
  defp attempts_ok(_), do: {:error, {:unsupported, :max_attempts}}

  defp clock?({mod, _arg}) when is_atom(mod), do: true
  defp clock?(_), do: false

  defp in_range?(n, max), do: is_integer(n) and n > 0 and n <= max

  defp only_keys(opts, known) do
    case Map.keys(opts) -- known do
      [] -> :ok
      [key | _] -> {:error, {:invalid, key}}
    end
  end

  defp req(opts, key, valid?) do
    case Map.fetch(opts, key) do
      {:ok, v} -> if valid?.(v), do: {:ok, v}, else: {:error, {:invalid, key}}
      :error -> {:error, {:invalid, key}}
    end
  end

  defp opt(opts, key, valid?, default) do
    case Map.fetch(opts, key) do
      {:ok, v} -> if valid?.(v), do: {:ok, v}, else: {:error, {:invalid, key}}
      :error -> {:ok, default}
    end
  end
end
