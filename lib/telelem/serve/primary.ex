defmodule Telelem.Serve.Primary.Config do
  @moduledoc """
  Trusted, validated primary composition configuration. Build only with
  `Telelem.Serve.Primary.config/1`; `start/4` re-checks the struct's shape
  only. `Inspect` shows only the numeric policy fields.
  """

  @enforce_keys [
    :ingress,
    :authority,
    :store,
    :producer,
    :tree,
    :units,
    :limits,
    :coordinator_ms,
    :grace_ms,
    :activation_ms
  ]
  @derive {Inspect, only: [:units, :coordinator_ms, :grace_ms, :activation_ms]}
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          ingress: Telelem.Serve.Ingress.Config.t(),
          authority: pid(),
          store: pid(),
          producer: Telelem.Receipts.Producer.t(),
          tree: Telelem.Serve.Tree.Pins.t(),
          units: pos_integer(),
          limits: Telelem.Wire.Limits.t(),
          coordinator_ms: pos_integer(),
          grace_ms: pos_integer(),
          activation_ms: pos_integer()
        }
end

defmodule Telelem.Serve.Primary.Handle do
  @moduledoc """
  What a successful `Telelem.Serve.Primary.start/4` returns: identities only.

    * `request_id`: the gateway-generated opaque id (receipt lookup key).
    * `handoff`: the active coordinator pid (a child of the pinned
      `:coordinators` branch); the caller is its requester.
    * `guard`, `owner`, `witness`, `ref`: exactly the `Telelem.Wire.OwnerGuard`
      handle (the guard is a child of the pinned `:guards` branch). Observer
      reports carry `ref`; controller messages carry `owner`.
    * `deadline_ms`: the one pinned system deadline `D`.

  No body, authentication input, principal or target. Success means only
  that the coordinator's activation was confirmed and the one GO was sent to
  `owner`, never that anything was admitted, dialed or served.
  """
  @enforce_keys [:request_id, :handoff, :guard, :owner, :witness, :ref, :deadline_ms]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          request_id: String.t(),
          handoff: pid(),
          guard: pid(),
          owner: pid(),
          witness: pid(),
          ref: reference(),
          deadline_ms: integer()
        }
end

defmodule Telelem.Serve.Primary do
  @moduledoc """
  W2c: listener-free composition of one primary attempt on a
  `Telelem.Serve.Tree`. One `Telelem.Serve.Ingress` preparation, one
  coordinator started and activated by `Telelem.Serve.CoordinatorStart` on
  the pinned `:coordinators` branch (`coordinator_grant`, `terminal_ack`,
  receipts), and one guarded `Telelem.Wire.Upstream` owner launched by
  `Telelem.Wire.OwnerGuard` on the pinned `:guards` branch. It opens no
  socket itself, starts no listener, retries nothing, kills nothing and never
  selects a second attempt.

      config(opts) -> {:ok, Config} | {:error, {:invalid, field}}
      start(config, auth_input, body_reader, opts \\\\ []) -> {:ok, Handle} | {:error, reason}

  `start/4` must run in the **requester** process (the downstream owner): it
  becomes the coordinator's requester and the guard monitors it. After
  `{:ok, handle}` the caller runs the requester side of the `Telelem.Handoff`
  protocol (`{:telelem_handoff, handle.handoff, msg}`, `terminal_written/2`,
  credit to `handle.owner`); nothing here does that.

  ## Trusted setup (`config/1`, once)

  Everything is pinned once, at setup, independently of any request:

    * `:ingress`: a `Telelem.Serve.Ingress.Config` whose clock is
      `{Telelem.Instance.SystemClock, _}`, so the prepared deadline is in the
      `System.monotonic_time(:millisecond)` domain that `Handoff`,
      `CoordinatorStart`, `OwnerGuard` and `Upstream` use. Any other clock is
      `{:invalid, :ingress_clock}`.
    * `:authority`: the pid of a live local `Telelem.Admission.Authority`
      (checked by its initial call). Never re-resolved.
    * `:store`: the pid of a live local `Telelem.Receipts.Store` (initial call
      checked first). The producer handle is resolved here, once, with
      `Store.producer/1` (a 5 s `GenServer.call`: a busy store delays setup,
      never a request) and accepted only if `Producer.binding/2` confirms it
      is that store's canonical handle, with actual mode `:sandbox` and
      limits within the §2 ceilings (256 / 1024 / 512).
    * `:tree`: the exact root pid of a `Telelem.Serve.Tree`. It is resolved
      **once**, here, with `Telelem.Serve.Tree.resolve/2` (default 5 s
      bound); any resolve error is `{:invalid, :tree}`. The resulting
      `%Telelem.Serve.Tree.Pins{}` (root, `:coordinators`, `:guards`) is
      kept as is. Nothing re-resolves it: if that tree stops or is replaced,
      the config keeps naming the old pids (see "Stale pins"); a new setup
      must call `config/1` again.
    * `:units` (`1..2^31-1`), `:limits` (a `Telelem.Wire.Limits`, validated
      in full per request by the payload preflight), `:coordinator_ms`,
      `:grace_ms` and `:activation_ms` (each `1..60_000`).

  ## Order of `start/4` (each step only if every earlier one succeeded)

    1. The config **shape** is re-checked: local pids, a well-formed `Pins`
       with three distinct pids, bounded integers. No process is contacted
       and no liveness is checked; nothing is looked up or rebound.
    2. `Ingress.prepare/3` runs **once**. Its deadline is the one system
       deadline `D`.
    3. Preflight, before any process exists: `Handoff.receipt_meta/1`; the
       exact gated payload (`id: :primary`, the prepared body and target,
       `gate` deadline and `terminal_deadline` both `D`) through
       `Upstream.validate_guarded!/1` with the caller standing in for the
       coordinator pid; the guard options. Only the coordinator pid differs
       in the real launch.
    4. Clock reading `t1`: `t1 < D`, else `{:deadline_exceeded,
       :before_coordinator}`. Then `S = min(t1 + coordinator_ms, D)`.
    5. **One** `CoordinatorStart.start(pins.coordinators, S, opts)` with
       `admission_authority`, `terminal_deadline: D`, `coordinator_grant:
       {:one_primary, units}`, `receipts: [store:, producer:, store_mode:
       :sandbox, meta:]` and `register_timeout: D - t1`. Its own fixed
       options add `requester: self()`, `attempts: [:primary]`,
       `terminal_ack: true`, `activation_limit: S`. Every wait in it is
       bounded by `S`. The registration timer is armed only at activation,
       at or after `t1`, so it fires **no earlier than `D`**.
    6. Only on `{:ok, h}` (the activation reply `:ok` received and checked
       before `S`): a fresh clock reading `< D`, else the launch is not
       attempted (`{:launch, :deadline_exceeded, _}`).
    7. **One** `OwnerGuard.start(pins.guards, opts)` with the real payload,
       contained. OwnerGuard has its own budget,
       `A = min(now + activation_ms, D)`.

  No body, authentication input, principal or target is part of the
  coordinator options, the guard's child spec (OwnerGuard sends the payload
  only in the owner's GO), any error or the returned handle.

  ## Results: observed evidence only

  Every error after step 4 carries a `failure` map:

      %{request_id: id, handoff: pid | nil,
        activation: :not_sent | :refused | :unconfirmed | :confirmed,
        go: :not_sent | :unconfirmed,
        cancel: :submitted | :not_submitted}

    * `activation`: what step 5 established. `:not_sent`: no activation
      was submitted (`handoff: nil`; a late inactive child may exist, see
      `CoordinatorStart` "late child"; it is silent and stops by itself at
      `S`). `:refused`: the coordinator's own refusal, received before `S`
      (not active, except `:duplicate`, which cannot happen for this child's
      only activation). `:unconfirmed`: submitted with no reply checked
      before `S`, so `handoff` may be active. `:confirmed`: the reply `:ok`
      was checked before `S`.
    * `go`: `:not_sent` when OwnerGuard was never called (every
      `:coordinator` error and `:deadline_exceeded`) or returned one of its
      typed errors (it sends GO only on its success path); `:unconfirmed`
      when it raised, threw, exited or returned an unknown shape.
    * `cancel`: whether this call submitted the one requester cancel (below).

  Errors:

  | Reason | Started |
  | --- | --- |
  | `:invalid_config` | nothing |
  | `{:invalid, :opts}` | nothing (bad `start/4` options) |
  | `{:ingress, Ingress.reason()}` | nothing |
  | `{:preflight, :receipt_meta \\| :upstream \\| :guard}` | nothing |
  | `{:deadline_exceeded, :before_coordinator}` | nothing |
  | `{:coordinator, {:not_sent, CoordinatorStart.not_sent()}, failure}` | no activation submitted |
  | `{:coordinator, {:refused, atom}, failure}` | a coordinator that refused (it stops) |
  | `{:coordinator, :unconfirmed, failure}` | a coordinator that may be active |
  | `{:coordinator, :unrecognized, failure}` | unknown (see below) |
  | `{:launch, why, failure}` | an active coordinator, maybe a guard/owner |

  `why` for `:launch` is `:deadline_exceeded` (step 6), OwnerGuard's typed
  `:expired`, `:start_failed`, `:guard_down`, `:witness_down`, or
  `:unconfirmed`. `{:coordinator, :unrecognized, _}` means
  `CoordinatorStart.start/3` raised, threw, exited or returned an unknown
  shape (it raises only before spawning, on options this module builds):
  `handoff: nil`, `activation: :unconfirmed`, no cancel (no pid to cancel).

  ## The one cancel: known possibly active coordinator only

  Exactly when the exact coordinator pid is known and it may be active,
  i.e. `{:coordinator, :unconfirmed, _}`, `{:coordinator, {:refused,
  :duplicate}, _}` and every `{:launch, _, _}`, the requester submits
  **one** `Telelem.Handoff.submit_cancel(h, :launch_failed)`: one
  `:gen_server.send_request/2` immediately abandoned with
  `:gen_server.receive_response(req, 0)`. It does not wait for the
  coordinator (a suspended or busy coordinator does not delay the return),
  and its reply, if any, is discarded. `cancel: :submitted` means only that
  the request was sent: not that the coordinator was alive, processed it,
  or ended the request. Because `Telelem.Handoff`'s requester calls arrive
  in send order, the cancel is handled after the activation request; an
  inactive coordinator answers it `{:error, :inactive}` and changes nothing
  (it stops at `S` by itself).

  In no case is anything else done: no retry, no second cancel, no
  `Handoff.cleaned/3` on anyone's behalf, no kill of the coordinator, guard,
  owner, helper or witness, no wait. This is **not a bounded rollback** and
  nothing here confirms cleanup: a guard or owner OwnerGuard started may run
  until its own activation limit, the coordinator's end, or its fence.

  Nothing in the caller's mailbox is consumed: the coordinator's messages to
  the requester (`{:failed, :launch_failed}`, `{:closed, report}`, keyed by
  `failure.handoff`), owner messages to the controller and guard/witness
  reports to the observer are left in place. On a launch failure OwnerGuard
  returns no `ref`, guard, owner or witness, so its witness (and a guard)
  may still report under a `ref` this module never learned, and those
  reports cannot be attributed to this request by this API and must not be.

  ## Stale pins

  `start/4` uses exactly the pins resolved at setup. If that tree has
  stopped, the coordinator start on the dead `:coordinators` pid is
  `{:coordinator, {:not_sent, :start_unconfirmed}, _}` (nothing submitted,
  nothing cancelled); a replacement tree is never used. If only a guard
  start reaches a dead `:guards` pin, OwnerGuard's `:start_failed` follows.

  ## Not covered

  A listener, the requester's downstream protocol, any dial (listener-free
  tests stop before it), real-time scheduling (every bound is a monotonic
  clock check), any overall latency, shutdown or cleanup bound, and hard
  bounds on Ingress's callbacks. OwnerGuard's own helper/witness lifetime
  after a launcher failure is OwnerGuard's (separate) contract.
  """

  alias Telelem.Admission.Authority
  alias Telelem.Handoff
  alias Telelem.Receipts.{Producer, Store}
  alias Telelem.Serve.{CoordinatorStart, Ingress, Tree}
  alias Telelem.Serve.Ingress.Prepared
  alias Telelem.Serve.Primary.{Config, Handle}
  alias Telelem.Serve.Tree.Pins
  alias Telelem.Wire.{Limits, OwnerGuard, Upstream}

  @config_keys [
    :ingress,
    :authority,
    :store,
    :tree,
    :units,
    :limits,
    :coordinator_ms,
    :grace_ms,
    :activation_ms
  ]
  @start_opt_keys [:controller, :observer]
  @max_units 2_147_483_647
  @max_coordinator_ms 60_000
  # Contract §2 initial ceilings (the same ones Handoff checks at init).
  @ceilings %{max_pending: 256, max_retained: 1_024, max_event_bytes: 512}
  @launch_failures [:expired, :start_failed, :guard_down, :witness_down]
  @cancel_reason :launch_failed

  @type activation :: :not_sent | :refused | :unconfirmed | :confirmed
  @type launch_why ::
          :deadline_exceeded
          | :expired
          | :start_failed
          | :guard_down
          | :witness_down
          | :unconfirmed
  @type failure :: %{
          request_id: String.t(),
          handoff: pid() | nil,
          activation: activation(),
          go: :not_sent | :unconfirmed,
          cancel: :submitted | :not_submitted
        }
  @type reason ::
          :invalid_config
          | {:invalid, :opts}
          | {:ingress, Ingress.reason()}
          | {:preflight, :receipt_meta | :upstream | :guard}
          | {:deadline_exceeded, :before_coordinator}
          | {:coordinator,
             {:not_sent, CoordinatorStart.not_sent()}
             | {:refused, atom()}
             | :unconfirmed
             | :unrecognized, failure()}
          | {:launch, launch_why(), failure()}

  # ── Setup ───────────────────────────────────────────────────────────────

  @doc "Validates and pins the trusted setup once. See the module doc."
  @spec config(keyword() | map()) :: {:ok, Config.t()} | {:error, {:invalid, atom()}}
  def config(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and length(Enum.uniq(Keyword.keys(opts))) == length(opts),
      do: config(Map.new(opts)),
      else: {:error, {:invalid, :config}}
  end

  def config(%{} = opts) when not is_struct(opts) do
    with :ok <- only_keys(opts),
         {:ok, ingress} <- req(opts, :ingress, &match?(%Ingress.Config{}, &1)),
         :ok <- check(system_clock?(ingress), :ingress_clock),
         {:ok, auth} <- req(opts, :authority, &authority?/1),
         {:ok, store} <- req(opts, :store, &receipts_store?/1),
         {:ok, producer} <- canonical_producer(store),
         {:ok, root} <- req(opts, :tree, &local_pid?/1),
         {:ok, pins} <- resolve_tree(root),
         {:ok, units} <- req(opts, :units, &in_range?(&1, @max_units)),
         {:ok, limits} <- req(opts, :limits, &match?(%Limits{}, &1)),
         {:ok, cms} <- req(opts, :coordinator_ms, &in_range?(&1, @max_coordinator_ms)),
         {:ok, grace} <- req(opts, :grace_ms, &in_range?(&1, OwnerGuard.bounds().grace_ms)),
         {:ok, act} <-
           req(opts, :activation_ms, &in_range?(&1, OwnerGuard.bounds().activation_ms)) do
      {:ok,
       %Config{
         ingress: ingress,
         authority: auth,
         store: store,
         producer: producer,
         tree: pins,
         units: units,
         limits: limits,
         coordinator_ms: cms,
         grace_ms: grace,
         activation_ms: act
       }}
    end
  end

  def config(_), do: {:error, {:invalid, :config}}

  # The one resolution of the exact root (Tree.resolve/2 checks the root's
  # initial call and takes one bounded which_children snapshot).
  defp resolve_tree(root) do
    case contained(fn -> Tree.resolve(root) end) do
      {:returned, {:ok, %Pins{root: ^root} = pins}} -> {:ok, pins}
      _ -> {:error, {:invalid, :tree}}
    end
  end

  # Resolved once from the pinned store and confirmed against that same pin
  # (ETS reads only): canonical, actual mode :sandbox, limits within §2.
  defp canonical_producer(store) do
    with {:returned, %Producer{} = p} <- contained(fn -> Store.producer(store) end),
         {:ok, %{mode: :sandbox, limits: l}} <- Producer.binding(p, store),
         true <- Enum.all?(@ceilings, fn {k, max} -> Map.fetch!(l, k) <= max end) do
      {:ok, p}
    else
      _ -> {:error, {:invalid, :store}}
    end
  end

  # ── Start ───────────────────────────────────────────────────────────────

  @doc """
  Prepares and launches one primary attempt from the calling (requester)
  process. `opts`: optional `:controller` and `:observer` local pids (default
  `self()`), which receive the owner's `{:telelem_wire, ...}` messages and the
  guard/witness reports. See the module doc for order and results.
  """
  @spec start(Config.t(), term(), Ingress.body_reader(), keyword()) ::
          {:ok, Handle.t()} | {:error, reason()}
  def start(cfg, auth_input, body_reader, opts \\ [])

  def start(%Config{} = cfg, auth_input, body_reader, opts) do
    cond do
      not config_intact?(cfg) -> {:error, :invalid_config}
      not is_function(body_reader, 2) -> {:error, :invalid_config}
      true -> with {:ok, roles} <- start_opts(opts), do: run(cfg, auth_input, body_reader, roles)
    end
  end

  def start(_cfg, _auth_input, _body_reader, _opts), do: {:error, :invalid_config}

  defp run(cfg, auth_input, body_reader, roles) do
    with {:ok, %Prepared{} = p} <- prepare(cfg, auth_input, body_reader),
         {:ok, meta} <- receipt_meta(p),
         d = p.deadline_ms,
         {:ok, payload} <- preflight_payload(cfg, p, d, roles),
         {:ok, guard_opts} <- preflight_guard(cfg, roles),
         {:ok, t1} <- before_coordinator(d) do
      s = min(t1 + cfg.coordinator_ms, d)
      id = p.gateway_id

      case coordinator(cfg, meta, d, s, d - t1) do
        {:ok, h} -> launch(cfg, id, h, d, payload, guard_opts)
        {:error, why, h, activation} -> coordinator_failed(why, id, h, activation)
      end
    end
  end

  defp prepare(cfg, auth_input, body_reader) do
    case Ingress.prepare(cfg.ingress, auth_input, body_reader) do
      {:ok, %Prepared{}} = ok -> ok
      {:error, reason} -> {:error, {:ingress, reason}}
    end
  end

  defp receipt_meta(p) do
    case Handoff.receipt_meta(p) do
      {:ok, meta} -> {:ok, meta}
      _ -> {:error, {:preflight, :receipt_meta}}
    end
  end

  # The exact payload of the real launch, validated by the launcher's own
  # check with the caller standing in for the not-yet-started coordinator
  # (only `is_pid/1` and locality depend on it). The coordinator pid is put
  # in afterwards; nothing else changes.
  defp preflight_payload(cfg, p, d, roles) do
    payload = [
      handoff: self(),
      id: :primary,
      controller: roles.controller,
      body: p.body,
      limits: cfg.limits,
      gate: [target: p.target, deadline: d],
      terminal_deadline: d
    ]

    case contained(fn -> Upstream.validate_guarded!(payload) end) do
      {:returned, %{deadline: ^d}} -> {:ok, payload}
      _ -> {:error, {:preflight, :upstream}}
    end
  end

  # OwnerGuard's own option rules for everything except `:upstream`.
  defp preflight_guard(cfg, roles) do
    b = OwnerGuard.bounds()

    ok =
      Enum.all?([self(), roles.observer], &local_pid?/1) and
        in_range?(cfg.grace_ms, b.grace_ms) and in_range?(cfg.activation_ms, b.activation_ms)

    if ok,
      do:
        {:ok,
         [
           requester: self(),
           observer: roles.observer,
           grace_ms: cfg.grace_ms,
           activation_ms: cfg.activation_ms
         ]},
      else: {:error, {:preflight, :guard}}
  end

  defp before_coordinator(d) do
    t1 = now()
    if t1 < d, do: {:ok, t1}, else: {:error, {:deadline_exceeded, :before_coordinator}}
  end

  # One CoordinatorStart on the pinned coordinators branch, bounded by S.
  # `register_timeout` is `D - t1`, armed at activation (at or after t1), so
  # it fires no earlier than D.
  defp coordinator(cfg, meta, d, s, register_ms) do
    opts = [
      admission_authority: cfg.authority,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, cfg.units},
      receipts: [store: cfg.store, producer: cfg.producer, store_mode: :sandbox, meta: meta],
      register_timeout: register_ms
    ]

    start_result(fn -> CoordinatorStart.start(cfg.tree.coordinators, s, opts) end)
  end

  @doc false
  # Classifies one CoordinatorStart result, contained (pure apart from
  # running `fun`). Only a confirmed activation proceeds to the launch.
  # Returns `{:ok, h}` or `{:error, why, h | nil, activation}`.
  def start_result(fun) do
    case contained(fun) do
      {:returned, {:ok, h}} when is_pid(h) ->
        {:ok, h}

      {:returned, {:error, {:not_sent, why}}} ->
        {:error, {:not_sent, why}, nil, :not_sent}

      {:returned, {:error, {:refused, reason, h}}} when is_atom(reason) and is_pid(h) ->
        {:error, {:refused, reason}, h, :refused}

      {:returned, {:error, {:unconfirmed, h}}} when is_pid(h) ->
        {:error, :unconfirmed, h, :unconfirmed}

      _ ->
        {:error, :unrecognized, nil, :unconfirmed}
    end
  end

  # Possibly active and exactly known: one cancel. Otherwise none.
  defp coordinator_failed(why, id, h, activation) do
    cancel =
      case why do
        :unconfirmed -> submit_cancel(h)
        {:refused, :duplicate} -> submit_cancel(h)
        _ -> :not_submitted
      end

    {:error,
     {:coordinator, why,
      %{request_id: id, handoff: h, activation: activation, go: :not_sent, cancel: cancel}}}
  end

  # From here on the coordinator's activation is confirmed.
  defp launch(cfg, id, h, d, payload, guard_opts) do
    if now() < d do
      opts = [{:upstream, Keyword.put(payload, :handoff, h)} | guard_opts]

      case launch_result(fn -> OwnerGuard.start(cfg.tree.guards, opts) end) do
        {:go_sent, %{guard: g, owner: o, witness: w, ref: ref}} ->
          {:ok,
           %Handle{
             request_id: id,
             handoff: h,
             guard: g,
             owner: o,
             witness: w,
             ref: ref,
             deadline_ms: d
           }}

        {:not_sent, why} ->
          launch_failed(why, :not_sent, id, h)

        :unconfirmed ->
          launch_failed(:unconfirmed, :unconfirmed, id, h)
      end
    else
      launch_failed(:deadline_exceeded, :not_sent, id, h)
    end
  end

  @doc false
  # Classifies one OwnerGuard launch, contained. Only its handle is GO sent;
  # only its typed errors are GO not sent; anything else is unconfirmed.
  def launch_result(fun) do
    case contained(fun) do
      {:returned, {:ok, %{guard: g, owner: o, witness: w, ref: ref} = handle}}
      when is_pid(g) and is_pid(o) and is_pid(w) and is_reference(ref) and map_size(handle) == 4 ->
        {:go_sent, handle}

      {:returned, {:error, why}} when why in @launch_failures ->
        {:not_sent, why}

      _ ->
        :unconfirmed
    end
  end

  defp launch_failed(why, go, id, h) do
    {:error,
     {:launch, why,
      %{request_id: id, handoff: h, activation: :confirmed, go: go, cancel: submit_cancel(h)}}}
  end

  # The one requester cancel: submitted, never awaited. Nothing consumed.
  # Not contained: for a pid it only sends and abandons (it cannot fail), and
  # a raise would not show whether the request left, so none is reported.
  defp submit_cancel(h) when is_pid(h), do: Handoff.submit_cancel(h, @cancel_reason)

  @doc "The reason the requester's one cancel carries."
  def cancel_reason, do: @cancel_reason

  # ── Checks ──────────────────────────────────────────────────────────────

  defp start_opts(opts) do
    me = self()

    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- @start_opt_keys,
         true <- length(Enum.uniq(Keyword.keys(opts))) == length(opts),
         roles = %{
           controller: Keyword.get(opts, :controller, me),
           observer: Keyword.get(opts, :observer, me)
         },
         true <- local_pid?(roles.controller) and local_pid?(roles.observer) do
      {:ok, roles}
    else
      _ -> {:error, {:invalid, :opts}}
    end
  end

  # Shape only; no process is contacted and nothing is re-resolved. The pins
  # were checked in config/1.
  defp config_intact?(%Config{} = c) do
    b = OwnerGuard.bounds()

    match?(%Ingress.Config{}, c.ingress) and system_clock?(c.ingress) and
      Enum.all?([c.authority, c.store], &local_pid?/1) and pins_shape?(c.tree) and
      match?(%Producer{}, c.producer) and in_range?(c.units, @max_units) and
      match?(%Limits{}, c.limits) and in_range?(c.coordinator_ms, @max_coordinator_ms) and
      in_range?(c.grace_ms, b.grace_ms) and in_range?(c.activation_ms, b.activation_ms)
  end

  defp pins_shape?(%Pins{root: r, coordinators: c, guards: g}),
    do: Enum.all?([r, c, g], &local_pid?/1) and length(Enum.uniq([r, c, g])) == 3

  defp pins_shape?(_), do: false

  defp system_clock?(%Ingress.Config{clock: {Telelem.Instance.SystemClock, _}}), do: true
  defp system_clock?(_), do: false

  defp authority?(pid),
    do: live_local?(pid) and initial_call(pid) == {Authority, :init, 1}

  # Checked before `Store.producer/1` is called, so an arbitrary pid is
  # refused without a call. The binding check is what accepts the store.
  defp receipts_store?(pid),
    do: live_local?(pid) and initial_call(pid) == {Store, :init, 1}

  defp initial_call(pid) do
    case contained(fn -> :proc_lib.translate_initial_call(pid) end) do
      {:returned, mfa} -> mfa
      :failed -> nil
    end
  end

  defp live_local?(p), do: local_pid?(p) and Process.alive?(p)
  defp local_pid?(p), do: is_pid(p) and node(p) == node()
  defp in_range?(n, max), do: is_integer(n) and n >= 1 and n <= max

  defp check(true, _field), do: :ok
  defp check(false, field), do: {:error, {:invalid, field}}

  defp only_keys(opts) do
    case Map.keys(opts) -- @config_keys do
      [] -> :ok
      [k | _] when is_atom(k) -> {:error, {:invalid, k}}
      _ -> {:error, {:invalid, :config}}
    end
  end

  defp req(opts, key, valid?) do
    case Map.fetch(opts, key) do
      {:ok, v} -> if valid?.(v), do: {:ok, v}, else: {:error, {:invalid, key}}
      :error -> {:error, {:invalid, key}}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)

  defp contained(fun) do
    {:returned, fun.()}
  catch
    _kind, _reason -> :failed
  end
end
