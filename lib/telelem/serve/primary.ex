defmodule Telelem.Serve.Primary.Config do
  @moduledoc """
  Trusted, validated primary composition configuration. Build only with
  `Telelem.Serve.Primary.config/1`; `start/4` re-checks the struct's shape.
  `Inspect` shows only the numeric policy fields.
  """

  @enforce_keys [
    :ingress,
    :authority,
    :store,
    :producer,
    :supervisor,
    :units,
    :limits,
    :grace_ms,
    :activation_ms
  ]
  @derive {Inspect, only: [:units, :grace_ms, :activation_ms]}
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          ingress: Telelem.Serve.Ingress.Config.t(),
          authority: pid(),
          store: pid(),
          producer: Telelem.Receipts.Producer.t(),
          supervisor: pid(),
          units: pos_integer(),
          limits: Telelem.Wire.Limits.t(),
          grace_ms: pos_integer(),
          activation_ms: pos_integer()
        }
end

defmodule Telelem.Serve.Primary.Handle do
  @moduledoc """
  What a successful `Telelem.Serve.Primary.start/4` returns: identities only.

    * `request_id`: the gateway-generated opaque id (receipt lookup key).
    * `handoff`: the coordinator pid; the caller is its requester.
    * `guard`, `owner`, `witness`, `ref`: exactly the `Telelem.Wire.OwnerGuard`
      handle. Observer reports carry `ref`; controller messages carry `owner`.
    * `deadline_ms`: the one pinned system deadline `D`.

  No body, authentication input, principal or target. Success means only
  that the one GO was sent to `owner` (see the module doc of
  `Telelem.Serve.Primary`), never that anything was admitted, dialed or served.
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
  Listener-free M2a composition of one primary attempt: one
  `Telelem.Serve.Ingress` preparation, one `Telelem.Handoff` coordinator
  (`coordinator_grant`, `terminal_ack: true`, receipts) and one guarded
  `Telelem.Wire.Upstream` owner launched by `Telelem.Wire.OwnerGuard`. It
  opens no socket itself, starts no listener, retries nothing and never
  selects a second attempt.

      config(opts) -> {:ok, Config} | {:error, {:invalid, field}}
      start(config, auth_input, body_reader, opts \\\\ []) -> {:ok, Handle} | {:error, reason}

  `start/4` must run in the **requester** process (the downstream owner): it
  becomes the coordinator's requester and the guard monitors it. After
  `{:ok, handle}` the caller runs the requester side of the `Telelem.Handoff`
  protocol (`{:telelem_handoff, handle.handoff, msg}`, `terminal_written/2`,
  credit to `handle.owner`); nothing here does that.

  ## Trusted setup (`config/1`)

  Everything is pinned once, at setup, independently of the request:

    * `:ingress`: a `Telelem.Serve.Ingress.Config` (from `Ingress.config/1`)
      whose clock is `{Telelem.Instance.SystemClock, _}`, so the prepared
      deadline is in the same `System.monotonic_time(:millisecond)` domain
      that `Handoff`, `OwnerGuard` and `Upstream` use. Any other clock is
      refused (`{:invalid, :ingress_clock}`).
    * `:authority`: the pid of a live local `Telelem.Admission.Authority`
      (checked by its initial call, as `Handoff` does). Never re-resolved.
    * `:store`: the pid of a live local `Telelem.Receipts.Store` (its initial
      call is checked first). The producer handle is resolved here, once,
      with `Store.producer/1` (a `GenServer.call`, 5 s; a busy store delays
      setup, never a request), and accepted only
      if `Producer.binding/2` confirms it is that store's canonical handle,
      with actual mode `:sandbox` and limits within the §2 ceilings
      (256 / 1024 / 512). The store is never derived from the handle.
    * `:supervisor`: the pid of a live local `DynamicSupervisor` for guards.
      Only "a live local supervisor" is checked (a DynamicSupervisor's initial
      call is the generic `{:supervisor, Supervisor.Default, 1}`); that it is
      a DynamicSupervisor is trusted. Any other supervisor makes OwnerGuard's
      `start_child/2` fail, which is `{:launch, :start_failed, _}`.
    * `:units` (`1..2^31-1`), `:limits` (a `Telelem.Wire.Limits`, validated
      in full per request by the payload preflight), `:grace_ms` and
      `:activation_ms` (`1..OwnerGuard.bounds()`).

  ## Order of `start/4` (each step only if every earlier one succeeded)

    1. The config shape is re-checked (no process is contacted).
    2. `Ingress.prepare/3` runs **once** with `auth_input` and `body_reader`.
    3. Preflight, before any process exists: `Handoff.receipt_meta/1` of the
       prepared value; the exact gated payload (`id: :primary`, the prepared
       body and target, `gate` deadline and `terminal_deadline` both `D =
       prepared.deadline_ms`) through `Upstream.validate_guarded!/1`, with the
       caller standing in for the coordinator pid; and the guard options
       (local requester/controller/observer pids, the bounded grace and
       activation). Only the coordinator pid differs in the real launch.
    4. Clock reading `t1`: `t1 < D` or `{:deadline_exceeded, :before_handoff}`.
    5. `Handoff.start/1` (unlinked) with `requester: self()`, `attempts:
       [:primary]`, `admission_authority`, `terminal_deadline: D`,
       `coordinator_grant: {:one_primary, units}`, `terminal_ack: true`,
       `receipts: [store:, producer:, store_mode: :sandbox, meta:]` and
       `register_timeout: D - t1`, so the registration timer, armed at or
       after `t1`, fires **no earlier than `D`**. This is a synchronous
       `GenServer.start/2` with the default `:infinity` timeout: its duration
       is **not bounded** here (it does not block on the authority or store,
       but nothing interrupts it). The coordinator is not supervised.
    6. Clock reading `t2`: `t2 < D`, else the launch has expired (below).
    7. `OwnerGuard.start/2` with the real payload, called once, contained.

  No body, authentication input, principal or target is part of the
  coordinator options, the guard's child spec (OwnerGuard sends the payload
  only in the owner's GO), any error or the returned handle.

  ## What a result means

    * `{:ok, %Handle{}}`: OwnerGuard returned its handle, i.e. **the one GO
      was sent** to `handle.owner`. Not admission, dispatch or serving
      success: the owner may still be refused (for example the authority
      denies the reservation and it ends before dialing).
    * Errors before step 5: nothing was started.
    * `{:handoff_start, reason}`: `Handoff.start/1` returned an error; no
      coordinator is running.
    * `{:launch, why, failure}` after a **confirmed** coordinator start:
        - `why` in `:deadline_exceeded` (step 6), `:expired`, `:start_failed`,
          `:guard_down`, `:witness_down` (OwnerGuard's typed failures):
          `failure.go == :not_sent`. OwnerGuard sends GO only on its success
          path, so a typed failure establishes that this launch sent no GO.
        - `why == :unconfirmed`: OwnerGuard raised, threw, exited or returned
          an unknown shape; whether a GO was sent is not known
          (`failure.go == :unconfirmed`).

      In every `:launch` case the requester then calls
      `Handoff.cancel(h, :launch_failed)` **once**, best effort, and
      `failure.cancel` is exactly the reply it observed (`:ok`,
      `{:error, :timeout}`, `{:error, :handoff_down}`, `{:error,
      :not_requester}`; anything else, or a raise, is `:unrecognized`).
      Nothing else is done: no retry, no `Handoff.cleaned/3` on anyone's
      behalf, no kill of the coordinator, guard, owner, helper or witness.
      This is **not a bounded rollback**: a guard or owner that OwnerGuard
      started may still be running until its own activation limit, the
      coordinator's cancel, or its fence, and nothing here waits for or
      confirms any of that. The coordinator's own messages to the requester
      (`{:failed, :launch_failed}`, `{:closed, report}`) are left in the
      caller's mailbox, keyed by `failure.handoff`.

      On a launch failure OwnerGuard returns no `ref`, guard, owner or
      witness. Its witness (and a guard, if one was started) may still send
      `{:telelem_owner_witness, ref, _}` / `{:telelem_owner_guard, ref, ...}`
      to the observer under a `ref` this module never learned, and an owner
      may still send `{:telelem_wire, owner, :primary, _}` to the controller.
      Those reports cannot be attributed to this request by this API and must
      not be.

  ## Errors

  Bounded terms only:

  | Reason | Started |
  | --- | --- |
  | `:invalid_config` | nothing |
  | `{:invalid, :opts}` | nothing (bad `start/4` options) |
  | `{:ingress, Ingress.reason()}` | nothing |
  | `{:preflight, :receipt_meta \\| :upstream \\| :guard}` | nothing |
  | `{:deadline_exceeded, :before_handoff}` | nothing |
  | `{:handoff_start, {:bad_option, atom} \\| {:linked, atom} \\| :other}` | nothing running |
  | `{:launch, why, %{request_id:, handoff:, go:, cancel:}}` | coordinator, maybe guard/owner |

  ## Not covered

  A listener, the requester's downstream protocol, any dial (listener-free
  tests stop before it), and hard bounds on the blocking steps (Ingress's
  callbacks, `Handoff.start/1`, `Handoff.cancel/2`'s 5 s call).
  """

  alias Telelem.Admission.Authority
  alias Telelem.Handoff
  alias Telelem.Receipts.{Producer, Store}
  alias Telelem.Serve.Ingress
  alias Telelem.Serve.Ingress.Prepared
  alias Telelem.Serve.Primary.{Config, Handle}
  alias Telelem.Wire.{Limits, OwnerGuard, Upstream}

  @config_keys [
    :ingress,
    :authority,
    :store,
    :supervisor,
    :units,
    :limits,
    :grace_ms,
    :activation_ms
  ]
  @start_opt_keys [:controller, :observer]
  @max_units 2_147_483_647
  # Contract §2 initial ceilings (the same ones Handoff checks at init).
  @ceilings %{max_pending: 256, max_retained: 1_024, max_event_bytes: 512}
  @launch_failures [:expired, :start_failed, :guard_down, :witness_down]
  @cancel_replies [:ok, {:error, :timeout}, {:error, :handoff_down}, {:error, :not_requester}]
  @cancel_reason :launch_failed

  @type launch_why ::
          :deadline_exceeded
          | :expired
          | :start_failed
          | :guard_down
          | :witness_down
          | :unconfirmed
  @type failure :: %{
          request_id: String.t(),
          handoff: pid(),
          go: :not_sent | :unconfirmed,
          cancel: :ok | {:error, :timeout | :handoff_down | :not_requester} | :unrecognized
        }
  @type reason ::
          :invalid_config
          | {:invalid, :opts}
          | {:ingress, Ingress.reason()}
          | {:preflight, :receipt_meta | :upstream | :guard}
          | {:deadline_exceeded, :before_handoff}
          | {:handoff_start, term()}
          | {:launch, launch_why(), failure()}

  # ── Setup ───────────────────────────────────────────────────────────────

  @doc "Validates and pins the trusted setup. See the module doc."
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
         {:ok, sup} <- req(opts, :supervisor, &dynamic_supervisor?/1),
         {:ok, units} <- req(opts, :units, &in_range?(&1, @max_units)),
         {:ok, limits} <- req(opts, :limits, &match?(%Limits{}, &1)),
         {:ok, grace} <- req(opts, :grace_ms, &in_range?(&1, OwnerGuard.bounds().grace_ms)),
         {:ok, act} <-
           req(opts, :activation_ms, &in_range?(&1, OwnerGuard.bounds().activation_ms)) do
      {:ok,
       %Config{
         ingress: ingress,
         authority: auth,
         store: store,
         producer: producer,
         supervisor: sup,
         units: units,
         limits: limits,
         grace_ms: grace,
         activation_ms: act
       }}
    end
  end

  def config(_), do: {:error, {:invalid, :config}}

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
         {:ok, t1} <- before_handoff(d),
         {:ok, h} <- start_handoff(cfg, meta, d, d - t1) do
      launch(cfg, p.gateway_id, h, d, payload, guard_opts)
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

  defp before_handoff(d) do
    t1 = now()
    if t1 < d, do: {:ok, t1}, else: {:error, {:deadline_exceeded, :before_handoff}}
  end

  # Synchronous and not bounded here (GenServer.start/2, timeout :infinity).
  # `register_timeout` is `D - t1` with `t1` read before this call, so the
  # timer armed in init (at or after t1) fires no earlier than D.
  defp start_handoff(cfg, meta, d, register_ms) do
    opts = [
      requester: self(),
      attempts: [:primary],
      admission_authority: cfg.authority,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, cfg.units},
      terminal_ack: true,
      receipts: [store: cfg.store, producer: cfg.producer, store_mode: :sandbox, meta: meta],
      register_timeout: register_ms
    ]

    case contained(fn -> Handoff.start(opts) end) do
      {:returned, {:ok, h}} when is_pid(h) -> {:ok, h}
      {:returned, {:error, reason}} -> {:error, {:handoff_start, start_reason(reason)}}
      _ -> {:error, {:handoff_start, :other}}
    end
  end

  defp start_reason({:bad_option, o} = r) when is_atom(o), do: r
  defp start_reason({:bad_option, {a, b}} = r) when is_atom(a) and is_atom(b), do: r
  defp start_reason({:linked, role} = r) when is_atom(role), do: r
  defp start_reason(_), do: :other

  # From here on the coordinator is confirmed running.
  defp launch(cfg, request_id, h, d, payload, guard_opts) do
    if now() < d do
      opts = [{:upstream, Keyword.put(payload, :handoff, h)} | guard_opts]

      case launch_result(fn -> OwnerGuard.start(cfg.supervisor, opts) end) do
        {:go_sent, %{guard: g, owner: o, witness: w, ref: ref}} ->
          {:ok,
           %Handle{
             request_id: request_id,
             handoff: h,
             guard: g,
             owner: o,
             witness: w,
             ref: ref,
             deadline_ms: d
           }}

        {:not_sent, why} ->
          failed(why, :not_sent, request_id, h)

        :unconfirmed ->
          failed(:unconfirmed, :unconfirmed, request_id, h)
      end
    else
      failed(:deadline_exceeded, :not_sent, request_id, h)
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

  # One best-effort cancel by the requester; its reply is reported as
  # observed. No cleaned/3, no kill, no wait, nothing consumed.
  defp failed(why, go, request_id, h) do
    cancel =
      case contained(fn -> Handoff.cancel(h, @cancel_reason) end) do
        {:returned, reply} when reply in @cancel_replies -> reply
        _ -> :unrecognized
      end

    {:error, {:launch, why, %{request_id: request_id, handoff: h, go: go, cancel: cancel}}}
  end

  @doc "The reason the requester passes to `Handoff.cancel/2` after a launch failure."
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

  # Shape only; no process is contacted. The pins were checked in config/1.
  defp config_intact?(%Config{} = c) do
    b = OwnerGuard.bounds()

    match?(%Ingress.Config{}, c.ingress) and system_clock?(c.ingress) and
      Enum.all?([c.authority, c.store, c.supervisor], &local_pid?/1) and
      match?(%Producer{}, c.producer) and in_range?(c.units, @max_units) and
      match?(%Limits{}, c.limits) and in_range?(c.grace_ms, b.grace_ms) and
      in_range?(c.activation_ms, b.activation_ms)
  end

  defp system_clock?(%Ingress.Config{clock: {Telelem.Instance.SystemClock, _}}), do: true
  defp system_clock?(_), do: false

  defp authority?(pid),
    do: live_local?(pid) and initial_call(pid) == {Authority, :init, 1}

  # Checked before `Store.producer/1` is called, so an arbitrary pid is
  # refused without a call. The binding check is what accepts the store.
  defp receipts_store?(pid),
    do: live_local?(pid) and initial_call(pid) == {Store, :init, 1}

  # A DynamicSupervisor reports the generic supervisor initial call
  # (`{:supervisor, Supervisor.Default, 1}`), so only "a supervisor" is
  # checkable here; that it is a DynamicSupervisor is trusted setup. Any other
  # supervisor makes OwnerGuard's start_child fail: `:start_failed`.
  defp dynamic_supervisor?(pid),
    do: live_local?(pid) and match?({:supervisor, _, 1}, initial_call(pid))

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
