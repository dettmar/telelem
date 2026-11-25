defmodule Telelem.HandoffReceiptsTest do
  @moduledoc """
  TEL-013: coordinator-owned receipt publication of `Telelem.Handoff`
  (`receipts: [store:, producer:, store_mode:, meta:]`, opt-in on top of
  `coordinator_grant: {:one_primary, units}` and `terminal_ack: true`).

  Listener-free. One `Telelem.Instance` with no workers and its real
  `Telelem.Admission.Authority`, one real `Telelem.Receipts.Store` per case,
  one real handoff per request and one scripted fake attempt (no socket: its
  HTTP send is a `{:fake_http, fake, marker}` message to the test). Evidence
  is read only through `Store.lookup/3` with an authenticated-shape reader,
  the store's stats and `Producer.stats/1`.

  Ordering claims ("published before X was sent") are taken from an isolated
  `:trace` session on the coordinator itself (its sends and its calls of
  `:ets.insert_new/2` and `System.monotonic_time/1`, in the order it made
  them); no hook in the product code. Barriers are messages, mailbox reads
  and `trace_delivered`; no sleeps.

      scripts/mix test test/telelem/handoff_receipts_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Admission.Authority
  alias Telelem.{Handoff, Instance}
  alias Telelem.Receipts.{Producer, Receipt, Store, Writer}
  alias Telelem.Route.{Plan, Revision}
  alias Telelem.Serve.Ingress.Prepared

  @moduletag :tmp_dir
  @t 1_000
  @window 300
  @units 4
  @tenant "local"
  @dep "a"
  @http_marker "POST /v1/fake (simulated, no socket)"
  # Distinct sentinels that must never reach receipts, status, report or trace.
  @marker "data: [DONE] MARKER-5be1\n\n"
  @chunk "data: {\"x\":\"CHUNK-7f3a\"}\n\n"
  @terminal {:upstream_terminal, "TERMINAL-9c1e"}
  @sentinels [@marker, @chunk, "TERMINAL-9c1e", @terminal, @http_marker]

  @reader %{
    tenant_id: @tenant,
    subject_id: "operator",
    mode: :sandbox,
    permissions: [:receipt_read]
  }

  setup %{tmp_dir: dir} do
    sup =
      start_supervised!({
        Instance,
        id: :handoff_receipts_gw,
        limits: %{slots: 4, units: 40},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(sup)
    auth = Instance.authority(inst)
    store = start_store!(:store)
    %{inst: inst, auth: auth, store: store}
  end

  # ── options ─────────────────────────────────────────────────────────────

  test "opt-in needs the coordinator grant, terminal_ack and a checked handle; default shapes unchanged",
       ctx do
    p = Store.producer(ctx.store)
    meta = meta("opts")
    good = [store: ctx.store, producer: p, store_mode: :sandbox, meta: meta]
    d = now() + 60_000

    base = [
      requester: idle(),
      attempts: [:primary],
      admission_authority: ctx.auth,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, @units},
      terminal_ack: true
    ]

    # Default: no receipts key anywhere, nothing published.
    {:ok, h0} = Handoff.start(base)
    refute Map.has_key?(Handoff.status(h0), :receipts)
    Process.exit(h0, :kill)

    forged_table = :ets.new(:forged, [:set, :public])
    forged = %{p | table: forged_table}
    ref_bad = %{p | ref: :atomics.new(3, signed: true)}

    # Shape and handle-field cases; genuine other stores are in "store binding".
    bad_receipts = [
      false,
      p,
      [producer: p, store_mode: :sandbox, meta: meta],
      Keyword.delete(good, :store_mode),
      Keyword.put(good, :extra, 1),
      Keyword.put(good, :store, nil),
      # A registered-name style atom, not a pin.
      Keyword.put(good, :store, :ets.info(p.table, :name)),
      Keyword.put(good, :store, idle()),
      Keyword.put(good, :store_mode, :serving),
      Keyword.put(good, :meta, Map.put(meta, :body, "{}")),
      Keyword.put(good, :meta, Map.delete(meta, :mode)),
      Keyword.put(good, :meta, %{meta | request_id: "bad id"}),
      Keyword.put(good, :meta, %{meta | route_rev: 0}),
      Keyword.put(good, :meta, %{meta | deployment_id: :none}),
      Keyword.put(good, :producer, Map.from_struct(p)),
      Keyword.put(good, :producer, Map.put(p, :extra, 1)),
      Keyword.put(good, :producer, forged),
      Keyword.put(good, :producer, ref_bad),
      Keyword.put(good, :producer, %{p | max_pending: 257}),
      Keyword.put(good, :producer, %{p | max_event_bytes: 4_096}),
      # Smaller than the store's actual limits: still not the canonical handle.
      Keyword.put(good, :producer, %{p | max_pending: 8}),
      Keyword.put(good, :producer, %{p | max_event_bytes: 128}),
      Keyword.put(good, :producer, %{p | loss_slots: p.loss_slots + 1}),
      Keyword.put(good, :producer, %{p | table: :not_a_table}),
      Keyword.put(good, :producer, %{p | descriptor: :not_a_table}),
      Keyword.put(good, :producer, %{p | table: p.descriptor, descriptor: p.table})
    ]

    for r <- bad_receipts do
      assert {:error, {:bad_option, :receipts}} =
               Handoff.start(Keyword.put(base, :receipts, r)),
             inspect(r, limit: 5)
    end

    # Receipts need the coordinator grant and terminal_ack.
    for drop <- [[:coordinator_grant], [:terminal_ack]] do
      assert {:error, {:bad_option, :receipts}} =
               Handoff.start(Keyword.drop(base, drop) ++ [receipts: good]),
             inspect(drop)
    end

    assert %{pending: 0, retained: 0, receipts: 0, rejected: 0, overflow: 0} =
             Store.stats(ctx.store)

    assert Authority.snapshot(ctx.auth).grants == %{}

    # Accepted: exactly one ingress, nothing else, before any attempt.
    {:ok, h} = Handoff.start(Keyword.put(base, :receipts, good))
    on_exit(fn -> Process.exit(h, :kill) end)
    assert %{receipts: :recording, grant: :idle} = Handoff.status(h)

    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "opts")
    assert %{completeness: :incomplete, gaps: [:no_terminal]} = lookup!(ctx.store, "opts")
  end

  # ── store binding (separate genuine stores, no listener) ────────────────

  test "binding: only the pinned store's own handle, with its actual mode and limits within the ceilings",
       ctx do
    a = ctx.store
    b = start_store!(:other_sandbox)
    serving = start_store!(:serving, mode: :serving)
    [pa, pb, ps] = Enum.map([a, b, serving], &Store.producer/1)

    # Same-mode mix-up, both directions, with unaltered genuine handles.
    refused!(ctx, "mix", a, pb)
    refused!(ctx, "mix", b, pa)

    # One field of the pinned store's handle taken from another genuine store.
    for field <- [:table, :descriptor, :ref] do
      refused!(ctx, "splice", a, Map.put(pa, field, Map.fetch!(pb, field)))
    end

    # The store's actual mode differs from the trusted statement and meta.
    refused!(ctx, "mode", serving, ps, :sandbox)
    refused!(ctx, "mode", a, pa, :serving)

    # Each actual ceiling exceeded alone (the handle is the genuine one).
    over =
      for {id, opt} <- [
            over_pending: [max_pending: 257],
            over_retained: [max_retained: 1_025],
            over_bytes: [max_event_bytes: 513]
          ] do
        st = start_store!(id, opt)
        refused!(ctx, "ceiling", st, Store.producer(st))
        st
      end

    for st <- [a, b, serving | over] do
      assert %{pending: 0, retained: 0, receipts: 0, rejected: 0} = Store.stats(st)
    end

    # Positive controls: exactly at the ceilings, and each store with its own
    # handle and actual mode. Evidence lands only in the pinned store.
    at = start_store!(:at_ceiling, max_pending: 256, max_retained: 1_024, max_event_bytes: 512)
    {:ok, _} = try_h(ctx, "at", at, Store.producer(at))
    {:ok, _} = try_h(ctx, "own-b", b, pb)
    {:ok, _} = try_h(ctx, "own-s", serving, ps, :serving)

    assert [{:ingress, 0, :none, :none, :none}] = events!(at, "at")
    assert [{:ingress, 0, :none, :none, :none}] = events!(b, "own-b")
    assert {:error, :not_found} = Store.lookup(a, @reader, "own-b")

    assert {:ok, %Receipt{events: [%{kind: :ingress}]}} =
             Store.lookup(serving, %{@reader | mode: :serving}, "own-s")

    assert %{receipts: 0} = Store.stats(a)
  end

  test "binding: a stopped or replaced store fails closed in every pairing", ctx do
    old = start_store!(:replaced)
    p_old = Store.producer(old)
    stop_supervised!(:replaced)
    new = start_store!(:replaced)
    p_new = Store.producer(new)

    refused!(ctx, "stale", old, p_old)
    refused!(ctx, "stale", new, p_old)
    refused!(ctx, "stale", old, p_new)
    assert %{pending: 0, receipts: 0, rejected: 0} = Store.stats(new)

    {:ok, _} = try_h(ctx, "fresh", new, p_new)
    assert [{:ingress, 0, :none, :none, :none}] = events!(new, "fresh")
  end

  test "binding reads only ETS: a suspended store delays neither the start nor ingress", ctx do
    store = start_store!(:suspended)
    p = Store.producer(store)
    true = :erlang.suspend_process(store)

    r =
      try do
        # start_h! asserts {:ok, _}: a call to the store would block init.
        # The handle was resolved before the suspension.
        r = start_h!(ctx, "suspended", store: store, producer: p)
        assert_receive {:tr, {:receipt, :ingress, :ok}}, @t
        assert %{receipts: :recording} = Handoff.status(r.h)
        # Published straight into the pending table; the store cannot sweep.
        assert :ets.info(p.table, :size) == 1
        r
      after
        true = :erlang.resume_process(store)
      end

    assert [{:ingress, 0, :none, :none, :none}] = events!(store, "suspended")
    done!(r)
    assert %{completeness: :complete, observed_outcome: :done} = lookup!(store, "suspended")
  end

  # The killed store is reported by the test supervisor; keep it out of the output.
  @tag :capture_log
  test "store killed after a valid binding: later records drop, the request completes, no rebinding",
       ctx do
    store = start_supervised!({Store, mode: :sandbox}, id: :lost, restart: :temporary)
    r = start_h!(ctx, "lost", store: store)
    assert_receive {:tr, {:receipt, :ingress, :ok}}, @t
    p = Store.producer(store)

    mon = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^mon, :process, ^store, :killed}, @t
    # A genuine same-mode store is available, but the coordinator never asks.
    replacement = start_store!(:replacement)

    done!(r)
    for k <- [:commit, :terminal], do: assert_receive({:tr, {:receipt, ^k, :dropped}}, @t)
    assert %{store_down: 3, loss?: true} = Producer.stats(p)
    assert %{receipts: :recording, outcome: {:done, @terminal}} = Handoff.status(r.h)
    assert %{pending: 0, retained: 0, receipts: 0, rejected: 0} = Store.stats(replacement)
    assert {:error, :not_found} = Store.lookup(replacement, @reader, "lost")
  end

  test "receipt_meta/1 projects only identifiers from a consistent Prepared" do
    rev =
      Revision.new!(
        route_id: "chat",
        rev: 3,
        deployments: ["a", "b"],
        preferred: "b",
        fallback_on_refusal: false,
        deadline_ms: 5_000,
        max_attempts: 1
      )

    plan = %Plan{
      rev: 3,
      digest: rev.digest,
      primary: "b",
      backup: :none,
      absolute_deadline_ms: 5_000,
      max_attempts: 1
    }

    prepared = %Prepared{
      gateway_id: "gw-1",
      tenant_id: @tenant,
      subject_id: "SUBJECT-41d0",
      mode: :sandbox,
      route_id: "chat",
      revision: rev,
      plan: plan,
      target: nil,
      max_attempts: 1,
      accepted_at_ms: 0,
      deadline_ms: 5_000,
      body: "BODY-a2c4"
    }

    assert {:ok, meta} = Handoff.receipt_meta(prepared)

    assert meta == %{
             tenant_id: @tenant,
             request_id: "gw-1",
             route_rev: 3,
             deployment_id: "b",
             mode: :sandbox
           }

    for bad <- [
          %{prepared | plan: %{plan | rev: 4}},
          %{prepared | plan: %{plan | primary: "c"}},
          %{prepared | plan: %{plan | digest: String.duplicate("0", 64)}},
          %{prepared | route_id: "other"},
          %{prepared | gateway_id: "no spaces"},
          Map.from_struct(prepared)
        ] do
      assert {:error, :invalid_prepared} = Handoff.receipt_meta(bad)
    end
  end

  # ── happy path, immediate lookups, privacy ──────────────────────────────

  test "done: ingress, dispatch, commit, terminal; each visible right after the step that recorded it; nothing private",
       ctx do
    r = start_h!(ctx, "happy")
    %{h: h, fake: fake, store: store} = r
    statuses = [Handoff.status(h)]

    permit!(r)
    assert [:ingress, :dispatch] = kinds!(store, "happy")

    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:transfer, @chunk})
    assert [:ingress, :dispatch, :commit] = kinds!(store, "happy")
    assert_receive {:req, {:telelem_handoff, ^h, {:data, @chunk}}}, @t

    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t

    # Upstream complete, not yet written: no terminal, nothing observed.
    assert %{observed_outcome: :not_observed, gaps: [:no_terminal]} = lookup!(store, "happy")
    statuses = [Handoff.status(h) | statuses]

    ack!(r, :ok)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t

    # Looked up as soon as the requester learned the outcome.
    assert %Receipt{
             completeness: :complete,
             gaps: [],
             observed_outcome: :done,
             exposure: :possible,
             usage: :unknown
           } = receipt = lookup!(store, "happy")

    assert [
             {:ingress, 0, :none, :none, :none},
             {:dispatch, 1, @dep, :none, :possible},
             {:commit, 1, @dep, :none, :possible},
             {:terminal, 0, :none, :done, :possible}
           ] = tuples(receipt)

    assert Enum.all?(receipt.events, &(&1.route_rev == 1 and &1.tenant_id == @tenant))
    at = Enum.map(receipt.events, & &1.at_ms)
    assert at == Enum.sort(at)

    hmon = monitor!(h)
    statuses = [Handoff.status(h) | statuses]
    assert :ok = step!(fake, {:cleanup, :possible})
    assert_receive {:req, {:telelem_handoff, ^h, {:closed, report}}}, @t
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert %{receipts: :recording, outcome: {:done, @terminal}} = report

    # Nothing after the terminal; a foreign tenant cannot see it.
    assert receipt == lookup!(store, "happy")
    assert {:error, :not_found} = Store.lookup(store, %{@reader | tenant_id: "other"}, "happy")

    assert for({:receipt, k, res} <- collected(r), do: {k, res}) ==
             [ingress: :ok, dispatch: :ok, commit: :ok, terminal: :ok]

    {gid, _} = only_grant!(ctx.auth)
    private = [gid, ctx.inst.instance_id]

    # The receipt holds no payload, terminal term, grant or process.
    for needle <- private ++ [fake, r.req, h | @sentinels] do
      refute contains?(receipt, needle), inspect(needle)
    end

    # Outward views gain only fixed atoms. (The upstream terminal term is in
    # status, report and trace by the existing, default design.)
    for term <- [report | statuses] ++ collected(r),
        needle <- private ++ [@marker, @chunk, "happy", Store.producer(store).table] do
      refute contains?(term, needle), inspect({needle, term}, limit: 8)
    end

    assert Enum.all?(statuses, &(&1.receipts == :recording))
  end

  # ── release order (from the coordinator's own trace) ────────────────────

  test "dispatch is published right after the permission reply, which follows the deadline check directly",
       ctx do
    r = start_h!(ctx, "order-dispatch")
    %{h: h, fake: fake} = r
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)

    session = trace_h!(h)
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_http, ^fake, @http_marker}, @t
    seq = trace_stop!(session, h)

    reply = index!(seq, &match?({:send, {_, :ok}, _}, &1))
    insert = index!(seq, &match?({:insert, :dispatch}, &1))
    # Nothing (no clock read, send or publication) between the last deadline
    # check and the permission; the dispatch record follows it, before any
    # other send (trace, observer).
    assert Enum.at(seq, reply - 1) == :clock
    assert reply < insert
    refute Enum.any?(Enum.slice(seq, (reply + 1)..(insert - 1)//1), &match?({:send, _, _}, &1))

    assert [{:send, {:telelem_handoff_trace, ^h, {:receipt, :dispatch, :ok}}, _} | _] =
             for({:send, _, _} = e <- Enum.drop(seq, insert + 1), do: e)
  end

  test "finish-first: marker and winner decision are sent before commit evidence; terminal before :done",
       ctx do
    r = start_h!(ctx, "order-finish")
    %{h: h, fake: fake, req: req} = r
    permit!(r)
    assert :release = step!(fake, :ready)

    session = trace_h!(h)
    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t
    ack!(r, :ok)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t
    seq = trace_stop!(session, h)

    marker = index!(seq, &match?({:send, {_, _, {:terminal, @marker, _}}, ^req}, &1))
    decision = index!(seq, &match?({:send, {_, _, {:terminal_decision, _, :ok}}, ^fake}, &1))
    commit = index!(seq, &match?({:insert, :commit}, &1))
    terminal = index!(seq, &match?({:insert, :terminal}, &1))
    done = index!(seq, &match?({:send, {_, _, {:done, @terminal}}, ^req}, &1))

    assert marker < decision and decision < commit
    assert commit < terminal and terminal < done
    assert [:ingress, :dispatch, :commit, :terminal] = kinds!(r.store, "order-finish")
  end

  test "transfer-first: commit evidence precedes the first forwarded chunk", ctx do
    r = start_h!(ctx, "order-transfer")
    %{h: h, fake: fake, req: req} = r
    permit!(r)
    assert :release = step!(fake, :ready)

    session = trace_h!(h)
    assert :ok = step!(fake, {:transfer, @chunk})
    assert :ok = step!(fake, {:transfer, @chunk})
    seq = trace_stop!(session, h)

    commit = index!(seq, &match?({:insert, :commit}, &1))
    data = index!(seq, &match?({:send, {_, _, {:data, @chunk}}, ^req}, &1))
    assert commit < data
    # One commit only.
    assert 1 == Enum.count(seq, &match?({:insert, :commit}, &1))
    assert [:ingress, :dispatch, :commit] = kinds!(r.store, "order-transfer")
  end

  # ── authority loss ──────────────────────────────────────────────────────

  test "authority loss before the permission: authority_down then failed terminal, no exposure",
       ctx do
    r = start_h!(ctx, "auth-reserved")
    %{h: h, fake: fake} = r
    assert {:ok, _} = step!(fake, :attach)

    Process.exit(ctx.auth, :kill)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, {:authority_down, :killed}}}}, @t

    assert %{completeness: :complete, observed_outcome: :failed, exposure: :none} =
             receipt = lookup!(r.store, "auth-reserved")

    assert [
             {:ingress, 0, :none, :none, :none},
             {:authority_down, 0, :none, :none, :none},
             {:terminal, 0, :none, :failed, :none}
           ] = tuples(receipt)
  end

  test "authority loss while committed and streaming: authority_down then incomplete terminal",
       ctx do
    r = start_h!(ctx, "auth-streaming")
    %{h: h, fake: fake} = r
    permit!(r)
    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:transfer, @chunk})

    Process.exit(ctx.auth, :kill)

    assert_receive {:req, {:telelem_handoff, ^h, {:incomplete, {:authority_down, :killed}}}},
                   @t

    assert %{observed_outcome: :incomplete, exposure: :possible, completeness: :complete} =
             receipt = lookup!(r.store, "auth-streaming")

    assert [:ingress, :dispatch, :commit, :authority_down, :terminal] = kinds(receipt)

    assert {:authority_down, 0, :none, :none, :possible} =
             receipt |> tuples() |> Enum.at(3)
  end

  test "authority loss after the terminal grant, before the write ack: recorded, then done",
       ctx do
    r = start_h!(ctx, "auth-granted")
    %{h: h, fake: fake} = r
    permit!(r)
    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t

    Process.exit(ctx.auth, :kill)
    assert_receive {:tr, {:authority_down, _, :killed}}, @t
    assert %{outcome: nil, authority: :lost} = Handoff.status(h)
    assert [:ingress, :dispatch, :commit, :authority_down] = kinds!(r.store, "auth-granted")

    ack!(r, :ok)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t

    assert %{observed_outcome: :done, completeness: :complete, exposure: :possible} =
             receipt = lookup!(r.store, "auth-granted")

    assert [:ingress, :dispatch, :commit, :authority_down, :terminal] = kinds(receipt)
  end

  test "authority loss after the outcome records nothing", ctx do
    r = start_h!(ctx, "auth-after")
    %{h: h} = r
    done!(r)
    before = lookup!(r.store, "auth-after")

    Process.exit(ctx.auth, :kill)
    assert_receive {:tr, {:authority_down, _, :killed}}, @t
    assert %{outcome: {:done, _}} = Handoff.status(h)

    assert before == lookup!(r.store, "auth-after")
    assert %{completeness: :complete, observed_outcome: :done} = before
    refute_received {:tr, {:receipt, :authority_down, _}}
  end

  # ── other outcomes: exposure follows the permission bit ─────────────────

  test "requester cancel before the permission: failed, no exposure; after it: failed, possible",
       ctx do
    a = start_h!(ctx, "cancel-reserved")
    assert {:ok, _} = step!(a.fake, :attach)
    assert {:ok, :simulated} = step!(a.fake, :connect)
    cancel!(a, :client_gone)

    assert [{:ingress, _, _, _, :none}, {:terminal, 0, :none, :failed, :none}] =
             events!(a.store, "cancel-reserved")

    b = start_h!(ctx, "cancel-permitted")
    permit!(b)
    cancel!(b, :client_gone)

    assert [_, {:dispatch, 1, @dep, :none, :possible}, {:terminal, 0, :none, :failed, :possible}] =
             events!(b.store, "cancel-permitted")

    # The cleanup claim (:none) does not lower the evidence: only the bit.
    assert :ok = step!(b.fake, {:cleanup, :none})
    assert %{exposure: :possible} = lookup!(b.store, "cancel-permitted")
    refute Enum.any?(lookup!(b.store, "cancel-permitted").events, &(&1.kind == :connect_refused))
  end

  test "done only on a timely write ack: write error and missing ack give incomplete", ctx do
    a = start_h!(ctx, "write-error")
    to_terminal!(a)
    ack!(a, {:error, :closed})
    assert_receive {:req, {:telelem_handoff, h, {:incomplete, {:downstream_write, :closed}}}}, @t
    assert h == a.h

    assert %{observed_outcome: :incomplete, completeness: :complete} =
             lookup!(a.store, "write-error")

    b = start_h!(ctx, "write-late", deadline: now() + @window)
    to_terminal!(b)
    hb = b.h
    # The coordinator's own deadline decides; the requester never acks.
    assert_receive {:req,
                    {:telelem_handoff, ^hb, {:incomplete, {:terminal_unconfirmed, :deadline}}}},
                   @window + @t

    assert [:ingress, :dispatch, :commit, :terminal] = kinds!(b.store, "write-late")
    assert %{observed_outcome: :incomplete} = lookup!(b.store, "write-late")
  end

  test "the deadline before the permission: failed terminal, no exposure", ctx do
    r = start_h!(ctx, "deadline-reserved", deadline: now() + @window)
    h = r.h
    assert {:ok, _} = step!(r.fake, :attach)

    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :dispatch_deadline}}}, @window + @t

    assert [_, {:terminal, 0, :none, :failed, :none}] = events!(r.store, "deadline-reserved")
  end

  # ── publication loss never gates the request ────────────────────────────

  test "overflow: a dropped dispatch leaves a declared gap; the permission and the request are unaffected",
       ctx do
    store = start_store!(:tiny, max_pending: 1, sweep_ms: 3_600_000)
    r = start_h!(ctx, "overflow", store: store)
    %{h: h, fake: fake} = r

    # Ingress took the one pending slot; move it out, then fill the slot.
    _ = Store.stats(store)
    {:ok, filler} = Writer.open(Store.producer(store), @tenant, "filler")
    {:ok, _} = Writer.record(filler, event("filler", 0, :ingress))

    # Not gated: the permission was released and the fake sent.
    assert :dropped = permit!(r)
    _ = Store.stats(store)

    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:transfer, @chunk})
    _ = Store.stats(store)
    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t
    ack!(r, :ok)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t

    assert %{
             completeness: :incomplete,
             observed_outcome: :done,
             exposure: :unknown,
             usage: :unknown,
             gaps: gaps
           } = receipt = lookup!(store, "overflow")

    assert [:ingress, :commit, :terminal] = kinds(receipt)
    assert :missing_sequence in gaps and :reported_drop in gaps and :loss in gaps
    assert %{overflow: 1} = Store.stats(store)
    assert %{receipts: :recording} = Handoff.status(h)
  end

  test "store down mid-request: every later record drops; the request still completes", ctx do
    store = start_store!(:doomed)
    r = start_h!(ctx, "store-down", store: store)
    p = Store.producer(store)
    stop_supervised!(:doomed)

    done!(r)

    for k <- [:commit, :terminal], do: assert_receive({:tr, {:receipt, ^k, :dropped}}, @t)
    assert %{store_down: 3, loss?: true} = Producer.stats(p)
    assert %{receipts: :recording, outcome: {:done, @terminal}} = Handoff.status(r.h)
  end

  test "retention eviction: partly evicted reads incomplete, fully evicted is not found", ctx do
    store = start_store!(:small, max_retained: 2)
    r = start_h!(ctx, "evicted", store: store)
    done!(r)

    assert %{completeness: :incomplete, observed_outcome: :done, gaps: gaps} =
             receipt = lookup!(store, "evicted")

    assert [:commit, :terminal] = kinds(receipt)
    assert :evicted in gaps and :no_ingress in gaps

    {:ok, w} = Writer.open(Store.producer(store), @tenant, "later")
    {:ok, w} = Writer.record(w, event("later", 0, :ingress))
    {:ok, _} = Writer.record(w, event("later", 0, :authority_down))

    assert {:error, :not_found} = Store.lookup(store, @reader, "evicted")
    assert %{receipts: :recording} = Handoff.status(r.h)
  end

  test "request-id reuse by a second coordinator is a writer conflict, never complete", ctx do
    a = start_h!(ctx, "reused")
    b = start_h!(ctx, "reused")
    done!(a)

    assert %{completeness: :incomplete, gaps: gaps} = lookup!(a.store, "reused")
    assert :writer_conflict in gaps and :duplicate_ingress in gaps
    Process.exit(b.h, :kill)
  end

  # ── abnormal stop ───────────────────────────────────────────────────────

  # The abnormal stop is logged by gen_server; keep it out of the run output.
  @tag :capture_log
  test "terminate/2 publishes nothing: an abnormal stop leaves no terminal", ctx do
    test = self()
    {hopts, r} = h_opts(ctx, "terminated", [])

    # The parent (as a supervisor would be) is the only link.
    parent =
      spawn(fn ->
        send(test, {:started, Handoff.start_link(hopts)})
        receive do: (:never -> :ok)
      end)

    assert_receive {:started, {:ok, h}}, @t
    on_exit(fn -> Process.exit(h, :kill) end)
    r = Map.put(r, :h, h)
    r = Map.put(r, :fake, fake(r))
    assert :ok = permit!(r)
    hmon = monitor!(h)
    Process.exit(parent, :boom)

    assert_receive {:req, {:telelem_handoff, ^h, {:failed, {:handoff_down, :boom}}}}, @t
    assert_receive {:DOWN, ^hmon, :process, _, :boom}, @t

    assert %{completeness: :incomplete, gaps: [:no_terminal], observed_outcome: :not_observed} =
             receipt = lookup!(r.store, "terminated")

    assert [:ingress, :dispatch] = kinds(receipt)
  end

  # ── setup helpers ───────────────────────────────────────────────────────

  defp start_store!(id, opts \\ []) do
    start_supervised!({Store, Keyword.merge([mode: :sandbox], opts)}, id: id)
  end

  defp meta(request_id),
    do: %{
      tenant_id: @tenant,
      request_id: request_id,
      route_rev: 1,
      deployment_id: @dep,
      mode: :sandbox
    }

  defp start_h!(ctx, request_id, opts \\ []) do
    {hopts, r} = h_opts(ctx, request_id, opts)
    {:ok, h} = Handoff.start(hopts)
    on_exit(fn -> Process.exit(h, :kill) end)
    r = Map.put(r, :h, h)
    Map.put(r, :fake, fake(r))
  end

  defp h_opts(ctx, request_id, opts) do
    store = Keyword.get(opts, :store, ctx.store)
    req = requester()
    col = collector()
    d = Keyword.get(opts, :deadline, now() + 60_000)

    hopts = [
      requester: req,
      attempts: [:primary],
      admission_authority: ctx.auth,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, @units},
      terminal_ack: true,
      receipts: [
        store: store,
        producer: Keyword.get_lazy(opts, :producer, fn -> Store.producer(store) end),
        store_mode: :sandbox,
        meta: meta(request_id)
      ],
      trace: col,
      on_event: fn ev -> send(col, {:ev, ev}) end,
      cleanup_timeout: 60_000,
      register_timeout: 60_000
    ]

    {hopts, %{req: req, col: col, d: d, auth: ctx.auth, store: store}}
  end

  # A handoff with an explicit pin, handle and mode (trusted statement and
  # meta agree); returns Handoff.start/1's result.
  defp try_h(ctx, request_id, pin, producer, mode \\ :sandbox) do
    {hopts, _} = h_opts(ctx, request_id, [])

    receipts = [
      store: pin,
      producer: producer,
      store_mode: mode,
      meta: %{meta(request_id) | mode: mode}
    ]

    with {:ok, h} <- Handoff.start(Keyword.put(hopts, :receipts, receipts)) do
      on_exit(fn -> Process.exit(h, :kill) end)
      {:ok, h}
    end
  end

  defp refused!(ctx, request_id, pin, producer, mode \\ :sandbox) do
    assert {:error, {:bad_option, :receipts}} = try_h(ctx, request_id, pin, producer, mode)
  end

  # Attached, reserved, connected, permitted (one fake send). The dispatch
  # record follows the permission reply, so its trace entry is the barrier
  # before a lookup. Returns that publication result.
  defp permit!(%{fake: fake}) do
    assert {:ok, _} = step!(fake, :attach)
    assert {:ok, :simulated} = step!(fake, :connect)
    assert :ok = step!(fake, :begin_dispatch)
    assert_receive {:fake_http, ^fake, @http_marker}, @t
    assert_receive {:tr, {:receipt, :dispatch, result}}, @t
    result
  end

  defp to_terminal!(%{h: h, fake: fake} = r) do
    permit!(r)
    assert :release = step!(fake, :ready)
    assert :ok = step!(fake, {:transfer, @chunk})
    assert :ok = step!(fake, {:finish, @terminal, @marker})
    assert_receive {:req, {:telelem_handoff, ^h, {:terminal, @marker, _}}}, @t
  end

  defp done!(%{h: h} = r) do
    to_terminal!(r)
    ack!(r, :ok)
    assert_receive {:req, {:telelem_handoff, ^h, {:done, @terminal}}}, @t
  end

  defp ack!(%{h: h} = r, result) do
    ref = req_do(r, fn -> Handoff.terminal_written(h, result) end)
    assert_receive {:req_did, ^ref, :ok}, @t
  end

  defp cancel!(%{h: h} = r, reason) do
    ref = req_do(r, fn -> Handoff.cancel(h, reason) end)
    assert_receive {:req_did, ^ref, :ok}, @t
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, ^reason}}}, @t
  end

  defp event(request_id, attempt, kind) do
    %{
      tenant_id: @tenant,
      request_id: request_id,
      route_rev: 1,
      attempt: attempt,
      deployment_id: :none,
      at_ms: now(),
      kind: kind,
      outcome: :none,
      exposure: :none
    }
  end

  # ── evidence reads ──────────────────────────────────────────────────────

  defp lookup!(store, request_id) do
    assert {:ok, %Receipt{} = receipt} = Store.lookup(store, @reader, request_id)
    receipt
  end

  defp tuples(%Receipt{events: evs}),
    do: for(e <- evs, do: {e.kind, e.attempt, e.deployment_id, e.outcome, e.exposure})

  defp kinds(%Receipt{events: evs}), do: Enum.map(evs, & &1.kind)
  defp events!(store, id), do: tuples(lookup!(store, id))
  defp kinds!(store, id), do: kinds(lookup!(store, id))

  # ── coordinator trace session (sends, clock reads, publications) ────────

  defp trace_h!(h) do
    session = :trace.session_create(:handoff_receipts, self(), [])
    1 = :trace.process(session, h, true, [:send, :call])
    1 = :trace.function(session, {:ets, :insert_new, 2}, true, [])
    1 = :trace.function(session, {System, :monotonic_time, 1}, true, [])
    session
  end

  # Everything the coordinator did while traced, in its order, reduced to
  # {:send, msg, to}, :clock and {:insert, event_kind}.
  defp trace_stop!(session, h) do
    ref = :trace.delivered(session, h)
    assert_receive {:trace_delivered, ^h, ^ref}, @t
    :trace.session_destroy(session)
    drain_trace(h, [])
  end

  defp drain_trace(h, acc) do
    receive do
      {:trace, ^h, :send, msg, to} ->
        drain_trace(h, [{:send, msg, to} | acc])

      {:trace, ^h, :call, {System, :monotonic_time, _}} ->
        drain_trace(h, [:clock | acc])

      {:trace, ^h, :call, {:ets, :insert_new, [_, row]}} ->
        drain_trace(h, [{:insert, elem(row, tuple_size(row) - 1).kind} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp index!(list, pred) do
    i = Enum.find_index(list, pred)
    assert i != nil, "not found in #{inspect(list, limit: :infinity)}"
    i
  end

  # ── fake attempt (no socket) ────────────────────────────────────────────

  defp fake(r) do
    test = self()

    pid =
      spawn(fn ->
        fake_loop(%{
          test: test,
          h: r.h,
          d: r.d,
          amon: Process.monitor(r.auth),
          connected: false,
          cancelled: nil,
          auth_down: nil
        })
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp fake_loop(s) do
    receive do
      {:step, ref, step} ->
        {reply, s} = fake_step(step, s)
        send(s.test, {:fake, self(), ref, reply})
        fake_loop(s)

      {:telelem_handoff, h, {:cancel, reason}} when h == s.h ->
        fake_loop(%{s | cancelled: reason})

      {:DOWN, ref, :process, _, reason} when ref == s.amon ->
        fake_loop(%{s | auth_down: reason})
    end
  end

  defp fake_step(:attach, s), do: {Handoff.attach(s.h, :primary, deadline: s.d), s}
  defp fake_step(:connect, s), do: {{:ok, :simulated}, %{s | connected: true}}

  defp fake_step(:begin_dispatch, s) do
    result = Handoff.begin_dispatch(s.h, :primary, deadline: s.d)
    s = drain(s)

    # Sends only on the permission, if nothing invalidated it; its own clock
    # check is the last thing before the send.
    if result == :ok and s.connected and s.auth_down == nil and s.cancelled == nil and
         now() < s.d,
       do: send(s.test, {:fake_http, self(), @http_marker})

    {result, s}
  end

  defp fake_step(:ready, s), do: {Handoff.ready(s.h, :primary), s}
  defp fake_step({:transfer, chunk}, s), do: {Handoff.transfer(s.h, chunk), s}
  defp fake_step({:finish, t, m}, s), do: {Handoff.finish(s.h, t, m, deadline: s.d), s}
  defp fake_step({:cleanup, charge}, s), do: {Handoff.cleaned(s.h, :primary, charge), s}

  defp drain(s) do
    receive do
      {:telelem_handoff, h, {:cancel, reason}} when h == s.h -> drain(%{s | cancelled: reason})
      {:DOWN, ref, :process, _, reason} when ref == s.amon -> drain(%{s | auth_down: reason})
    after
      0 -> s
    end
  end

  defp step!(fake, step) do
    ref = make_ref()
    send(fake, {:step, ref, step})
    assert_receive {:fake, ^fake, ^ref, reply}, @t
    reply
  end

  # ── requester and trace collector ───────────────────────────────────────

  defp requester do
    test = self()
    pid = spawn(fn -> req_loop(test) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp req_loop(test) do
    receive do
      {:do, ref, fun} -> send(test, {:req_did, ref, fun.()})
      msg -> send(test, {:req, msg})
    end

    req_loop(test)
  end

  defp req_do(%{req: req}, fun) do
    ref = make_ref()
    send(req, {:do, ref, fun})
    ref
  end

  defp collector do
    test = self()
    pid = spawn(fn -> col_loop(test, []) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp col_loop(test, acc) do
    receive do
      {:get, from, ref} ->
        send(from, {:collected, ref, Enum.reverse(acc)})
        col_loop(test, acc)

      {:telelem_handoff_trace, _h, ev} ->
        send(test, {:tr, ev})
        col_loop(test, [ev | acc])

      {:ev, ev} ->
        col_loop(test, [{:event, ev} | acc])
    end
  end

  defp collected(%{col: col}) do
    ref = make_ref()
    send(col, {:get, self(), ref})
    assert_receive {:collected, ^ref, evs}, @t
    evs
  end

  # ── misc ────────────────────────────────────────────────────────────────

  defp only_grant!(auth) do
    assert [{gid, g}] = Map.to_list(Authority.snapshot(auth).grants)
    {gid, g}
  end

  defp monitor!(pid) do
    mon = Process.monitor(pid)
    assert {:monitored_by, by} = Process.info(pid, :monitored_by)
    assert self() in by
    mon
  end

  defp contains?(term, needle) when term === needle, do: true
  defp contains?(t, n) when is_binary(t) and is_binary(n), do: :binary.match(t, n) != :nomatch
  defp contains?(t, n) when is_tuple(t), do: t |> Tuple.to_list() |> contains?(n)
  defp contains?([h | t], n), do: contains?(h, n) or contains?(t, n)
  defp contains?(%{} = m, n), do: m |> Map.to_list() |> contains?(n)
  defp contains?(_, _), do: false

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp now, do: System.monotonic_time(:millisecond)
end
