defmodule Telelem.Serve.RequesterTest.Writer do
  @moduledoc false
  # A checked-in barrier writer. Every call is announced to the test as
  # `{:writer, pid, call, ref}`. With `gate: true` it then blocks until the
  # test answers `{:writer_reply, ref, reply}`; `reply` picks the result:
  # `:ok`, `{:error, r}`, `:raise`, `:exit`, `:throw` or `:bad_shape`.
  @behaviour Telelem.Serve.Requester.Writer

  @impl true
  def header(st), do: call(st, :header)

  @impl true
  def chunk(st, bytes), do: call(st, {:chunk, bytes})

  defp call(%{test: test, gate: gate} = st, call) do
    ref = make_ref()
    send(test, {:writer, self(), call, ref})

    reply =
      if gate do
        receive do: ({:writer_reply, ^ref, r} -> r)
      else
        :ok
      end

    case reply do
      :ok -> {:ok, %{st | n: st.n + 1}}
      {:error, _} = e -> e
      :raise -> raise "writer boom"
      :exit -> exit(:writer_exit)
      :throw -> throw(:writer_throw)
      :bad_shape -> :ok
    end
  end
end

defmodule Telelem.Serve.RequesterTest do
  @moduledoc """
  W2d `Telelem.Serve.Requester`, listener-free and offline.

    * End to end through the real `Telelem.Serve.Primary` on a real
      `Telelem.Serve.Tree` (as in `PrimaryTest`): the real pinned authority
      denies the reservation, so the GO'd owner ends before any dial;
      startup failures (ingress, launch expiry) return Primary's typed error
      verbatim with no write and no second cancel.
    * The protocol (`serve/3`) against a real `Telelem.Handoff`
      (`admission_authority` = the real `Telelem.Admission.Authority`,
      `terminal_ack: true`, `attempts: [:primary]`) with a cooperative
      no-socket attempt run by a test actor, which is also the credit owner.

  Barriers only: the checked-in gate writer blocks in a writer call until
  the test answers; `:sys.suspend/1` on the coordinator; and
  `:erlang.suspend_process/1` on the requester. Calls to
  `Handoff.submit_cancel/2` and `Handoff.terminal_written/2` from the
  requester are counted with an isolated `:trace` session. Clock waits only
  for an absolute `D` to pass. One case takes Handoff's own 5 s
  `terminal_written/2` call timeout.

  Not covered (no hook without changing Handoff): a `terminal_written/2`
  reply lost *after* the coordinator sent `{:done, _}` (it replies in the
  same step it sends the outcome, so a reply is never lost without killing
  the requester); `terminal_written/2` processed by Handoff before its `D`
  timer but after `D` (Handoff's own `{:error, :deadline}` branch); a
  writer call already started when `D` passes cannot be interrupted, so the
  bounds here are clock checks, not wall-clock limits. No real socket,
  Plug, framing or peer receipt is exercised.

      scripts/mix test test/telelem/serve/requester_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.{Handoff, Instance}
  alias Telelem.Ops.{Context, MemoryStore}
  alias Telelem.Receipts.Store
  alias Telelem.Route.Revision
  alias Telelem.Sandbox.{Bootstrap, Principal, VerifiedTarget}
  alias Telelem.Serve.{Ingress, Primary, Requester, Tree}
  alias Telelem.Serve.Primary.Handle
  alias Telelem.Serve.RequesterTest.Writer
  alias Telelem.Wire.Limits

  @moduletag :tmp_dir
  @t 2_000
  @units 4
  @instance_units 2
  @auth "AUTH-MARKER-5e21"
  @body ~s({"marker":"BODY-MARKER-a07d"})
  @tenant "local"
  @marker "data: [DONE]\n\n"
  @empty %{active: 0, specs: 0, supervisors: 0, workers: 0}

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :serve_requester_gw,
        limits: %{slots: 2, units: @instance_units},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)
    auth = Instance.authority(inst)
    root = start_supervised!({Tree, []}, id: :tree, restart: :temporary)
    {:ok, pins} = Tree.resolve(root)
    store = start_supervised!({Store, mode: :sandbox}, id: :receipts)
    routes = start_supervised!(MemoryStore, id: :routes)
    :ok = MemoryStore.seed(routes, @tenant, rev1())
    on_exit(fn -> for p <- [auth, pins.coordinators, pins.guards], do: resume(p) end)
    {bindings, target} = bindings!(dir)

    %{
      auth: auth,
      root: root,
      pins: pins,
      store: store,
      routes: routes,
      bindings: bindings,
      target: target
    }
  end

  # ── through the real Primary ────────────────────────────────────────────

  test "real Primary, pre-dial denial: one start, no write, no cancel; unrelated mail (Plug sentinel, witness, foreign handoff) and the closed report stay queued",
       ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    test = self()
    rd = reader(@body)
    r = owner!()
    seed!(r)
    tr = trace!(r, [{Primary, :start, 4}])

    go(r, fn -> Requester.run(cfg, @auth, rd, writer(test), controller: r, observer: r) end)
    assert_receive {:result, ^r, res}, @t

    assert {:no_write, {:outcome, {:failed, :admission_refused}}, ev} = res
    h = ev.handoff
    assert %{header: :not_called, chunks: 0, credit: 0, terminal: :none} = ev
    assert %{report: :not_sent, cancel: :not_submitted, elected: nil} = ev
    assert ev.outcome == {:failed, :admission_refused}
    assert is_binary(ev.request_id) and is_integer(ev.deadline_ms)
    for x <- [@auth, @body, ctx.target.host], do: refute(contains?(res, x))

    assert_received {:authenticate, @auth}
    refute_received {:authenticate, _}
    refute_received {:writer, _, _, _}
    assert calls(tr, r) == [{Primary, :start, 4}]

    # The owner ends before any dial; its controller and observer reports
    # went to the explicitly routed owner process and are left there.
    hm = Process.monitor(h)
    assert_receive {:DOWN, ^hm, :process, ^h, _}, @t
    mb = await_mailbox!(r, &match?({:telelem_owner_witness, _, :owner_down, _}, &1))
    assert Enum.take(mb, 3) == seeded()
    assert Enum.any?(mb, &match?({:telelem_handoff, ^h, {:closed, _}}, &1))
    assert Enum.any?(mb, &match?({:telelem_wire, _, :primary, {:ended, _, _}}, &1))
    refute Enum.any?(mb, &match?({:telelem_wire, _, _, {:connect_failed, _, _}}, &1))
    refute Enum.any?(mb, &match?({:telelem_handoff, ^h, {:failed, _}}, &1))
  end

  test "Primary startup failure is returned verbatim with no write: ingress error starts nothing; launch expiry keeps Primary's one cancel and adds none",
       ctx do
    test = self()

    {:ok, bad_ingress} =
      Ingress.config(
        ingress_opts(ctx,
          authenticate: fn input ->
            send(test, {:authenticate, input})
            :error
          end
        )
      )

    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :ingress, bad_ingress))
    rd = reader(@body)
    r = owner!()
    go(r, fn -> Requester.run(cfg, @auth, rd, writer(test)) end)
    assert_receive {:result, ^r, {:no_write, {:start, {:ingress, :unauthenticated}}}}, @t
    refute_received {:read_body, _, _}
    assert DynamicSupervisor.count_children(ctx.pins.coordinators) == @empty

    # Launch expiry: the guards branch is held, OwnerGuard's activation limit passes.
    {:ok, cfg} = Primary.config(Keyword.put(cfg_opts(ctx), :activation_ms, 50))
    :ok = :sys.suspend(ctx.pins.guards)
    r = owner!()
    tr = trace!(r, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    go(r, fn -> Requester.run(cfg, @auth, rd, writer(test)) end)
    assert_receive {:result, ^r, res}, @t

    assert {:no_write, {:start, {:launch, :expired, failure}}} = res
    assert %{activation: :confirmed, go: :not_sent, cancel: :submitted, handoff: h} = failure
    assert map_size(failure) == 5
    reason = Primary.cancel_reason()
    assert calls(tr, r, :args) == [{Handoff, :submit_cancel, [h, reason]}]
    refute_received {:writer, _, _, _}

    # Primary's cancel ends the request; its messages stay in the owner's mailbox.
    hm = Process.monitor(h)
    assert_receive {:DOWN, ^hm, :process, ^h, _}, @t
    mb = mailbox(r)
    assert Enum.any?(mb, &match?({:telelem_handoff, ^h, {:failed, ^reason}}, &1))
    assert Enum.any?(mb, &match?({:telelem_handoff, ^h, {:closed, _}}, &1))
    :ok = :sys.resume(ctx.pins.guards)
  end

  test "invalid writer or options: nothing prepared, started or sent", ctx do
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    rd = reader(@body)
    me = self()

    for {w, o, why} <- [
          {{NoSuchWriter, %{}}, [], :writer},
          {:nope, [], :writer},
          {{Enum, %{}}, [], :writer},
          {writer(me), [observe_ms: -1], :opts},
          {writer(me), [stream_grace_ms: 60_001], :opts},
          {writer(me), [controller: :name], :opts},
          {writer(me), [x: 1], :opts},
          {writer(me), :nope, :opts}
        ] do
      assert Requester.run(cfg, @auth, rd, w, o) == {:no_write, {:invalid, why}}
    end

    assert Requester.serve(:nope, writer(me)) == {:no_write, {:invalid, :handle}}
    refute_received {:authenticate, _}
    assert DynamicSupervisor.count_children(ctx.pins.coordinators) == @empty
  end

  # ── against a real Handoff with a cooperative attempt ───────────────────

  test "full local success: election writes nothing; header once; exact per-chunk credit to the owner only after each write; marker once with no credit; one :ok report; done; closed left queued",
       ctx do
    c = start!(ctx, 5_000)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    seed!(c.req)
    serve!(c, writer(self(), true))

    # Election is informational: consumed, nothing written.
    elect!(c)
    await_consumed!(c.req, &match?({:telelem_handoff, _, {:elected, _}}, &1))
    refute_received {:writer, _, _, _}

    h = c.h
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    ref = assert_writer!(:header)
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
    reply(ref, :ok)
    ref = assert_writer!({:chunk, "c1"})
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
    reply(ref, :ok)
    assert_receive {:att, {:telelem_wire_credit, ^h, 2}}, @t

    assert att!(c, fn -> Handoff.transfer(h, "chunk-2") end) == :ok
    reply(assert_writer!({:chunk, "chunk-2"}), :ok)
    assert_receive {:att, {:telelem_wire_credit, ^h, 7}}, @t

    finish(c, :term)
    reply(assert_writer!({:chunk, @marker}), :ok)
    assert_receive {:result, _, res}, @t
    assert_receive {:finish, :ok}, @t

    assert {:complete, ev} = res
    assert %{header: :ok, chunks: 2, credit: 9, terminal: :written, written_late: false} = ev
    assert %{report: {:sent, :ok}, outcome: {:done, :term}, cancel: :not_submitted} = ev
    assert ev.elected == :primary and ev.writer.n == 4
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
    refute_received {:writer, _, _, _}
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, :ok]}]

    # Closed is never awaited: it (and the seeded mail) is left queued.
    clean!(c)
    mb = await_mailbox!(c.req, &match?({:telelem_handoff, ^h, {:closed, _}}, &1))
    assert Enum.take(mb, 3) == seeded()
  end

  test "data write failure: no credit for it, exactly one cancel and no terminal report; earlier credit kept",
       ctx do
    c = start!(ctx, 5_000)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h

    assert att!(c, fn -> Handoff.transfer(h, "abc") end) == :ok
    reply(assert_writer!(:header), :ok)
    reply(assert_writer!({:chunk, "abc"}), :ok)
    assert_receive {:att, {:telelem_wire_credit, ^h, 3}}, @t
    assert att!(c, fn -> Handoff.transfer(h, "de") end) == :ok
    reply(assert_writer!({:chunk, "de"}), {:error, :closed})

    assert_receive {:result, _, {:aborted, {:data, :failed}, ev}}, @t
    assert %{header: :ok, chunks: 1, credit: 3, cancel: :submitted, report: :not_sent} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :submit_cancel, [h, :write_failed]}]
    assert_receive {:att, {:telelem_handoff, ^h, {:cancel, :write_failed}}}, @t
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
    refute_received {:writer, _, _, _}
  end

  test "ambiguous header (raise, exit, throw, unknown shape, error): abort, no data write, no credit, one cancel",
       ctx do
    for {how, why, reason} <- [
          {:raise, :unconfirmed, :header_unconfirmed},
          {:exit, :unconfirmed, :header_unconfirmed},
          {:throw, :unconfirmed, :header_unconfirmed},
          {:bad_shape, :unconfirmed, :header_unconfirmed},
          {{:error, :closed}, :failed, :header_failed}
        ] do
      c = start!(ctx, 5_000)
      tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
      serve!(c, writer(self(), true))
      elect!(c)
      h = c.h
      assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
      reply(assert_writer!(:header), how)

      assert_receive {:result, _, {:aborted, {:header, ^why}, ev}}, @t
      assert %{header: ^why, chunks: 0, credit: 0, cancel: :submitted} = ev
      assert calls(tr, c.req, :args) == [{Handoff, :submit_cancel, [h, reason]}]
      refute_received {:writer, _, _, _}
      sync!(c)
      refute_received {:att, {:telelem_wire_credit, _, _}}
      :trace.session_destroy(tr)
    end
  end

  test "D crossed while the header write blocks: the separate check before the data write refuses it; one cancel, no credit",
       ctx do
    c = start!(ctx, 400)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    ref = assert_writer!(:header)
    wait_until!(c.d)
    reply(ref, :ok)

    assert_receive {:result, _, {:aborted, {:data, :deadline}, ev}}, @t
    assert %{header: :ok, chunks: 0, credit: 0, cancel: :submitted} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :submit_cancel, [h, :deadline]}]
    refute_received {:writer, _, _, _}
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
  end

  test "data observed at or after D: the check before the header refuses it; nothing written, one cancel",
       ctx do
    c = start!(ctx, 400)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    :erlang.suspend_process(c.req)
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    wait_until!(c.d)
    true = :erlang.resume_process(c.req)

    assert_receive {:result, _, {:no_write, {:data, :deadline}, ev}}, @t
    assert %{header: :not_called, cancel: :submitted, credit: 0} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :submit_cancel, [h, :deadline]}]
    refute_received {:writer, _, _, _}
  end

  test "late terminal write: the marker write returns after D; the actual :ok is reported once anyway; Handoff's deadline outcome stands; no cancel",
       ctx do
    c = start!(ctx, 600)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    reply(assert_writer!(:header), :ok)
    reply(assert_writer!({:chunk, "c1"}), :ok)
    assert_receive {:att, {:telelem_wire_credit, ^h, 2}}, @t
    finish(c, :term)
    ref = assert_writer!({:chunk, @marker})
    assert_receive {:finish, :ok}, @t
    wait_until!(c.d)
    reply(ref, :ok)

    assert_receive {:result, _, {:aborted, why, ev}}, @t
    assert why == {:outcome, {:incomplete, {:terminal_unconfirmed, :deadline}}}
    assert %{terminal: :written, written_late: true, cancel: :not_submitted, credit: 2} = ev
    assert ev.report == {:sent, {:error, :ended}}
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, :ok]}]
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
  end

  test "timely accepted done observed after D (reply and done read late): complete, no cancel",
       ctx do
    c = start!(ctx, 800)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    finish(c, :term)
    reply(assert_writer!(:header), :ok)
    ref = assert_writer!({:chunk, @marker})
    assert_receive {:finish, :ok}, @t

    # The coordinator is held so the ack call queues; then the requester is
    # held while the coordinator decides (before D).
    :ok = :sys.suspend(h)
    reply(ref, :ok)
    await_mailbox!(h, &match?({:"$gen_call", _, {:terminal_written, :ok}}, &1))
    :erlang.suspend_process(c.req)
    :ok = :sys.resume(h)
    assert %{outcome: {:done, :term}} = Handoff.status(h)
    assert now() < c.d
    wait_until!(c.d)
    true = :erlang.resume_process(c.req)

    assert_receive {:result, _, {:complete, ev}}, @t
    assert %{terminal: :written, written_late: false, outcome: {:done, :term}} = ev
    assert %{report: {:sent, :ok}, cancel: :not_submitted, credit: 0} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, :ok]}]
    sync!(c)
    refute_received {:att, {:telelem_wire_credit, _, _}}
  end

  test "ack lost before any outcome: Handoff's 5 s call fences the held coordinator; unconfirmed, not complete; no cancel or second report",
       ctx do
    c = start!(ctx, 20_000)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true), observe_ms: 500)
    elect!(c)
    h = c.h
    hm = Process.monitor(h)
    finish(c, :term)
    reply(assert_writer!(:header), :ok)
    ref = assert_writer!({:chunk, @marker})
    assert_receive {:finish, :ok}, @t
    :ok = :sys.suspend(h)
    reply(ref, :ok)

    assert_receive {:DOWN, ^hm, :process, ^h, :killed}, 7_000
    assert_receive {:result, _, {:aborted, :handoff_down, ev}}, @t
    assert %{terminal: :written, report: {:sent, {:error, :unconfirmed}}} = ev
    assert %{outcome: :not_observed, cancel: :not_submitted} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, :ok]}]
  end

  test "terminal write failure and finish-first terminal observed after D: one error report each, no cancel",
       ctx do
    # Write failure.
    c = start!(ctx, 5_000)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    finish(c, :term)
    reply(assert_writer!(:header), :ok)
    reply(assert_writer!({:chunk, @marker}), {:error, :closed})
    assert_receive {:result, _, {:aborted, {:terminal, :failed}, ev}}, @t
    assert ev.outcome == {:incomplete, {:downstream_write, :write_failed}}
    assert %{terminal: :failed, report: {:sent, :ok}, cancel: :not_submitted} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, {:error, :write_failed}]}]
    :trace.session_destroy(tr)

    # Finish first, observed at or after D: nothing written.
    c = start!(ctx, 500)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    h = c.h
    :erlang.suspend_process(c.req)
    finish(c, :term)
    assert_receive {:finish, :ok}, @t
    wait_until!(c.d)
    true = :erlang.resume_process(c.req)

    assert_receive {:result, _, {:no_write, {:terminal, :deadline}, ev}}, @t
    assert ev.outcome == {:incomplete, {:terminal_unconfirmed, :deadline}}
    assert %{header: :not_called, terminal: :deadline, cancel: :not_submitted} = ev
    assert elem(ev.report, 0) == :sent
    assert calls(tr, c.req, :args) == [{Handoff, :terminal_written, [h, {:error, :deadline}]}]
    refute_received {:writer, _, _, _}
  end

  test "stream wait expires at D + grace: one cancel, no write; coordinator death before an outcome: no cancel",
       ctx do
    c = start!(ctx, 200)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}])
    serve!(c, writer(self(), true), stream_grace_ms: 100)
    elect!(c)
    h = c.h
    assert_receive {:result, _, {:no_write, :receive_timeout, ev}}, @t
    assert now() >= c.d + 100
    assert %{cancel: :submitted, elected: :primary} = ev
    assert calls(tr, c.req, :args) == [{Handoff, :submit_cancel, [h, :receive_timeout]}]
    assert_receive {:att, {:telelem_handoff, ^h, {:cancel, :receive_timeout}}}, @t
    :trace.session_destroy(tr)

    c = start!(ctx, 5_000)
    tr = trace!(c.req, [{Handoff, :submit_cancel, 2}, {Handoff, :terminal_written, 2}])
    serve!(c, writer(self(), true))
    elect!(c)
    Process.exit(c.h, :kill)
    assert_receive {:result, _, {:no_write, :handoff_down, ev}}, @t
    assert %{cancel: :not_submitted, outcome: :not_observed} = ev
    assert calls(tr, c.req, :args) == []
  end

  # ── helpers: real Handoff + cooperative attempt ─────────────────────────

  # A real coordinator whose requester is a fresh owner process and whose
  # only attempt (:primary) is a test actor, also the credit owner.
  defp start!(ctx, d_ms) do
    req = owner!()
    att = actor()
    d = now() + d_ms

    {:ok, h} =
      Handoff.start(
        requester: req,
        attempts: [:primary],
        admission_authority: ctx.auth,
        terminal_deadline: d,
        terminal_ack: true
      )

    on_exit(fn -> Process.exit(h, :kill) end)

    handle = %Handle{
      request_id: "rq-test",
      handoff: h,
      guard: idle(),
      owner: att,
      witness: idle(),
      ref: make_ref(),
      deadline_ms: d
    }

    %{h: h, req: req, att: att, d: d, handle: handle}
  end

  defp serve!(c, w, opts \\ []), do: go(c.req, fn -> Requester.serve(c.handle, w, opts) end)

  defp elect!(c) do
    h = c.h
    assert {:ok, _} = att!(c, fn -> Handoff.attach(h, :primary) end)
    assert att!(c, fn -> Handoff.begin_dispatch(h, :primary) end) == :ok
    assert att!(c, fn -> Handoff.ready(h, :primary) end) == :release
  end

  defp finish(c, term) do
    h = c.h
    d = c.d
    test = self()

    send(
      c.att,
      {:call, fn -> send(test, {:finish, Handoff.finish(h, term, @marker, deadline: d)}) end}
    )
  end

  defp clean!(c) do
    h = c.h

    assert att!(c, fn -> Handoff.cleaned(h, :primary, :possible) end) in [
             :ok,
             {:error, :handoff_down}
           ]
  end

  # A round trip through the attempt actor: everything it was sent before
  # (credit included) has been forwarded to the test.
  defp sync!(c), do: att!(c, fn -> :ok end)

  defp att!(c, f) do
    send(c.att, {:call, f, self()})
    assert_receive {:res, att, r} when att == c.att, @t
    r
  end

  defp actor do
    test = self()

    pid =
      spawn(fn ->
        Stream.repeatedly(fn ->
          receive do
            {:call, f, from} -> send(from, {:res, self(), f.()})
            {:call, f} -> f.()
            msg -> send(test, {:att, msg})
          end
        end)
        |> Stream.run()
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # The downstream owner: runs one function on {:go, f}, reports, then
  # stays alive (the coordinator monitors it) without reading its mailbox.
  defp owner! do
    test = self()

    pid =
      spawn(fn ->
        receive do: ({:go, f} -> send(test, {:result, self(), f.()}))
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp go(r, f), do: send(r, {:go, f})

  defp seeded do
    [
      {:plug_conn, :sent},
      {:telelem_owner_witness, :seeded_ref, :owner_down, :seeded},
      {:telelem_handoff, :c.pid(0, 1, 0), {:data, "foreign"}}
    ]
  end

  defp seed!(r), do: for(m <- seeded(), do: send(r, m))

  defp writer(test, gate \\ false), do: {Writer, %{test: test, gate: gate, n: 0}}

  # The writer runs in the owner process and blocks on exactly this ref.
  defp assert_writer!(call) do
    assert_receive {:writer, pid, ^call, ref}, @t
    {pid, ref}
  end

  defp reply({pid, ref}, r), do: send(pid, {:writer_reply, ref, r})

  # ── helpers: tracing ────────────────────────────────────────────────────

  defp trace!(pid, mfas) do
    session = :trace.session_create(:serve_requester, self(), [])
    on_exit(fn -> :trace.session_destroy(session) end)
    for mfa <- mfas, do: :trace.function(session, mfa, true, [])
    1 = :trace.process(session, pid, true, [:call])
    session
  end

  defp calls(session, pid, mode \\ :mfa) do
    # Every traced call precedes the owner's result message, which was
    # already received; a sync on the session makes the tracer queue whole.
    ref = :trace.delivered(session, pid)
    assert_receive {:trace_delivered, ^pid, ^ref}, @t
    drain_calls(pid, mode, [])
  end

  defp drain_calls(pid, mode, acc) do
    receive do
      {:trace, ^pid, :call, {m, f, args}} ->
        c = if mode == :args, do: {m, f, args}, else: {m, f, length(args)}
        drain_calls(pid, mode, [c | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # ── helpers: Primary setup (as in PrimaryTest) ──────────────────────────

  defp cfg_opts(ctx, extra \\ []) do
    {:ok, ingress} = Ingress.config(ingress_opts(ctx))

    Keyword.merge(
      [
        ingress: ingress,
        authority: ctx.auth,
        store: ctx.store,
        tree: ctx.root,
        units: @units,
        limits: Limits.new(),
        coordinator_ms: 5_000,
        grace_ms: 5_000,
        activation_ms: 5_000
      ],
      extra
    )
  end

  defp ingress_opts(ctx, extra \\ []) do
    me = self()

    Keyword.merge(
      [
        authenticate: fn input ->
          send(me, {:authenticate, input})
          Principal.new(@tenant, "caller", :sandbox, [:dispatch])
        end,
        context: %Context{mode: :sandbox, store: {MemoryStore, ctx.routes}, revisions: Revision},
        bindings: ctx.bindings,
        route_id: "chat",
        max_attempts: 1,
        deadline_ceiling_ms: 60_000,
        max_body_bytes: 4096,
        clock: {Telelem.Instance.SystemClock, nil}
      ],
      extra
    )
  end

  defp reader(body) do
    me = self()

    fn max, remaining ->
      send(me, {:read_body, max, remaining})
      {:ok, body}
    end
  end

  defp rev1 do
    Revision.new!(
      route_id: "chat",
      rev: 1,
      deployments: ["a", "b"],
      preferred: "a",
      fallback_on_refusal: false,
      deadline_ms: 30_000,
      max_attempts: 1
    )
  end

  defp bindings!(dir) do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem requester test CA", [])
    pem = :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
    fx = Path.join(dir, "fx")
    sb = Path.join(dir, "sbx")
    store_dir = Path.join(sb, "store")
    File.mkdir_p!(fx)
    File.mkdir_p!(store_dir)
    File.chmod!(store_dir, 0o700)

    doc = %{
      "version" => 2,
      "tenant_id" => @tenant,
      "mode" => "sandbox",
      "fixture_targets" => %{
        "a" => %{"host" => "127.0.0.1", "port" => 1, "ca_pem" => pem},
        "b" => %{"host" => "127.0.0.1", "port" => 2, "ca_pem" => pem}
      },
      "sandbox_store_root" => store_dir,
      "sandbox_signer_ref" => "sandbox:local-fixture"
    }

    {:ok, b} = Bootstrap.from_map(doc, fixture_root: fx, sandbox_root: sb)
    {:ok, p} = Principal.new(@tenant, "caller", :sandbox, [:dispatch])
    {:ok, %VerifiedTarget{} = t} = Telelem.Sandbox.resolve(p, b, "a")
    {b, t}
  end

  # ── helpers: generic ────────────────────────────────────────────────────

  defp await_mailbox!(pid, pred, until \\ nil) do
    until = until || now() + @t
    mb = mailbox(pid)

    cond do
      Enum.any?(mb, pred) -> mb
      now() >= until -> flunk("no matching message in #{inspect(pid)}: #{inspect(mb)}")
      true -> await_mailbox!(pid, pred, until)
    end
  end

  defp await_consumed!(pid, pred, until \\ nil) do
    until = until || now() + @t

    cond do
      not Enum.any?(mailbox(pid), pred) -> :ok
      now() >= until -> flunk("message never consumed by #{inspect(pid)}")
      true -> await_consumed!(pid, pred, until)
    end
  end

  defp wait_until!(t) do
    left = t - now()

    if left >= 0 do
      receive do
      after
        left + 1 -> wait_until!(t)
      end
    end
  end

  defp mailbox(pid), do: elem(Process.info(pid, :messages), 1)

  defp resume(p) do
    if Process.alive?(p), do: :sys.resume(p)
  catch
    :exit, _ -> :ok
  end

  defp contains?(term, needle) when term === needle, do: true
  defp contains?(b, n) when is_binary(b) and is_binary(n), do: String.contains?(b, n)
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
