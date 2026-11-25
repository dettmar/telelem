defmodule Telelem.Serve.CoordinatorStartTest do
  @moduledoc """
  W2a: `Telelem.Serve.CoordinatorStart.start/3`, one inactive
  `Telelem.Handoff` under a public `DynamicSupervisor`, activated by the
  exact requester before one absolute `s`.

  Listener-free and offline: one `Telelem.Instance` with no workers and its
  real `Telelem.Admission.Authority`, one real `Telelem.Receipts.Store` per
  case, a real `DynamicSupervisor` and real coordinators. No attempt, no
  socket, no hook in the product code. The requester is a test process that
  runs `start/3` on command and forwards everything else it receives.

  Barriers are messages, monitors, isolated `:trace` sessions (the
  requester with `set_on_spawn`, so its watchdog and worker are traced from
  their first instruction; the supervisor with `set_on_spawn` for its
  children's exits), `:trace.delivered/2` and synchronous calls. A start is
  held in the supervisor's mailbox with `:sys.suspend/1`; the requester or
  a coordinator is held with `:erlang.suspend_process/1`. The only clock
  wait is "until `s` has passed".

      scripts/mix test test/telelem/serve/coordinator_start_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.{Handoff, Instance}
  alias Telelem.Receipts.{Receipt, Store}
  alias Telelem.Serve.CoordinatorStart

  @moduletag :tmp_dir
  @t 2_000
  # How far ahead `s` is when a case needs to cross it.
  @window 1_000
  @units 4
  @tenant "local"

  @reader %{
    tenant_id: @tenant,
    subject_id: "operator",
    mode: :sandbox,
    permissions: [:receipt_read]
  }

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :coordinator_start_gw,
        limits: %{slots: 4, units: 40},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)

    %{
      auth: Instance.authority(inst),
      store: start_supervised!({Store, mode: :sandbox}, id: :store, restart: :temporary),
      sup: dsup!([])
    }
  end

  # ── options ─────────────────────────────────────────────────────────────

  test "only trusted metadata keys are accepted; anything else raises before spawning, naming the key only",
       ctx do
    r = requester()
    {opts, _} = trusted(ctx, r, "opts")
    s = now() + 30_000
    secret = "SECRET-BODY-7f3e"

    session = :trace.session_create(:coordinator_start_opts, self(), [])
    1 = :trace.process(session, r.req, true, [:procs, :set_on_spawn])

    bad = [
      {opts ++ [body: secret], ":body"},
      {opts ++ [headers: [{"authorization", secret}]], ":headers"},
      {opts ++ [authorization: secret], ":authorization"},
      {opts ++ [on_event: fn _ -> secret end], ":on_event"},
      {opts ++ [authority: self()], ":authority"},
      {opts ++ [requester: self()], ":requester"},
      {opts ++ [attempts: [:primary]], ":attempts"},
      {opts ++ [terminal_ack: true], ":terminal_ack"},
      {opts ++ [activation_limit: s], ":activation_limit"},
      {Keyword.delete(opts, :receipts), ":receipts"},
      {opts ++ [observer: self()], "at most once"},
      {[{"body", secret} | opts], "keyword list"}
    ]

    for {o, needle} <- bad do
      msg = req_call!(r, fn -> raised(fn -> CoordinatorStart.start(ctx.sup, s, o) end) end)
      assert {:raised, text} = msg
      assert text =~ needle, text
      refute text =~ secret, text
    end

    for {sup, s2, needle} <- [
          {:not_a_pid, s, "sup"},
          {ctx.sup, :soon, "integer"},
          {ctx.sup, now() + Handoff.max_deadline_ahead_ms() + 60_000, "ahead"}
        ] do
      assert {:raised, text} =
               req_call!(r, fn -> raised(fn -> CoordinatorStart.start(sup, s2, opts) end) end)

      assert text =~ needle, text
    end

    # S already reached (equality included): expired, nothing spawned.
    assert {:error, {:not_sent, :expired}} =
             req_call!(r, fn -> CoordinatorStart.start(ctx.sup, now(), opts) end)

    assert {:error, {:not_sent, :expired}} =
             req_call!(r, fn -> CoordinatorStart.start(ctx.sup, now() - 1, opts) end)

    ref = :trace.delivered(session, r.req)
    assert_receive {:trace_delivered, _, ^ref}, @t
    :trace.session_destroy(session)
    refute_received {:trace, _, :spawn, _, _}
    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
    req_quiet!(r)
  end

  # ── timely path ─────────────────────────────────────────────────────────

  test "the actual submitted spec, the process topology, then one timely exact activation", ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "timely")
    s = now() + 30_000
    tr = traced!(:coordinator_start_timely, r)
    :ok = :sys.suspend(ctx.sup)

    {ref, wmon, kmon} =
      try do
        # Given in a scrambled order; passed on in the fixed order.
        ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, Enum.reverse(opts)) end)
        {wd, worker} = blocked!(tr, ctx.sup)

        # The watchdog: unlinked, trapping, monitors the requester before the
        # worker exists (its own trace order), linked to the worker only.
        wd_events = split!(tr, wd)

        mon =
          index!(
            wd_events,
            &match?({:call, {:erlang, :monitor, [:process, p]}} when p == r.req, &1)
          )

        spawn = index!(wd_events, &match?({:spawn, ^worker, {CoordinatorStart, :worker, _}}, &1))
        assert mon < spawn, inspect(wd_events)

        assert {:links, [^worker]} = Process.info(wd, :links)
        assert {:trap_exit, true} = Process.info(wd, :trap_exit)
        assert {:process, r.req} in elem(Process.info(wd, :monitors), 1)
        refute wd in elem(Process.info(r.req, :links), 1)
        assert {:monitored_by, by} = Process.info(wd, :monitored_by)
        assert r.req in by

        # The worker: linked to the watchdog only, not trapping, blocked in
        # the public start_child/2 with exactly the fixed spec.
        assert {:links, [^wd]} = Process.info(worker, :links)
        assert {:trap_exit, false} = Process.info(worker, :trap_exit)

        assert_received {:trace, ^worker, :call,
                         {DynamicSupervisor, :start_child, [sup_arg, spec]}}

        assert sup_arg == ctx.sup

        assert spec == %{
                 id: Handoff,
                 start:
                   {Handoff, :start_link,
                    [
                      [
                        requester: r.req,
                        attempts: [:primary],
                        terminal_ack: true,
                        activation_limit: s
                      ] ++ opts
                    ]},
                 restart: :temporary,
                 shutdown: 5_000,
                 type: :worker,
                 modules: [Handoff]
               }

        {Handoff, :start_link, [hopts]} = spec.start

        assert Keyword.keys(hopts) ==
                 [:requester, :attempts, :terminal_ack, :activation_limit] ++
                   CoordinatorStart.trusted_keys()

        # No request body, header or authentication input anywhere in it.
        refute Enum.any?(
                 Keyword.keys(hopts),
                 &(&1 in [:body, :headers, :authorization, :on_event])
               )

        {ref, Process.monitor(wd), Process.monitor(worker)}
      after
        :ok = :sys.resume(ctx.sup)
      end

    assert_receive {:req_did, ^ref, {:ok, h}}, @t
    assert now() < s
    :trace.session_destroy(tr)

    # Both helpers are gone (the race between the requester's kill and the
    # watchdog's own return on the worker's :EXIT decides only the reason).
    assert_receive {:DOWN, ^wmon, :process, _, wr}, @t
    assert wr in [:normal, :killed]
    assert_receive {:DOWN, ^kmon, :process, _, kr}, @t
    assert kr in [:normal, :killed]

    # Supervised, temporary, monitoring the exact requester; active with one
    # ingress, and the requester kept no monitor and got no message.
    assert [{:undefined, ^h, :worker, [Handoff]}] = DynamicSupervisor.which_children(ctx.sup)
    assert {:links, [sup]} = Process.info(h, :links)
    assert sup == ctx.sup
    assert {:process, r.req} in elem(Process.info(h, :monitors), 1)
    assert %{activation: :active, receipts: :recording, outcome: nil} = Handoff.status(h)
    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "timely")
    assert [{:receipt, :ingress, :ok}, :activated] = collected(col)
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)

    # Temporary: a dead child is not restarted.
    hmon = Process.monitor(h)
    Process.exit(h, :kill)
    assert_receive {:DOWN, ^hmon, :process, _, :killed}, @t
    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
  end

  @tag :capture_log
  test "timely explicit startup refusals: not sent, nothing published", ctx do
    r = requester()

    # The supervisor's own refusal.
    full = dsup!(max_children: 0)
    {opts, col} = trusted(ctx, r, "full")

    assert {:error, {:not_sent, {:start_refused, :max_children}}} =
             req_call!(r, fn -> CoordinatorStart.start(full, now() + 30_000, opts) end)

    # The coordinator's init refusal (S beyond its terminal deadline).
    {opts2, col2} = trusted(ctx, r, "late-d", terminal_deadline: now() + 10_000)

    assert {:error, {:not_sent, {:start_refused, {:bad_option, :activation_limit}}}} =
             req_call!(r, fn -> CoordinatorStart.start(ctx.sup, now() + 20_000, opts2) end)

    assert %{active: 0} = DynamicSupervisor.count_children(ctx.sup)
    assert collected(col) == [] and collected(col2) == []
    assert Store.stats(ctx.store).receipts == 0
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)
  end

  # ── queued and late startup ─────────────────────────────────────────────

  @tag :capture_log
  test "a start queued across s: :start_unconfirmed at s, helpers ended, the queued request not withdrawn and refused late",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "queued")
    s = now() + @window
    tr = traced!(:coordinator_start_queued, r)
    :ok = :sys.suspend(ctx.sup)

    try do
      ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
      {wd, worker} = blocked!(tr, ctx.sup)
      wmon = Process.monitor(wd)
      kmon = Process.monitor(worker)

      # Returned with the supervisor still suspended: never waited on it.
      assert_receive {:req_did, ^ref, {:error, {:not_sent, :start_unconfirmed}}}, @window + @t
      assert now() >= s
      assert_receive {:DOWN, ^kmon, :process, _, :killed}, @t
      assert_receive {:DOWN, ^wmon, :process, _, wr}, @t
      assert wr in [:normal, :killed]

      # Killing the helpers withdrew nothing: the start is still queued.
      assert {:messages, msgs} = Process.info(ctx.sup, :messages)
      assert Enum.any?(msgs, &match?({:"$gen_call", _, {:start_child, _}}, &1)), inspect(msgs)
    after
      :trace.session_destroy(tr)
      :ok = :sys.resume(ctx.sup)
    end

    # The requester's scoped alias is deactivated: anything sent to it after
    # the return (a late startup reply included) is dropped by the runtime.
    send(alias_of!(), {:late_startup_reply_probe, self()})

    # Processed after s: refused by the coordinator's own init; its reply
    # went to the dead worker.
    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
    assert collected(col) == []
    assert Store.stats(ctx.store).receipts == 0
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)
  end

  test "a startup reply observed at s is not used: the late inactive child is never activated and stops at s, silently",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "late-reply")
    s = now() + @window
    tr = traced!(:coordinator_start_late_reply, r)
    :ok = :sys.suspend(ctx.sup)
    ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
    {wd, worker} = blocked!(tr, ctx.sup)
    wmon = Process.monitor(wd)
    kmon = Process.monitor(worker)
    true = :erlang.suspend_process(r.req)

    try do
      :ok = :sys.resume(ctx.sup)
      assert [{:undefined, h, :worker, [Handoff]}] = DynamicSupervisor.which_children(ctx.sup)
      hmon = Process.monitor(h)
      assert now() < s
      assert %{activation: :inactive} = Handoff.status(h)

      # The worker delivered its result and returned; the watchdog then
      # returned :normal on its :EXIT (the suspended requester killed nothing).
      assert_receive {:trace, ^worker, :send, {_, _, {:ok, ^h}}, _alias}, @t
      assert_receive {:DOWN, ^kmon, :process, _, :normal}, @t
      assert_receive {:DOWN, ^wmon, :process, _, :normal}, @t

      # Never activated: it stops :normal at s on its own.
      assert_receive {:DOWN, ^hmon, :process, _, :normal}, @window + @t
      assert now() >= s
    after
      :trace.session_destroy(tr)
      true = :erlang.resume_process(r.req)
    end

    # The result was in the requester's mailbox, but read at or after s; it
    # was drained, and the alias no longer delivers.
    assert_receive {:req_did, ^ref, {:error, {:not_sent, :start_unconfirmed}}}, @t
    send(alias_of!(), {:late_startup_reply_probe, self()})
    assert collected(col) == []
    assert Store.stats(ctx.store).receipts == 0
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)
  end

  @tag :capture_log
  test "the watchdog's own :normal return at s kills the exact blocked worker (requester suspended)",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "wd-normal")
    s = now() + @window
    tr = traced!(:coordinator_start_wd_normal, r)
    :ok = :sys.suspend(ctx.sup)

    try do
      ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
      {wd, worker} = blocked!(tr, ctx.sup)
      wmon = Process.monitor(wd)
      kmon = Process.monitor(worker)
      true = :erlang.suspend_process(r.req)

      try do
        # The link alone would not end a non-trapping worker on a :normal
        # exit; the watchdog kills the exact worker first (the requester is
        # suspended, so nobody else could have).
        assert_receive {:DOWN, ^wmon, :process, _, :normal}, @window + @t
        assert_receive {:DOWN, ^kmon, :process, _, :killed}, @t
        assert now() >= s
      after
        true = :erlang.resume_process(r.req)
      end

      assert_receive {:req_did, ^ref, {:error, {:not_sent, :start_unconfirmed}}}, @t
    after
      :trace.session_destroy(tr)
      :ok = :sys.resume(ctx.sup)
    end

    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
    assert collected(col) == []
    req_quiet!(r)
  end

  @tag :capture_log
  test "on failure the requester ends the exact (lagging) watchdog itself without waiting; the worker dies by the link",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "wd-lagging")
    s = now() + @window
    tr = traced!(:coordinator_start_wd_lagging, r)
    :ok = :sys.suspend(ctx.sup)

    try do
      ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
      {wd, worker} = blocked!(tr, ctx.sup)
      wmon = Process.monitor(wd)
      kmon = Process.monitor(worker)
      # The watchdog cannot run (so cannot act at s on its own).
      true = :erlang.suspend_process(wd)

      try do
        assert_receive {:req_did, ^ref, {:error, {:not_sent, :start_unconfirmed}}}, @window + @t
        assert now() >= s
      after
        # Badarg if it is already gone (killed while suspended).
        _ =
          try do
            :erlang.resume_process(wd)
          catch
            :error, :badarg -> :gone
          end
      end

      # Ended by the requester's kill, not by its own :normal return.
      assert_receive {:DOWN, ^wmon, :process, _, :killed}, @t
      assert_receive {:DOWN, ^kmon, :process, _, :killed}, @t
    after
      :trace.session_destroy(tr)
      :ok = :sys.resume(ctx.sup)
    end

    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
    assert collected(col) == []
    req_quiet!(r)
  end

  # ── watchdog death ──────────────────────────────────────────────────────

  test "watchdog killed while the requester is suspended: the worker dies by the link, the requester returns before s; a late child stays silent",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "wd-killed")
    s = now() + @window
    tr = traced!(:coordinator_start_wd_killed, r)
    str = sup_traced!(:coordinator_start_wd_killed_sup, ctx.sup)
    :ok = :sys.suspend(ctx.sup)

    try do
      ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
      {wd, worker} = blocked!(tr, ctx.sup)
      wmon = Process.monitor(wd)
      kmon = Process.monitor(worker)
      true = :erlang.suspend_process(r.req)

      try do
        Process.exit(wd, :kill)
        assert_receive {:DOWN, ^wmon, :process, _, :killed}, @t
        assert_receive {:DOWN, ^kmon, :process, _, :killed}, @t
      after
        true = :erlang.resume_process(r.req)
      end

      assert_receive {:req_did, ^ref, {:error, {:not_sent, :start_unconfirmed}}}, @t
      # Ended by the watchdog's :DOWN, not by s.
      assert now() < s
    after
      :trace.session_destroy(tr)
      :ok = :sys.resume(ctx.sup)
    end

    # The queued start is processed before s: a late, inactive child nobody
    # activates. It stops :normal at s and sends, emits and publishes nothing.
    assert_receive {:trace, _sup, :spawn, h, {:proc_lib, :init_p, _}}, @t
    assert %{activation: :inactive} = Handoff.status(h)
    assert_receive {:trace, ^h, :exit, :normal}, @window + @t
    assert now() >= s
    :trace.session_destroy(str)

    assert collected(col) == []
    assert Store.stats(ctx.store).receipts == 0
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)
  end

  test "requester dead while the start is queued: the watchdog kills the worker and returns; a late child stops on the requester's :DOWN, silently",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "req-dead")
    s = now() + 30_000
    tr = traced!(:coordinator_start_req_dead, r)
    str = sup_traced!(:coordinator_start_req_dead_sup, ctx.sup)
    :ok = :sys.suspend(ctx.sup)

    try do
      _ = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
      {wd, worker} = blocked!(tr, ctx.sup)
      wmon = Process.monitor(wd)
      kmon = Process.monitor(worker)
      rmon = Process.monitor(r.req)
      Process.exit(r.req, :kill)
      assert_receive {:DOWN, ^rmon, :process, _, :killed}, @t
      assert_receive {:DOWN, ^kmon, :process, _, :killed}, @t
      assert_receive {:DOWN, ^wmon, :process, _, :normal}, @t
    after
      :trace.session_destroy(tr)
      :ok = :sys.resume(ctx.sup)
    end

    # Processed before s, with the requester already dead.
    assert_receive {:trace, _sup, :spawn, h, {:proc_lib, :init_p, _}}, @t
    assert_receive {:trace, ^h, :exit, :normal}, @t
    assert now() < s
    :trace.session_destroy(str)

    assert %{active: 0, specs: 0} = DynamicSupervisor.count_children(ctx.sup)
    assert collected(col) == []
    assert Store.stats(ctx.store).receipts == 0
  end

  # ── activation after a timely start ─────────────────────────────────────

  test "an activation accepted before s whose reply is observed at s: {:unconfirmed, h}, active with ingress, no rollback",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "late-activation")
    s = now() + @window
    tr = traced!(:coordinator_start_late_activation, r)
    :ok = :sys.suspend(ctx.sup)
    ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
    {_wd, _worker} = blocked!(tr, ctx.sup)
    # Hold the requester, let the start complete, then hold the coordinator.
    true = :erlang.suspend_process(r.req)
    :ok = :sys.resume(ctx.sup)
    assert [{:undefined, h, :worker, [Handoff]}] = DynamicSupervisor.which_children(ctx.sup)
    true = :erlang.suspend_process(h)

    try do
      true = :erlang.resume_process(r.req)
      # Timely start result: the requester submits the one activation.
      assert_receive {:trace, _, :send, {:"$gen_call", _, {:activate, ^s}}, ^h}, @t
      true = :erlang.suspend_process(r.req)
    after
      true = :erlang.resume_process(h)
    end

    try do
      # Decided before s (queued after the activation, status sees it), and
      # the reply is sent to the suspended requester's still-active alias.
      assert %{activation: :active} = Handoff.status(h)
      assert now() < s
      wait_until(s)
    after
      :trace.session_destroy(tr)
      true = :erlang.resume_process(r.req)
    end

    assert_receive {:req_did, ^ref, {:error, {:unconfirmed, ^h}}}, @t
    req_quiet!(r)

    # Active with ingress published, untouched by s; the request goes on.
    assert %{activation: :active, outcome: nil, phase: :open} = Handoff.status(h)
    assert [{:ingress, 0, :none, :none, :none}] = events!(ctx.store, "late-activation")
    assert [{:receipt, :ingress, :ok}, :activated] = collected(col)
    assert :ok = req_call!(r, fn -> Handoff.cancel(h, :gone) end)
    assert_receive {:req, {:telelem_handoff, ^h, {:failed, :gone}}}, @t
    assert [:ingress, :terminal] = kinds!(ctx.store, "late-activation")
  end

  test "a timely explicit activation refusal: the store gone after the start is {:refused, :receipts_unbound, h}, nothing published",
       ctx do
    r = requester()
    {opts, col} = trusted(ctx, r, "unbound")
    s = now() + 30_000
    tr = traced!(:coordinator_start_unbound, r)
    :ok = :sys.suspend(ctx.sup)
    ref = req_do(r, fn -> CoordinatorStart.start(ctx.sup, s, opts) end)
    {_wd, _worker} = blocked!(tr, ctx.sup)
    true = :erlang.suspend_process(r.req)

    {h, hmon} =
      try do
        :ok = :sys.resume(ctx.sup)
        assert [{:undefined, h, :worker, [Handoff]}] = DynamicSupervisor.which_children(ctx.sup)
        assert %{activation: :inactive} = Handoff.status(h)
        hmon = Process.monitor(h)
        # Gone between the timely start and the activation.
        stop_supervised!(:store)
        {h, hmon}
      after
        :trace.session_destroy(tr)
        true = :erlang.resume_process(r.req)
      end

    assert_receive {:req_did, ^ref, {:error, {:refused, :receipts_unbound, ^h}}}, @t
    assert now() < s
    assert_receive {:DOWN, ^hmon, :process, _, :normal}, @t
    assert collected(col) == []
    assert {:monitors, []} = Process.info(r.req, :monitors)
    req_quiet!(r)
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  # The trusted options in the fixed order of `trusted_keys/0`.
  defp trusted(ctx, _r, request_id, over \\ []) do
    col = collector()
    store = ctx.store

    opts = [
      admission_authority: ctx.auth,
      terminal_deadline: Keyword.get(over, :terminal_deadline, now() + 60_000),
      coordinator_grant: {:one_primary, @units},
      receipts: [
        store: store,
        producer: Store.producer(store),
        store_mode: :sandbox,
        meta: %{
          tenant_id: @tenant,
          request_id: request_id,
          route_rev: 1,
          deployment_id: "a",
          mode: :sandbox
        }
      ],
      observer: col,
      trace: col,
      cleanup_timeout: 60_000,
      register_timeout: 60_000
    ]

    assert Keyword.keys(opts) == CoordinatorStart.trusted_keys()
    {opts, col}
  end

  defp dsup!(opts) do
    test = self()

    holder =
      spawn(fn ->
        {:ok, sup} = DynamicSupervisor.start_link([strategy: :one_for_one] ++ opts)
        send(test, {:dsup, self(), sup})
        receive do: (:never -> :ok)
      end)

    assert_receive {:dsup, ^holder, sup}, @t

    on_exit(fn ->
      Process.exit(sup, :kill)
      Process.exit(holder, :kill)
    end)

    sup
  end

  # The requester and everything it spawns: process events, sends, and the
  # calls to `start_child/2` and `erlang:monitor/2`.
  defp traced!(name, r) do
    Code.ensure_loaded!(DynamicSupervisor)
    session = :trace.session_create(name, self(), [])
    1 = :trace.process(session, r.req, true, [:procs, :send, :call, :set_on_spawn])
    assert :trace.function(session, {DynamicSupervisor, :start_child, 2}, true, []) > 0
    assert :trace.function(session, {:erlang, :monitor, 2}, true, []) > 0
    session
  end

  # The supervisor and its children: process events only.
  defp sup_traced!(name, sup) do
    session = :trace.session_create(name, self(), [])
    1 = :trace.process(session, sup, true, [:procs, :set_on_spawn])
    session
  end

  # The requester spawned the watchdog, the watchdog the worker, and the
  # worker's start request is in the (suspended) supervisor's mailbox.
  # The watchdog's own trace messages are left in the mailbox (see split!/2).
  defp blocked!(_tr, sup) do
    assert_receive {:trace, _req, :spawn, wd, {CoordinatorStart, :watchdog, [%{alias: al}]}}, @t
    Process.put(:requester_alias, al)
    assert_receive {:trace, worker, :send, {:"$gen_call", _, {:start_child, _}}, ^sup}, @t
    assert {:links, [^wd]} = Process.info(worker, :links)
    {wd, worker}
  end

  # The requester's scoped alias, as handed to its watchdog (traced spawn
  # arguments).
  defp alias_of! do
    al = Process.get(:requester_alias)
    assert is_reference(al)
    al
  end

  # Everything traced for `pid` so far, in its own order (already-consumed
  # messages excluded).
  defp split!(session, pid) do
    ref = :trace.delivered(session, pid)
    assert_receive {:trace_delivered, ^pid, ^ref}, @t
    drain_trace(pid, [])
  end

  defp drain_trace(pid, acc) do
    receive do
      {:trace, ^pid, :call, mfa} -> drain_trace(pid, [{:call, mfa} | acc])
      {:trace, ^pid, :spawn, p, mfa} -> drain_trace(pid, [{:spawn, p, mfa} | acc])
      {:trace, ^pid, kind, a} -> drain_trace(pid, [{kind, a} | acc])
      {:trace, ^pid, kind, a, b} -> drain_trace(pid, [{kind, a, b} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp index!(list, pred) do
    i = Enum.find_index(list, pred)
    assert i != nil, "not found in #{inspect(list, limit: :infinity)}"
    i
  end

  defp raised(fun) do
    fun.()
    :no_raise
  rescue
    e in ArgumentError -> {:raised, Exception.message(e)}
  end

  # ── requester, collector ────────────────────────────────────────────────

  defp requester do
    test = self()
    pid = spawn(fn -> req_loop(test) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    %{req: pid}
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

  defp req_call!(r, fun, timeout \\ @t) do
    ref = req_do(r, fun)
    assert_receive {:req_did, ^ref, result}, timeout
    result
  end

  # Nothing reached the requester (barrier: a later request it handles).
  defp req_quiet!(r) do
    assert :pong = req_call!(r, fn -> :pong end)
    refute_received {:req, _}
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
        col_loop(test, [ev | acc])

      {:telelem_handoff, _h, ev} ->
        col_loop(test, [{:event, ev} | acc])
    end
  end

  defp collected(col) do
    ref = make_ref()
    send(col, {:get, self(), ref})
    assert_receive {:collected, ^ref, evs}, @t
    evs
  end

  # ── evidence reads ──────────────────────────────────────────────────────

  defp lookup!(store, request_id) do
    assert {:ok, %Receipt{} = receipt} = Store.lookup(store, @reader, request_id)
    receipt
  end

  defp events!(store, id) do
    for e <- lookup!(store, id).events,
        do: {e.kind, e.attempt, e.deployment_id, e.outcome, e.exposure}
  end

  defp kinds!(store, id), do: Enum.map(lookup!(store, id).events, & &1.kind)

  defp wait_until(t) do
    case t - now() do
      left when left > 0 ->
        receive do
        after
          left -> wait_until(t)
        end

      _ ->
        :ok
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
