defmodule Telelem.Serve.TreeTest.Lookalike do
  @moduledoc false
  # Same flags and children as `Telelem.Serve.Tree`; only the module differs.
  use Supervisor
  @impl true
  def init(:ok), do: Telelem.Serve.Tree.init(:ok)
end

defmodule Telelem.Serve.TreeTest do
  @moduledoc """
  W2b: `Telelem.Serve.Tree`, the root (`one_for_all`, zero restarts) with
  the `:coordinators` then `:guards` branch `DynamicSupervisor`s, and
  `resolve/2`, the setup-time pins.

  Listener-free and offline: one `Telelem.Instance` with no workers and its
  real `Telelem.Admission.Authority`, one real `Telelem.Receipts.Store`, real
  trees, real `Telelem.Serve.CoordinatorStart` coordinators and real
  `Telelem.Wire.OwnerGuard` guards, witnesses and owners. No socket is
  opened: the target is a `VerifiedTarget` for `127.0.0.1` port 1, and every
  owner is parked before it dials (the authority held with `:sys.suspend/1`,
  so the owner waits in `attach_grant/3` behind the coordinator's
  reservation) and ends there; `{:connect_failed, _, _}` is refuted.

  Covers: the installed root flags and branch specs, the start order and one
  `which_children` call per resolution (isolated `:trace` sessions); refusal
  of a non-pid, remote, lookalike, dead or unresponsive root and of every
  invalid snapshot shape (a forged root process answering crafted
  snapshots); no rebinding of old pins to a new tree; the actual specs
  `CoordinatorStart` and `OwnerGuard` submit to the resolved branches
  (separate trace sessions; no body, authentication input or target); an
  orderly stop held at a shutdown barrier in a coordinator-branch fixture,
  where the guard branch, its guard and that guard's owner are already dead
  and the coordinator branch and root are not; a forced kill of the exact
  guard while its owner is parked (witness confirms, no restart, tree
  untouched); and the loss of either branch (sibling and root down, no
  spawn by the root afterwards).

  Ordering uses messages, monitors, `:trace.delivered/2`, mailboxes read
  back with `Process.info/2` (bounded) and the barrier. No clock waits.

  Not covered: TLS/socket teardown, slot or ledger quiescence at the
  barrier, any overall shutdown bound, a guard killed by its own shutdown
  timer during a stop.

      scripts/mix test test/telelem/serve/tree_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.{Handoff, Instance}
  alias Telelem.Receipts.Store
  alias Telelem.Sandbox.VerifiedTarget
  alias Telelem.Serve.{CoordinatorStart, Tree}
  alias Telelem.Serve.Tree.Pins
  alias Telelem.Serve.TreeTest.Lookalike
  alias Telelem.Wire.{Limits, OwnerGuard}

  @moduletag :tmp_dir
  @t 2_000
  @units 4
  @tenant "local"
  @secret "sk-test-SECRET-body-marker"
  @empty %{active: 0, specs: 0, supervisors: 0, workers: 0}

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :serve_tree_gw,
        limits: %{slots: 4, units: 40},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)
    :ok = Instance.subscribe(inst)
    auth = Instance.authority(inst)

    on_exit(fn ->
      try do
        if Process.alive?(auth), do: :sys.resume(auth)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem tree test CA", [])
    {:ok, target} = VerifiedTarget.build("a", "127.0.0.1", 1, [der])

    %{
      auth: auth,
      tag: inst.tag,
      store: start_supervised!({Store, mode: :sandbox}, id: :store, restart: :temporary),
      target: target
    }
  end

  # ── installed tree and resolution ───────────────────────────────────────

  test "installed flags and branch specs; coordinators then guards; one snapshot resolves exact pins" do
    assert {:ok, {flags, specs}} = Tree.init(:ok)
    assert flags == %{strategy: :one_for_all, intensity: 0, period: 5, auto_shutdown: :never}
    assert specs == [branch(:coordinators), branch(:guards)]

    assert Tree.child_spec([]) == %{
             id: Tree,
             start: {Tree, :start_link, [[]]},
             restart: :permanent,
             type: :supervisor,
             shutdown: :infinity,
             modules: [Tree]
           }

    assert_raise ArgumentError, fn -> Tree.child_spec(name: Tree) end
    assert_raise ArgumentError, fn -> Tree.start_link(name: Tree) end

    # The holder and everything it spawns, from the root's first instruction.
    session = :trace.session_create(:serve_tree_start, self(), [])
    root = tree!(session)
    events = trace!(session, :all)
    :trace.session_destroy(session)

    assert [c, g] = for({:trace, ^root, :spawn, p, _} <- events, do: p)

    # The live root holds exactly the installed specs, in that start order.
    for id <- [:coordinators, :guards] do
      assert {:ok, spec} = :supervisor.get_childspec(root, id)
      assert Map.pop(spec, :significant) == {false, branch(id)}
    end

    assert {:trap_exit, true} = Process.info(root, :trap_exit)
    assert %{specs: 2, active: 2, supervisors: 2, workers: 0} = Supervisor.count_children(root)
    assert Process.info(root, :registered_name) == {:registered_name, []}

    # One call to the root: its which_children request; nothing else.
    {res, sends} = traced_resolve(root)
    assert res == {:ok, %Pins{root: root, coordinators: c, guards: g}}
    assert [{^root, {:"$gen_call", _, :which_children}}] = for({^root, _} = s <- sends, do: s)
    assert [{_, {:resolved, _, ^res}}] = for({to, _} = s <- sends, to != root, do: s)

    for b <- [c, g] do
      assert DynamicSupervisor.count_children(b) == @empty
      assert {:links, links} = Process.info(b, :links)
      assert root in links
    end
  end

  test "refused roots: non-pid, remote, lookalike, plain processes, dead, unresponsive; and invalid snapshots" do
    assert Tree.resolve(:not_a_pid) == {:error, :invalid_root}
    assert Tree.resolve(Tree) == {:error, :invalid_root}
    assert Tree.resolve(remote_pid()) == {:error, :invalid_root}

    for bad <- [0, 60_001, :infinity, 1.5],
        do: assert_raise(ArgumentError, fn -> Tree.resolve(self(), bad) end)

    # Lookalike (same children, another module), a bare DynamicSupervisor
    # and a plain process: refused by the initial call, never called.
    look = held!(fn -> Supervisor.start_link(Lookalike, :ok) end)
    assert [_, _] = Supervisor.which_children(look)
    dsup = held!(fn -> DynamicSupervisor.start_link(strategy: :one_for_one) end)
    plain = idle()

    for p <- [look, dsup, plain] do
      assert {{:error, :not_tree}, sends} = traced_resolve(p)
      assert [{_, {:resolved, _, _}}] = sends
    end

    # Dead: a stopped tree and an exited process.
    root = tree!()
    :ok = Supervisor.stop(root, :normal)
    assert Tree.resolve(root) == {:error, :root_down}
    {gone, m} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^m, :process, ^gone, :normal}, @t
    assert Tree.resolve(gone) == {:error, :root_down}

    # A process claiming the tree's initial call (a forged dictionary is not
    # detected by the initial-call check; see the module doc) and answering
    # each which_children call as told.
    c = idle()
    g = idle()
    {dead, m} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^m, :process, ^dead, :normal}, @t
    remote = remote_pid()
    ok_c = {:coordinators, c, :supervisor, [DynamicSupervisor]}
    ok_g = {:guards, g, :supervisor, [DynamicSupervisor]}

    forged = forged!()

    # Control: a well-formed snapshot (either order) is accepted from it.
    for snap <- [[ok_c, ok_g], [ok_g, ok_c]] do
      assert {{:ok, %Pins{root: ^forged, coordinators: ^c, guards: ^g}}, _} =
               traced_resolve(forged, answer(forged, snap))
    end

    invalid = [
      :not_a_list,
      [],
      [ok_c],
      [ok_g],
      [ok_c, ok_g, {:extra, idle(), :supervisor, [DynamicSupervisor]}],
      [ok_c, ok_c],
      [ok_c, {:coordinators, g, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guard, g, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, g, :supervisor, [Supervisor]}],
      [ok_c, {:guards, g, :supervisor, [DynamicSupervisor, Supervisor]}],
      [ok_c, {:guards, g, :worker, [DynamicSupervisor]}],
      [ok_c, {:guards, :restarting, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, :undefined, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, remote, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, dead, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, c, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, forged, :supervisor, [DynamicSupervisor]}],
      [ok_c, {:guards, g, :supervisor}],
      [:a, :b]
    ]

    for snap <- invalid do
      assert {{:error, :invalid_snapshot}, sends} = traced_resolve(forged, answer(forged, snap))

      assert [{^forged, {:"$gen_call", _, :which_children}}] =
               for({^forged, _} = s <- sends, do: s)
    end

    # Missing snapshot: no reply within the timeout, or the root exits on
    # the call.
    assert {{:error, :no_snapshot}, [_, _]} =
             traced_resolve(forged, answer(forged, :no_reply), 50)

    fm = Process.monitor(forged)
    assert {{:error, :root_down}, [_, _]} = traced_resolve(forged, answer(forged, :exit))
    assert_receive {:DOWN, ^fm, :process, ^forged, :boom}, @t
  end

  test "no rebinding: old pins keep the old dead pids; starts on them fail; the new tree is untouched",
       ctx do
    root_a = tree!()
    {:ok, pins_a} = Tree.resolve(root_a)
    ms = for p <- [root_a, pins_a.coordinators, pins_a.guards], do: {p, Process.monitor(p)}
    :ok = Supervisor.stop(root_a, :normal)
    for {p, m} <- ms, do: assert_receive({:DOWN, ^m, :process, ^p, _}, @t)

    root_b = tree!()
    {:ok, pins_b} = Tree.resolve(root_b)
    assert Tree.resolve(root_a) == {:error, :root_down}

    assert MapSet.disjoint?(
             MapSet.new(Map.values(Map.from_struct(pins_a))),
             MapSet.new(Map.values(Map.from_struct(pins_b)))
           )

    r = requester()
    d = now() + 60_000

    assert {:error, {:not_sent, :start_unconfirmed}} =
             req_call!(r, fn ->
               CoordinatorStart.start(pins_a.coordinators, now() + 30_000, trusted(ctx, "a", d))
             end)

    {:ok, h} = Handoff.start(requester: idle(), attempts: [:primary])
    on_exit(fn -> Process.exit(h, :kill) end)

    assert {:error, :start_failed} =
             OwnerGuard.start(pins_a.guards, gopts(ctx, h, d, r.req))

    assert DynamicSupervisor.count_children(pins_b.coordinators) == @empty
    assert DynamicSupervisor.count_children(pins_b.guards) == @empty
  end

  # ── what the real start functions submit to the resolved branches ───────

  test "actual CoordinatorStart and OwnerGuard specs at the resolved branches; no body, auth or target",
       ctx do
    root = tree!()
    {:ok, pins} = Tree.resolve(root)
    r = requester()
    d = now() + 60_000
    s = now() + 30_000
    opts = trusted(ctx, "tree-specs", d)

    # Trace 1: the requester and everything it spawns, for CoordinatorStart.
    tr = calls_traced!(:serve_tree_coordinator_spec, r.req)
    assert {:ok, h} = req_call!(r, fn -> CoordinatorStart.start(pins.coordinators, s, opts) end)
    assert [{sup, cspec}] = start_child_calls!(tr)
    :trace.session_destroy(tr)

    assert sup == pins.coordinators

    assert cspec == %{
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

    assert [{:undefined, ^h, :worker, [Handoff]}] =
             DynamicSupervisor.which_children(pins.coordinators)

    assert {:links, [l]} = Process.info(h, :links)
    assert l == pins.coordinators
    assert DynamicSupervisor.count_children(pins.guards) == @empty

    # Trace 2 (a new session): the same requester launching the guard.
    :ok = :sys.suspend(ctx.auth)
    gopts = gopts(ctx, h, d, r.req)
    tr = calls_traced!(:serve_tree_guard_spec, r.req)

    assert {:ok, %{guard: g, owner: o, witness: w, ref: gref}} =
             req_call!(r, fn -> OwnerGuard.start(pins.guards, gopts) end)

    assert [{sup, gspec}] = start_child_calls!(tr)
    :trace.session_destroy(tr)

    assert sup == pins.guards
    assert {OwnerGuard, :start_link, [meta]} = gspec.start

    assert gspec == %{
             id: OwnerGuard,
             start: {OwnerGuard, :start_link, [meta]},
             restart: :temporary,
             shutdown: 2 * 50 + OwnerGuard.bounds().shutdown_slack_ms,
             type: :worker,
             modules: [OwnerGuard]
           }

    assert Enum.sort(Map.keys(meta)) ==
             [:deadline, :grace_ms, :handoff, :observer, :ref, :requester, :until, :witness]

    assert %{handoff: ^h, requester: req, observer: obs, witness: ^w, ref: ^gref, deadline: ^d} =
             meta

    assert req == r.req and obs == self()

    assert [{:undefined, ^g, :worker, [OwnerGuard]}] =
             DynamicSupervisor.which_children(pins.guards)

    # Neither submitted spec carries the body, the target or any request,
    # header or authentication field; the payload went only in the GO.
    body = Keyword.fetch!(gopts[:upstream], :body)

    for spec <- [cspec, gspec],
        needle <-
          [@secret, body, ctx.target, ctx.target.host, gopts[:upstream]] ++
            [:body, :headers, :authorization, :upstream, :gate, :target] do
      refute contains?(spec, needle), inspect(needle)
    end

    # The owner is parked before its dial, behind the held reservation.
    await_reserve!(ctx.auth, h, o)

    # End it without a dial: the requester's loss reaches both the guard
    # (trigger) and the coordinator (which cancels the attach); either may
    # end the owner first, so only the confirmed end is asserted here.
    Process.exit(r.req, :kill)
    assert_receive {:telelem_owner_guard, ^gref, :owner_down, cause, _how}, @t
    assert cause in [:requester_down, :owner_exited]
    assert_receive {:telelem_owner_witness, ^gref, :owner_down, ^o}, @t
    refute Process.alive?(o)
    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
    assert Process.alive?(root)
  end

  # ── orderly stop ────────────────────────────────────────────────────────

  test "orderly stop: at the coordinator fixture's shutdown barrier the guard branch, its guard and owner are dead",
       ctx do
    root = tree!()
    {:ok, pins} = Tree.resolve(root)
    r = requester()
    d = now() + 60_000

    assert {:ok, h} =
             req_call!(r, fn ->
               CoordinatorStart.start(pins.coordinators, now() + 30_000, trusted(ctx, "stop", d))
             end)

    assert {:ok, fixture} = DynamicSupervisor.start_child(pins.coordinators, fixture_spec())

    :ok = :sys.suspend(ctx.auth)

    # Built here: `self()` (observer, controller) is the test.
    gopts = gopts(ctx, h, d, r.req)

    assert {:ok, %{guard: g, owner: o, ref: gref}} =
             req_call!(r, fn -> OwnerGuard.start(pins.guards, gopts) end)

    await_reserve!(ctx.auth, h, o)
    send(fixture, {:watch, [pins.guards, g, o, pins.coordinators, root]})

    ms =
      for p <- [root, pins.coordinators, pins.guards, g, o, fixture, h],
          into: %{},
          do: {p, Process.monitor(p)}

    test = self()
    stopper = spawn(fn -> send(test, {:stopped, self(), Supervisor.stop(root, :shutdown)}) end)

    # Barrier: the fixture holds its own shutdown, so its branch and the
    # root are still waiting on it.
    assert_receive {:fixture_shutdown, ^fixture, :shutdown, seen}, @t
    # What the fixture saw on its shutdown signal: guards branch, guard and
    # owner dead; its own branch and the root alive.
    assert seen == [false, false, false, true, true]

    refute Process.alive?(pins.guards)
    refute Process.alive?(g)
    refute Process.alive?(o)
    assert Process.alive?(fixture) and Process.alive?(pins.coordinators) and Process.alive?(root)
    refute_received {:stopped, ^stopper, _}
    refute_received {:DOWN, _, :process, ^fixture, _}

    send(fixture, :release)
    assert_receive {:stopped, ^stopper, :ok}, @t

    for {p, why} <- [
          {pins.guards, :shutdown},
          {g, :shutdown},
          {fixture, :shutdown},
          {pins.coordinators, :shutdown},
          {root, :shutdown}
        ] do
      m = Map.fetch!(ms, p)
      assert_receive {:DOWN, ^m, :process, ^p, ^why}, @t
    end

    for p <- [o, h] do
      m = Map.fetch!(ms, p)
      assert_receive {:DOWN, ^m, :process, ^p, _}, @t
    end

    # The guard confirmed its owner (fenced while parked in attach).
    assert_receive {:telelem_owner_guard, ^gref, :owner_down, :parent_exit, :killed}, @t
    assert_receive {:telelem_owner_witness, ^gref, :owner_down, ^o}, @t
    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  # ── losses ──────────────────────────────────────────────────────────────

  test "forced loss of the exact guard while its owner is parked: witness confirms, no restart, tree untouched",
       ctx do
    root = tree!()
    {:ok, pins} = Tree.resolve(root)
    r = requester()
    d = now() + 60_000

    assert {:ok, h} =
             req_call!(r, fn ->
               CoordinatorStart.start(pins.coordinators, now() + 30_000, trusted(ctx, "loss", d))
             end)

    :ok = :sys.suspend(ctx.auth)

    gopts = gopts(ctx, h, d, r.req, grace_ms: 10_000)

    assert {:ok, %{guard: g, owner: o, witness: w, ref: gref}} =
             req_call!(r, fn -> OwnerGuard.start(pins.guards, gopts) end)

    await_reserve!(ctx.auth, h, o)

    session = :trace.session_create(:serve_tree_guard_loss, self(), [])
    1 = :trace.process(session, pins.guards, true, [:procs])
    tree_ms = for p <- [root, pins.coordinators, pins.guards], do: Process.monitor(p)
    gm = Process.monitor(g)
    wm = Process.monitor(w)

    Process.exit(g, :kill)

    assert_receive {:DOWN, ^gm, :process, ^g, :killed}, @t
    assert_receive {:telelem_owner_witness, ^gref, :owner_down, ^o}, @t
    refute Process.alive?(o)
    assert_receive {:DOWN, ^wm, :process, ^w, :normal}, @t
    refute_received {:telelem_owner_guard, ^gref, :owner_down, _, _}

    # The branch has handled the guard's exit (the temporary child is gone)
    # and spawned nothing for it.
    await!(fn -> DynamicSupervisor.which_children(pins.guards) == [] end)
    assert DynamicSupervisor.count_children(pins.guards) == @empty
    events = trace!(session, pins.guards)
    :trace.session_destroy(session)
    assert [] = for({:trace, _, :spawn, _, _} = e <- events, do: e)
    assert {:trace, pins.guards, :getting_unlinked, g} in events

    # The tree is the same tree: no branch or root went down.
    for m <- tree_ms, do: refute_received({:DOWN, ^m, _, _, _})
    # (The coordinator `h` closes by itself once its only owner is gone; that
    # is its own decision, not the tree's.)
    assert Tree.resolve(root) == {:ok, pins}

    :ok = :sys.resume(ctx.auth)
    owner_gone!(ctx, o)
    refute_received {:telelem_wire, _, _, {:connect_failed, _, _}}
  end

  @tag :capture_log
  test "branch loss (either branch): the sibling and the root go down; nothing is restarted or replaced" do
    for id <- [:coordinators, :guards] do
      root = tree!()
      {:ok, pins} = Tree.resolve(root)
      lost = Map.fetch!(pins, id)
      sib = Map.fetch!(pins, if(id == :coordinators, do: :guards, else: :coordinators))

      session = :trace.session_create(:serve_tree_branch_loss, self(), [])
      1 = :trace.process(session, root, true, [:procs])
      [rm, lm, sm] = for p <- [root, lost, sib], do: Process.monitor(p)

      Process.exit(lost, :kill)

      assert_receive {:DOWN, ^lm, :process, ^lost, :killed}, @t
      assert_receive {:DOWN, ^sm, :process, ^sib, :shutdown}, @t
      assert_receive {:DOWN, ^rm, :process, ^root, :shutdown}, @t

      events = trace!(session, root)
      :trace.session_destroy(session)
      assert [] = for({:trace, _, :spawn, _, _} = e <- events, do: e)
      assert {:trace, root, :exit, :shutdown} in events
      assert Tree.resolve(root) == {:error, :root_down}
    end
  end

  # ── coordinator-branch fixture (barrier) ────────────────────────────────

  defp fixture_spec do
    %{
      id: :fixture,
      start: {__MODULE__, :start_fixture, [self()]},
      restart: :temporary,
      shutdown: 30_000,
      type: :worker,
      modules: [__MODULE__]
    }
  end

  @doc false
  # Runs in the branch: its pid is the parent.
  def start_fixture(test), do: :proc_lib.start_link(__MODULE__, :fixture_init, [test, self()])

  @doc false
  def fixture_init(test, parent) do
    Process.flag(:trap_exit, true)
    :proc_lib.init_ack({:ok, self()})
    fixture_loop(test, parent, [])
  end

  # On its parent's shutdown signal: report what is alive, then hold the
  # shutdown until released.
  defp fixture_loop(test, parent, watched) do
    receive do
      {:watch, pids} ->
        fixture_loop(test, parent, pids)

      {:EXIT, ^parent, reason} ->
        send(test, {:fixture_shutdown, self(), reason, Enum.map(watched, &Process.alive?/1)})
        receive do: (:release -> exit(reason))
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  defp branch(id) do
    %{
      id: id,
      start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
      restart: :permanent,
      type: :supervisor,
      shutdown: :infinity,
      modules: [DynamicSupervisor]
    }
  end

  defp tree!(session \\ nil), do: held!(fn -> Tree.start_link([]) end, session)

  # Started by a holder process (its parent), optionally traced from before
  # the start.
  defp held!(start, session \\ nil) do
    test = self()

    holder =
      spawn(fn ->
        receive do: (:go -> :ok)
        {:ok, pid} = start.()
        send(test, {:held, self(), pid})
        receive do: (:never -> :ok)
      end)

    if session, do: 1 = :trace.process(session, holder, true, [:procs, :set_on_spawn])
    send(holder, :go)
    assert_receive {:held, ^holder, pid}, @t

    on_exit(fn ->
      # Orderly when possible (quiet logs); killed otherwise.
      try do
        if Process.alive?(pid), do: Supervisor.stop(pid, :shutdown, 5_000)
      catch
        :exit, _ -> Process.exit(pid, :kill)
      end

      Process.exit(holder, :kill)
    end)

    pid
  end

  # Runs resolve/2 in a fresh process traced for sends; returns its result
  # and every message it sent, as {to, msg}.
  defp traced_resolve(root, before \\ fn -> :ok end, timeout \\ 5_000) do
    test = self()

    p =
      spawn(fn ->
        receive do: (:go -> :ok)
        send(test, {:resolved, self(), Tree.resolve(root, timeout)})
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(p, :kill) end)
    session = :trace.session_create(:serve_tree_resolve, self(), [])
    1 = :trace.process(session, p, true, [:send])
    before.()
    send(p, :go)
    assert_receive {:resolved, ^p, res}, @t + timeout
    events = trace!(session, p)
    :trace.session_destroy(session)
    {res, for({:trace, ^p, :send, msg, to} <- events, do: {to, msg})}
  end

  # A process with the tree's initial call that answers which_children as
  # told by `answer/2`.
  defp forged! do
    test = self()

    pid =
      spawn(fn ->
        Process.put(:"$initial_call", {:supervisor, Tree, 1})
        send(test, {:forged, self()})
        forged_loop(:no_reply)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    assert_receive {:forged, ^pid}, @t
    pid
  end

  defp answer(forged, reply) do
    fn ->
      ref = make_ref()
      send(forged, {:answer, self(), ref, reply})
      assert_receive {:answering, ^ref}, @t
    end
  end

  defp forged_loop(reply) do
    receive do
      {:answer, from, ref, next} ->
        send(from, {:answering, ref})
        forged_loop(next)

      {:"$gen_call", from, :which_children} ->
        case reply do
          :no_reply -> :ok
          :exit -> exit(:boom)
          snap -> GenServer.reply(from, snap)
        end

        forged_loop(reply)
    end
  end

  # NEW_PID_EXT for a node that does not exist.
  defp remote_pid do
    n = "telelem-nowhere@127.0.0.1"
    :erlang.binary_to_term(<<131, 88, 119, byte_size(n)>> <> n <> <<1::32, 0::32, 1::32>>)
  end

  # The requester and everything it spawns: calls to start_child/2.
  defp calls_traced!(name, pid) do
    Code.ensure_loaded!(DynamicSupervisor)
    session = :trace.session_create(name, self(), [])
    1 = :trace.process(session, pid, true, [:call, :set_on_spawn])
    assert :trace.function(session, {DynamicSupervisor, :start_child, 2}, true, []) > 0
    session
  end

  defp start_child_calls!(session) do
    for {:trace, _, :call, {DynamicSupervisor, :start_child, [sup, spec]}} <-
          trace!(session, :all),
        do: {sup, spec}
  end

  # Everything traced for `tracee` (or `:all`) so far, in arrival order.
  defp trace!(session, tracee) do
    ref = :trace.delivered(session, tracee)
    assert_receive {:trace_delivered, ^tracee, ^ref}, @t
    drain_trace([])
  end

  defp drain_trace(acc) do
    receive do
      m when is_tuple(m) and tuple_size(m) >= 4 and elem(m, 0) == :trace ->
        drain_trace([m | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp trusted(ctx, request_id, d) do
    opts = [
      admission_authority: ctx.auth,
      terminal_deadline: d,
      coordinator_grant: {:one_primary, @units},
      receipts: [
        store: ctx.store,
        producer: Store.producer(ctx.store),
        store_mode: :sandbox,
        meta: %{
          tenant_id: @tenant,
          request_id: request_id,
          route_rev: 1,
          deployment_id: "a",
          mode: :sandbox
        }
      ],
      cleanup_timeout: 60_000,
      register_timeout: 60_000
    ]

    assert Keyword.keys(opts) -- CoordinatorStart.trusted_keys() == []
    opts
  end

  defp gopts(ctx, h, d, requester, extra \\ []) do
    Keyword.merge(
      [
        upstream: [
          handoff: h,
          id: :primary,
          controller: self(),
          body: ~s({"marker":"#{@secret}"}),
          limits: Limits.new(),
          gate: [target: ctx.target, deadline: d]
        ],
        requester: requester,
        observer: self(),
        grace_ms: 50,
        activation_ms: 5_000
      ],
      extra
    )
  end

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

  defp req_call!(%{req: req}, fun) do
    ref = make_ref()
    send(req, {:do, ref, fun})
    assert_receive {:req_did, ^ref, result}, @t
    result
  end

  defp await_reserve!(auth, h, o) do
    await!(fn ->
      {:messages, msgs} = Process.info(auth, :messages)
      Enum.any?(msgs, &match?({:"$gen_call", {^h, _}, {:reserve, _, ^o, @units}}, &1))
    end)
  end

  defp owner_gone!(%{tag: tag}, o) do
    assert_receive {:telelem_admission, ^tag, {:owner_down, ^o, [{_gid, {:ok, _}}]}}, @t
  end

  defp await!(pred, until \\ nil) do
    until = until || now() + @t

    cond do
      pred.() -> :ok
      now() >= until -> flunk("condition not reached")
      true -> await!(pred, until)
    end
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
