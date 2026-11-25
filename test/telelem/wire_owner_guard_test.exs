defmodule Telelem.WireOwnerGuardTest do
  @moduledoc """
  TEL-012 `Telelem.Wire.OwnerGuard`, listener-free.

  No socket, listener, TLS, real `Upstream`, `Handoff` or authority is used.
  The pinned "Handoff" is a plain fake process; the supervisor is a real
  `DynamicSupervisor` per test (or, for caller-side start failures, a fake
  GenServer that answers `DynamicSupervisor.start_child/2`'s
  `{:start_child, _}` call).

  What this covers: the closed option interface and its limits (refused in
  the caller, nothing started), the metadata-only child entry (any other shape
  refused before init ack), the temporary finite-shutdown child spec, startup
  shape (trapping guard, non-trapping inert owner linked and monitored only by
  the guard), one-shot activation after the start return and its bounded wait
  on both sides, refusal before activation (timeout, cutoff, caller, Handoff,
  supervisor), cancellation while active (caller `:DOWN`, Handoff `:DOWN`,
  supervisor shutdown and kill), natural owner exit, the absolute cutoff,
  per-turn bound checks with a busy mailbox, the unconfirmed kill-to-DOWN
  path, non-exact signals being ignored, reports only after the owner's
  `:DOWN`, and no restart.

  Ordering uses `:erlang.suspend_process/1` on the guard and the mailbox
  read back with `Process.info/2` (ordered after the test's own earlier
  signals), re-read until the expected state is there, bounded. The only
  clock waits are for a cutoff instant to pass.

  What it does not cover: socket/TLS teardown, successor ordering, a slow
  kill-to-DOWN in the runtime itself, a supervisor that never answers.

      scripts/mix test test/telelem/wire_owner_guard_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  alias Telelem.Wire.OwnerGuard

  @tag_ :telelem_owner_guard
  @junk 300_000

  defmodule FakeSupervisor do
    @moduledoc false
    use GenServer

    def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

    @impl true
    def init(arg), do: {:ok, arg}

    @impl true
    def handle_call({:start_child, _child}, _from, {test, :silent_child} = s) do
      child =
        spawn(fn ->
          receive do
            :never -> :ok
          end
        end)

      send(test, {:fake_child, child})
      {:reply, {:ok, child}, s}
    end

    def handle_call({:start_child, _child}, _from, {_test, {:reply, reply}} = s),
      do: {:reply, reply, s}
  end

  setup do
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :guard_sup)
    h = fake()
    on_exit(fn -> Process.exit(h, :kill) end)
    %{sup: sup, h: h}
  end

  # ── Options and limits ───────────────────────────────────────────────────

  test "the closed option interface is refused in the caller; nothing starts", %{sup: sup, h: h} do
    good = opts(h)

    bad = [
      {:not_a_list, "keyword list"},
      {[{"handoff", h}], "keyword list"},
      {good ++ [grace_ms: 5], "at most once"},
      {good ++ [extra: 1], ":extra is not accepted"},
      {Keyword.delete(good, :cutoff), ":cutoff is required"},
      {Keyword.delete(good, :activation_ms), ":activation_ms is required"},
      {Keyword.put(good, :handoff, :name), ":handoff must be a local pid"},
      {Keyword.put(good, :grace_ms, 0), ":grace_ms must be an integer"},
      {Keyword.put(good, :grace_ms, OwnerGuard.bounds().grace_ms + 1), ":grace_ms"},
      {Keyword.put(good, :grace_ms, 1.5), ":grace_ms"},
      {Keyword.put(good, :activation_ms, 0), ":activation_ms must be an integer"},
      {Keyword.put(good, :activation_ms, OwnerGuard.bounds().activation_ms + 1),
       ":activation_ms"},
      {Keyword.put(good, :cutoff, :soon), ":cutoff must be an integer"},
      {Keyword.put(good, :cutoff, now() + 4_294_967_295), ":cutoff plus :grace_ms"}
    ]

    for {o, msg} <- bad do
      e = assert_raise ArgumentError, fn -> OwnerGuard.start(sup, o) end
      assert e.message =~ msg
    end

    # Messages name the field, never its value.
    e =
      assert_raise ArgumentError, fn ->
        OwnerGuard.start(sup, Keyword.put(good, :grace_ms, 987_654_321))
      end

    refute e.message =~ "987654321"
    refute e.message =~ "987_654_321"

    assert DynamicSupervisor.which_children(sup) == []
    assert {:messages, []} = Process.info(self(), :messages)
  end

  test "a passed cutoff starts nothing", %{sup: sup, h: h} do
    assert {:error, :cutoff} = OwnerGuard.start(sup, opts(h, cutoff: now()))
    assert {:error, :cutoff} = OwnerGuard.start(sup, opts(h, cutoff: now() - 10))
    assert DynamicSupervisor.which_children(sup) == []
  end

  test "child spec: temporary with a finite shutdown of grace + slack" do
    spec = OwnerGuard.child_spec(meta(self(), self(), grace_ms: 250))
    assert spec.restart == :temporary
    assert spec.shutdown == 250 + OwnerGuard.bounds().shutdown_slack_ms
    assert {OwnerGuard, :start_link, [%{grace_ms: 250}]} = spec.start

    # An invalid shape still gets a finite shutdown (init refuses it anyway).
    assert OwnerGuard.child_spec(:junk).shutdown == OwnerGuard.bounds().shutdown_slack_ms
  end

  test "the child entry accepts only the exact metadata shape", %{sup: sup, h: h} do
    m = meta(self(), h)

    bad = [
      Map.put(m, :extra, 1),
      Map.delete(m, :activation_until),
      %{m | caller: fn -> :ok end},
      %{m | handoff: {:via, :x, :y}},
      %{m | ref: :not_a_ref},
      %{m | grace_ms: 0},
      %{m | grace_ms: OwnerGuard.bounds().grace_ms + 1},
      %{m | activation_until: m.cutoff + 1},
      %{m | cutoff: now() + 4_294_967_295},
      Map.to_list(m),
      {OwnerGuard, :inert, []}
    ]

    for b <- bad do
      assert {:error, :invalid_metadata} =
               DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(b))
    end

    assert DynamicSupervisor.which_children(sup) == []
    refute_received {@tag_, _, _, _}
  end

  # ── Startup ──────────────────────────────────────────────────────────────

  test "startup: trapping guard, fixed inert non-trapping owner, linked and monitored",
       %{sup: sup, h: h} do
    assert {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h))
    assert is_reference(r)

    assert [{:undefined, ^g, :worker, [OwnerGuard]}] = DynamicSupervisor.which_children(sup)
    assert {:trap_exit, true} = Process.info(g, :trap_exit)
    assert {:trap_exit, false} = Process.info(o, :trap_exit)
    assert {:initial_call, {OwnerGuard, :inert, 0}} = Process.info(o, :initial_call)

    {:links, glinks} = Process.info(g, :links)
    assert Enum.sort(glinks) == Enum.sort([sup, o])
    assert {:links, [^g]} = Process.info(o, :links)
    assert {:monitored_by, [^g]} = Process.info(o, :monitored_by)

    {:monitors, mons} = Process.info(g, :monitors)
    assert Enum.sort(for {:process, p} <- mons, do: p) == Enum.sort([self(), h, o])

    # Inert: messages are discarded; a :normal exit signal is ignored.
    send(o, :hello)
    Process.exit(o, :normal)
    assert Process.alive?(o)
    eventually(fn -> Process.info(o, :message_queue_len) == {:message_queue_len, 0} end)

    assert {:messages, []} = Process.info(self(), :messages)
  end

  test "activation is one-shot, exact-ref and spawns the owner only then", %{sup: sup, h: h} do
    m = meta(self(), h)
    {:ok, g} = DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(m))
    ref = m.ref

    # Started, not active: no owner exists.
    assert {:links, [^sup]} = Process.info(g, :links)

    send(g, {@tag_, make_ref(), :activate})
    send(g, {@tag_, ref, :activate})
    assert_receive {@tag_, ^ref, :active, o}
    assert {:links, links} = Process.info(g, :links)
    assert Enum.sort(links) == Enum.sort([sup, o])

    send(g, {@tag_, ref, :activate})
    eventually(fn -> Process.info(g, :message_queue_len) == {:message_queue_len, 0} end)
    assert {:links, ^links} = Process.info(g, :links)
    refute_received {@tag_, _, :active, _}

    # Still one owner and one report.
    send(h, :stop)
    assert_receive {@tag_, ^ref, :owner_down, :handoff_down}
    refute Process.alive?(o)
    refute_received {@tag_, _, _, _}
  end

  test "guard-side activation wait is bounded; no owner is spawned", %{sup: sup, h: h} do
    t0 = now()
    m = meta(self(), h, activation_until: t0 + 50)
    {:ok, g} = DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(m))
    gmon = Process.monitor(g)
    ref = m.ref
    assert {:links, [^sup]} = Process.info(g, :links)

    assert_receive {@tag_, ^ref, :refused, :activation_timeout}, 2_000
    assert_receive {:DOWN, ^gmon, :process, ^g, {:shutdown, :activation_timeout}}
    assert now() >= t0 + 50
    assert DynamicSupervisor.which_children(sup) == []
    assert Process.alive?(h)

    # A late activation reaches nobody.
    send(g, {@tag_, ref, :activate})
    refute_received {@tag_, ^ref, :active, _}
  end

  test "activation bounded by the cutoff refuses with :cutoff", %{sup: sup, h: h} do
    c = now() + 50
    m = meta(self(), h, cutoff: c, activation_until: c)
    {:ok, g} = DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(m))
    gmon = Process.monitor(g)
    ref = m.ref
    assert_receive {@tag_, ^ref, :refused, :cutoff}, 2_000
    assert_receive {:DOWN, ^gmon, :process, ^g, {:shutdown, :cutoff}}
  end

  test "caller-side activation wait is bounded; the silent child is killed" do
    fsup = start_supervised!({FakeSupervisor, {self(), :silent_child}})
    h = fake()
    t0 = now()

    assert {:error, :activation_timeout} =
             OwnerGuard.start(fsup, opts(h, activation_ms: 50, grace_ms: 200))

    assert now() >= t0 + 50
    assert_received {:fake_child, child}
    refute Process.alive?(child)
    # The activation message went to the exact returned pid, nothing else did.
    refute_received {@tag_, _, _, _}
    assert Process.alive?(h)
  end

  test "caller-side start failures map to :start_failed" do
    h = fake()

    for reply <- [{:error, :boom}, :ignore] do
      fsup = start_supervised!({FakeSupervisor, {self(), {:reply, reply}}}, id: make_ref())
      assert {:error, :start_failed} = OwnerGuard.start(fsup, opts(h))
    end

    dead = fake()
    Process.exit(dead, :kill)
    refute Process.alive?(dead)
    assert {:error, :start_failed} = OwnerGuard.start(dead, opts(h))
  end

  # ── Cancellation before activation ───────────────────────────────────────

  test "a Handoff already down refuses the start", %{sup: sup} do
    h = fake()
    Process.exit(h, :kill)
    refute Process.alive?(h)
    assert {:error, :handoff_down} = OwnerGuard.start(sup, opts(h))
    eventually(fn -> DynamicSupervisor.which_children(sup) == [] end)
  end

  test "caller down before activation ends the guard without an owner", %{sup: sup, h: h} do
    caller = fake()
    m = meta(caller, h)
    {:ok, g} = DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(m))
    gmon = Process.monitor(g)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^gmon, :process, ^g, {:shutdown, :caller_down}}
    assert Process.alive?(h)
  end

  test "supervisor shutdown before activation refuses with :parent_exit", %{h: h} do
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :pre_sup)
    m = meta(self(), h)
    {:ok, g} = DynamicSupervisor.start_child(sup, OwnerGuard.child_spec(m))
    gmon = Process.monitor(g)
    ref = m.ref
    :ok = stop_supervised(:pre_sup)
    assert_receive {@tag_, ^ref, :refused, :parent_exit}
    assert_receive {:DOWN, ^gmon, :process, ^g, :shutdown}
  end

  # ── Active: natural exit, cancellation, cutoff ───────────────────────────

  test "natural owner exit is reported after its :DOWN; no restart", %{sup: sup, h: h} do
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h))
    gmon = Process.monitor(g)

    Process.exit(o, :boom)
    assert_receive {@tag_, ^r, :owner_down, :owner_exited}
    refute Process.alive?(o)
    assert_receive {:DOWN, ^gmon, :process, ^g, :normal}
    assert DynamicSupervisor.which_children(sup) == []
    assert Process.alive?(h)
    refute_received {@tag_, _, _, _}
  end

  test "Handoff :DOWN forces the owner; report only after its :DOWN", %{sup: sup, h: h} do
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h))
    gmon = Process.monitor(g)
    omon = Process.monitor(o)

    send(h, :stop)
    assert_receive {@tag_, ^r, :owner_down, :handoff_down}
    refute Process.alive?(o)
    assert_receive {:DOWN, ^omon, :process, ^o, :killed}
    assert_receive {:DOWN, ^gmon, :process, ^g, :normal}
    assert DynamicSupervisor.which_children(sup) == []
  end

  test "caller :DOWN forces the owner and signals nothing else", %{sup: sup, h: h} do
    test = self()

    caller =
      spawn(fn ->
        send(test, {:started, OwnerGuard.start(sup, opts(h))})

        receive do
          :never -> :ok
        end
      end)

    assert_receive {:started, {:ok, %{guard: g, owner: o}}}
    gmon = Process.monitor(g)
    omon = Process.monitor(o)

    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^omon, :process, ^o, :killed}
    assert_receive {:DOWN, ^gmon, :process, ^g, :normal}
    assert Process.alive?(h)
    assert Process.alive?(sup)
  end

  test "supervisor shutdown: owner forced, reported, guard exits :shutdown in time", %{h: h} do
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one}, id: :act_sup)
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h, grace_ms: 500))
    gmon = Process.monitor(g)
    omon = Process.monitor(o)

    t0 = now()
    :ok = stop_supervised(:act_sup)
    # Well inside the declared finite shutdown (grace + slack), no supervisor kill.
    assert now() - t0 < 500 + OwnerGuard.bounds().shutdown_slack_ms

    assert_receive {@tag_, ^r, :owner_down, :parent_exit}
    assert_receive {:DOWN, ^gmon, :process, ^g, :shutdown}
    assert_receive {:DOWN, ^omon, :process, ^o, :killed}
    assert Process.alive?(h)
  end

  @tag :capture_log
  test "supervisor killed: owner forced, reported, guard exits with its reason", %{h: h} do
    {:ok, sup} = DynamicSupervisor.start_link(strategy: :one_for_one)
    Process.unlink(sup)
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h))
    gmon = Process.monitor(g)

    Process.exit(sup, :kill)
    assert_receive {@tag_, ^r, :owner_down, :parent_exit}
    refute Process.alive?(o)
    assert_receive {:DOWN, ^gmon, :process, ^g, :killed}
  end

  test "non-exact EXIT and DOWN are ignored; a forged parent EXIT only ends early",
       %{sup: sup} do
    test = self()

    h =
      spawn(fn ->
        receive do
          {:go, g} ->
            Process.exit(g, :shutdown)
            Process.exit(g, :normal)
            send(g, {:EXIT, test, :shutdown})
            send(g, {:DOWN, make_ref(), :process, test, :forged})
            send(g, {:DOWN, make_ref(), :process, self(), :forged})
            send(g, {@tag_, make_ref(), :activate})
            # Then exit for real: signals from one sender keep their order.
        end
      end)

    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h))
    send(h, {:go, g})
    assert_receive {@tag_, ^r, :owner_down, :handoff_down}
    refute Process.alive?(o)

    # Documented: `{:EXIT, parent, _}` as a plain message cannot be told apart.
    h2 = fake()
    {:ok, %{guard: g2, owner: o2, ref: r2}} = OwnerGuard.start(sup, opts(h2))
    gmon2 = Process.monitor(g2)
    send(g2, {:EXIT, sup, :shutdown})
    assert_receive {@tag_, ^r2, :owner_down, :parent_exit}
    refute Process.alive?(o2)
    assert_receive {:DOWN, ^gmon2, :process, ^g2, :shutdown}
    assert Process.alive?(sup)
    assert Process.alive?(h2)
  end

  test "the absolute cutoff forces the owner", %{sup: sup, h: h} do
    c = now() + 100
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h, cutoff: c))
    gmon = Process.monitor(g)
    omon = Process.monitor(o)

    assert_receive {:DOWN, ^omon, :process, ^o, :killed}, 2_000
    assert now() >= c
    assert_receive {@tag_, ^r, :owner_down, :cutoff}
    assert now() <= c + 1_000
    assert_receive {:DOWN, ^gmon, :process, ^g, :normal}
    assert Process.alive?(h)
  end

  # ── Busy mailbox and the grace bound ─────────────────────────────────────

  test "busy mailbox: the cutoff is checked at every turn, not after the backlog",
       %{sup: sup, h: h} do
    c = now() + 50

    {:ok, %{guard: g, owner: o, ref: r}} =
      OwnerGuard.start(sup, opts(h, cutoff: c, grace_ms: 10_000))

    omon = Process.monitor(o)

    :erlang.suspend_process(g)
    for _ <- 1..@junk, do: send(g, :junk)
    wait_until(c + 1)
    :erlang.resume_process(g)

    assert_receive {:DOWN, ^omon, :process, ^o, :killed}, 5_000
    {:message_queue_len, left} = Process.info(g, :message_queue_len)
    assert left > 0, "owner was killed only after the backlog was drained"

    assert_receive {@tag_, ^r, :owner_down, :cutoff}, 10_000
  end

  test "kill-to-DOWN past the grace: no report, unconfirmed exit", %{sup: sup, h: h} do
    {:ok, %{guard: g, owner: o, ref: r}} = OwnerGuard.start(sup, opts(h, grace_ms: 1))
    gmon = Process.monitor(g)
    omon = Process.monitor(o)

    :erlang.suspend_process(g)
    send(h, :stop)
    eventually(fn -> queued?(g, &match?({:DOWN, _, :process, ^h, _}, &1)) end)
    # The owner's :DOWN will queue behind this backlog.
    for _ <- 1..@junk, do: send(g, :junk)
    :erlang.resume_process(g)

    assert_receive {:DOWN, ^omon, :process, ^o, :killed}, 5_000
    assert_receive {:DOWN, ^gmon, :process, ^g, {:shutdown, :owner_down_unconfirmed}}, 10_000
    refute_received {@tag_, ^r, _, _}
  end

  test "a parent EXIT while waiting keeps the cause and sets the exit reason",
       %{sup: sup, h: h} do
    {:ok, %{guard: g, ref: r}} = OwnerGuard.start(sup, opts(h))
    gmon = Process.monitor(g)

    :erlang.suspend_process(g)
    send(h, :stop)
    eventually(fn -> queued?(g, &match?({:DOWN, _, :process, ^h, _}, &1)) end)
    send(g, {:EXIT, sup, :shutdown})
    :erlang.resume_process(g)

    assert_receive {@tag_, ^r, :owner_down, :handoff_down}
    assert_receive {:DOWN, ^gmon, :process, ^g, :shutdown}
  end

  # ── Helpers ──────────────────────────────────────────────────────────────

  defp now, do: System.monotonic_time(:millisecond)

  defp fake do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  defp opts(h, kw \\ []) do
    Keyword.merge(
      [handoff: h, cutoff: now() + 60_000, grace_ms: 1_000, activation_ms: 5_000],
      kw
    )
  end

  defp meta(caller, h, kw \\ []) do
    t = now()

    Map.merge(
      %{
        caller: caller,
        ref: make_ref(),
        handoff: h,
        cutoff: t + 60_000,
        grace_ms: 1_000,
        activation_until: t + 5_000
      },
      Map.new(kw)
    )
  end

  defp queued?(pid, pred) do
    {:messages, ms} = Process.info(pid, :messages)
    Enum.any?(ms, pred)
  end

  defp eventually(fun, bound \\ 2_000), do: eventually(fun, now() + bound, :go)

  defp eventually(fun, until, :go) do
    cond do
      fun.() ->
        :ok

      now() > until ->
        flunk("condition not reached in time")

      true ->
        :erlang.yield()
        eventually(fun, until, :go)
    end
  end

  # Waits for a clock instant (not a synchronisation point).
  defp wait_until(t) do
    receive do
    after
      max(t - now(), 0) -> :ok
    end
  end
end
