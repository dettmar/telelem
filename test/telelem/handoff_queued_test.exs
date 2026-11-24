defmodule Telelem.HandoffQueuedTest do
  @moduledoc """
  Queued-race regressions adapted from the independent TEL-003 review probe
  (adf31c9). Races are forced by suspending the coordinator, queueing the
  competing calls and signals in a known mailbox order, then resuming. The
  only wait is a bounded spin on `message_queue_len`; there are no sleeps.

      scripts/mix test test/telelem/handoff_queued_test.exs
  """
  # Busy spins; keep them off the async pool.
  use ExUnit.Case, async: false

  alias Telelem.Handoff

  @t 1_000

  defp spawn_tracked(fun) do
    pid = spawn(fun)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp fwd(tag) do
    test = self()

    spawn_tracked(fn ->
      Stream.repeatedly(fn ->
        receive do: ({:telelem_handoff, _, m} -> send(test, {tag, m}))
      end)
      |> Stream.run()
    end)
  end

  defp att(id) do
    test = self()

    spawn_tracked(fn ->
      Stream.repeatedly(fn ->
        receive do
          {:call, f} -> send(test, {:res, id, f.()})
          {:telelem_handoff, _, m} -> send(test, {:msg, id, m})
        end
      end)
      |> Stream.run()
    end)
  end

  defp run(pid, f), do: send(pid, {:call, f})

  defp res(id) do
    assert_receive {:res, ^id, r}, @t
    r
  end

  defp start(ids, opts \\ []) do
    req = fwd(:req)
    auth = fwd(:auth)
    {:ok, h} = Handoff.start([requester: req, attempts: ids, authority: auth] ++ opts)
    on_exit(fn -> Process.exit(h, :kill) end)
    hm = Process.monitor(h)
    as = Map.new(ids, &{&1, att(&1)})

    for {id, p} <- as do
      run(p, fn -> Handoff.attach(h, id) |> elem(0) end)
      assert res(id) == :ok
    end

    %{h: h, hm: hm, req: req, auth: auth, as: as}
  end

  defp qlen(h), do: elem(Process.info(h, :message_queue_len), 1)

  defp wait_q(h, n, i \\ 0) do
    cond do
      qlen(h) >= n -> :ok
      i > 50_000_000 -> flunk("queue never reached #{n}")
      true -> wait_q(h, n, i + 1)
    end
  end

  defp kill(p) do
    m = Process.monitor(p)
    Process.exit(p, :kill)
    assert_receive {:DOWN, ^m, _, _, _}, @t
  end

  defp drain(tag, acc \\ []) do
    receive do
      {^tag, {:closed, _} = e} -> Enum.reverse([e | acc])
      {^tag, e} -> drain(tag, [e | acc])
    after
      @t -> Enum.reverse(acc)
    end
  end

  for order <- [[:a, :b], [:b, :a]] do
    @order order
    test "concurrent ready queued #{inspect(order)}: exactly one release, first in mailbox" do
      [x, y] = @order
      c = start([:a, :b])
      :sys.suspend(c.h)
      run(c.as[x], fn -> Handoff.ready(c.h, x) end)
      wait_q(c.h, 1)
      run(c.as[y], fn -> Handoff.ready(c.h, y) end)
      wait_q(c.h, 2)
      :sys.resume(c.h)
      assert res(x) == :release
      assert res(y) == {:cancel, {:lost_to, x}}
    end
  end

  test "winner killed with transfer in flight; loser ready queued after DOWN: no reselection" do
    c = start([:a, :b])
    run(c.as[:a], fn -> Handoff.ready(c.h, :a) end)
    assert res(:a) == :release
    :sys.suspend(c.h)
    run(c.as[:a], fn -> Handoff.transfer(c.h, "frag") end)
    wait_q(c.h, 1)
    kill(c.as[:a])
    wait_q(c.h, 2)
    run(c.as[:b], fn -> Handoff.ready(c.h, :b) end)
    wait_q(c.h, 3)
    :sys.resume(c.h)
    assert res(:b) == {:cancel, {:lost_to, :a}}
    run(c.as[:b], fn -> Handoff.cleaned(c.h, :b, :none) end)
    assert res(:b) == :ok
    assert_receive {:DOWN, _, _, _, :normal}, @t

    assert [{:elected, :a}, {:data, "frag"}, {:incomplete, {:winner_down, :killed}}, {:closed, r}] =
             drain(:req)

    assert r.attempts.a.charge == :possible and r.attempts.b.charge == :possible
  end

  test "authority DOWN queued before first transfer: fails closed, no bytes" do
    c = start([:a, :b])
    run(c.as[:a], fn -> Handoff.ready(c.h, :a) end)
    assert res(:a) == :release
    :sys.suspend(c.h)
    kill(c.auth)
    wait_q(c.h, 1)
    run(c.as[:a], fn -> Handoff.transfer(c.h, "late") end)
    wait_q(c.h, 2)
    :sys.resume(c.h)
    assert res(:a) == {:error, :cancelled}

    for id <- [:a, :b] do
      run(c.as[id], fn -> Handoff.cleaned(c.h, id, :possible) end)
      assert res(id) == :ok
    end

    assert [{:elected, :a}, {:failed, {:authority_down, :killed}}, {:closed, r}] = drain(:req)
    refute r.committed
  end

  test "authority DOWN queued after first transfer: that chunk lands, then the stream ends incomplete" do
    c = start([:a, :b])
    run(c.as[:a], fn -> Handoff.ready(c.h, :a) end)
    assert res(:a) == :release
    :sys.suspend(c.h)
    run(c.as[:a], fn -> Handoff.transfer(c.h, "c1") end)
    wait_q(c.h, 1)
    kill(c.auth)
    wait_q(c.h, 2)
    :sys.resume(c.h)
    assert res(:a) == :ok
    assert_receive {:msg, :a, {:cancel, {:authority_down, :killed}}}, @t
    run(c.as[:a], fn -> Handoff.finish(c.h, :end) end)
    assert res(:a) == {:error, :cancelled}

    for id <- [:a, :b] do
      run(c.as[id], fn -> Handoff.cleaned(c.h, id, :possible) end)
      assert res(id) == :ok
    end

    assert [
             {:elected, :a},
             {:data, "c1"},
             {:incomplete, {:authority_down, :killed}},
             {:closed, r}
           ] =
             drain(:req)

    assert r.possible_charges == [:a, :b]
  end

  test "requester DOWN, both readies and a late attach all queued together" do
    c = start([:a, :b, :c])
    :sys.suspend(c.h)
    kill(c.req)
    wait_q(c.h, 1)
    run(c.as[:a], fn -> Handoff.ready(c.h, :a) end)
    wait_q(c.h, 2)
    run(c.as[:b], fn -> Handoff.ready(c.h, :b) end)
    wait_q(c.h, 3)
    :sys.resume(c.h)
    assert {:cancel, {:requester_down, :killed}} = res(:a)
    assert {:cancel, {:requester_down, :killed}} = res(:b)
  end

  test "cleanup deadline kill queued ahead of an in-flight ack (never dispatched): one close, :ack wins, :none holds" do
    c = start([:a, :b], cleanup_timeout: 100)
    run(c.as[:b], fn -> Handoff.ready(c.h, :b) end)
    assert res(:b) == :release
    :sys.suspend(c.h)
    # The deadline timer for :a lands in the mailbox, then :a's ack.
    wait_q(c.h, 1)
    run(c.as[:a], fn -> Handoff.cleaned(c.h, :a, :none) end)
    wait_q(c.h, 2)
    :sys.resume(c.h)
    st = Handoff.status(c.h)
    assert st.attempts.a.cleanup == :ack
    assert st.attempts.a.charge == :none
  end

  test "re-attach after cancellation returns the cancellation" do
    c = start([:a, :b])
    run(c.as[:b], fn -> Handoff.ready(c.h, :b) end)
    assert res(:b) == :release
    assert_receive {:msg, :a, {:cancel, {:lost_to, :b}}}, @t
    run(c.as[:a], fn -> Handoff.attach(c.h, :a) end)
    assert res(:a) == {:cancel, {:lost_to, :b}}
  end

  test "authority as start_link parent is rejected" do
    test = self()
    req = fwd(:req)

    spawn_tracked(fn ->
      # init/1's {:stop, _} also exits the linked caller.
      Process.flag(:trap_exit, true)
      send(test, {:start, Handoff.start_link(requester: req, attempts: [:a], authority: self())})
    end)

    assert_receive {:start, {:error, {:linked, :authority}}}, @t
  end

  test "attempt as start_link parent cannot attach, ready or transfer" do
    test = self()
    req = fwd(:req)

    spawn_tracked(fn ->
      {:ok, h} = Handoff.start_link(requester: req, attempts: [:a], register_timeout: 50)

      send(
        test,
        {:results, Handoff.attach(h, :a), Handoff.ready(h, :a), Handoff.transfer(h, "x")}
      )

      receive do: (:never -> :ok)
    end)

    assert_receive {:results, {:error, :linked_to_handoff}, {:error, :not_attached},
                    {:error, :not_winner}},
                   @t

    assert [{:failed, :no_winner}, {:closed, _}] = drain(:req)
  end
end
