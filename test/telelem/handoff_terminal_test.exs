defmodule Telelem.HandoffTerminalTest do
  @moduledoc """
  Terminal authorization against the pinned authority, and upstream completion
  vs. confirmed downstream write (M2a day two).

  Independent finding reproduced: authority death between the winner's
  `transfer([DONE])` and `finish` left the marker queued to the requester with
  outcome `incomplete`. Now the marker travels only inside `finish/3`, which
  checks the pinned authority itself and hands the marker over in the same
  serialized step. Races are forced by suspending the coordinator and queueing
  the call and the `:DOWN` in a known mailbox order; the only wait is a bounded
  spin on `message_queue_len`.

      scripts/mix test test/telelem/handoff_terminal_test.exs
  """
  use ExUnit.Case, async: false

  alias Telelem.Handoff

  @t 1_000
  @marker "data: [DONE]\n\n"

  defp spawn_tracked(fun) do
    pid = spawn(fun)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # Runs closures on request and forwards every handoff message as {tag, msg}.
  defp actor(tag) do
    test = self()

    spawn_tracked(fn ->
      Stream.repeatedly(fn ->
        receive do
          {:call, f} -> send(test, {:res, tag, f.()})
          {:telelem_handoff, _, m} -> send(test, {tag, m})
        end
      end)
      |> Stream.run()
    end)
  end

  defp run(pid, f), do: send(pid, {:call, f})

  defp run!(c, tag, f) do
    run(c.p[tag], f)
    assert_receive {:res, ^tag, r}, @t
    r
  end

  defp start(opts \\ []) do
    req = actor(:req)
    obs = actor(:obs)
    auth = actor(:auth)

    {:ok, h} =
      Handoff.start(
        [requester: req, attempts: [:a, :b], authority: auth, observer: obs, trace: self()] ++
          opts
      )

    on_exit(fn -> Process.exit(h, :kill) end)
    hm = Process.monitor(h)
    c = %{h: h, hm: hm, auth: auth, p: %{req: req, obs: obs, a: actor(:a), b: actor(:b)}}

    for id <- [:a, :b], do: assert(run!(c, id, fn -> Handoff.attach(h, id) |> elem(0) end) == :ok)
    assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == :release
    assert run!(c, :b, fn -> Handoff.ready(h, :b) end) == {:cancel, {:lost_to, :a}}
    assert run!(c, :b, fn -> Handoff.cleaned(h, :b, :possible) end) == :ok
    assert run!(c, :a, fn -> Handoff.transfer(h, "c1") end) == :ok
    c
  end

  defp wait_q(h, n, until \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      elem(Process.info(h, :message_queue_len), 1) >= n -> :ok
      System.monotonic_time(:millisecond) > until -> flunk("queue never reached #{n}")
      true -> wait_q(h, n, until)
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

  defp outcomes(evs), do: for({k, _} = e <- evs, k in [:done, :incomplete, :failed], do: e)

  # ── fence vs. terminal authorization: both mailbox orders, both modes ──

  for ack <- [false, true], order <- [:down_behind_finish, :down_ahead_of_finish] do
    @ack ack
    @order order
    test "authority :DOWN #{order} (terminal_ack: #{ack}): refused, marker never handed out, incomplete" do
      c = start(terminal_ack: @ack)
      :sys.suspend(c.h)

      if @order == :down_ahead_of_finish do
        kill(c.auth)
        wait_q(c.h, 1)
        run(c.p.a, fn -> Handoff.finish(c.h, {:upstream_done, "stop"}, @marker) end)
      else
        run(c.p.a, fn -> Handoff.finish(c.h, {:upstream_done, "stop"}, @marker) end)
        wait_q(c.h, 1)
        kill(c.auth)
      end

      wait_q(c.h, 2)
      :sys.resume(c.h)
      assert_receive {:res, :a, {:error, :cancelled}}, @t
      assert_receive {:a, {:cancel, {:authority_down, :killed}}}, @t
      assert run!(c, :a, fn -> Handoff.cleaned(c.h, :a, :possible) end) == :ok
      assert_receive {:DOWN, _, _, _, :normal}, @t

      reason = {:authority_down, :killed}

      assert [{:elected, :a}, {:data, "c1"}, {:incomplete, ^reason}, {:closed, r}] = drain(:req)
      assert r.authority == :lost and r.committed and r.terminal == nil
      obs = drain(:obs)
      refute Enum.any?(obs, &match?({:upstream_complete, _}, &1))
      assert {:outcome, {:incomplete, reason}} in obs
      refute Enum.any?(obs, &match?({:outcome, {:done, _}}, &1))
    end
  end

  # ── ack mode: upstream complete is not done ──

  test "authority dies after authorization, before the write result: done only after the confirmed write" do
    c = start(terminal_ack: true)
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, {:upstream_done, "stop"}, @marker) end) == :ok
    assert_receive {:req, {:terminal, @marker}}, @t
    assert_receive {:telelem_handoff_trace, ^h, {:terminal_authorized, :a, _}}, @t

    st = Handoff.status(h)
    assert st.outcome == nil
    assert st.terminal == %{upstream: :complete, downstream: :unconfirmed}

    # Fence after authorization: reported, but the authorized terminal stands.
    kill(c.auth)
    assert_receive {:telelem_handoff_trace, ^h, {:authority_down, :committed, :killed}}, @t
    # Winner cleanup after authorization does not end the stream either.
    assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
    assert Handoff.status(h).outcome == nil
    refute_received {:req, {:incomplete, _}}
    refute_received {:a, {:cancel, _}}

    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == :ok
    assert_receive {:DOWN, _, _, _, :normal}, @t

    # ({:terminal, _} was asserted above, before any outcome existed.)
    assert [{:elected, :a}, {:data, "c1"}, {:done, t}, {:closed, r}] = drain(:req)

    assert t == {:upstream_done, "stop"}
    assert r.authority == :lost and r.terminal == %{upstream: :complete, downstream: :written}
    assert r.possible_charges == [:a, :b]

    obs = drain(:obs)
    ix = fn ev -> Enum.find_index(obs, &(&1 == ev)) end
    assert ix.({:upstream_complete, :a}) < ix.({:outcome, {:done, t}})

    assert ix.({:attempt_closed, :a, %{cleanup: :ack, charge: :possible}}) <
             ix.({:outcome, {:done, t}})
  end

  test "downstream write failure after authorization: incomplete, never done" do
    for {how, expect} <- [
          {fn h -> Handoff.terminal_written(h, {:error, :closed}) end,
           {:incomplete, {:downstream_write, :closed}}},
          # The M1 controller's write-failure path (Handoff.cancel) also ends it.
          {fn h -> Handoff.cancel(h, {:downstream_closed, :closed}) end,
           {:incomplete, {:downstream_closed, :closed}}}
        ] do
      c = start(terminal_ack: true)
      h = c.h
      assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
      assert_receive {:req, {:terminal, @marker}}, @t
      assert run!(c, :req, fn -> how.(h) end) == :ok
      assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
      assert_receive {:DOWN, _, _, _, :normal}, @t

      evs = drain(:req)
      assert outcomes(evs) == [expect]
      {:closed, r} = List.last(evs)
      assert r.outcome == expect
      assert r.terminal.upstream == :complete and r.terminal.downstream != :written
      refute Enum.any?(drain(:obs), &match?({:outcome, {:done, _}}, &1))
    end
  end

  test "requester dies holding the marker: incomplete (write result unknown), never done" do
    c = start(terminal_ack: true)
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert_receive {:req, {:terminal, @marker}}, @t
    kill(c.p.req)

    assert_receive {:telelem_handoff_trace, ^h,
                    {:outcome, {:incomplete, {:requester_down, :killed}}}},
                   @t

    assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
    assert_receive {:DOWN, _, _, _, :normal}, @t
    refute Enum.any?(drain(:obs), &match?({:outcome, {:done, _}}, &1))
  end

  @tag :capture_log
  test "coordinator exits while the write result is pending: incomplete, never done" do
    test = self()
    req = actor(:req)

    parent =
      spawn_tracked(fn ->
        {:ok, h} =
          Handoff.start_link(requester: req, attempts: [:a], terminal_ack: true, trace: test)

        send(test, {:h, h})
        receive do: (:never -> :ok)
      end)

    assert_receive {:h, h}, @t
    hm = Process.monitor(h)
    c = %{h: h, p: %{req: req, a: actor(:a)}}
    assert run!(c, :a, fn -> Handoff.attach(h, :a) |> elem(0) end) == :ok
    assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == :release
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    Process.exit(parent, :kill)
    assert_receive {:DOWN, ^hm, _, _, :killed}, @t

    assert [{:elected, :a}, {:terminal, @marker}, {:incomplete, {:handoff_down, :killed}}] =
             drain(:req)
  end

  test "after authorization nothing else is accepted; write result only from the requester, once" do
    c = start(terminal_ack: true)
    h = c.h
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == {:error, :no_terminal}
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    assert run!(c, :a, fn -> Handoff.transfer(h, "late") end) == {:error, :cancelled}
    assert run!(c, :a, fn -> Handoff.finish(h, :again, @marker) end) == {:error, :cancelled}
    assert run!(c, :a, fn -> Handoff.begin_dispatch(h, :a) end) == {:cancel, :ended}
    assert run!(c, :a, fn -> Handoff.ready(h, :a) end) == {:cancel, :ended}
    assert run!(c, :a, fn -> Handoff.terminal_written(h, :ok) end) == {:error, :not_requester}
    assert Handoff.status(h).outcome == nil
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == :ok
    assert run!(c, :req, fn -> Handoff.terminal_written(h, {:error, :x}) end) == {:error, :ended}
    assert Handoff.status(h).outcome == {:done, :t}
  end

  # ── default (M1) mode ──

  test "default mode: marker and done are adjacent; a later authority death does not rewrite done" do
    c = start()
    h = c.h
    assert run!(c, :a, fn -> Handoff.finish(h, :t, @marker) end) == :ok
    kill(c.auth)
    assert_receive {:telelem_handoff_trace, ^h, {:authority_down, :committed, :killed}}, @t
    assert run!(c, :req, fn -> Handoff.terminal_written(h, :ok) end) == {:error, :no_terminal}
    assert run!(c, :a, fn -> Handoff.cleaned(h, :a, :possible) end) == :ok
    assert_receive {:DOWN, _, _, _, :normal}, @t

    assert [{:elected, :a}, {:data, "c1"}, {:data, @marker}, {:done, :t}, {:closed, r}] =
             drain(:req)

    assert r.authority == :lost and r.terminal == %{upstream: :complete, downstream: :not_tracked}
    refute Enum.any?(drain(:obs), &match?({:upstream_complete, _}, &1))
  end
end
