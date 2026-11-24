defmodule Telelem.WireUpstreamTest do
  # TEL-005 transport-logic tests for Telelem.Wire.Upstream over real loopback
  # TLS sockets: Mint (attempt) -> Bandit (an in-BEAM upstream scripted by the test).
  # The test process is the Handoff requester and grants credit by hand.
  #
  # These are NOT the M1 wire evidence (no independent fixture, no SDK): they pin
  # the reader's credit/arming, bounds, partial-frame and ownership rules offline.
  # The M1 evidence is test/telelem/wire_m1_test.exs via integration/m1/run.
  use ExUnit.Case, async: true

  alias Telelem.Handoff
  alias Telelem.Wire.{Limits, Upstream}

  @tag_h :telelem_handoff

  defmodule ScriptedUpstream do
    @moduledoc false
    import Plug.Conn

    def init(test_pid), do: test_pid

    def call(conn, test_pid) do
      {:ok, body, conn} = read_body(conn)
      send(test_pid, {:upstream_request, self(), body})

      receive do
        {:respond, :sse} ->
          conn
          |> put_resp_content_type("text/event-stream")
          |> send_chunked(200)
          |> serve(test_pid)

        {:respond, code} when is_integer(code) ->
          send_resp(conn, code, ~s({"error":{"message":"scripted"}}))
      end
    end

    defp serve(conn, test_pid) do
      receive do
        {:send, bin} ->
          {:ok, conn} = chunk(conn, bin)
          send(test_pid, {:sent, byte_size(bin)})
          serve(conn, test_pid)

        :finish ->
          conn

        :abort ->
          # Bandit closes the socket without the chunked terminator.
          raise "scripted abort"
      end
    end
  end

  @san {:Extension, {2, 5, 29, 17}, false, [dNSName: ~c"localhost", iPAddress: <<127, 0, 0, 1>>]}

  setup do
    opts = [digest: :sha256, key: {:namedCurve, :secp256r1}]

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{
        server_chain: %{root: opts, peer: [{:extensions, [@san]} | opts]},
        client_chain: %{root: opts, peer: opts}
      })

    bandit =
      start_supervised!(
        {Bandit,
         plug: {ScriptedUpstream, self()},
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false,
         http_options: [compress: false, log_exceptions_with_status_codes: []],
         thousand_island_options: [
           shutdown_timeout: 200,
           transport_options: [cert: server[:cert], key: server[:key], cacerts: server[:cacerts]]
         ]}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(bandit)
    sup = start_supervised!(DynamicSupervisor)
    %{port: port, cacerts: client[:cacerts], sup: sup}
  end

  @small [
    window: 1_024,
    max_event: 512,
    high_water: 2_048,
    hard_cap: 65_536,
    cleanup_timeout_ms: 300
  ]

  defp start(ctx, limits_kw \\ @small, lane_kw \\ []) do
    limits = Limits.new(limits_kw)

    {:ok, h} =
      DynamicSupervisor.start_child(
        ctx.sup,
        {Handoff,
         requester: self(), attempts: [:primary], cleanup_timeout: limits.cleanup_timeout_ms}
      )

    lane =
      Map.merge(
        %{name: "t", host: "127.0.0.1", port: ctx.port, cacerts: ctx.cacerts},
        Map.new(lane_kw)
      )

    {:ok, pid} =
      DynamicSupervisor.start_child(
        ctx.sup,
        {Upstream,
         handoff: h,
         id: :primary,
         controller: self(),
         lane: lane,
         body: ~s({"stream":true}),
         limits: limits}
      )

    %{h: h, pid: pid, limits: limits, mon: Process.monitor(pid)}
  end

  defp upstream! do
    assert_receive {:upstream_request, up, _body}, 5_000
    send(up, {:respond, :sse})
    up
  end

  @stop "data: " <>
          JSON.encode!(%{
            "choices" => [%{"index" => 0, "delta" => %{}, "finish_reason" => "stop"}]
          }) <>
          "\n\n"
  @done "data: [DONE]\n\n"

  defp finish_ev(reason),
    do:
      "data: " <>
        JSON.encode!(%{"choices" => [%{"index" => 0, "delta" => %{}, "finish_reason" => reason}]}) <>
        "\n\n"

  defp event(i, pad \\ 300) do
    payload = %{
      "choices" => [
        %{"index" => 0, "delta" => %{"content" => "#{i}:" <> String.duplicate("x", pad)}}
      ]
    }

    "data: " <> JSON.encode!(payload) <> "\n\n"
  end

  defp push(up, bin) do
    send(up, {:send, bin})
    assert_receive {:sent, _}, 5_000
  end

  defp probe(pid) do
    ref = make_ref()
    send(pid, {:telelem_wire_probe, self(), ref})
    assert_receive {^ref, snap}, 2_000
    snap
  end

  defp data_bytes(h, acc \\ 0) do
    receive do
      {@tag_h, ^h, {:data, c}} -> data_bytes(h, acc + byte_size(c))
    after
      100 -> acc
    end
  end

  defp sockets_to(port) do
    for p <- Port.list(),
        Port.info(p, :name) == {:name, ~c"tcp_inet"},
        match?({:ok, {_, ^port}}, :inet.peername(p)),
        do: p
  end

  defp eventually(fun, ms \\ 2_000) do
    cond do
      fun.() -> true
      ms <= 0 -> false
      true -> Process.sleep(10) && eventually(fun, ms - 10)
    end
  end

  test "credit gates transfer, reader stops arming at high_water and stops reading", ctx do
    a = start(ctx)
    up = upstream!()
    for i <- 1..20, do: push(up, event(i))
    ev_size = byte_size(event(1))

    # No credit returned: what reached the requester is within the window.
    Process.sleep(200)
    got = data_bytes(a.h)
    assert got <= a.limits.window and got >= a.limits.window - ev_size

    s1 = probe(a.pid)
    Process.sleep(200)
    s2 = probe(a.pid)
    assert s1.paused and not s1.armed
    assert s1.retained >= a.limits.high_water and s1.retained <= a.limits.hard_cap
    assert s1.in_flight <= a.limits.window
    assert s2.stats.deliveries == s1.stats.deliveries

    # Return credit: everything flows, in order, and finishes only on [DONE] + end.
    send(a.pid, {:telelem_wire_credit, a.h, 1_000_000})
    push(up, @stop)
    push(up, @done)
    send(up, :finish)
    assert_receive {@tag_h, _, {:done, {:upstream_done, "stop"}}}, 5_000
    assert_receive {@tag_h, _, {:closed, report}}, 5_000
    assert report.attempts.primary.cleanup == :ack
    assert eventually(fn -> sockets_to(ctx.port) == [] end)
  end

  test "cancel while paused for credit closes the TLS socket within the cleanup timeout", ctx do
    a = start(ctx)
    up = upstream!()
    for i <- 1..20, do: push(up, event(i))
    Process.sleep(100)
    assert probe(a.pid).paused
    assert [_] = sockets_to(ctx.port)

    t0 = System.monotonic_time(:millisecond)
    :ok = Handoff.cancel(a.h, :test_cancel)
    assert_receive {@tag_h, _, {:incomplete, :test_cancel}}, 1_000
    assert_receive {@tag_h, _, {:closed, report}}, 1_000
    assert report.attempts.primary.cleanup == :ack
    assert System.monotonic_time(:millisecond) - t0 < a.limits.cleanup_timeout_ms
    assert_receive {:DOWN, _, :process, _, :normal}, 1_000
    assert eventually(fn -> sockets_to(ctx.port) == [] end)
  end

  test "killing the attempt (owner) closes its TLS socket; the coordinator records the death",
       ctx do
    a = start(ctx)
    up = upstream!()
    push(up, event(1))
    assert_receive {@tag_h, _, {:data, _}}, 2_000
    assert [_] = sockets_to(ctx.port)

    Process.exit(a.pid, :kill)
    assert_receive {@tag_h, _, {:incomplete, {:winner_down, :killed}}}, 1_000
    assert_receive {@tag_h, _, {:closed, report}}, 1_000
    assert report.attempts.primary == %{cleanup: {:down, :killed}, charge: :possible, ready: true}
    assert eventually(fn -> sockets_to(ctx.port) == [] end)
  end

  test "a clean HTTP end without [DONE] after commit is incomplete, never done", ctx do
    _a = start(ctx)
    up = upstream!()
    push(up, event(1))
    send(up, :finish)
    assert_receive {@tag_h, _, {:data, _}}, 2_000
    assert_receive {@tag_h, _, {:incomplete, :winner_closed}}, 2_000
    refute_received {@tag_h, _, {:done, _}}
  end

  test "an abrupt close mid-event forwards complete events only and discards the partial frame",
       ctx do
    a = start(ctx)
    up = upstream!()
    full = event(1)
    push(up, full)
    push(up, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tru")
    Process.sleep(50)
    send(up, :abort)

    assert_receive {@tag_h, _, {:data, ^full}}, 2_000
    assert_receive {@tag_h, _, {:incomplete, :winner_closed}}, 2_000
    refute_received {@tag_h, _, {:data, _}}

    assert_receive {:telelem_wire, _, :primary,
                    {:ended, {:upstream_broken, _, %{done_held: false}}, stats}},
                   2_000

    assert stats.partial_discarded > 0
    assert_receive {:DOWN, ref, :process, _, :normal} when ref == a.mon, 1_000
  end

  test "an event larger than max_event aborts (declared overflow action), before commit", ctx do
    _a = start(ctx)
    up = upstream!()
    push(up, event(1, 2_000))
    assert_receive {:telelem_wire, _, :primary, {:ended, {:overflow, {:max_event, _}}, _}}, 2_000
    assert_receive {@tag_h, _, {:failed, _}}, 2_000
    refute_received {@tag_h, _, {:data, _}}
  end

  test "a non-200 upstream status fails before commit; nothing is forwarded", ctx do
    _a = start(ctx)
    assert_receive {:upstream_request, up, _}, 5_000
    send(up, {:respond, 503})

    assert_receive {:telelem_wire, _, :primary,
                    {:ended, {:upstream_status, 503, _}, %{claim: :possible}}},
                   2_000

    assert_receive {@tag_h, _, {:failed, _}}, 2_000
    refute_received {@tag_h, _, {:data, _}}
  end

  @tag :capture_log
  test "an untrusted upstream certificate is an ambiguous connect failure, not a refusal", ctx do
    %{client_config: other} =
      :public_key.pkix_test_data(%{
        server_chain: %{
          root: [key: {:namedCurve, :secp256r1}],
          peer: [key: {:namedCurve, :secp256r1}]
        },
        client_chain: %{
          root: [key: {:namedCurve, :secp256r1}],
          peer: [key: {:namedCurve, :secp256r1}]
        }
      })

    _a = start(ctx, @small, cacerts: other[:cacerts])

    assert_receive {:telelem_wire, _, :primary,
                    {:connect_failed, :ambiguous, {:tls_alert, {:unknown_ca, _}}}},
                   5_000

    refute_received {:upstream_request, _, _}
  end

  test "a refused port is classified :refused with charge :none", ctx do
    {:ok, l} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(l)
    :ok = :gen_tcp.close(l)

    _a = start(ctx, @small, port: port)
    assert_receive {:telelem_wire, _, :primary, {:connect_failed, :refused, :econnrefused}}, 5_000
    assert_receive {@tag_h, _, {:failed, :no_winner}}, 2_000
    assert_receive {@tag_h, _, {:closed, report}}, 2_000
    assert report.attempts.primary.charge == :none
  end

  # ── terminal grammar (offline; the M1 file repeats these through the real SDK) ──

  # Collects everything the requester receives until {:closed, _}.
  defp collect(h, acc \\ []) do
    receive do
      {@tag_h, ^h, {:closed, report}} -> {Enum.reverse(acc), report}
      {@tag_h, ^h, msg} -> collect(h, [msg | acc])
    after
      3_000 -> flunk("no close; got #{inspect(Enum.reverse(acc))}")
    end
  end

  defp run_script(ctx, steps) do
    a = start(ctx)
    up = upstream!()
    send(a.pid, {:telelem_wire_credit, a.h, 1_000_000})

    for step <- steps do
      case step do
        {:sleep, ms} -> Process.sleep(ms)
        bin when is_binary(bin) -> push(up, bin)
        ctl when ctl in [:finish, :abort] -> send(up, ctl)
      end
    end

    {msgs, report} = collect(a.h)
    assert_receive {:telelem_wire, _, :primary, {:ended, reason, stats}}, 2_000
    data = for {:data, d} <- msgs, do: d
    outcome = Enum.find(msgs, &match?({k, _} when k in [:done, :incomplete, :failed], &1))
    %{data: data, outcome: outcome, reason: reason, stats: stats, report: report}
  end

  test "[DONE] is held until the body completes: [DONE] then abort forwards no [DONE]", ctx do
    r = run_script(ctx, [event(1), @stop, @done, {:sleep, 30}, :abort])
    assert {:incomplete, _} = r.outcome
    assert {:upstream_broken, _, %{done_held: true}} = r.reason
    assert List.last(r.data) == @stop
    refute @done in r.data
  end

  test "only exact [DONE] after a supported finish_reason, with a complete body, is done", ctx do
    r = run_script(ctx, [event(1), @stop, @done, :finish])
    assert r.outcome == {:done, {:upstream_done, "stop"}}
    assert List.last(r.data) == @done

    r = run_script(ctx, [event(1), finish_ev("tool_calls"), @done, :finish])
    assert r.outcome == {:done, {:upstream_done, "tool_calls"}}
  end

  test "non-exact sentinels, unsupported finish reasons and missing finish never become done",
       ctx do
    for {steps, reason} <- [
          {[event(1), @stop, "data: [DONE] \n\n", :finish],
           {:protocol_violation, {:bad_sentinel, 15}}},
          {[event(1), @stop, "data:[DONE]x\n\n", :finish],
           {:protocol_violation, {:bad_sentinel, 14}}},
          {[event(1), finish_ev("length"), @done, :finish],
           {:unsupported_finish_reason, "length"}},
          {[event(1), @done, :finish], {:done_without_finish_reason, :no_finish}},
          {[event(1), @stop, @done, event(2), :finish],
           {:protocol_violation, {:event_after_done, :semantic}}},
          {[event(1), "event: x\ndata: {}\n\n", @stop, @done, :finish],
           {:protocol_violation, {:unsupported, 19}}}
        ] do
      r = run_script(ctx, steps)
      assert {:incomplete, _} = r.outcome, inspect(steps)
      assert r.reason == reason
      refute Enum.any?(r.data, &String.starts_with?(&1, "data: [DONE]")), inspect(steps)
      refute Enum.any?(r.data, &String.starts_with?(&1, "data:[DONE]"))
    end
  end

  test "an error event ends the stream: forwarded after content (incomplete), failed before content",
       ctx do
    err = "data: " <> JSON.encode!(%{"error" => %{"message" => "boom"}}) <> "\n\n"

    r = run_script(ctx, [event(1), err, @done, :finish])
    assert {:incomplete, _} = r.outcome
    assert r.reason == {:upstream_error_event, "boom"}
    assert List.last(r.data) == err

    r = run_script(ctx, [err, @done, :finish])
    assert {:failed, _} = r.outcome
    assert r.reason == {:upstream_error_event, "boom"}
    assert r.data == []
  end
end
