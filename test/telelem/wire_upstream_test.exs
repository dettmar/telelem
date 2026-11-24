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
    %{port: port, cacerts: client[:cacerts], sup: sup, server: server}
  end

  @small [
    window: 1_024,
    max_event: 512,
    high_water: 2_048,
    hard_cap: 65_536,
    cleanup_timeout_ms: 300
  ]

  defp start(ctx, limits_kw \\ @small, lane_kw \\ [], handoff_kw \\ []) do
    limits = Limits.new(limits_kw)

    {:ok, h} =
      DynamicSupervisor.start_child(
        ctx.sup,
        {Handoff,
         [requester: self(), attempts: [:primary], cleanup_timeout: limits.cleanup_timeout_ms] ++
           handoff_kw}
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

  # ── terminal authorization through the pinned admission authority (M2a) ──
  #
  # A real Telelem.Admission.Authority grants the terminal. Orders are forced
  # by suspending the authority while the attempt's grant request is queued;
  # the only wait is a bounded spin on message_queue_len.

  defp wait_q(p, n, until \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      elem(Process.info(p, :message_queue_len), 1) >= n ->
        :ok

      System.monotonic_time(:millisecond) > until ->
        flunk("queue of #{inspect(p)} never reached #{n}")

      true ->
        wait_q(p, n, until)
    end
  end

  defp kill!(p) do
    m = Process.monitor(p)
    Process.exit(p, :kill)
    assert_receive {:DOWN, ^m, _, _, _}, 1_000
  end

  defp authority!(tmp_dir) do
    sup =
      start_supervised!(
        {Telelem.Instance,
         id: :wire_upstream_authority,
         limits: %{slots: 4, units: 20},
         spool_dir: Path.join(tmp_dir, "spool"),
         transport: {Telelem.Test.Admission.TcpTransport, %{port: 1, label: "unused"}},
         reserve_ttl: nil}
      )

    Telelem.Instance.authority(Telelem.Instance.handle(sup))
  end

  # Released, everything before [DONE] transferred; returns the live pieces.
  defp streaming_to_done(ctx, handoff_kw) do
    auth = authority!(ctx.tmp_dir)

    a =
      start(
        ctx,
        @small,
        [],
        [
          admission_authority: auth,
          terminal_deadline: System.monotonic_time(:millisecond) + 10_000
        ] ++ handoff_kw
      )

    up = upstream!()
    send(a.pid, {:telelem_wire_credit, a.h, 1_000_000})
    ev = event(1)
    push(up, ev)
    push(up, @stop)
    h = a.h
    assert_receive {@tag_h, ^h, {:elected, :primary}}, 2_000
    assert_receive {@tag_h, ^h, {:data, ^ev}}, 2_000
    assert_receive {@tag_h, ^h, {:data, @stop}}, 2_000
    Map.merge(a, %{auth: auth, up: up})
  end

  @tag :tmp_dir
  test "authority dies with the attempt's grant request unanswered: no [DONE] reaches the requester, incomplete",
       ctx do
    a = streaming_to_done(ctx, [])
    :sys.suspend(a.auth)
    push(a.up, @done)
    send(a.up, :finish)
    # The attempt's terminal request is now queued at the suspended authority...
    wait_q(a.auth, 1)
    # ...and the authority dies without answering it.
    kill!(a.auth)

    {msgs, report} = collect(a.h)

    refute {:data, @done} in msgs,
           "marker delivered although the authority died first: #{inspect(msgs)}"

    assert {:incomplete, {:authority_down, :killed}} in msgs
    refute Enum.any?(msgs, &match?({:done, _}, &1))
    assert report.authority == :lost and report.committed and report.authorization == :admission
    assert_receive {:telelem_wire, _, :primary, {:ended, {:finish_refused, :cancelled}, _}}, 2_000
  end

  @tag :tmp_dir
  test "admission default mode: the granted [DONE] and done are adjacent", ctx do
    a = streaming_to_done(ctx, [])
    push(a.up, @done)
    send(a.up, :finish)
    {msgs, report} = collect(a.h)
    assert Enum.take(msgs, -2) == [{:data, @done}, {:done, {:upstream_done, "stop"}}]
    assert report.authorization == :admission and report.authority == :up
  end

  @tag :tmp_dir
  test "authority killed before the body completes: winner cancelled, [DONE] never forwarded",
       ctx do
    a = streaming_to_done(ctx, [])
    kill!(a.auth)
    {msgs, report} = collect(a.h)
    assert msgs == [{:incomplete, {:authority_down, :killed}}]
    assert report.authority == :lost

    assert_receive {:telelem_wire, _, :primary,
                    {:ended, {:cancelled, {:authority_down, :killed}}, _}},
                   2_000
  end

  @tag :tmp_dir
  test "terminal_ack: [DONE] handed over as {:terminal, _, deadline}; done only after the confirmed write, even if the authority dies first",
       ctx do
    a = streaming_to_done(ctx, terminal_ack: true)
    h = a.h
    push(a.up, @done)
    send(a.up, :finish)
    assert_receive {@tag_h, ^h, {:terminal, @done, d}} when is_integer(d), 2_000
    assert_receive {:telelem_wire, _, :primary, {:ended, :finished, stats}}, 2_000
    assert stats.events_forwarded == 3
    # Upstream complete and the attempt closed; still no outcome.
    assert Handoff.status(h).outcome == nil
    kill!(a.auth)
    assert Handoff.status(h).authority == :lost
    refute_received {@tag_h, ^h, {_, _}}
    assert :ok = Handoff.terminal_written(h, :ok)
    {msgs, report} = collect(h)
    assert msgs == [{:done, {:upstream_done, "stop"}}]
    assert report.terminal == %{upstream: :complete, downstream: :written}
  end

  @tag :tmp_dir
  test "terminal_ack: a failed downstream write of [DONE] is incomplete, never done", ctx do
    a = streaming_to_done(ctx, terminal_ack: true)
    h = a.h
    push(a.up, @done)
    send(a.up, :finish)
    assert_receive {@tag_h, ^h, {:terminal, @done, _}}, 2_000
    assert :ok = Handoff.terminal_written(h, {:error, :closed})
    {msgs, report} = collect(h)
    assert msgs == [{:incomplete, {:downstream_write, :closed}}]
    assert report.terminal.downstream == {:error, :closed}
  end

  # ── supported framing only: raw loopback TLS upstream (no Bandit) ──
  #
  # A hand-written HTTP/1.1 server on 127.0.0.1 sends exactly the bytes below.
  # Mint reports {:done, ref} for a close-delimited body (shown directly by the
  # first test), so :done alone must not count as completion.

  defp chunked(events),
    do:
      Enum.map_join(events, &(Integer.to_string(byte_size(&1), 16) <> "\r\n" <> &1 <> "\r\n")) <>
        "0\r\n\r\n"

  defp sse_events, do: [event(1), @stop, @done]

  @sse_head "content-type: text/event-stream\r\n"

  defp raw_response(:close_delimited),
    do: "HTTP/1.1 200 OK\r\n" <> @sse_head <> "\r\n" <> Enum.join(sse_events())

  defp raw_response(:content_length) do
    body = Enum.join(sse_events())
    "HTTP/1.1 200 OK\r\n" <> @sse_head <> "content-length: #{byte_size(body)}\r\n\r\n" <> body
  end

  defp raw_response(:http10_chunked),
    do:
      "HTTP/1.0 200 OK\r\n" <>
        @sse_head <> "transfer-encoding: chunked\r\n\r\n" <> chunked(sse_events())

  defp raw_response(:gzip_chunked),
    do:
      "HTTP/1.1 200 OK\r\n" <>
        @sse_head <> "transfer-encoding: gzip, chunked\r\n\r\n" <> chunked(sse_events())

  defp raw_response(:chunked),
    do:
      "HTTP/1.1 200 OK\r\n" <>
        @sse_head <> "transfer-encoding: chunked\r\n\r\n" <> chunked(sse_events())

  # One-shot TLS server: reads the request, writes `parts` (pausing between
  # them, so the status line can straddle deliveries) and closes cleanly.
  defp raw_server(ctx, parts) do
    {:ok, l} =
      :ssl.listen(0,
        ip: {127, 0, 0, 1},
        cert: ctx.server[:cert],
        key: ctx.server[:key],
        cacerts: ctx.server[:cacerts],
        active: false,
        mode: :binary,
        reuseaddr: true
      )

    {:ok, {_, port}} = :ssl.sockname(l)
    test = self()

    pid =
      spawn(fn ->
        {:ok, t} = :ssl.transport_accept(l, 5_000)
        {:ok, s} = :ssl.handshake(t, 5_000)
        send(test, {:raw_request, recv_request(s, "")})

        for {p, i} <- Enum.with_index(parts) do
          if i > 0, do: Process.sleep(30)
          _ = :ssl.send(s, p)
        end

        send(test, :raw_sent)
        _ = :ssl.close(s)
        :ssl.close(l)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    port
  end

  defp recv_request(s, acc) do
    if String.ends_with?(acc, ~s({"stream":true})) do
      acc
    else
      {:ok, d} = :ssl.recv(s, 0, 5_000)
      recv_request(s, acc <> d)
    end
  end

  defp run_raw(ctx, parts) do
    a = start(ctx, @small, port: raw_server(ctx, parts))
    send(a.pid, {:telelem_wire_credit, a.h, 1_000_000})
    assert_receive {:raw_request, "POST /v1/chat/completions HTTP/1.1\r\n" <> _}, 5_000
    {msgs, report} = collect(a.h)
    assert_receive {:telelem_wire, _, :primary, {:ended, reason, _}}, 2_000
    %{msgs: msgs, report: report, reason: reason}
  end

  test "control: Mint itself reports {:done, ref} for a close-delimited SSE body", ctx do
    port = raw_server(ctx, [raw_response(:close_delimited)])

    {:ok, conn} =
      Mint.HTTP.connect(:https, "127.0.0.1", port,
        protocols: [:http1],
        transport_opts: [cacerts: ctx.cacerts]
      )

    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", "/v1/chat/completions", [], ~s({"stream":true}))
    assert_receive :raw_sent, 5_000
    {responses, _conn} = mint_until_done(conn, ref, [])
    assert {:done, ref} in responses
    body = for({:data, ^ref, d} <- responses, do: d) |> IO.iodata_to_binary()
    assert String.ends_with?(body, @done)
  end

  defp mint_until_done(conn, ref, acc) do
    receive do
      msg ->
        case Mint.HTTP.stream(conn, msg) do
          {:ok, conn, rs} ->
            acc = acc ++ rs
            if {:done, ref} in rs, do: {acc, conn}, else: mint_until_done(conn, ref, acc)

          {:error, conn, _e, rs} ->
            {acc ++ rs, conn}

          :unknown ->
            mint_until_done(conn, ref, acc)
        end
    after
      3_000 -> flunk("Mint never finished: #{inspect(acc)}")
    end
  end

  test "close-delimited, content-length, HTTP/1.0 and non-plain chunked bodies never complete; nothing is forwarded",
       ctx do
    for {kind, framing} <- [
          close_delimited: :close_delimited,
          content_length: :content_length,
          http10_chunked: {:http_version, {:http, {1, 0}}},
          gzip_chunked: {:transfer_encoding, ["gzip", "chunked"]}
        ] do
      r = run_raw(ctx, [raw_response(kind)])
      assert r.reason == {:protocol_violation, {:unsupported_framing, framing}}, inspect(kind)
      assert r.msgs == [{:failed, :no_winner}], inspect({kind, r.msgs})
      refute r.report.committed
    end
  end

  test "valid control: the same raw server with HTTP/1.1 chunked framing completes with the exact [DONE]",
       ctx do
    resp = raw_response(:chunked)
    # Also split inside the status line, so it straddles two TLS deliveries.
    for parts <- [[resp], [binary_part(resp, 0, 5), binary_part(resp, 5, byte_size(resp) - 5)]] do
      r = run_raw(ctx, parts)
      assert r.reason == :finished
      assert List.last(for {:data, d} <- r.msgs, do: d) == @done
      assert {:done, {:upstream_done, "stop"}} in r.msgs
    end
  end
end
