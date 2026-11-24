defmodule Telelem.WireM1Test do
  # TEL-005 M1 transport experiment over real sockets:
  #
  #   real OpenAI SDK (host, max_retries=0) --TLS--> Bandit test gateway (this BEAM)
  #     --> Telelem.Handoff --> Telelem.Wire.Upstream (Mint, TLS) --> TEL-004 fixture lanes a/b
  #
  # Needs the host fixture and SDK agent started by `integration/m1/run`; without
  # them every test here is skipped (never passed). Evidence JSON per case goes to
  # $TELELEM_TEST_M1_RUN_WORK/evidence/.
  use ExUnit.Case, async: false

  alias Telelem.Wire.{Gateway, Limits, Recorder}

  @control System.get_env("TELELEM_TEST_M1_FIXTURE_CONTROL")
  @moduletag :m1
  @moduletag timeout: 120_000
  if @control == nil,
    do: @moduletag(skip: "run via integration/m1/run (needs host fixture + SDK agent)")

  @fragment ~s({"city": "Sto)
  # The fragment as it appears inside the SSE JSON bytes.
  @fragment_wire @fragment |> JSON.encode!() |> String.slice(1..-2//1)

  setup_all do
    env = &System.fetch_env!/1

    ctx = %{
      control: env.("TELELEM_TEST_M1_FIXTURE_CONTROL"),
      fixture_ca: env.("TELELEM_TEST_M1_FIXTURE_CA"),
      sdk: env.("TELELEM_TEST_M1_SDK_AGENT"),
      run_work: env.("TELELEM_TEST_M1_RUN_WORK"),
      run_host: env.("TELELEM_TEST_M1_RUN_HOST")
    }

    info = get!(ctx.control <> "/info")
    sdk_versions = get!(ctx.sdk <> "/versions")
    File.mkdir_p!(Path.join(ctx.run_work, "evidence"))

    write_evidence(ctx, "versions", %{
      fixture: Map.take(info, ["schema", "lanes", "ca_sha256"]),
      sdk_agent: sdk_versions,
      gateway: %{
        elixir: System.version(),
        otp: System.otp_release(),
        ssl: vsn(:ssl),
        mint: vsn(:mint),
        bandit: vsn(:bandit),
        thousand_island: vsn(:thousand_island)
      }
    })

    Map.put(ctx, :lanes, info["lanes"])
  end

  setup ctx do
    post!(ctx.control <> "/reset", %{})
    for lane <- ["a", "b"], do: post!(ctx.control <> "/lanes/#{lane}/fault", %{kind: "valid"})
    :ok
  end

  # ── reference case 1 ────────────────────────────────────────────────────

  test "refusal before connection: primary refused, zero primary arrivals, one backup arrival, complete SDK terminal",
       ctx do
    post!(ctx.control <> "/lanes/a/fault", %{kind: "refuse"})
    # The fixture keeps lane a's port bound but not listening: the kernel answers RST.
    assert get!(ctx.control <> "/info")["lanes"]["a"]["listening"] == false

    gw = start_gateway(ctx, "refusal")
    sdk = sdk_stream(ctx, gw)
    {rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)

    write_evidence(ctx, "refusal_before_connection", %{
      limits: Limits.to_map(gw.limits),
      sdk: sdk,
      fixture_counts: counts,
      gateway_ingress: length(Recorder.rids(gw.recorder)),
      gateway_events: evs,
      handoff_report: report
    })

    # One ingress call, SDK retries disabled.
    assert Recorder.rids(gw.recorder) == [rid]
    assert sdk["max_retries"] == 0

    # Connect attempts (gateway) versus arrivals (fixture).
    assert [{:primary, "a"}, {:backup, "b"}] ==
             for(%{ev: {:upstream, id, {:connect_attempt, l, _, _}}} <- evs, do: {id, l})

    assert [{:primary, :refused, :econnrefused}] ==
             for(%{ev: {:upstream, id, {:connect_failed, _, c, r}}} <- evs, do: {id, c, r})

    assert counts["a"]["tcp_accepts"] == 0 and counts["a"]["requests"] == 0
    assert counts["b"]["tcp_accepts"] == 1 and counts["b"]["chat_completions"] == 1
    assert counts["b"]["outcomes"] == %{"complete" => 1}

    # Complete SDK terminal, from lane b.
    assert sdk["exception"] == nil and sdk["iterator_ended"] == true and sdk["status"] == 200
    assert sdk["finish_reason"] == "stop" and sdk["content"] == "Hello from lane b."

    assert {:done, {:upstream_done, "stop"}} = report.outcome
    assert report.attempts.primary.charge == :none
    assert report.attempts.backup.charge == :possible
    assert downstream_end(evs) == :terminator
  end

  # ── reference case 2 ────────────────────────────────────────────────────

  test "reset after commit: SDK iterator shows the tool fragment, then an incomplete termination; no backup, no [DONE]",
       ctx do
    post!(ctx.control <> "/lanes/a/queue", %{
      faults: [%{kind: "reset_after_tool_fragment", hold: true, close: "rst"}]
    })

    gw = start_gateway(ctx, "reset")

    task = Task.async(fn -> sdk_stream(ctx, gw) end)

    # The fixture holds after sending the fragment; reset only once the gateway has
    # written the fragment downstream, so the RST cannot overtake it in the gateway.
    holding = wait_fixture_request(ctx, "a", &(&1["phase"] == "holding"))
    [rid] = wait_rids(gw, 1)

    {:ok, _} =
      Recorder.wait(
        gw.recorder,
        rid,
        fn evs -> Enum.find(writes(evs), &String.contains?(&1, @fragment_wire)) end,
        10_000
      )

    assert post!(ctx.control <> "/release", %{request_id: holding["id"]})["released"] == [
             holding["id"]
           ]

    sdk = Task.await(task, 30_000)
    {^rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)
    fixture_req = get!(ctx.control <> "/requests/" <> holding["id"])

    write_evidence(ctx, "reset_after_commit", %{
      sdk: sdk,
      fixture_counts: counts,
      fixture_request:
        Map.take(fixture_req, ~w(id outcome events_sent body_bytes_sent wire_bytes_sent conn_id)),
      gateway_events: evs,
      handoff_report: report
    })

    # Public SDK iterator: fragment first, then the exception; no finish, no success.
    kinds = for e <- sdk["log"], do: e["kind"]
    frag_at = Enum.find_index(sdk["log"], &(&1["kind"] == "chunk" and &1["tool_args"] != ""))
    exc_at = Enum.find_index(kinds, &(&1 == "exception"))
    assert frag_at != nil and exc_at != nil and frag_at < exc_at
    assert sdk["tool_name"] == "get_weather" and sdk["tool_args"] == @fragment
    assert sdk["finish_reason"] == nil and sdk["iterator_ended"] == false
    assert sdk["exception"]["type"] == "openai.APIConnectionError"
    # HTTP 200 is not the pass condition; it is recorded to show it was not enough.
    assert sdk["status"] == 200

    assert fixture_req["outcome"] == "reset"
    assert counts["a"]["chat_completions"] == 1
    assert counts["b"]["tcp_accepts"] == 0 and counts["b"]["requests"] == 0

    assert {:incomplete, _} = report.outcome
    assert report.committed and report.winner == :primary
    assert report.attempts.backup.cleanup == :never_registered
    assert report.possible_charges == [:primary]
    refute Enum.any?(writes(evs), &String.contains?(&1, "[DONE]"))
    assert downstream_end(evs) == :aborted

    assert [{:upstream_broken, _, %{done_held: false}}] =
             for(%{ev: {:upstream, :primary, {:ended, r, _}}} <- evs, do: r)
  end

  test "reset after commit, immediate RST (no hold): whatever the race, never a success or a fallback",
       ctx do
    post!(ctx.control <> "/lanes/a/queue", %{
      faults: [%{kind: "reset_after_tool_fragment", close: "rst"}]
    })

    gw = start_gateway(ctx, "reset_immediate")
    sdk = sdk_stream(ctx, gw)
    {_rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)

    path =
      case report.outcome do
        {:incomplete, _} -> :committed_then_incomplete
        {:failed, _} -> :failed_before_commit
        other -> {:unexpected, other}
      end

    write_evidence(ctx, "reset_after_commit_immediate", %{
      observed_path: path,
      sdk: sdk,
      fixture_counts: counts,
      gateway_events: evs,
      handoff_report: report
    })

    assert counts["b"]["tcp_accepts"] == 0
    assert sdk["exception"] != nil and sdk["finish_reason"] == nil
    refute Enum.any?(writes(evs), &String.contains?(&1, "[DONE]"))

    case path do
      :committed_then_incomplete ->
        assert sdk["tool_args"] == @fragment
        assert sdk["exception"]["type"] == "openai.APIConnectionError"

      :failed_before_commit ->
        # The reset destroyed the unread fragment in the gateway's socket before it
        # was read: nothing was committed, so the client gets a status error.
        assert sdk["exception"]["status_code"] == 502
    end
  end

  # ── slow consumer, pressure and disconnect ─────────────────────────────

  @slow_events 400
  @slow_piece 30_000
  @read_chunks 3

  test "slow consumer: pressure reaches the gateway, disconnect, bounded buffers, cleanup within the declared deadline",
       ctx do
    limits = Limits.new()

    # Declared before running (written first, then the run starts).
    declared = %{
      limits: Limits.to_map(limits),
      bounds: %{
        attempt_retained_hard_cap: limits.hard_cap,
        in_flight_window: limits.window,
        overflow_action: limits.overflow,
        cleanup_deadline_ms: limits.cleanup_deadline_ms,
        forced_kill_after_ms: limits.cleanup_timeout_ms
      },
      plan: %{
        upstream_events: @slow_events,
        piece_bytes: @slow_piece,
        sdk_reads_then_stops_after_chunks: @read_chunks,
        pressure_condition:
          "a downstream chunk/2 blocked >= 1000 ms AND the attempt paused (not armed)"
      },
      not_measured: [
        "client (SDK) kernel receive buffer, fixture kernel send buffer: kernel, not application",
        "OTP ssl connection-process internal buffers (process memory sampled only)",
        "whether the remote fixture/provider stopped work or billing (fixture outcome is reported separately)"
      ]
    }

    write_evidence(ctx, "slow_consumer.declared", declared)

    text = List.duplicate(String.duplicate("x", @slow_piece), @slow_events)
    post!(ctx.control <> "/lanes/a/queue", %{faults: [%{kind: "valid", text: text}]})
    gw = start_gateway(ctx, "slow", limits)

    slow =
      post!(ctx.sdk <> "/slow/start", %{
        base_url: gw.base_url,
        ca_file: gw.ca_host,
        read_chunks: @read_chunks
      })

    assert slow["paused"] == true
    [rid] = wait_rids(gw, 1)
    attempt = attempt_pid(gw, rid, :primary)

    # Pressure: the downstream write is blocked in chunk/2 and the reader stopped arming.
    {:ok, blocked} = wait_pressure(gw, rid, 1_000, 30_000)

    probe1 = probe(attempt)
    ssl_mem1 = probe1.ssl_process_memory
    fixture_req = wait_fixture_request(ctx, "a", &(&1["phase"] == "streaming"))
    f1 = fixture_req["body_bytes_sent"]
    Process.sleep(500)
    probe2 = probe(attempt)
    f2 = get!(ctx.control <> "/requests/" <> fixture_req["id"])["body_bytes_sent"]
    total_planned = @slow_events * @slow_piece

    assert probe1.armed == false and probe2.armed == false
    assert probe2.stats.deliveries == probe1.stats.deliveries, "reader kept reading while paused"
    assert probe2.retained <= limits.hard_cap and probe2.in_flight <= limits.window

    # Disconnect: the SDK closes its stream while the gateway write is blocked.
    closed = post!(ctx.sdk <> "/slow/close", %{id: slow["id"]})
    {:ok, _} = Recorder.wait(gw.recorder, rid, &find_closed/1, 10_000)
    {^rid, evs, report} = settled(gw)

    t_detect = first_t(evs, &match?({:downstream, {:downstream_closed, _, _}}, &1))
    t_closed = first_t(evs, &match?({:handoff, {:closed, _}}, &1))
    detect_wall = first_wall(evs, &match?({:downstream, {:downstream_closed, _, _}}, &1))
    cleanup_ms = (t_closed - t_detect) / 1000

    Process.sleep(200)
    handler = ingress_handler(evs)

    after_state = %{
      attempt_alive: Process.alive?(attempt),
      handoff_alive: Process.alive?(report_handoff(gw, rid)),
      handler_alive: Process.alive?(handler),
      supervisor_children: DynamicSupervisor.count_children(gw.sup),
      sockets_to_lane_a: sockets_to(ctx.lanes["a"]["port"]),
      sockets_on_gateway_port: sockets_on(gw.port)
    }

    slow_counts = quiesced_counts(ctx, gw)
    fixture_final = wait_fixture_request(ctx, "a", &(&1["phase"] == "done"), fixture_req["id"])
    [{_, ended_stats}] = for %{ev: {:upstream, :primary, {:ended, r, st}}} <- evs, do: {r, st}

    write_evidence(ctx, "slow_consumer", %{
      declared: declared,
      pressure: %{
        blocked_write: blocked,
        probe_before: probe1,
        probe_after_500ms: probe2,
        ssl_connection_process_memory_bytes: ssl_mem1,
        fixture_body_bytes_sent: [f1, f2],
        fixture_planned_body_bytes_at_least: total_planned
      },
      sdk_slow_start: slow,
      sdk_close: closed,
      disconnect: %{
        detected_close_to_local_cleanup_ms: cleanup_ms,
        sdk_close_start_to_gateway_detect_ms_wall:
          (detect_wall - closed["close"]["close_start_wall_ns"]) / 1_000_000
      },
      attempt_end_stats: ended_stats,
      after: after_state,
      fixture_request_final:
        Map.take(
          fixture_final,
          ~w(id outcome client_closed body_bytes_sent wire_bytes_sent duration_ms)
        ),
      fixture_counts: slow_counts,
      handoff_report: report,
      gateway_events:
        Enum.reject(
          evs,
          &match?(%{ev: {:downstream, {w, _, _, _}}} when w in [:write_start, :write_done], &1)
        )
    })

    # Pressure reached the gateway (and, via TCP, the fixture stalled short of its script).
    assert blocked.blocked_ms >= 1_000
    assert f1 == f2 and f2 < total_planned

    # Bounds (application bytes).
    assert ended_stats.peak_retained <= limits.hard_cap
    assert ended_stats.max_delivery <= limits.hard_cap - limits.high_water

    # Cleanup.
    assert {:incomplete, {:downstream_closed, _}} = report.outcome
    assert cleanup_ms <= limits.cleanup_deadline_ms
    assert report.attempts.primary.cleanup == :ack
    assert report.possible_charges == [:primary]
    refute after_state.attempt_alive or after_state.handoff_alive or after_state.handler_alive
    assert after_state.supervisor_children.active == 0
    assert after_state.sockets_to_lane_a == []
    assert fixture_final["outcome"] == "client_closed"
    assert slow_counts["b"]["tcp_accepts"] == 0
  end

  # ── trust ───────────────────────────────────────────────────────────────

  @tag :capture_log
  test "trust: an upstream not signed by the configured CA fails closed (no failover); untrusted gateway is refused by the SDK",
       ctx do
    gw = start_gateway(ctx, "trust", Limits.new(), primary_cacertfile: :gateway_ca)
    sdk = sdk_stream(ctx, gw)
    {_rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)

    # The SDK trusting only the fixture CA must refuse the gateway's certificate.
    sdk_wrong_ca =
      sdk_stream(ctx, %{gw | ca_host: Path.join([ctx.run_host, "fixture", "ca.pem"])})

    ingress_after = length(Recorder.rids(gw.recorder))

    write_evidence(ctx, "trust", %{
      sdk_upstream_untrusted: sdk,
      sdk_gateway_untrusted: sdk_wrong_ca,
      fixture_counts: counts,
      gateway_events: evs,
      handoff_report: report,
      gateway_ingress_total: ingress_after
    })

    assert [{:primary, :ambiguous, {:tls_alert, {:unknown_ca, _}}}] =
             for(%{ev: {:upstream, id, {:connect_failed, _, c, r}}} <- evs, do: {id, c, r})

    assert counts["a"]["requests"] == 0 and counts["b"]["tcp_accepts"] == 0
    assert {:failed, _} = report.outcome
    assert sdk["exception"]["status_code"] == 502
    assert sdk_wrong_ca["exception"]["type"] == "openai.APIConnectionError"
    assert ingress_after == 1
  end

  test "baseline: valid primary stream completes through the gateway, backup untouched", ctx do
    gw = start_gateway(ctx, "baseline")
    sdk = sdk_stream(ctx, gw)
    {_rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)

    write_evidence(ctx, "baseline", %{
      sdk: sdk,
      fixture_counts: counts,
      gateway_events: evs,
      handoff_report: report
    })

    assert sdk["exception"] == nil and sdk["content"] == "Hello from lane a." and
             sdk["finish_reason"] == "stop"

    assert counts["a"]["chat_completions"] == 1 and counts["b"]["tcp_accepts"] == 0
    assert {:done, {:upstream_done, "stop"}} = report.outcome
  end

  # ── SSE terminal grammar (scripted upstream) ───────────────────────────
  #
  # The independent fixture has no fault for these byte sequences, so the primary
  # lane here is an in-BEAM Bandit TLS upstream that writes exactly the scripted
  # bytes. The client is still the real SDK through the real gateway; lane b is
  # the fixture and must stay untouched. Upstream bytes are test-authored, not
  # independent fixture evidence.

  defmodule ScriptedUpstream do
    @moduledoc false
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, %{steps: steps, test: test}) do
      {:ok, _body, conn} = read_body(conn)
      send(test, {:scripted_arrival, self()})
      conn = conn |> put_resp_content_type("text/event-stream") |> send_chunked(200)

      Enum.reduce(steps, conn, fn
        {:send, bin}, conn ->
          {:ok, conn} = chunk(conn, bin)
          conn

        {:sleep, ms}, conn ->
          Process.sleep(ms)
          conn

        # Close the TLS connection without the chunked terminator.
        :abort, _conn ->
          raise "scripted abort"
      end)
    end
  end

  defmodule Bytes do
    @moduledoc false
    def chunk_ev(delta, fin \\ nil) do
      obj = %{
        "id" => "chatcmpl-s",
        "object" => "chat.completion.chunk",
        "created" => 1_700_000_000,
        "model" => "telelem-wire-model",
        "choices" => [
          %{"index" => 0, "delta" => delta, "logprobs" => nil, "finish_reason" => fin}
        ]
      }

      {:send, "data: " <> JSON.encode!(obj) <> "\n\n"}
    end
  end

  @role Bytes.chunk_ev(%{"role" => "assistant", "content" => ""}) |> elem(1)
  @hello Bytes.chunk_ev(%{"content" => "Hello"}) |> elem(1)
  @stop Bytes.chunk_ev(%{}, "stop") |> elem(1)
  @done "data: [DONE]\n\n"
  @error_ev "data: " <>
              JSON.encode!(%{
                "error" => %{"message" => "scripted upstream error", "type" => "server_error"}
              }) <> "\n\n"

  defp scripted_gateway(ctx, name, steps) do
    opts = [digest: :sha256, key: {:namedCurve, :secp256r1}]
    san = {:Extension, {2, 5, 29, 17}, false, [iPAddress: <<127, 0, 0, 1>>]}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{
        server_chain: %{root: opts, peer: [{:extensions, [san]} | opts]},
        client_chain: %{root: opts, peer: opts}
      })

    up =
      start_supervised!(
        {Bandit,
         plug: {ScriptedUpstream, %{steps: steps, test: self()}},
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false,
         http_options: [compress: false, log_exceptions_with_status_codes: []],
         thousand_island_options: [
           shutdown_timeout: 200,
           transport_options: [cert: server[:cert], key: server[:key], cacerts: server[:cacerts]]
         ]},
        id: {:scripted, name}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(up)
    lane = %{name: "scripted", host: "127.0.0.1", port: port, cacerts: client[:cacerts]}
    start_gateway(ctx, name, Limits.new(), primary: lane)
  end

  defp scripted_case(ctx, name, steps) do
    gw = scripted_gateway(ctx, name, [{:send, @role} | steps])
    sdk = sdk_stream(ctx, gw)
    assert_received {:scripted_arrival, _}
    {_rid, evs, report} = settled(gw)
    counts = quiesced_counts(ctx, gw)

    write_evidence(ctx, "grammar_" <> name, %{
      upstream: "in-BEAM scripted Bandit (test-authored bytes), not the independent fixture",
      steps: steps,
      sdk: sdk,
      fixture_counts_lane_b: counts["b"],
      gateway_events: evs,
      handoff_report: report
    })

    assert counts["b"]["tcp_accepts"] == 0
    {sdk, evs, report}
  end

  defp sdk_success?(sdk), do: sdk["exception"] == nil and sdk["iterator_ended"] == true

  test "grammar control: finish chunk + exact [DONE] + complete body is the only success", ctx do
    {sdk, evs, report} =
      scripted_case(ctx, "control", [{:send, @hello}, {:send, @stop}, {:send, @done}])

    assert sdk_success?(sdk) and sdk["content"] == "Hello" and sdk["finish_reason"] == "stop"
    assert {:done, _} = report.outcome
    assert downstream_end(evs) == :terminator
  end

  test "grammar: [DONE] then a reset before the chunked terminator is not a success", ctx do
    {sdk, evs, report} =
      scripted_case(ctx, "done_then_reset", [
        {:send, @hello},
        {:send, @stop},
        {:send, @done},
        {:sleep, 50},
        :abort
      ])

    refute sdk_success?(sdk), "SDK saw a clean end although the upstream body never completed"
    assert sdk["exception"]["type"] == "openai.APIConnectionError"

    assert sdk["finish_reason"] == "stop",
           "the observed upstream finish chunk is forwarded unchanged"

    assert {:incomplete, _} = report.outcome
    refute Enum.any?(writes(evs), &String.contains?(&1, "[DONE]"))
    assert downstream_end(evs) == :aborted
  end

  test "grammar: a non-exact [DONE] sentinel is never forwarded and never success", ctx do
    {sdk, evs, report} =
      scripted_case(ctx, "bad_sentinel", [
        {:send, @hello},
        {:send, @stop},
        {:send, "data: [DONE]junk\n\n"}
      ])

    refute sdk_success?(sdk), "the SDK stops at any data starting with [DONE]"
    refute match?({:done, _}, report.outcome)
    refute Enum.any?(writes(evs), &String.contains?(&1, "[DONE]"))
    assert downstream_end(evs) == :aborted
  end

  test "grammar: [DONE] without a supported finish_reason is never success", ctx do
    {sdk, _evs, report} =
      scripted_case(ctx, "done_without_finish", [{:send, @hello}, {:send, @done}])

    refute sdk_success?(sdk)
    refute match?({:done, _}, report.outcome)
  end

  test "grammar: a top-level error event + [DONE] + clean body is an error, never :done", ctx do
    {sdk, evs, report} =
      scripted_case(ctx, "error_then_done", [{:send, @hello}, {:send, @error_ev}, {:send, @done}])

    assert sdk["exception"]["type"] == "openai.APIError"
    assert sdk["exception"]["message"] == "scripted upstream error"
    assert {:incomplete, _} = report.outcome

    assert [{:upstream_error_event, "scripted upstream error"}] =
             for(%{ev: {:upstream, :primary, {:ended, r, _}}} <- evs, do: r)

    refute Enum.any?(writes(evs), &String.contains?(&1, "[DONE]"))
  end

  test "grammar: an error event before any content fails before commit (502), nothing forwarded",
       ctx do
    {sdk, evs, report} = scripted_case(ctx, "error_first", [{:send, @error_ev}, {:send, @done}])
    assert sdk["exception"]["status_code"] == 502
    assert {:failed, _} = report.outcome
    assert writes(evs) == []
  end

  # ── helpers: gateway ────────────────────────────────────────────────────

  defp start_gateway(ctx, name, limits \\ Limits.new(), opts \\ []) do
    ca_work = Path.join([ctx.run_work, name, "gateway-ca.pem"])
    ca_host = Path.join([ctx.run_host, name, "gateway-ca.pem"])

    lane = fn l ->
      %{name: l, host: "127.0.0.1", port: ctx.lanes[l]["port"], cacertfile: ctx.fixture_ca}
    end

    primary =
      case {opts[:primary], opts[:primary_cacertfile]} do
        {%{} = scripted, _} -> scripted
        {nil, :gateway_ca} -> %{lane.("a") | cacertfile: ca_work}
        {nil, nil} -> lane.("a")
      end

    sup =
      start_supervised!(
        {Gateway,
         lanes: %{primary: primary, backup: lane.("b")}, limits: limits, ca_out: ca_work},
        id: {:gateway, name}
      )

    info = Gateway.info(sup)

    Map.merge(info, %{
      base_url: "https://127.0.0.1:#{info.port}/v1",
      ca_host: ca_host,
      limits: limits
    })
  end

  # Gateway quiescence first (no attempt left that could still dispatch), then the
  # fixture's settle barrier, then its independent counters.
  defp quiesced_counts(ctx, gw) do
    wait_until(
      fn -> DynamicSupervisor.count_children(gw.sup).active == 0 end,
      5_000,
      "gateway quiescence"
    )

    %{"ok" => true} = post!(ctx.control <> "/wait", %{settled: true, timeout: 10})
    get!(ctx.control <> "/counts")
  end

  defp wait_until(fun, ms, what) do
    cond do
      fun.() -> :ok
      ms <= 0 -> flunk("timed out waiting for #{what}")
      true -> Process.sleep(20) && wait_until(fun, ms - 20, what)
    end
  end

  defp sdk_stream(ctx, gw),
    do: post!(ctx.sdk <> "/stream", %{base_url: gw.base_url, ca_file: gw.ca_host})

  defp wait_rids(gw, n, tries \\ 200) do
    case Recorder.rids(gw.recorder) do
      rids when length(rids) >= n -> rids
      _ when tries > 0 -> Process.sleep(25) && wait_rids(gw, n, tries - 1)
      rids -> flunk("expected #{n} ingress, got #{inspect(rids)}")
    end
  end

  # Waits for the controller's final event and returns {rid, events, handoff report}.
  defp settled(gw) do
    [rid | _] = gw |> wait_rids(1) |> Enum.reverse()
    {:ok, _} = Recorder.wait(gw.recorder, rid, &find_closed/1, 15_000)
    {:ok, _} = Recorder.wait(gw.recorder, rid, &downstream_end/1, 5_000)
    evs = Recorder.events(gw.recorder, rid)
    {rid, evs, find_closed(evs)}
  end

  defp find_closed(evs) do
    Enum.find_value(evs, fn
      %{ev: {:handoff, {:closed, report}}} -> report
      _ -> nil
    end)
  end

  defp downstream_end(evs) do
    Enum.find_value(evs, fn
      %{ev: {:downstream, {:downstream_end, e}}} -> e
      _ -> nil
    end)
  end

  defp writes(evs),
    do: for(%{ev: {:downstream, {:write_start, _, _, b}}} when is_binary(b) <- evs, do: b)

  defp first_t(evs, pred), do: Enum.find_value(evs, fn %{ev: e, t: t} -> if pred.(e), do: t end)

  defp first_wall(evs, pred),
    do: Enum.find_value(evs, fn %{ev: e, wall_ns: w} -> if pred.(e), do: w end)

  defp ingress_handler(evs),
    do:
      Enum.find_value(evs, fn
        %{ev: {:ingress, m}} -> m.handler
        _ -> nil
      end)

  defp attempt_pid(gw, rid, id) do
    {:ok, pid} =
      Recorder.wait(
        gw.recorder,
        rid,
        fn evs ->
          Enum.find_value(evs, fn
            %{ev: {:downstream, {:attempt_started, ^id, p}}} -> p
            _ -> nil
          end)
        end,
        5_000
      )

    pid
  end

  defp report_handoff(gw, rid) do
    Recorder.events(gw.recorder, rid)
    |> Enum.find_value(fn
      %{ev: {:downstream, {:handoff_started, h}}} -> h
      _ -> nil
    end)
  end

  # Polls until a downstream write has been blocked >= min_ms and the attempt is paused.
  defp wait_pressure(gw, rid, min_ms, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_pressure(gw, rid, min_ms, deadline)
  end

  defp do_wait_pressure(gw, rid, min_ms, deadline) do
    evs = Recorder.events(gw.recorder, rid)
    now = System.monotonic_time(:microsecond)

    starts =
      for %{ev: {:downstream, {:write_start, n, size, _}}, t: t} <- evs,
          into: %{},
          do: {n, {t, size}}

    dones = for %{ev: {:downstream, {:write_done, n, _, _}}} <- evs, into: MapSet.new(), do: n
    open = for {n, {t, size}} <- starts, not MapSet.member?(dones, n), do: {n, t, size}
    paused = Enum.any?(evs, &match?(%{ev: {:upstream, :primary, {:paused, _}}}, &1))

    case open do
      [{n, t, size}] when paused and now - t >= min_ms * 1000 ->
        {:ok,
         %{
           write_no: n,
           write_bytes: size,
           blocked_ms: div(now - t, 1000),
           writes_completed: MapSet.size(dones)
         }}

      _ ->
        if System.monotonic_time(:millisecond) > deadline,
          do:
            flunk(
              "no pressure observed: open=#{inspect(open)} paused=#{paused} writes=#{MapSet.size(dones)}"
            ),
          else: Process.sleep(100) && do_wait_pressure(gw, rid, min_ms, deadline)
    end
  end

  defp probe(pid) do
    ref = make_ref()
    send(pid, {:telelem_wire_probe, self(), ref})

    receive do
      {^ref, snap} -> snap
    after
      2_000 -> flunk("attempt did not answer probe")
    end
  end

  defp tcp_ports do
    for p <- Port.list(), Port.info(p, :name) == {:name, ~c"tcp_inet"}, do: p
  end

  defp sockets_to(port) do
    for p <- tcp_ports(), match?({:ok, {_, ^port}}, :inet.peername(p)), do: inspect(p)
  end

  defp sockets_on(port) do
    for p <- tcp_ports(),
        match?({:ok, {_, ^port}}, :inet.sockname(p)),
        match?({:ok, _}, :inet.peername(p)),
        do: inspect(p)
  end

  defp wait_fixture_request(ctx, lane, pred, id \\ nil, tries \\ 300) do
    reqs =
      if id,
        do: [get!(ctx.control <> "/requests/" <> id)],
        else: get!(ctx.control <> "/requests?lane=" <> lane)

    case Enum.find(reqs, pred) do
      nil when tries > 0 ->
        Process.sleep(50) && wait_fixture_request(ctx, lane, pred, id, tries - 1)

      nil ->
        flunk(
          "fixture request on #{lane} never matched: #{inspect(Enum.map(reqs, &Map.take(&1, ~w(id phase outcome))))}"
        )

      r ->
        r
    end
  end

  # ── helpers: loopback JSON over plain HTTP (fixture control, SDK agent) ─

  defp get!(url), do: http!(:get, url, nil)
  defp post!(url, body), do: http!(:post, url, body)

  # One fresh connection per call (no reuse), passive Mint, plain HTTP to 127.0.0.1.
  defp http!(method, url, body) do
    uri = URI.parse(url)
    "127.0.0.1" = uri.host

    {:ok, conn} =
      Mint.HTTP.connect(:http, uri.host, uri.port, mode: :passive, protocols: [:http1])

    path = uri.path <> if(uri.query, do: "?" <> uri.query, else: "")
    {verb, payload} = if method == :get, do: {"GET", nil}, else: {"POST", JSON.encode!(body)}
    headers = if payload, do: [{"content-type", "application/json"}], else: []
    {:ok, conn, ref} = Mint.HTTP.request(conn, verb, path, headers, payload)

    {conn, code, resp} =
      recv_all(conn, ref, nil, [], System.monotonic_time(:millisecond) + 90_000)

    Mint.HTTP.close(conn)
    unless code in 200..299, do: flunk("#{verb} #{url} -> #{code}: #{String.slice(resp, 0, 500)}")
    JSON.decode!(resp)
  end

  defp recv_all(conn, ref, code, acc, deadline) do
    left = max(deadline - System.monotonic_time(:millisecond), 1)
    {:ok, conn, rs} = Mint.HTTP.recv(conn, 0, left)

    {code, acc, done?} =
      Enum.reduce(rs, {code, acc, false}, fn
        {:status, ^ref, c}, {_, a, d} -> {c, a, d}
        {:data, ^ref, b}, {c, a, d} -> {c, [a | b], d}
        {:done, ^ref}, {c, a, _} -> {c, a, true}
        _, x -> x
      end)

    if done?,
      do: {conn, code, IO.iodata_to_binary(acc)},
      else: recv_all(conn, ref, code, acc, deadline)
  end

  defp vsn(app) do
    _ = Application.load(app)
    to_string(Application.spec(app, :vsn))
  end

  defp write_evidence(ctx, name, data) do
    path = Path.join([ctx.run_work, "evidence", name <> ".json"])
    File.write!(path, JSON.encode!(jsonable(data)))
  end

  defp jsonable(%_{} = s), do: s |> Map.from_struct() |> jsonable()
  defp jsonable(m) when is_map(m), do: Map.new(m, fn {k, v} -> {key(k), jsonable(v)} end)

  defp jsonable(l) when is_list(l),
    do: if(List.ascii_printable?(l) and l != [], do: to_string(l), else: Enum.map(l, &jsonable/1))

  defp jsonable(t) when is_tuple(t), do: t |> Tuple.to_list() |> Enum.map(&jsonable/1)

  defp jsonable(b) when is_binary(b),
    do: if(String.printable?(b), do: b, else: %{base64: Base.encode64(b)})

  defp jsonable(x) when is_pid(x) or is_reference(x) or is_port(x) or is_function(x),
    do: inspect(x)

  defp jsonable(x), do: x

  defp key(k) when is_binary(k) or is_atom(k), do: k
  defp key(k), do: inspect(k)
end
