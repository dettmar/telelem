defmodule Telelem.Wire.Gateway do
  @moduledoc """
  TEL-005 **test gateway**: Bandit (TLS, downstream) → `Telelem.Handoff` →
  `Telelem.Wire.Upstream` attempts (Mint TLS, upstream). It exists to run the
  M1 transport experiment and is not a product surface.

  `start_link/1` starts, under one supervisor:

    * a `Telelem.Wire.Recorder` (evidence),
    * a `DynamicSupervisor` for coordinators and attempts. The coordinator is
      never parented by the requester or by an attempt,
    * Bandit on `127.0.0.1:0` with a certificate from a fresh in-memory CA.
      Only the CA certificate is written (to `:ca_out`), for the client.

  ## Downstream controller (one per `POST /v1/chat/completions`)

  The Bandit handler process is the Handoff *requester*. It starts the
  `:primary` attempt. It starts `:backup` **only** if the primary reports
  `{:connect_failed, :refused, _}` (no request byte written). Any other
  failure is not a refusal, so the controller abstains and ends the handoff.

  It writes nothing downstream until the first `{:data, _}`. Each `{:data, ev}`
  is written with `Plug.Conn.chunk/2` (blocking under TCP backpressure); credit
  for those bytes goes back to the winner only after the write returned `:ok`.

  Endings:

    * `{:done, _}`: the attempt forwarded the upstream's exact `[DONE]` only
      after the complete HTTP body and a supported finish_reason (see
      `Telelem.Wire.Upstream`). The response then ends with the chunked
      terminator. The controller never writes `[DONE]` itself.
    * `{:incomplete, _}`, a downstream write error, or a deadline after commit:
      the connection is closed **without** the chunked terminator (the handler
      raises `Telelem.Wire.Gateway.Aborted`; Bandit closes the socket), so the
      client sees an incomplete body, never a synthetic success.
    * `{:failed, _}` before any byte: `502` JSON error.
  """

  use Supervisor

  alias Telelem.Handoff
  alias Telelem.Wire.{Limits, Recorder, Upstream}

  defmodule Aborted do
    @moduledoc "Raised in the handler to close the downstream without a terminator."
    defexception [:reason, plug_status: 502]
    @impl true
    def message(e), do: "downstream aborted: #{inspect(e.reason)}"
  end

  @san_ext_id {2, 5, 29, 17}

  @doc """
  Options:

    * `:lanes` (required): `%{primary: lane, backup: lane | nil}`, each
      `%{name:, host:, port:, cacertfile: | cacerts:}`.
    * `:limits`: `Telelem.Wire.Limits` (default `Limits.new()`).
    * `:ca_out`: path to write the downstream CA certificate (PEM) to.
    * `:name`: optional supervisor name.

  Returns `{:ok, sup}`. Use `info/1` for the port, CA and component PIDs.
  """
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "Port, CA PEM path, limits and component PIDs."
  def info(sup) do
    children = Map.new(Supervisor.which_children(sup), fn {id, pid, _, _} -> {id, pid} end)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(children[:bandit])
    cfg = :persistent_term.get({__MODULE__, sup})

    %{
      port: port,
      recorder: children[:recorder],
      sup: children[:attempts],
      ca_out: cfg.ca_out,
      limits: cfg.limits
    }
  end

  @impl true
  def init(opts) do
    limits = Keyword.get(opts, :limits) || Limits.new()
    lanes = Keyword.fetch!(opts, :lanes)
    %{server: server, client_cacerts: client_cacerts} = pki()
    ca_out = Keyword.get(opts, :ca_out)

    if ca_out do
      pem =
        :public_key.pem_encode(for der <- client_cacerts, do: {:Certificate, der, :not_encrypted})

      File.mkdir_p!(Path.dirname(ca_out))
      File.write!(ca_out, pem)
    end

    {:ok, rec} = Recorder.start_link()
    {:ok, dsup} = DynamicSupervisor.start_link(strategy: :one_for_one)
    # The recorder and attempt supervisor are started here and handed over as
    # supervised children so their PIDs exist before Bandit's plug options.
    cfg = %{lanes: lanes, limits: limits, recorder: rec, sup: dsup, ca_out: ca_out}
    :persistent_term.put({__MODULE__, self()}, cfg)

    bandit =
      {Bandit,
       plug: {__MODULE__.Plug, cfg},
       scheme: :https,
       ip: {127, 0, 0, 1},
       port: 0,
       startup_log: false,
       http_options: [compress: false, log_exceptions_with_status_codes: []],
       thousand_island_options: [
         num_acceptors: 4,
         transport_options: [
           cert: server[:cert],
           key: server[:key],
           cacerts: server[:cacerts],
           sndbuf: limits.downstream_sndbuf,
           send_timeout: limits.response_timeout_ms,
           send_timeout_close: true
         ]
       ]}

    children = [
      %{id: :recorder, start: {__MODULE__, :adopt, [rec]}},
      %{id: :attempts, start: {__MODULE__, :adopt, [dsup]}, type: :supervisor},
      Supervisor.child_spec(bandit, id: :bandit)
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc false
  # Re-links an already started process to the supervisor that adopts it.
  def adopt(pid) do
    Process.link(pid)
    {:ok, pid}
  end

  # In-memory CA + leaf for 127.0.0.1 / localhost. Keys are never written.
  # SKI/AKI are added so strict X.509 verifiers (Python 3.13 VERIFY_X509_STRICT)
  # accept the chain; only the root that issued the leaf is exported.
  defp pki do
    gen = fn -> :public_key.generate_key({:namedCurve, :secp256r1}) end
    [rk, lk, crk, clk] = for _ <- 1..4, do: gen.()
    ski = fn k -> :crypto.hash(:sha, elem(k, 4)) end
    ski_ext = fn k -> {:Extension, {2, 5, 29, 14}, false, ski.(k)} end

    aki_ext = fn k ->
      {:Extension, {2, 5, 29, 35}, false,
       {:AuthorityKeyIdentifier, ski.(k), :asn1_NOVALUE, :asn1_NOVALUE}}
    end

    san = {:Extension, @san_ext_id, false, [dNSName: ~c"localhost", iPAddress: <<127, 0, 0, 1>>]}

    data =
      :public_key.pkix_test_data(%{
        server_chain: %{
          root: [digest: :sha256, key: rk, extensions: [ski_ext.(rk)]],
          peer: [digest: :sha256, key: lk, extensions: [san, aki_ext.(rk), ski_ext.(lk)]]
        },
        client_chain: %{root: [digest: :sha256, key: crk], peer: [digest: :sha256, key: clk]}
      })

    leaf = data.server_config[:cert]
    roots = for c <- data.client_config[:cacerts], :public_key.pkix_is_issuer(leaf, c), do: c
    %{server: data.server_config, client_cacerts: roots}
  end

  # ── downstream controller ────────────────────────────────────────────────

  defmodule Plug do
    @moduledoc false
    import Elixir.Plug.Conn
    alias Telelem.Wire.Gateway

    def init(cfg), do: cfg

    def call(%{method: "POST", path_info: ["v1", "chat", "completions"]} = conn, cfg),
      do: Gateway.serve(conn, cfg)

    def call(conn, _cfg) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(404, ~s({"error":{"message":"not found"}}))
    end
  end

  @tag :telelem_handoff

  @doc false
  def serve(conn, cfg) do
    rec = cfg.recorder
    l = cfg.limits
    {peer_ip, peer_port} = {conn.remote_ip, conn.port}

    rid =
      Recorder.ingress(rec, %{
        handler: self(),
        method: conn.method,
        path: conn.request_path,
        peer: {peer_ip, peer_port},
        limits: Limits.to_map(l)
      })

    case Elixir.Plug.Conn.read_body(conn, length: l.max_request_body) do
      {:ok, body, conn} ->
        case JSON.decode(body) do
          {:ok, %{"stream" => true}} ->
            relay(conn, cfg, rid, body)

          _ ->
            reject(conn, rec, rid, 400, "this test gateway relays streaming JSON requests only")
        end

      {:more, _, conn} ->
        reject(conn, rec, rid, 413, "request body over max_request_body")

      {:error, reason} ->
        Recorder.note(rec, rid, {:downstream, {:read_error, reason}})
        raise Aborted, reason: {:read_error, reason}
    end
  end

  defp reject(conn, rec, rid, code, msg) do
    Recorder.note(rec, rid, {:downstream, {:rejected, code}})

    conn
    |> Elixir.Plug.Conn.put_resp_content_type("application/json")
    |> Elixir.Plug.Conn.send_resp(
      code,
      JSON.encode!(%{error: %{message: msg, type: "telelem_gateway"}})
    )
  end

  defp relay(conn, cfg, rid, body) do
    lanes = cfg.lanes
    ids = if lanes[:backup], do: [:primary, :backup], else: [:primary]

    {:ok, h} =
      DynamicSupervisor.start_child(
        cfg.sup,
        {Handoff,
         requester: self(),
         attempts: ids,
         cleanup_timeout: cfg.limits.cleanup_timeout_ms,
         register_timeout: cfg.limits.register_timeout_ms,
         trace: cfg.recorder}
      )

    :ok = Recorder.bind(cfg.recorder, rid, h)
    Recorder.note(cfg.recorder, rid, {:downstream, {:handoff_started, h}})

    st = %{
      conn: conn,
      cfg: cfg,
      rid: rid,
      h: h,
      hmon: Process.monitor(h),
      body: body,
      attempts: %{},
      winner: nil,
      sent?: false,
      written: 0,
      writes: 0,
      outcome: nil,
      down: nil,
      dropped_after_close: 0
    }

    st = start_attempt(st, :primary)
    controller(st)
  end

  defp start_attempt(st, id) do
    lane = Map.fetch!(st.cfg.lanes, id)

    spec =
      {Upstream,
       handoff: st.h,
       id: id,
       controller: self(),
       lane: lane,
       body: st.body,
       limits: st.cfg.limits,
       recorder: st.cfg.recorder,
       rid: st.rid}

    {:ok, pid} = DynamicSupervisor.start_child(st.cfg.sup, spec)
    note(st, {:attempt_started, id, pid})
    put_in(st.attempts[id], pid)
  end

  defp controller(st) do
    h = st.h
    hmon = st.hmon

    deadline =
      if st.outcome,
        do: st.cfg.limits.cleanup_deadline_ms,
        else: st.cfg.limits.response_timeout_ms

    receive do
      {:telelem_wire, _pid, :primary, {:connect_failed, :refused, reason}} ->
        if st.cfg.lanes[:backup] && st.outcome == nil do
          note(st, {:failover, :primary_refused, reason})
          st |> start_attempt(:backup) |> controller()
        else
          _ = Handoff.cancel(h, {:no_backup, reason})
          controller(st)
        end

      {:telelem_wire, _pid, id, {:connect_failed, class, reason}} ->
        # Not a provable refusal (or the backup itself failed): abstain.
        note(st, {:no_failover, id, class, reason})
        _ = Handoff.cancel(h, {:connect_failed, id, class, reason})
        controller(st)

      {:telelem_wire, _pid, _id, {:ended, _reason, _stats}} ->
        controller(st)

      {@tag, ^h, {:elected, id}} ->
        controller(%{st | winner: Map.fetch!(st.attempts, id)})

      {@tag, ^h, {:data, chunk}} ->
        controller(write(st, chunk))

      {@tag, ^h, {kind, _} = outcome} when kind in [:done, :incomplete, :failed] ->
        note(st, {:outcome, outcome})
        controller(%{st | outcome: outcome})

      {@tag, ^h, {:closed, report}} ->
        Process.demonitor(hmon, [:flush])
        finish(st, report)

      {:DOWN, ^hmon, :process, _, reason} ->
        note(st, {:handoff_down, reason})
        finish(%{st | outcome: st.outcome || {:failed, {:handoff_down, reason}}}, nil)
    after
      deadline ->
        if st.outcome do
          note(st, {:cleanup_deadline_missed, st.cfg.limits.cleanup_deadline_ms})
          finish(st, nil)
        else
          _ = Handoff.cancel(h, :gateway_response_timeout)
          controller(st)
        end
    end
  end

  defp write(%{down: d} = st, chunk) when d != nil do
    # Accepted by the coordinator before it saw our cancel; never written.
    %{st | dropped_after_close: st.dropped_after_close + byte_size(chunk)}
  end

  defp write(st, chunk) do
    conn =
      if st.sent? do
        st.conn
      else
        note(st, {:downstream_headers, 200})

        st.conn
        |> Elixir.Plug.Conn.put_resp_header("content-type", "text/event-stream; charset=utf-8")
        |> Elixir.Plug.Conn.put_resp_header("cache-control", "no-cache")
        |> Elixir.Plug.Conn.put_resp_header("x-telelem-rid", Integer.to_string(st.rid))
        |> Elixir.Plug.Conn.send_chunked(200)
      end

    n = byte_size(chunk)

    shown =
      if n <= 2048,
        do: chunk,
        else: {:sha256, Base.encode16(:crypto.hash(:sha256, chunk), case: :lower)}

    note(st, {:write_start, st.writes + 1, n, shown})
    t0 = System.monotonic_time(:microsecond)

    case Elixir.Plug.Conn.chunk(conn, chunk) do
      {:ok, conn} ->
        dt = System.monotonic_time(:microsecond) - t0
        note(st, {:write_done, st.writes + 1, n, dt})
        send(st.winner, {:telelem_wire_credit, st.h, n})
        %{st | conn: conn, sent?: true, written: st.written + n, writes: st.writes + 1}

      {:error, reason} ->
        note(
          st,
          {:downstream_closed, reason, %{blocked_us: System.monotonic_time(:microsecond) - t0}}
        )

        _ = Handoff.cancel(st.h, {:downstream_closed, reason})
        %{st | conn: conn, sent?: true, down: reason}
    end
  end

  defp finish(st, report) do
    note(
      st,
      {:closed, report,
       %{written: st.written, writes: st.writes, dropped_after_close: st.dropped_after_close}}
    )

    case {st.outcome, st.sent?, st.down} do
      {{:done, _}, true, nil} ->
        note(st, {:downstream_end, :terminator})
        st.conn

      {{:failed, reason}, false, nil} ->
        note(st, {:downstream_end, {:status, 502}})

        st.conn
        |> Elixir.Plug.Conn.put_resp_content_type("application/json")
        |> Elixir.Plug.Conn.send_resp(
          502,
          JSON.encode!(%{
            error: %{message: "upstream failed: #{inspect(reason)}", type: "telelem_gateway"}
          })
        )

      {outcome, _sent?, down} ->
        note(st, {:downstream_end, :aborted})
        raise Aborted, reason: {outcome, down}
    end
  end

  defp note(st, ev), do: Recorder.note(st.cfg.recorder, st.rid, {:downstream, ev})
end
