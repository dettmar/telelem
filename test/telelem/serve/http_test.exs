defmodule Telelem.Serve.HTTPTest.Transport do
  @moduledoc false
  # A no-I/O `ThousandIsland.Transport` stand-in for the pinned Bandit HTTP/1
  # stack: the "socket" is an Agent holding a receive script, a send script
  # and logs. Only `recv/3` and `send/2` are reached by `Bandit.Pipeline.run/5`
  # in these cases. No port, socket or network is used.
  #
  # recv script items: a binary (`{:ok, bin}`), `:timeout`, `:closed`; an
  # empty script answers `{:error, :timeout}`. send script items: `:ok` or
  # `{:error, reason}`; an empty script answers `:ok`.

  def new(recv \\ [], send \\ []) do
    {:ok, pid} =
      Agent.start(fn -> %{recv: recv, send: send, recvs: [], sends: [], out: []} end)

    pid
  end

  def recv(agent, length, timeout) do
    Agent.get_and_update(agent, fn s ->
      {item, rest} =
        case s.recv do
          [i | rest] -> {i, rest}
          [] -> {:timeout, []}
        end

      reply =
        case item do
          bin when is_binary(bin) -> {:ok, bin}
          :timeout -> {:error, :timeout}
          :closed -> {:error, :closed}
        end

      {reply, %{s | recv: rest, recvs: [{length, timeout} | s.recvs]}}
    end)
  end

  def send(agent, data) do
    Agent.get_and_update(agent, fn s ->
      {reply, rest} =
        case s.send do
          [r | rest] -> {r, rest}
          [] -> {:ok, []}
        end

      bin = IO.iodata_to_binary(data)
      out = if reply == :ok, do: [bin | s.out], else: s.out
      {reply, %{s | send: rest, sends: [{reply, byte_size(bin)} | s.sends], out: out}}
    end)
  end

  @doc "What was handed to the transport: written bytes, recv calls, send attempts."
  def log(agent) do
    s = Agent.get(agent, & &1)

    %{
      out: s.out |> Enum.reverse() |> IO.iodata_to_binary(),
      recvs: Enum.reverse(s.recvs),
      sends: Enum.reverse(s.sends)
    }
  end
end

defmodule Telelem.Serve.HTTPTest.Adapter do
  @moduledoc false
  # A no-I/O `Plug.Conn.Adapter`. Every payload carries a generation that each
  # returned payload increments, and every call is reported to the test with
  # the generation it was called with, so the test can see which conn was
  # used. `reads` scripts `read_req_body/2`.
  @behaviour Plug.Conn.Adapter

  def conn(test, reads, headers, opts \\ []) do
    payload = %{test: test, gen: 0, reads: reads}
    path = Keyword.get(opts, :path, "/v1/chat/completions")
    uri = %URI{scheme: "http", host: "h", port: 80, path: path, query: nil}
    Plug.Conn.Adapter.conn({__MODULE__, payload}, "POST", uri, {127, 0, 0, 1}, headers)
  end

  defp next(p), do: %{p | gen: p.gen + 1}

  @impl true
  def read_req_body(%{reads: [r | rest]} = p, opts) do
    send(p.test, {:adapter, :read, p.gen, opts})
    p = next(%{p | reads: rest})

    case r do
      {:ok, body} -> {:ok, body, p}
      {:more, body} -> {:more, body, p}
      {:error, e} -> {:error, e}
      {:raise, e} -> raise e
    end
  end

  @impl true
  def send_resp(p, status, headers, body) do
    send(p.test, {:adapter, :send_resp, p.gen, status, headers, body})
    {:ok, nil, next(p)}
  end

  @impl true
  def send_chunked(p, status, headers) do
    send(p.test, {:adapter, :send_chunked, p.gen, status, headers})
    {:ok, nil, next(p)}
  end

  @impl true
  def chunk(p, body) do
    send(p.test, {:adapter, :chunk, p.gen, body})
    {:ok, nil, next(p)}
  end

  @impl true
  def get_http_protocol(_p), do: :"HTTP/1.1"

  @impl true
  def send_file(_p, _s, _h, _path, _o, _l), do: raise("unused")
  @impl true
  def inform(_p, _s, _h), do: {:error, :not_supported}
  @impl true
  def upgrade(_p, _proto, _opts), do: {:error, :not_supported}
  @impl true
  def push(_p, _path, _h), do: {:error, :not_supported}
  @impl true
  def get_peer_data(_p), do: %{address: {127, 0, 0, 1}, port: 1, ssl_cert: nil}
  @impl true
  def get_sock_data(_p), do: %{address: {127, 0, 0, 1}, port: 2}
  @impl true
  def get_ssl_data(_p), do: nil
end

defmodule Telelem.Serve.HTTPTest.ReadTimeout do
  @moduledoc false
  defexception message: "read timeout", plug_status: :request_timeout
end

defmodule Telelem.Serve.HTTPTest do
  @moduledoc """
  W2e `Telelem.Serve.HTTP`, listener-free and offline.

    * **Pinned pipeline.** `Bandit.Pipeline.run/5` (Bandit 1.14.1, Plug
      1.20.4) over the real `Bandit.HTTP1.Socket` with a no-I/O
      `ThousandIsland.Socket` whose transport is an Agent
      (`HTTPTest.Transport`): real request parsing, `Bandit.Adapter`,
      `commit_response!` and the error path, with every byte handed to the
      transport recorded. No port, listener or socket.
    * **Real pre-dial denial.** The real `Telelem.Serve.Primary` on a real
      `Telelem.Serve.Tree` (setup as in `RequesterTest`): the real authority
      denies the reservation before any dial, so the response is the fixed
      `:unavailable`; real Ingress authentication, routing and the body read
      through Bandit.
    * **Completion and abort.** The real `Telelem.Serve.Requester.serve/3`
      against a real `Telelem.Handoff` with a cooperative test attempt,
      through `Telelem.Serve.HTTP.serve/3` (the same code as `call/2` with
      the requester entry point injected), and scripted dispositions for
      the inconsistent cases.
    * **Slot.** A no-I/O `Plug.Conn.Adapter` (`HTTPTest.Adapter`) whose
      payload generation shows which conn every call used.

  The recorded bytes are what Bandit handed to the transport module, not
  evidence of TCP/TLS behaviour, framing on a real socket or peer receipt.
  Log assertions hold for the test's default formatter only.

      scripts/mix test test/telelem/serve/http_test.exs --seed 0
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Telelem.{Handoff, Instance}
  alias Telelem.Ops.{Context, MemoryStore}
  alias Telelem.Receipts.Store
  alias Telelem.Route.Revision
  alias Telelem.Sandbox.{Bootstrap, Principal, VerifiedTarget}
  alias Telelem.Serve.{HTTP, Ingress, Primary, Requester, Tree}
  alias Telelem.Serve.HTTP.Aborted
  alias Telelem.Serve.HTTPTest.{Adapter, ReadTimeout, Transport}
  alias Telelem.Serve.Primary.Handle
  alias Telelem.Wire.Limits

  @moduletag :tmp_dir
  @moduletag :capture_log
  @t 2_000
  @units 4
  @instance_units 2
  @auth "Bearer AUTH-MARKER-91c3"
  @body ~s({"m":"BODY"})
  @tenant "local"
  @path "/v1/chat/completions"
  @marker "data: [DONE]\n\n"
  @sentinel {:plug_conn, :sent}

  setup %{tmp_dir: dir} do
    isup =
      start_supervised!({
        Instance,
        id: :serve_http_gw,
        limits: %{slots: 2, units: @instance_units},
        spool_dir: Path.join(dir, "spool"),
        transport: {:unused_no_workers, nil},
        reserve_ttl: nil
      })

    inst = Instance.handle(isup)
    auth = Instance.authority(inst)
    root = start_supervised!({Tree, []}, id: :tree, restart: :temporary)
    store = start_supervised!({Store, mode: :sandbox}, id: :receipts)
    routes = start_supervised!(MemoryStore, id: :routes)
    :ok = MemoryStore.seed(routes, @tenant, rev1())
    {bindings, target} = bindings!(dir)
    ctx = %{auth: auth, root: root, store: store, routes: routes, bindings: bindings}
    {:ok, cfg} = Primary.config(cfg_opts(ctx))
    Map.merge(ctx, %{target: target, cfg: cfg, opts: HTTP.init(primary: cfg, path: @path)})
  end

  # ── Real Primary through the pinned pipeline ────────────────────────────

  test "real pre-dial denial: one Requester.run, auth value passed unchanged, one body receive bounded by the remaining budget, fixed closed 503",
       ctx do
    o = owner!()
    tr = trace!(o, [{Requester, :run, 5}])
    sock = Transport.new([@body])

    {res, keys} =
      in_owner!(o, fn -> pipeline(sock, head(cl: byte_size(@body)), {HTTP, ctx.opts}) end)

    assert {:ok, _} = res
    assert keys == []
    assert_received {:authenticate, @auth}
    refute_received {:authenticate, _}
    assert calls(tr, o) == [{Requester, :run, 5}]

    log = Transport.log(sock)
    assert [{n, t}] = log.recvs
    assert n == byte_size(@body) and t > 0 and t <= 60_000
    assert_fixed!(log.out, :unavailable)
    for x <- [@auth, @body, ctx.target.host], do: refute(log.out =~ x)
  end

  test "n = 0: the one read needs no receive; then the same pre-dial denial", ctx do
    sock = Transport.new()
    assert {:ok, _} = pipeline(sock, head(cl: 0), {HTTP, ctx.opts})
    assert_received {:authenticate, @auth}
    log = Transport.log(sock)
    assert log.recvs == []
    assert_fixed!(log.out, :unavailable)
  end

  test "authentication runs before the body callback: an unauthenticated request reads no body byte",
       ctx do
    {:ok, ingress} = Ingress.config(ingress_opts(ctx, authenticate: fn _ -> :denied end))
    opts = HTTP.init(primary: primary!(ctx, ingress: ingress), path: @path)
    sock = Transport.new([@body])
    assert {:ok, _} = pipeline(sock, head(cl: byte_size(@body)), {HTTP, opts})
    log = Transport.log(sock)
    assert log.recvs == []
    assert_fixed!(log.out, :unauthenticated)
  end

  test "missing route, foreign tenant and an unknown path give byte-identical responses", ctx do
    foreign = fn _ -> Principal.new("other", "caller", :sandbox, [:dispatch]) end
    {:ok, i_foreign} = Ingress.config(ingress_opts(ctx, authenticate: foreign))
    {:ok, i_missing} = Ingress.config(ingress_opts(ctx, route_id: "nochat"))

    outs =
      for {ingress, path} <- [
            {i_foreign, @path},
            {i_missing, @path},
            {nil, @path <> "/"},
            {nil, "/v1/chat/%63ompletions"}
          ] do
        cfg = if ingress, do: primary!(ctx, ingress: ingress), else: ctx.cfg
        sock = Transport.new([@body])

        assert {:ok, _} =
                 pipeline(sock, head(cl: byte_size(@body), path: path), {HTTP, cfg_opts!(cfg)})

        log = Transport.log(sock)
        assert log.recvs == []
        assert_fixed!(log.out, :not_found)
        undate(log.out)
      end

    assert length(Enum.uniq(outs)) == 1
  end

  test "body receive timeout and close map to fixed 408 and 400; no retry", ctx do
    sock = Transport.new([:timeout, :timeout])
    assert {:ok, _} = pipeline(sock, head(cl: 5), {HTTP, ctx.opts})
    log = Transport.log(sock)
    # One body receive, then Bandit's own zero-timeout disconnect check.
    assert [{5, t}, {0, 0}] = log.recvs
    assert t > 0
    assert_fixed!(log.out, :request_timeout)

    sock = Transport.new([:closed])
    assert {:ok, _} = pipeline(sock, head(cl: 5), {HTTP, ctx.opts})
    assert [{5, _}] = Transport.log(sock).recvs
    assert_fixed!(Transport.log(sock).out, :bad_request)
  end

  test "request checks reject before Requester.run: no authentication, no read", ctx do
    o = owner!()
    tr = trace!(o, [{Requester, :run, 5}])

    cases = [
      {head(cl: 5, version: "HTTP/1.0"), :version_not_supported},
      {head(cl: 5, method: "GET"), :method_not_allowed},
      {head(cl: 5, method: "PUT", path: "/other"), :not_found},
      {head(cl: 5, extra: [{"expect", "100-continue"}]), :expectation_failed},
      {head(cl: nil, extra: [{"transfer-encoding", "chunked"}]), :bad_request},
      {head(cl: nil), :length_required},
      {head(cl: "5, 5"), :bad_request},
      {head(cl: 4097), :payload_too_large},
      {head(cl: 5, auth: nil), :unauthenticated},
      {head(cl: 5, extra: [{"authorization", "x"}]), :unauthenticated}
    ]

    socks = for {req, code} <- cases, do: {Transport.new([@body]), req, code}

    {results, keys} =
      in_owner!(o, fn ->
        for {sock, req, _} <- socks, do: pipeline(sock, req, {HTTP, ctx.opts})
      end)

    assert keys == []

    for {{sock, _, code}, res} <- Enum.zip(socks, results) do
      assert {:ok, _} = res, inspect(code)
      log = Transport.log(sock)
      assert log.recvs == [], inspect(code)
      assert_fixed!(log.out, code)
    end

    refute_received {:authenticate, _}
    assert calls(tr, o) == []
  end

  # ── Real Requester protocol, real Handoff ───────────────────────────────

  test "completion: real Requester.serve, chunked 200 with owned headers and no content-length; only the server writes the terminator, once",
       ctx do
    c = handoff_pipeline(ctx, Transport.new([@body]))
    h = c.h
    elect!(c)
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    assert_receive {:att, {:telelem_wire_credit, ^h, 2}}, @t
    finish(c, :term)
    assert_receive {:pipeline, res, keys}, @t
    assert {:ok, _} = res
    assert keys == []

    out = Transport.log(c.sock).out
    {status, headers, body} = split(out)
    assert status == "HTTP/1.1 200 OK"
    # Bandit adds only vary (compression negotiation; no-transform keeps the
    # body identity) and the chunked coding; no content-length, no request id.
    assert headers -- [{"vary", "accept-encoding"}] ==
             Enum.sort(HTTP.stream_headers() ++ [{"transfer-encoding", "chunked"}])

    assert body == "2\r\nc1\r\n" <> chunk(@marker) <> "0\r\n\r\n"
    assert count(out, "0\r\n\r\n") == 1
    refute out =~ @auth
  end

  test "partial: a failed data write aborts with the sentinel; no terminator, no status, no write after the failure",
       ctx do
    sock = Transport.new([@body], [:ok, :ok, {:error, :closed}])
    c = handoff_pipeline(ctx, sock)
    h = c.h
    elect!(c)
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    assert_receive {:att, {:telelem_wire_credit, ^h, 2}}, @t
    assert att!(c, fn -> Handoff.transfer(h, "c2") end) == :ok
    assert_receive {:pipeline, res, keys}, @t
    assert {:error, %Aborted{message: "telelem response aborted"} = e} = res
    assert Enum.sort(Map.keys(e)) == [:__exception__, :__struct__, :message]
    assert keys == []

    log = Transport.log(sock)
    assert [{:ok, _}, {:ok, _}, {{:error, :closed}, _}] = log.sends
    {"HTTP/1.1 200 OK", _, body} = split(log.out)
    assert body == "2\r\nc1\r\n"
  end

  test "ambiguous header: the header send fails; abort with no byte and no error status", ctx do
    sock = Transport.new([@body], [{:error, :closed}])
    c = handoff_pipeline(ctx, sock)
    h = c.h
    elect!(c)
    assert att!(c, fn -> Handoff.transfer(h, "c1") end) == :ok
    assert_receive {:pipeline, {:error, %Aborted{}}, []}, @t
    log = Transport.log(sock)
    assert [{{:error, :closed}, _}] = log.sends
    assert log.out == ""
  end

  # ── Scripted dispositions through the pinned pipeline ───────────────────

  test "inconsistent completion or a no_write after a writer call aborts with no status and no terminator",
       ctx do
    ev = %{header: :ok, terminal: :written, chunks: 0}
    complete = {:complete, ev, %{}}

    no_writer = fn _cfg, _auth, rd, _w, _o ->
      {:ok, _} = rd.(4096, 1_000)
      complete
    end

    header_only = fn _cfg, _auth, rd, {w, st}, _o ->
      {:ok, _} = rd.(4096, 1_000)
      {:ok, _} = w.header(st)
      complete
    end

    no_write_after = fn _cfg, _auth, rd, {w, st}, _o ->
      {:ok, _} = rd.(4096, 1_000)
      {:ok, _} = w.header(st)
      {:no_write, :handoff_down, %{}, %{}}
    end

    unread = fn _cfg, _auth, _rd, {w, st}, _o ->
      {:ok, st} = w.header(st)
      {:ok, _} = w.chunk(st, @marker)
      complete
    end

    for {runner, wrote_header} <- [
          {no_writer, false},
          {header_only, true},
          {no_write_after, true},
          {unread, true}
        ] do
      sock = Transport.new([@body])
      res = pipeline(sock, head(cl: byte_size(@body)), plug(ctx, runner))
      assert {:error, %Aborted{}} = res
      out = Transport.log(sock).out

      if wrote_header do
        assert {"HTTP/1.1 200 OK", _, body} = split(out)
        refute body =~ "0\r\n\r\n"
      else
        assert out == ""
      end

      assert slot_keys() == []
    end
  end

  test "the installed sentinel suppresses the server's error status; control: a bare raise gets Bandit's 500",
       ctx do
    aborted = fn _cfg, _auth, _rd, _w, _o -> {:aborted, :x, %{}, %{}} end
    sock = Transport.new()
    assert {:error, %Aborted{}} = pipeline(sock, head(cl: 0), plug(ctx, aborted))
    assert Transport.log(sock).out == ""

    control = Transport.new()
    assert {:error, _} = pipeline(control, head(cl: 0), {fn _, _ -> raise "boom" end, []})
    assert Transport.log(control).out =~ "HTTP/1.1 500 Internal Server Error\r\n"
  end

  test "a failed fixed-response send aborts: one attempt, no second write", ctx do
    sock = Transport.new([], [{:error, :closed}])
    assert {:error, %Aborted{}} = pipeline(sock, head(cl: 5, auth: nil), {HTTP, ctx.opts})
    log = Transport.log(sock)
    assert [{{:error, :closed}, _}] = log.sends
    assert log.out == ""
  end

  test "a raising or unknown runner: fixed 500 before any writer call, abort after one", ctx do
    for {runner, expect} <- [
          {fn _, _, _, _, _ -> raise "runner boom" end, :internal},
          {fn _, _, _, _, _ -> exit(:runner_exit) end, :internal},
          {fn _, _, _, _, _ -> :unknown end, :internal},
          {fn _, _, rd, {w, st}, _ ->
             {:ok, _} = rd.(4096, 1_000)
             {:ok, _} = w.header(st)
             raise "after header"
           end, :abort}
        ] do
      sock = Transport.new([@body])
      res = pipeline(sock, head(cl: byte_size(@body)), plug(ctx, runner))
      out = Transport.log(sock).out

      case expect do
        :internal ->
          assert {:ok, _} = res
          assert_fixed!(out, :internal)
          refute out =~ "boom"

        :abort ->
          assert {:error, %Aborted{}} = res
          assert {"HTTP/1.1 200 OK", _, ""} = split(out)
      end

      assert slot_keys() == []
    end
  end

  test "the abort log line is the fixed exception; the auth value is not in it (default formatter)",
       ctx do
    aborted = fn _cfg, _auth, _rd, _w, _o -> {:aborted, :x, %{}, %{}} end

    log =
      capture_log(fn ->
        assert {:error, %Aborted{}} =
                 pipeline(Transport.new(), head(cl: 0), plug(ctx, aborted))
      end)

    assert log =~ "telelem response aborted"
    refute log =~ "AUTH-MARKER"
  end

  # ── Slot: no-I/O Plug adapter ───────────────────────────────────────────

  test "body reader: one read_body with length n, read_length max(n, 1), read_timeout remaining; second call and bad arguments read nothing",
       ctx do
    test = self()

    runner = fn _cfg, auth, rd, _w, _o ->
      send(test, {:auth_input, auth})
      first = rd.(4096, 777)
      second = rd.(4096, 777)
      send(test, {:reads, first, second})
      {:no_write, {:start, {:ingress, :body_unavailable}}, %{}, %{}}
    end

    for {n, body} <- [{0, ""}, {3, "abc"}] do
      conn = Adapter.conn(test, [{:ok, body}], hdrs(n))
      sent = HTTP.serve(conn, ctx.opts, runner)
      assert_receive {:auth_input, @auth}
      assert_receive {:adapter, :read, 0, opts}
      assert opts == [length: n, read_length: max(n, 1), read_timeout: 777]
      assert_receive {:reads, {:ok, ^body}, {:error, :unavailable}}
      refute_received {:adapter, :read, _, _}
      # The fixed response used the conn the read returned.
      assert_receive {:adapter, :send_resp, 1, 400, _, _}
      assert sent.state == :sent
    end

    for {max, rem, want} <- [{2, 777, :too_large}, {4096, 0, :unavailable}] do
      runner = fn _cfg, _auth, rd, _w, _o ->
        send(test, {:read, rd.(max, rem)})
        {:no_write, {:start, {:ingress, :body_unavailable}}, %{}, %{}}
      end

      HTTP.serve(Adapter.conn(test, [{:ok, "abc"}], hdrs(3)), ctx.opts, runner)
      assert_receive {:read, {:error, ^want}}
      refute_received {:adapter, :read, _, _}
      assert_receive {:adapter, :send_resp, 0, 400, _, _}
    end
  end

  test "the latest returned conn is carried, also from an unsuccessful shape; timeouts map, nothing is retried",
       ctx do
    test = self()

    for {script, want, gen} <- [
          {{:more, "ab"}, {:error, :length}, 1},
          {{:ok, "ab"}, {:error, :length}, 1},
          {{:error, :timeout}, {:error, :timeout}, 0},
          {{:raise, %ReadTimeout{}}, {:error, :timeout}, 0},
          {{:raise, %RuntimeError{message: "x"}}, {:error, :unavailable}, 0},
          {{:error, :closed}, {:error, :unavailable}, 0}
        ] do
      runner = fn _cfg, _auth, rd, _w, _o ->
        send(test, {:read, rd.(4096, 500)})
        {:no_write, {:start, {:ingress, :body_unavailable}}, %{}, %{}}
      end

      HTTP.serve(Adapter.conn(test, [script], hdrs(3)), ctx.opts, runner)
      assert_receive {:adapter, :read, 0, _}
      assert_receive {:read, ^want}
      refute_received {:adapter, :read, _, _}
      assert_receive {:adapter, :send_resp, ^gen, 400, _, _}
      assert slot_keys() == []
    end
  end

  test "owned headers: earlier plugs' headers, cookies and before_send hooks never reach a response",
       ctx do
    test = self()

    prior = fn conn ->
      conn
      |> Plug.Conn.put_resp_header("x-request-id", "rid-from-earlier-plug")
      |> Plug.Conn.put_resp_cookie("c", "v")
      |> Plug.Conn.register_before_send(fn c ->
        send(test, :hook_ran)
        Plug.Conn.put_resp_header(c, "x-hook", "1")
      end)
    end

    runner = fn _cfg, _auth, rd, {w, st}, _o ->
      {:ok, _} = rd.(4096, 500)
      {:ok, st} = w.header(st)
      {:ok, st} = w.chunk(st, "x")
      {:ok, _} = w.chunk(st, @marker)
      {:complete, %{header: :ok, terminal: :written, chunks: 1}, %{}}
    end

    conn = prior.(Adapter.conn(test, [{:ok, "abc"}], hdrs(3)))
    done = HTTP.serve(conn, ctx.opts, runner)
    assert %Plug.Conn{state: :chunked, status: 200} = done
    assert_receive {:adapter, :send_chunked, 1, 200, headers}
    assert headers == HTTP.stream_headers()
    assert_receive {:adapter, :chunk, 2, "x"}
    assert_receive {:adapter, :chunk, 3, @marker}
    refute_received {:adapter, :chunk, _, ""}
    refute_received :hook_ran

    conn = prior.(Adapter.conn(test, [], hdrs(3, auth: nil)))
    HTTP.serve(conn, ctx.opts, runner)
    assert_receive {:adapter, :send_resp, 0, 401, headers, body}
    assert headers == HTTP.fixed_headers()
    assert {401, body} == HTTP.responses().unauthenticated
    refute_received :hook_ran
  end

  test "the fixed table is closed and pristine; init refuses unnormalized paths and unknown options",
       ctx do
    for {code, {status, body}} <- HTTP.responses() do
      assert is_atom(code) and status in 400..599
      assert body == ~s({"error":"#{code}"})
    end

    for p <- ["", "/", "v1", "/v1/", "//v1", "/v1//x", "/v1/./x", "/v1/../x", "/v1/%2e", "/v1 x"] do
      assert_raise ArgumentError, fn -> HTTP.init(primary: ctx.cfg, path: p) end
    end

    assert_raise ArgumentError, fn -> HTTP.init(primary: ctx.cfg, path: @path, x: 1) end
    assert_raise ArgumentError, fn -> HTTP.init(primary: :nope, path: @path) end

    assert_raise ArgumentError, fn ->
      HTTP.init(primary: ctx.cfg, path: @path, requester: [nope: 1])
    end

    refute inspect(ctx.opts) =~ "Primary"
  end

  test "a conn owned by another process, or already sent, is aborted without a status", ctx do
    conn = Adapter.conn(self(), [], hdrs(3))

    assert_raise Aborted, fn ->
      HTTP.serve(%{conn | owner: spawn(fn -> :ok end)}, ctx.opts, nil)
    end

    assert_received @sentinel
    assert_raise Aborted, fn -> HTTP.serve(%{conn | state: :sent}, ctx.opts, nil) end
    assert_received @sentinel
    refute_received {:adapter, _, _, _, _, _}
  end

  # ── helpers: pipeline ───────────────────────────────────────────────────

  defp pipeline(agent, head, plug) do
    span = %ThousandIsland.Telemetry{
      span_name: :connection,
      telemetry_span_context: make_ref(),
      start_time: System.monotonic_time(),
      start_metadata: %{},
      handler: Bandit.HTTP1.Handler,
      span_metadata: %{}
    }

    socket = %ThousandIsland.Socket{
      socket: agent,
      transport_module: Transport,
      read_timeout: 1_000,
      handshake_timeout: 1_000,
      silent_terminate_on_error: false,
      span: span,
      peername: {{127, 0, 0, 1}, 40_000}
    }

    transport = %Bandit.HTTP1.Socket{socket: socket, buffer: head, opts: %{http_1: []}}
    opts = %{http: [], http_1: [], http_2: [], websocket: []}
    Bandit.Pipeline.run(transport, plug, span, {false, {127, 0, 0, 1}}, opts)
  end

  defp plug(ctx, runner), do: {fn conn, o -> HTTP.serve(conn, o, runner) end, ctx.opts}

  defp head(o) do
    method = Keyword.get(o, :method, "POST")
    path = Keyword.get(o, :path, @path)
    version = Keyword.get(o, :version, "HTTP/1.1")
    auth = Keyword.get(o, :auth, @auth)
    cl = Keyword.get(o, :cl)

    hs =
      [{"host", "localhost"}, {"content-type", "application/json"}] ++
        if(auth, do: [{"authorization", auth}], else: []) ++
        if(cl, do: [{"content-length", to_string(cl)}], else: []) ++
        Keyword.get(o, :extra, [])

    IO.iodata_to_binary([
      method,
      " ",
      path,
      " ",
      version,
      "\r\n",
      Enum.map(hs, fn {k, v} -> [k, ": ", v, "\r\n"] end),
      "\r\n"
    ])
  end

  defp hdrs(n, o \\ []) do
    auth = Keyword.get(o, :auth, @auth)

    [{"host", "h"}, {"content-length", to_string(n)}] ++
      if(auth, do: [{"authorization", auth}], else: [])
  end

  defp split(out) do
    [head, body] = :binary.split(out, "\r\n\r\n")
    [status | lines] = String.split(head, "\r\n")

    headers =
      for l <- lines, [k, v] = String.split(l, ": ", parts: 2), k != "date", do: {k, v}

    {status, Enum.sort(headers), body}
  end

  defp assert_fixed!(out, code) do
    {status, body} = Map.fetch!(HTTP.responses(), code)
    {line, headers, got} = split(out)
    assert line =~ ~r/\AHTTP\/1\.[01] #{status} /
    assert got == body
    # Bandit adds content-length and vary (compression negotiation) to a
    # send_resp; nothing else is added and no request id is sent.
    want = HTTP.fixed_headers() ++ [{"content-length", "#{byte_size(body)}"}]
    assert headers -- [{"vary", "accept-encoding"}] == Enum.sort(want)
    refute Enum.any?(headers, fn {k, _} -> k =~ "request-id" end)
  end

  defp undate(out) do
    out |> String.split("\r\n") |> Enum.reject(&String.starts_with?(&1, "date: ")) |> Enum.join()
  end

  defp chunk(b), do: Integer.to_string(byte_size(b), 16) <> "\r\n" <> b <> "\r\n"
  defp count(s, sub), do: length(:binary.matches(s, sub))

  defp slot_keys, do: Enum.filter(Process.get_keys(), &match?({HTTP, _}, &1))

  # A pipeline process whose runner starts a real Handoff (requester: that
  # process), reads the body once and runs the real `Requester.serve/3`.
  defp handoff_pipeline(ctx, sock) do
    test = self()
    att = actor()
    d = now() + 5_000
    a = ctx.auth
    {guard, witness} = {idle(), idle()}

    runner = fn _cfg, _auth, rd, writer, ropts ->
      {:ok, @body} = rd.(4096, 1_000)

      {:ok, h} =
        Handoff.start(
          requester: self(),
          attempts: [:primary],
          admission_authority: a,
          terminal_deadline: d,
          terminal_ack: true
        )

      handle = %Handle{
        request_id: "rq-http",
        handoff: h,
        guard: guard,
        owner: att,
        witness: witness,
        ref: make_ref(),
        deadline_ms: d
      }

      send(test, {:handle, h})
      Requester.serve(handle, writer, ropts)
    end

    plug = plug(ctx, runner)
    req = head(cl: byte_size(@body))

    pid =
      spawn(fn ->
        res = pipeline(sock, req, plug)
        send(test, {:pipeline, res, slot_keys()})
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    assert_receive {:handle, h}, @t
    on_exit(fn -> Process.exit(h, :kill) end)
    %{h: h, att: att, d: d, sock: sock}
  end

  # A response-owner process: runs one function on request, reports its
  # result and its leftover slot keys, then stays alive (the coordinator
  # monitors its requester) without reading its mailbox.
  defp owner! do
    test = self()

    pid =
      spawn(fn ->
        receive do: ({:go, f} -> send(test, {:owner_result, self(), f.(), slot_keys()}))
        receive do: (:never -> :ok)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp in_owner!(pid, f) do
    send(pid, {:go, f})
    assert_receive {:owner_result, ^pid, res, keys}, @t
    {res, keys}
  end

  defp elect!(c) do
    h = c.h
    assert {:ok, _} = att!(c, fn -> Handoff.attach(h, :primary) end)
    assert att!(c, fn -> Handoff.begin_dispatch(h, :primary) end) == :ok
    assert att!(c, fn -> Handoff.ready(h, :primary) end) == :release
  end

  defp finish(c, term) do
    h = c.h
    d = c.d
    send(c.att, {:call, fn -> Handoff.finish(h, term, @marker, deadline: d) end})
  end

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

  defp idle do
    pid = spawn(fn -> receive do: (:never -> :ok) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  # ── helpers: tracing ────────────────────────────────────────────────────

  defp trace!(pid, mfas) do
    # A module that is not loaded yet would match no function.
    for {m, _, _} <- mfas, do: Code.ensure_loaded!(m)
    session = :trace.session_create(:serve_http, self(), [])
    on_exit(fn -> :trace.session_destroy(session) end)
    for mfa <- mfas, do: 1 = :trace.function(session, mfa, true, [])
    1 = :trace.process(session, pid, true, [:call])
    session
  end

  defp calls(session, pid) do
    ref = :trace.delivered(session, pid)
    assert_receive {:trace_delivered, ^pid, ^ref}, @t
    drain_calls(pid, [])
  end

  defp drain_calls(pid, acc) do
    receive do
      {:trace, ^pid, :call, {m, f, args}} -> drain_calls(pid, [{m, f, length(args)} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # ── helpers: Primary setup (as in RequesterTest) ────────────────────────

  defp primary!(ctx, extra) do
    {:ok, cfg} = Primary.config(Keyword.merge(cfg_opts(ctx), extra))
    cfg
  end

  defp cfg_opts!(cfg), do: HTTP.init(primary: cfg, path: @path)

  defp cfg_opts(ctx) do
    {:ok, ingress} = Ingress.config(ingress_opts(ctx))

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
    ]
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
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem http test CA", [])
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

  defp now, do: System.monotonic_time(:millisecond)
end
