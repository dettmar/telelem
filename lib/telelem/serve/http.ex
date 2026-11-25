defmodule Telelem.Serve.HTTP.Aborted do
  @moduledoc """
  Raised by `Telelem.Serve.HTTP` to end a response that must not be
  completed and must not get a status or any second write. Always a fresh
  struct with this fixed message; it carries no request, conn, reason or
  disposition data.
  """
  defexception message: "telelem response aborted"
end

defmodule Telelem.Serve.HTTP.Opts do
  @moduledoc """
  Validated `Telelem.Serve.HTTP` plug options (from `Telelem.Serve.HTTP.init/1`).
  `Inspect` shows only the path: these options can appear in a host's
  diagnostics (for example Bandit's error-log metadata names the plug and its
  options).
  """
  @enforce_keys [:primary, :path, :requester, :max_body]
  @derive {Inspect, only: [:path]}
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          primary: Telelem.Serve.Primary.Config.t(),
          path: String.t(),
          requester: keyword(),
          max_body: pos_integer()
        }
end

defmodule Telelem.Serve.HTTP.Writer do
  @moduledoc """
  The `Telelem.Serve.Requester.Writer` that `Telelem.Serve.HTTP` hands to
  `Telelem.Serve.Requester.run/5`. Its state is only the request's slot key;
  the conn it writes with is always the latest one in that slot (the body
  read may have replaced the conn the request started with), and every conn
  it gets back is put there. See `Telelem.Serve.HTTP`.

    * `header/1`: once, with a fresh header set owned by the adapter, no
      `before_send` hooks and no response cookies, then
      `Plug.Conn.send_chunked(conn, 200)`. `{:ok, key}` only for a returned
      conn in state `:chunked` with status 200.
    * `chunk/2`: only after a successful header and no failed call, only a
      non-empty binary (never the empty terminator chunk), one
      `Plug.Conn.chunk/2`. `{:ok, key}` only for a returned conn still in
      state `:chunked`.

  A raise, throw or exit from Plug or the server adapter is not caught here:
  `Telelem.Serve.Requester` treats it as unconfirmed. Every call is counted
  in the slot *before* its effect, so the adapter can tell an invoked call
  from a successful one whatever the call did.
  """
  @behaviour Telelem.Serve.Requester.Writer

  alias Telelem.Serve.HTTP

  @impl true
  def header(key) do
    case HTTP.slot_begin(key) do
      {:ok, %{header: :not_called, calls: 1} = slot} ->
        HTTP.slot_put(key, %{slot | header: :called})
        conn = Plug.Conn.send_chunked(HTTP.stream_conn(slot.conn), 200)

        case conn do
          %Plug.Conn{state: :chunked, status: 200} ->
            HTTP.slot_put(key, %{slot | conn: conn, header: :ok, ok_calls: 1})
            {:ok, key}

          %Plug.Conn{} ->
            HTTP.slot_put(key, %{slot | conn: conn, header: :called})
            {:error, :not_chunked}

          _ ->
            {:error, :not_chunked}
        end

      _ ->
        {:error, :state}
    end
  end

  @impl true
  def chunk(key, bytes) do
    case HTTP.slot_begin(key) do
      # `calls` was just incremented: every earlier call succeeded.
      {:ok, %{header: :ok, calls: c, ok_calls: ok} = slot}
      when c == ok + 1 and is_binary(bytes) and byte_size(bytes) > 0 ->
        # Pinned Plug 1.20.4: `{:ok, conn}` keeps state `:chunked`; an adapter
        # reply of another shape raises (CaseClauseError), which Requester
        # treats as unconfirmed.
        case Plug.Conn.chunk(slot.conn, bytes) do
          {:ok, %Plug.Conn{state: :chunked} = conn} ->
            HTTP.slot_put(key, %{slot | conn: conn, ok_calls: ok + 1})
            {:ok, key}

          {:error, _} ->
            {:error, :write}
        end

      _ ->
        {:error, :state}
    end
  end
end

defmodule Telelem.Serve.HTTP do
  @moduledoc """
  W2e: listener-free Plug adapter for the one primary route. It turns one
  `Plug.Conn` into **one** `Telelem.Serve.Requester.run/5` in the calling
  process (the response owner: the process that owns the conn) and ends the
  response from the returned disposition. It opens no socket and starts no
  listener; the server (Bandit in production, a fake in tests) owns those.

      init(primary: %Primary.Config{}, path: "/v1/chat/completions", requester: [])
      call(conn, opts) -> conn   # or raises Telelem.Serve.HTTP.Aborted

  `path` must be normalized: `/`-separated segments of `A-Z a-z 0-9 - . _ ~`,
  no empty, `.` or `..` segment, no trailing slash. `requester` is passed to
  `Requester.run/5` as its options (`:controller`, `:observer`,
  `:stream_grace_ms`, `:observe_ms`; Requester validates the values).

  ## Before `Requester.run/5` (no read, no start; a fixed response)

  In this order; the first failing check answers from the table below and
  `Requester.run/5` is not called:

    1. `Plug.Conn.get_http_protocol/1` is exactly `:"HTTP/1.1"`, else
       `:version_not_supported`.
    2. `conn.request_path` is byte-for-byte the configured path, else
       `:not_found` (the same answer as a missing or foreign route). The
       query string is neither checked nor used.
    3. `conn.method` is exactly `"POST"`, else `:method_not_allowed`.
    4. No `expect` request header (any value, any count), else
       `:expectation_failed`. No `100 Continue` is ever sent.
    5. No `transfer-encoding` request header (any value, any count), else
       `:bad_request`.
    6. Exactly one `content-length` header whose value is 1 to 16 ASCII
       digits (`n`): missing is `:length_required`, anything else
       `:bad_request`; `n` above the ingress `max_body_bytes` is
       `:payload_too_large`.
    7. Exactly one `authorization` header, else `:unauthenticated`. Its value
       is the `auth_input`, passed unchanged and never inspected or kept.

  A conn that is not in state `:unset`, or whose `owner` is set to another
  process, is aborted (see below) without a status. (Bandit leaves
  `conn.owner` nil; its adapter itself refuses calls from any process but the
  one that owns the connection.)

  ## The slot (process-dictionary handback)

  `Requester.run/5` hands the conn to neither the body reader nor the writer
  and returns no conn, so the latest conn travels in a process-dictionary
  entry keyed by `{Telelem.Serve.HTTP, make_ref()}`, written only by this
  module and deleted in an `after` once `run/5` returns, raises, throws or
  exits. It records the latest conn any call returned, whether the body was
  read, and how many writer calls were invoked and succeeded.

  ## The body reader

  Ingress calls it only after authentication and route preparation (never
  for an unauthenticated, unauthorized or unrouted request). It reads at
  most once (a second call is refused without reading) and only for
  `n <= max` and a positive integer `remaining`; then **one**
  `Plug.Conn.read_body(conn, length: n, read_length: max(n, 1),
  read_timeout: remaining)` (also for `n = 0`). No retry, no second read.

    * `{:ok, body, conn}` with `byte_size(body) == n`: `{:ok, body}`.
    * `{:ok, _, conn}` of another length or `{:more, _, conn}`: the conn is
      kept in the slot; `{:error, :length}` (Ingress: `:body_unavailable`).
    * `{:error, :timeout}`, or a raised exception whose
      `Plug.Exception.status/1` is 408 (Bandit's read timeout):
      `{:error, :timeout}` (Ingress: `:body_timeout`).
    * Anything else returned, raised, thrown or exited:
      `{:error, :unavailable}`.

  **Actual limits are per receive, not total.** `read_timeout` bounds each
  server-side receive. With pinned Bandit 1.14.1 over HTTP/1.1 and a
  `content-length` body, the read asks the transport once for all body bytes
  not already buffered (`read_length` is at least that), with `remaining`
  as that receive's timeout, plus Bandit's own zero-timeout receive after a
  timeout. A transport that returns fewer bytes than asked makes Bandit
  receive again with a fresh `remaining`, so the total is not bounded by
  `remaining`. Time to receive the request head is the server's, before
  this module runs.

  ## Response

  Every response this module starts has its own exact header list
  (replacing whatever earlier plugs set), no `before_send` hooks and no
  response cookies, and `connection: close`. No request ID header is sent.

    * **Fixed responses** (`responses/0`): one of a closed table of statuses
      and fixed JSON bodies, with `content-type: application/json`,
      `cache-control: no-store, no-transform` and `connection: close`, sent
      with `Plug.Conn.send_resp/3`. The body never carries diagnostics, a
      disposition's private part, the auth input or request data. A missing
      route and a foreign tenant (`{:ingress, :no_route}`) get the same
      `:not_found` as an unknown path.
    * **Stream**: status 200, `content-type: text/event-stream;
      charset=utf-8`, `cache-control: no-cache, no-transform` (so the server
      does not compress it) and `connection: close`, no `content-length`, so
      Bandit uses chunked transfer coding.

  From the disposition:

    * `{:complete, evidence, _}` returns the slot's latest conn normally, and
      only if it is consistent: evidence `header: :ok` and `terminal:
      :written`; the body was read once; every invoked writer call
      succeeded, exactly `evidence.chunks + 2` of them (header, data chunks,
      marker); the latest conn is in state `:chunked` with status 200, with
      the request's adapter module and owner. This module writes no empty
      chunk; returning the chunked conn lets the server end it (Bandit's
      `commit_response!` writes the terminator).
    * `{:no_write, why, ...}` with no writer call invoked: one fixed
      response chosen from the log-safe `why` only.
    * `{:aborted, ...}`, an inconsistent `:complete`, a `:no_write` after a
      writer call, an unknown result or a raise/throw/exit from `run/5`
      after a writer call, or a fixed response whose send raises, throws or
      exits: **abort**. (An unknown result or a raise before any writer
      call gets the fixed `:internal` response.)

  **Abort** sends the calling process one `{:plug_conn, :sent}` (Plug's
  already-sent sentinel; existing ones are never consumed), then raises a
  fresh `Telelem.Serve.HTTP.Aborted`. With pinned Bandit 1.14.1 its error
  path then finds the sentinel and writes no status, writes no chunked
  terminator, and `Bandit.Pipeline.run/5` returns `{:error, _}` (exercised
  in the tests); per its source the HTTP/1 handler then closes the
  connection (not exercised: no socket here). Nothing is retried or written
  a second time.

  ## Not covered

  Diagnostics are not contained here, and must be before production: Bandit
  logs the `Aborted` exception (with a stack trace) at error level with
  metadata that includes the original `Plug.Conn` (request headers, the
  `authorization` value included) and the plug options, and logs its own
  protocol errors. Messages left in the owner's mailbox by Requester's
  protocol are left for the server process. Nothing here claims real
  chunked framing on a socket, peer receipt of any byte, TLS, or anything
  beyond the pinned Bandit/Plug behaviour cited above.
  """

  @behaviour Plug

  alias Telelem.Serve.HTTP.{Aborted, Opts, Writer}
  alias Telelem.Serve.{Ingress, Primary, Requester}

  @sentinel {:plug_conn, :sent}
  @requester_opt_keys [:controller, :observer, :stream_grace_ms, :observe_ms]
  @max_length_digits 16

  @responses %{
    bad_request: {400, ~s({"error":"bad_request"})},
    unauthenticated: {401, ~s({"error":"unauthenticated"})},
    forbidden: {403, ~s({"error":"forbidden"})},
    not_found: {404, ~s({"error":"not_found"})},
    method_not_allowed: {405, ~s({"error":"method_not_allowed"})},
    request_timeout: {408, ~s({"error":"request_timeout"})},
    length_required: {411, ~s({"error":"length_required"})},
    payload_too_large: {413, ~s({"error":"payload_too_large"})},
    expectation_failed: {417, ~s({"error":"expectation_failed"})},
    internal: {500, ~s({"error":"internal"})},
    bad_gateway: {502, ~s({"error":"bad_gateway"})},
    unavailable: {503, ~s({"error":"unavailable"})},
    gateway_timeout: {504, ~s({"error":"gateway_timeout"})},
    version_not_supported: {505, ~s({"error":"version_not_supported"})}
  }

  @fixed_headers [
    {"content-type", "application/json"},
    {"cache-control", "no-store, no-transform"},
    {"connection", "close"}
  ]

  @stream_headers [
    {"content-type", "text/event-stream; charset=utf-8"},
    {"cache-control", "no-cache, no-transform"},
    {"connection", "close"}
  ]

  @forbidden [:permission_denied, :mode_mismatch, :target_denied]
  @timeouts [:terminal_deadline, :dispatch_deadline, :receive_timeout, :deadline]

  @doc "The closed status/body table of fixed responses."
  @spec responses() :: %{atom() => {100..599, binary()}}
  def responses, do: @responses

  @doc "The exact header list of every fixed response."
  def fixed_headers, do: @fixed_headers

  @doc "The exact header list of the stream response."
  def stream_headers, do: @stream_headers

  # ── Plug ────────────────────────────────────────────────────────────────

  @impl Plug
  def init(opts) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- [:primary, :path, :requester],
         true <- length(Enum.uniq(Keyword.keys(opts))) == length(opts),
         %Primary.Config{ingress: %Ingress.Config{max_body_bytes: max}} = cfg
         when is_integer(max) and max > 0 <- Keyword.get(opts, :primary),
         path when is_binary(path) <- Keyword.get(opts, :path),
         true <- normalized_path?(path),
         req = Keyword.get(opts, :requester, []),
         true <- requester_opts?(req) do
      %Opts{primary: cfg, path: path, requester: req, max_body: max}
    else
      _ -> raise ArgumentError, "invalid Telelem.Serve.HTTP options"
    end
  end

  @impl Plug
  def call(conn, opts), do: serve(conn, opts, &Requester.run/5)

  @doc false
  # `call/2` with the requester entry point as an argument, so listener-free
  # tests can drive the real `Requester.serve/3` protocol (or a scripted
  # disposition) through this module. Production uses `call/2`.
  def serve(%Plug.Conn{} = conn, opts, runner) do
    cond do
      conn.owner not in [nil, self()] or conn.state != :unset -> abort!()
      not match?(%Opts{}, opts) or not is_function(runner, 5) -> respond(conn, :internal)
      true -> checked(conn, opts, runner)
    end
  end

  def serve(_conn, _opts, _runner), do: abort!()

  defp checked(conn, opts, runner) do
    case check(conn, opts) do
      {:ok, n, auth} -> dispatch(conn, opts, n, auth, runner)
      {:reject, code} -> respond(conn, code)
    end
  end

  # ── Request checks (pure; no read) ──────────────────────────────────────

  defp check(conn, opts) do
    with :ok <- version(conn),
         :ok <- if(conn.request_path === opts.path, do: :ok, else: {:reject, :not_found}),
         :ok <- if(conn.method === "POST", do: :ok, else: {:reject, :method_not_allowed}),
         :ok <- absent(conn, "expect", :expectation_failed),
         :ok <- absent(conn, "transfer-encoding", :bad_request),
         {:ok, n} <- content_length(conn, opts.max_body),
         {:ok, auth} <- authorization(conn) do
      {:ok, n, auth}
    end
  end

  defp version(conn) do
    case contained(fn -> Plug.Conn.get_http_protocol(conn) end) do
      {:returned, :"HTTP/1.1"} -> :ok
      _ -> {:reject, :version_not_supported}
    end
  end

  defp absent(conn, name, code) do
    if Enum.any?(conn.req_headers, &match?({^name, _}, &1)), do: {:reject, code}, else: :ok
  end

  defp content_length(conn, max) do
    case values(conn, "content-length") do
      [] ->
        {:reject, :length_required}

      [v] when is_binary(v) and byte_size(v) in 1..@max_length_digits ->
        if digits?(v) do
          n = String.to_integer(v)
          if n <= max, do: {:ok, n}, else: {:reject, :payload_too_large}
        else
          {:reject, :bad_request}
        end

      _ ->
        {:reject, :bad_request}
    end
  end

  defp authorization(conn) do
    case values(conn, "authorization") do
      [v] when is_binary(v) -> {:ok, v}
      _ -> {:reject, :unauthenticated}
    end
  end

  defp values(conn, name), do: for({^name, v} <- conn.req_headers, do: v)

  defp digits?(<<>>), do: true
  defp digits?(<<c, rest::binary>>) when c in ?0..?9, do: digits?(rest)
  defp digits?(_), do: false

  # ── One Requester.run ───────────────────────────────────────────────────

  defp dispatch(conn, opts, n, auth, runner) do
    key = {__MODULE__, make_ref()}
    Process.put(key, new_slot(conn))

    {result, slot} =
      try do
        result =
          contained(fn ->
            runner.(opts.primary, auth, reader(key, n), {Writer, key}, opts.requester)
          end)

        {result, Process.get(key)}
      after
        Process.delete(key)
      end

    finish(result, slot)
  end

  defp finish(_result, slot) when not is_map(slot), do: abort!()

  defp finish({:returned, {:complete, ev, _private}}, slot) do
    if complete?(ev, slot), do: slot.conn, else: abort!()
  end

  defp finish({:returned, {:aborted, _why, _ev, _private}}, _slot), do: abort!()

  defp finish({:returned, {:no_write, why, _ev, _private}}, %{calls: 0} = slot),
    do: respond(slot.conn, code(why))

  defp finish({:returned, {:no_write, {:invalid, _}}}, %{calls: 0} = slot),
    do: respond(slot.conn, :internal)

  defp finish(_other, %{calls: 0} = slot), do: respond(slot.conn, :internal)
  defp finish(_other, _slot), do: abort!()

  defp complete?(%{header: :ok, terminal: :written, chunks: c}, slot) when is_integer(c) do
    slot.read == :ok and slot.header == :ok and slot.calls == slot.ok_calls and
      slot.ok_calls == c + 2 and
      chunked_conn?(slot.conn, slot.adapter, slot.owner)
  end

  defp complete?(_ev, _slot), do: false

  defp chunked_conn?(%Plug.Conn{state: :chunked, status: 200, adapter: {mod, _}} = c, mod, owner),
    do: c.owner === owner

  defp chunked_conn?(_conn, _mod, _owner), do: false

  # Fixed response codes from the log-safe `why` only.
  defp code({:start, {:ingress, :unauthenticated}}), do: :unauthenticated
  defp code({:start, {:ingress, r}}) when r in @forbidden, do: :forbidden
  defp code({:start, {:ingress, :no_route}}), do: :not_found
  defp code({:start, {:ingress, :body_too_large}}), do: :payload_too_large
  defp code({:start, {:ingress, :body_timeout}}), do: :request_timeout
  defp code({:start, {:ingress, :body_unavailable}}), do: :bad_request
  defp code({:start, {:ingress, :deadline_exceeded}}), do: :gateway_timeout
  defp code({:start, {:deadline_exceeded, :before_coordinator}}), do: :gateway_timeout
  defp code({:start, {:invalid, :opts}}), do: :internal
  defp code({:start, :invalid_config}), do: :internal
  defp code({:start, _}), do: :unavailable
  defp code({:outcome, {_, :admission_refused}}), do: :unavailable
  defp code({:outcome, {_, r}}) when r in @timeouts, do: :gateway_timeout
  defp code(:receive_timeout), do: :gateway_timeout
  defp code({stage, :deadline}) when stage in [:data, :terminal], do: :gateway_timeout
  defp code(_), do: :bad_gateway

  # ── Body reader ─────────────────────────────────────────────────────────

  defp reader(key, n), do: fn max, remaining -> read(key, n, max, remaining) end

  defp read(key, n, max, remaining) do
    case Process.get(key) do
      %{read: :not_called} = slot ->
        slot = %{slot | read: :called}
        Process.put(key, slot)

        cond do
          not (is_integer(max) and n <= max) -> read_failed(key, slot, :too_large)
          not (is_integer(remaining) and remaining > 0) -> read_failed(key, slot, :unavailable)
          true -> read_once(key, slot, n, remaining)
        end

      _ ->
        {:error, :unavailable}
    end
  end

  defp read_once(key, slot, n, remaining) do
    result =
      try do
        Plug.Conn.read_body(slot.conn, length: n, read_length: max(n, 1), read_timeout: remaining)
      rescue
        e -> if timeout_exception?(e), do: {:error, :timeout}, else: :failed
      catch
        _kind, _reason -> :failed
      end

    case result do
      {:ok, body, %Plug.Conn{} = conn} when is_binary(body) and byte_size(body) == n ->
        Process.put(key, %{slot | conn: conn, read: :ok})
        {:ok, body}

      {tag, _body, %Plug.Conn{} = conn} when tag in [:ok, :more] ->
        read_failed(key, %{slot | conn: conn}, :length)

      {:error, :timeout} ->
        read_failed(key, slot, :timeout)

      _ ->
        read_failed(key, slot, :unavailable)
    end
  end

  defp read_failed(key, slot, reason) do
    Process.put(key, %{slot | read: :failed})
    {:error, reason}
  end

  defp timeout_exception?(e) do
    contained(fn -> Plug.Exception.status(e) end) == {:returned, 408}
  end

  # ── Slot ────────────────────────────────────────────────────────────────

  defp new_slot(%Plug.Conn{adapter: {mod, _}, owner: owner} = conn) do
    %{
      conn: conn,
      adapter: mod,
      owner: owner,
      read: :not_called,
      header: :not_called,
      calls: 0,
      ok_calls: 0
    }
  end

  @doc false
  # Counts one writer call before its effect; returns the updated slot.
  def slot_begin({__MODULE__, ref} = key) when is_reference(ref) do
    case Process.get(key) do
      %{calls: c} = slot ->
        slot = %{slot | calls: c + 1}
        Process.put(key, slot)
        {:ok, slot}

      _ ->
        :error
    end
  end

  def slot_begin(_key), do: :error

  @doc false
  def slot_put({__MODULE__, ref} = key, slot) when is_reference(ref) and is_map(slot) do
    Process.put(key, slot)
    :ok
  end

  @doc false
  # The conn the stream header is sent from: adapter-owned headers only.
  def stream_conn(conn), do: owned(conn, @stream_headers)

  defp owned(%Plug.Conn{} = conn, headers) do
    %{
      conn
      | resp_headers: headers,
        resp_cookies: %{},
        resp_body: nil,
        private: Map.delete(conn.private, :before_send)
    }
  end

  # ── Endings ─────────────────────────────────────────────────────────────

  defp respond(conn, code) do
    {status, body} = Map.fetch!(@responses, code)

    case contained(fn -> Plug.Conn.send_resp(owned(conn, @fixed_headers), status, body) end) do
      {:returned, %Plug.Conn{state: :sent} = sent} -> sent
      _ -> abort!()
    end
  end

  # Plug's sentinel tells the server a response is already under way, so its
  # error path writes no status; the fresh fixed exception ends the request.
  defp abort! do
    send(self(), @sentinel)
    raise Aborted
  end

  # ── Helpers ─────────────────────────────────────────────────────────────

  defp contained(fun) do
    {:returned, fun.()}
  catch
    _kind, _reason -> :failed
  end

  defp normalized_path?("/" <> rest) when byte_size(rest) > 0 do
    rest
    |> :binary.split("/", [:global])
    |> Enum.all?(&segment?/1)
  end

  defp normalized_path?(_), do: false

  defp segment?(s) when s in ["", ".", ".."], do: false
  defp segment?(s), do: path_chars?(s)

  defp path_chars?(<<>>), do: true

  defp path_chars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c in ~c"-._~",
       do: path_chars?(rest)

  defp path_chars?(_), do: false

  defp requester_opts?(req) do
    Keyword.keyword?(req) and Keyword.keys(req) -- @requester_opt_keys == [] and
      length(Enum.uniq(Keyword.keys(req))) == length(req)
  end
end
