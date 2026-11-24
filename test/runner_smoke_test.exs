defmodule Telelem.RunnerSmokeTest do
  # TEL-001 runner smoke: the pinned Bandit server and Mint client complete a
  # verified TLS 1.3 chunked response over a loopback ephemeral port, offline.
  # Certificates are generated in memory per run; nothing is read from or
  # written to disk. Wire/SDK behaviour belongs to the fixture tests, not here.
  use ExUnit.Case, async: true

  defmodule ChunkedPlug do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, _opts) do
      conn = conn |> put_resp_content_type("text/event-stream") |> send_chunked(200)
      {:ok, conn} = chunk(conn, "data: one\n\n")
      {:ok, conn} = chunk(conn, "data: [DONE]\n\n")
      conn
    end
  end

  @san {:Extension, {2, 5, 29, 17}, false, [dNSName: ~c"localhost"]}

  setup do
    %{server_config: server, client_config: client} = test_pki()

    # start_link returns once the listener is bound, so the port is ready;
    # start_supervised stops the server when the test exits.
    pid =
      start_supervised!(
        {Bandit,
         plug: ChunkedPlug,
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false,
         thousand_island_options: [
           transport_options: [
             cert: server[:cert],
             key: server[:key],
             cacerts: server[:cacerts]
           ]
         ]}
      )

    {:ok, {{127, 0, 0, 1}, port}} = ThousandIsland.listener_info(pid)
    %{port: port, client_cacerts: client[:cacerts]}
  end

  test "Mint receives a chunked response from Bandit over verified TLS 1.3", ctx do
    {:ok, conn} = connect(ctx.port, ctx.client_cacerts)
    on_exit(fn -> Mint.HTTP.close(conn) end)

    assert {:ok, [protocol: :"tlsv1.3"]} =
             conn |> Mint.HTTP.get_socket() |> :ssl.connection_information([:protocol])

    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", "/v1/chat/completions", [], "{}")
    {conn, responses} = recv_until_done(conn, ref, [])
    {:ok, _conn} = Mint.HTTP.close(conn)

    assert {:status, ^ref, 200} = List.keyfind(responses, :status, 0)
    assert {:headers, ^ref, headers} = List.keyfind(responses, :headers, 0)
    assert {"transfer-encoding", "chunked"} in headers
    assert for({:data, ^ref, d} <- responses, into: "", do: d) == "data: one\n\ndata: [DONE]\n\n"
  end

  # OTP logs the expected unknown_ca alert at :notice; keep it out of the output.
  @tag :capture_log
  test "Mint rejects the server certificate when it does not trust the CA", ctx do
    %{client_config: other} = test_pki()

    assert {:error, %Mint.TransportError{reason: {:tls_alert, {:unknown_ca, _}}}} =
             connect(ctx.port, other[:cacerts])
  end

  defp connect(port, cacerts) do
    Mint.HTTP.connect(:https, "localhost", port,
      mode: :passive,
      protocols: [:http1],
      transport_opts: [
        cacerts: cacerts,
        verify: :verify_peer,
        versions: [:"tlsv1.3"],
        timeout: 5_000
      ]
    )
  end

  # Passive mode: explicit blocking reads with a deadline, no mailbox races.
  defp recv_until_done(conn, ref, acc) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)
    acc = acc ++ responses

    if Enum.any?(responses, &match?({:done, ^ref}, &1)),
      do: {conn, acc},
      else: recv_until_done(conn, ref, acc)
  end

  defp test_pki do
    opts = [digest: :sha256, key: {:rsa, 2048, 65537}]

    :public_key.pkix_test_data(%{
      server_chain: %{root: opts, peer: [{:extensions, [@san]} | opts]},
      client_chain: %{root: opts, peer: opts}
    })
  end
end
