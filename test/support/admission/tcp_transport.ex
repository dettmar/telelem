defmodule Telelem.Test.Admission.TcpTransport do
  @moduledoc """
  Test transport that holds a real loopback TCP socket (gen_tcp).

  The worker opens the socket in its own process and so owns it. The test
  holds the accepted peer, which sees `{:error, :closed}` on `recv/3` whether
  the worker closes the socket explicitly or exits. Wire lines:

      hello <label> <op_id>\\n     on checkout
      dispatch <request>\\n        on dispatch
  """
  @behaviour Telelem.Instance.Transport

  @impl true
  def checkout(%{port: port, label: label}, %{op_id: op_id}) do
    with {:ok, sock} <-
           :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :line], 1_000),
         :ok <- :gen_tcp.send(sock, "hello #{label} #{op_id}\n") do
      {:ok, sock}
    end
  end

  @impl true
  def dispatch(sock, request), do: :gen_tcp.send(sock, "dispatch #{request}\n")

  @impl true
  def close(sock) do
    :gen_tcp.close(sock)
    :ok
  end

  @doc "Listen on an ephemeral loopback port; returns `{listen_socket, port}`."
  def listen! do
    {:ok, l} =
      :gen_tcp.listen(0, [
        :binary,
        ip: {127, 0, 0, 1},
        active: false,
        packet: :line,
        reuseaddr: true
      ])

    {:ok, port} = :inet.port(l)
    {l, port}
  end

  @doc "Accept the next connection and read its hello line (bounded wait; failure detection only)."
  def accept!(listen, timeout \\ 1_000) do
    {:ok, peer} = :gen_tcp.accept(listen, timeout)
    {:ok, hello} = :gen_tcp.recv(peer, 0, timeout)
    {peer, String.trim_trailing(hello)}
  end
end
