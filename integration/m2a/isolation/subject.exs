# Isolation-harness subject: a gateway stand-in that exercises the real
# Telelem.Sandbox bootstrap/resolve code and a real Mint TLS client, until A1
# wires bindings into serving. The harness treats the subject as opaque and
# judges it only through external observers (strace, lanes, DNS sink, canary,
# sentinel hashes); the result file written here is a consistency check only.
#
#   elixir -pa EBIN... subject.exs good      BOOTSTRAP RESULT_JSON
#   elixir -pa EBIN... subject.exs misbehave CANARY_IP NONCE RESULT_JSON
#
# `misbehave` is the must-be-detected red control: it reads and writes
# production-shaped sentinels, makes a DNS lookup and dials the canary.

defmodule IsolationSubject do
  alias Telelem.Sandbox
  alias Telelem.Sandbox.{Bootstrap, Principal, VerifiedTarget}

  @opts [fixture_root: "/fx", sandbox_root: "/sbx"]
  @prod_config "/etc/telelem/production/telelem-prod-config.sentinel.json"
  @prod_store "/var/lib/telelem/production/store/telelem-prod-receipts.sentinel.db"
  @prod_key "/run/secrets/telelem-production/telelem-prod-signer.sentinel.key"

  def main(["good", bootstrap, out]) do
    {:ok, _} = Application.ensure_all_started(:ssl)
    {:ok, op} = Principal.new("local", "operator", :sandbox, Principal.permissions())
    {:ok, serving} = Principal.new("local", "operator", :serving, Principal.permissions())
    {:ok, reader} = Principal.new("local", "reader", :sandbox, [:receipt_read])

    {:ok, b} = Bootstrap.from_file(bootstrap, @opts)
    {:ok, good_doc} = File.read(bootstrap) |> then(fn {:ok, s} -> JSON.decode(s) end)

    steps = [
      {"bootstrap", "ok"},
      # fail-closed cases first: none of these may produce a lane/sink arrival
      {"resolve_missing_c", show(Sandbox.resolve(op, b, "c"))},
      {"resolve_serving_principal", show(Sandbox.resolve(serving, b, "a"))},
      {"resolve_without_dispatch", show(Sandbox.resolve(reader, b, "a"))},
      {"resolve_no_bindings", show(Sandbox.resolve(op, nil, "a"))},
      {"bootstrap_disallowed_target",
       show(Bootstrap.from_file("/sbx/bootstrap-disallowed.json", @opts))},
      {"bootstrap_prod_config_path", show(Bootstrap.from_file(@prod_config, @opts))},
      {"bootstrap_ca_is_prod_key",
       show(Bootstrap.from_map(put_target_ca(good_doc, "a", @prod_key), @opts))},
      {"bootstrap_prod_store_root",
       show(
         Bootstrap.from_map(
           Map.put(good_doc, "sandbox_store_root", "/var/lib/telelem/production/store"),
           @opts
         )
       )},
      {"bootstrap_prod_signer_ref",
       show(
         Bootstrap.from_map(Map.put(good_doc, "sandbox_signer_ref", "file:" <> @prod_key), @opts)
       )}
    ]

    # allowed fixture requests: exactly one per registered lane
    steps =
      steps ++ for id <- ["a", "b"], do: {"request_" <> id, request(Sandbox.resolve(op, b, id))}

    steps = steps ++ [{"resolve_missing_c_after", show(Sandbox.resolve(op, b, "c"))}]
    write(out, steps)
  end

  def main(["misbehave", canary_ip, nonce, out]) do
    {:ok, ip} = :inet.parse_strict_address(String.to_charlist(canary_ip))

    steps = [
      {"read_prod_config", show(File.read(@prod_config) |> ok_size())},
      {"write_prod_store", show(File.write(@prod_store, "red-subject write\n", [:append]))},
      # Explicit resolver query: the sandbox inetrc disables native/DNS lookup for
      # :inet, so the red control talks DNS directly to the resolv.conf server.
      {"dns_lookup",
       show(
         :inet_res.resolve(
           String.to_charlist("prod-provider-#{nonce}.isolation.invalid"),
           :in,
           :a,
           nameservers: [{{127, 0, 0, 53}, 53}],
           timeout: 2_000,
           retry: 1
         )
       )},
      {"dial_canary", dial(ip, nonce)}
    ]

    write(out, steps)
  end

  defp put_target_ca(doc, id, path) do
    t = doc["fixture_targets"][id] |> Map.drop(["ca_pem", "ca_file"]) |> Map.put("ca_file", path)
    put_in(doc, ["fixture_targets", id], t)
  end

  defp request({:ok, %VerifiedTarget{} = t}) do
    body =
      ~s({"model":"fixture","stream":true,"messages":[{"role":"user","content":"isolation probe"}]})

    with {:ok, conn} <-
           Mint.HTTP.connect(:https, t.host, t.port,
             mode: :passive,
             protocols: [:http1],
             transport_opts: VerifiedTarget.tls_opts(t) ++ [timeout: 5_000]
           ),
         {:ok, conn, ref} <-
           Mint.HTTP.request(
             conn,
             "POST",
             "/v1/chat/completions",
             [
               {"content-type", "application/json"},
               {"authorization", "Bearer telelem-sandbox-dummy-not-a-secret"}
             ],
             body
           ),
         {:ok, conn, status, data} <- drain(conn, ref, nil, "") do
      Mint.HTTP.close(conn)

      if status == 200 and String.contains?(data, "data: [DONE]"),
        do: "done",
        else: "bad_response:#{status}"
    else
      {:error, %{__exception__: true} = e} -> "error:" <> inspect(Map.get(e, :reason))
      {:error, _conn, e} -> "error:" <> inspect(Map.get(e, :reason, e))
      other -> "error:" <> inspect(other)
    end
  end

  defp request(denied), do: show(denied)

  defp drain(conn, ref, status, acc) do
    case Mint.HTTP.recv(conn, 0, 5_000) do
      {:ok, conn, resps} ->
        {status, acc, done} =
          Enum.reduce(resps, {status, acc, false}, fn
            {:status, ^ref, s}, {_, a, d} -> {s, a, d}
            {:data, ^ref, x}, {s, a, d} -> {s, a <> x, d}
            {:done, ^ref}, {s, a, _} -> {s, a, true}
            _, x -> x
          end)

        if done, do: {:ok, conn, status, acc}, else: drain(conn, ref, status, acc)

      {:error, _conn, e, _} ->
        {:error, e}
    end
  end

  defp dial(ip, nonce) do
    case :gen_tcp.connect(ip, 443, [:binary, active: false], 2_000) do
      {:ok, s} ->
        :ok = :gen_tcp.send(s, "TELELEM-RED-SUBJECT #{nonce}\n")
        r = :gen_tcp.recv(s, 0, 3_000)
        :gen_tcp.close(s)
        "connected:" <> show(r)

      {:error, r} ->
        "error:#{r}"
    end
  end

  defp ok_size({:ok, bin}), do: {:ok, byte_size(bin)}
  defp ok_size(e), do: e

  # Bounded, content-free outcome strings.
  defp show({:ok, %Telelem.Sandbox.Bindings{}}), do: "ok"
  defp show({:ok, %VerifiedTarget{deployment_id: id}}), do: "ok:" <> id
  defp show({:ok, n}) when is_integer(n), do: "ok"
  defp show({:ok, "OK\n"}), do: "ack"
  defp show({:ok, _}), do: "ok"
  defp show(:ok), do: "ok"
  defp show({:denied, r}), do: "denied:#{r}"
  defp show({:error, {r, id}}) when is_atom(r), do: "error:#{r}:#{id}"
  defp show({:error, r}) when is_atom(r), do: "error:#{r}"
  defp show(other), do: "other:" <> String.slice(inspect(other), 0, 80)

  defp write(out, steps) do
    File.write!(out, JSON.encode!(Map.new(steps)))
  end
end

IsolationSubject.main(System.argv())
