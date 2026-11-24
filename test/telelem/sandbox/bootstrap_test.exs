defmodule Telelem.Sandbox.BootstrapTest do
  use ExUnit.Case, async: true

  alias Telelem.Sandbox.{Bindings, Bootstrap, VerifiedTarget}

  @moduletag :tmp_dir

  # Throwaway material, generated in memory per test; not secrets.
  defp ca_pem do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem sandbox test CA", [])
    :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
  end

  defp leaf_pem do
    data =
      :public_key.pkix_test_data(%{
        server_chain: %{root: [], intermediates: [], peer: []},
        client_chain: %{root: [], intermediates: [], peer: []}
      })

    der = data[:server_config][:cert]
    :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
  end

  defp key_pem do
    k = :public_key.generate_key({:namedCurve, :secp256r1})
    :public_key.pem_encode([:public_key.pem_entry_encode(:ECPrivateKey, k)])
  end

  defp setup_roots(dir) do
    fx = Path.join(dir, "fixture")
    sb = Path.join(dir, "sandbox")
    File.mkdir_p!(fx)
    File.mkdir_p!(Path.join(sb, "store"))
    File.write!(Path.join(fx, "ca.pem"), ca_pem())
    %{fx: fx, sb: sb, opts: [fixture_root: fx, sandbox_root: sb]}
  end

  defp doc(r, overrides \\ %{}) do
    Map.merge(
      %{
        "version" => 1,
        "mode" => "sandbox",
        "fixture_targets" => %{
          "a" => %{"host" => "127.0.0.1", "port" => 18443, "ca_file" => Path.join(r.fx, "ca.pem")},
          "b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_pem" => ca_pem()}
        },
        "sandbox_store_root" => Path.join(r.sb, "store"),
        "sandbox_signer_ref" => "sandbox:local-fixture"
      },
      overrides
    )
  end

  defp target(r, id, t), do: %{"fixture_targets" => Map.put(doc(r)["fixture_targets"], id, t)}

  test "contract example: two explicit fixture targets with fixture CAs", %{tmp_dir: d} do
    r = setup_roots(d)
    assert {:ok, %Bindings{mode: :sandbox} = b} = Bootstrap.from_map(doc(r), r.opts)

    assert %{"a" => %VerifiedTarget{port: 18443, address: {127, 0, 0, 1}} = a, "b" => _} =
             b.fixture_targets

    assert [_] = a.cacerts
    assert VerifiedTarget.tls_opts(a)[:verify] == :verify_peer

    assert Keyword.keys(VerifiedTarget.tls_opts(a)) |> Enum.sort() == [
             :cacerts,
             :verify,
             :versions
           ]
  end

  test "from_file reads a bootstrap below the sandbox root", %{tmp_dir: d} do
    r = setup_roots(d)
    p = Path.join(r.sb, "bootstrap.json")
    File.write!(p, JSON.encode!(doc(r)))
    assert {:ok, %Bindings{}} = Bootstrap.from_file(p, r.opts)
  end

  test "launcher roots are required and may not be production-shaped", %{tmp_dir: d} do
    r = setup_roots(d)
    assert {:error, :missing_fixture_root} = Bootstrap.from_map(doc(r), sandbox_root: r.sb)
    assert {:error, :missing_sandbox_root} = Bootstrap.from_map(doc(r), fixture_root: r.fx)

    assert {:error, :missing_fixture_root} =
             Bootstrap.from_map(doc(r), fixture_root: "/", sandbox_root: r.sb)

    assert {:error, :fixture_root_denied} =
             Bootstrap.from_map(doc(r), fixture_root: "/etc", sandbox_root: r.sb)

    assert {:error, :sandbox_root_denied} =
             Bootstrap.from_map(doc(r), fixture_root: r.fx, sandbox_root: "/run/secrets/x")

    assert {:error, :fixture_root_denied} =
             Bootstrap.from_map(doc(r),
               fixture_root: r.fx,
               sandbox_root: r.sb,
               denied_roots: [r.fx]
             )
  end

  test "document shape is exact", %{tmp_dir: d} do
    r = setup_roots(d)

    assert {:error, :unknown_or_missing_keys} =
             Bootstrap.from_map(Map.put(doc(r), "resolver", "prod"), r.opts)

    assert {:error, :unknown_or_missing_keys} =
             Bootstrap.from_map(Map.delete(doc(r), "sandbox_signer_ref"), r.opts)

    assert {:error, :unsupported_version} = Bootstrap.from_map(doc(r, %{"version" => 2}), r.opts)

    assert {:error, :mode_not_sandbox} =
             Bootstrap.from_map(doc(r, %{"mode" => "serving"}), r.opts)

    assert {:error, :invalid_fixture_targets} =
             Bootstrap.from_map(doc(r, %{"fixture_targets" => %{}}), r.opts)

    assert {:error, :bootstrap_not_json_object} = Bootstrap.from_map([], r.opts)
  end

  test "signer ref must be a sandbox reference, never a path or other store", %{tmp_dir: d} do
    r = setup_roots(d)

    for ref <- [
          "file:/run/secrets/signer.key",
          "env:SIGNER",
          "vault:prod",
          "sandbox:",
          "sandbox:a/b",
          "/k",
          1
        ] do
      assert {:error, :invalid_signer_ref} =
               Bootstrap.from_map(doc(r, %{"sandbox_signer_ref" => ref}), r.opts)
    end
  end

  test "store root must be normalized, inside sandbox root, outside denied roots", %{tmp_dir: d} do
    r = setup_roots(d)
    s = &doc(r, %{"sandbox_store_root" => &1})

    assert {:error, :store_root_denied} =
             Bootstrap.from_map(s.("/var/lib/telelem/production/store"), r.opts)

    assert {:error, :store_root_denied} = Bootstrap.from_map(s.("/var"), r.opts)

    assert {:error, :store_root_outside_sandbox_root} =
             Bootstrap.from_map(s.("/tmp/elsewhere"), r.opts)

    assert {:error, :invalid_store_root} =
             Bootstrap.from_map(s.(r.sb <> "/store/../../x"), r.opts)

    assert {:error, :invalid_store_root} = Bootstrap.from_map(s.("relative"), r.opts)
  end

  test "targets: loopback IP literals only, valid ports, distinct endpoints", %{tmp_dir: d} do
    r = setup_roots(d)
    pem = ca_pem()
    t = fn host, port -> %{"host" => host, "port" => port, "ca_pem" => pem} end

    assert {:error, {:non_loopback_host, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("10.0.0.1", 443))), r.opts)

    assert {:error, {:non_loopback_host, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("::ffff:127.0.0.1", 443))), r.opts)

    assert {:error, {:host_not_ip_literal, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("localhost", 443))), r.opts)

    assert {:error, {:host_not_ip_literal, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("api.example.invalid", 443))), r.opts)

    assert {:error, {:invalid_port, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("127.0.0.1", 0))), r.opts)

    assert {:error, {:invalid_port, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("127.0.0.1", "443"))), r.opts)

    assert {:ok, _} = Bootstrap.from_map(doc(r, target(r, "a", t.("::1", 443))), r.opts)

    assert {:error, :duplicate_fixture_endpoint} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("127.0.0.1", 18444))), r.opts)

    assert {:error, {:invalid_deployment_id, :invalid}} =
             Bootstrap.from_map(doc(r, target(r, "bad id", t.("127.0.0.1", 1))), r.opts)

    assert {:error, {:unknown_or_missing_target_keys, "a"}} =
             Bootstrap.from_map(
               doc(r, target(r, "a", Map.put(t.("127.0.0.1", 1), "ca_file", "/x"))),
               r.opts
             )
  end

  test "fixture CA: certificates only, CA basic constraint, no OS store fallback", %{tmp_dir: d} do
    r = setup_roots(d)
    t = fn pem -> %{"host" => "127.0.0.1", "port" => 1, "ca_pem" => pem} end

    assert {:error, {:invalid_fixture_ca, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.(""))), r.opts)

    assert {:error, {:invalid_fixture_ca, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.("garbage"))), r.opts)

    assert {:error, {:invalid_fixture_ca, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.(leaf_pem()))), r.opts)

    assert {:error, {:ca_file_has_non_certificate, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", t.(ca_pem() <> key_pem()))), r.opts)
  end

  test "ca_file is policy-checked before any open", %{tmp_dir: d} do
    r = setup_roots(d)
    f = &target(r, "a", %{"host" => "127.0.0.1", "port" => 1, "ca_file" => &1})

    # These paths do not exist here; the point is the policy error, not :unreadable.
    assert {:error, {:ca_file_denied, "a"}} =
             Bootstrap.from_map(doc(r, f.("/run/secrets/telelem-production/signer.key")), r.opts)

    assert {:error, {:ca_file_outside_fixture_root, "a"}} =
             Bootstrap.from_map(doc(r, f.("/tmp/ca.pem")), r.opts)

    assert {:error, {:invalid_ca_file, "a"}} =
             Bootstrap.from_map(doc(r, f.(r.fx <> "/../fixture/ca.pem")), r.opts)

    File.write!(Path.join(r.fx, "key.pem"), key_pem())

    assert {:error, {:ca_file_has_non_certificate, "a"}} =
             Bootstrap.from_map(doc(r, f.(Path.join(r.fx, "key.pem"))), r.opts)

    outside = Path.join(d, "outside.pem")
    File.write!(outside, ca_pem())
    File.ln_s!(outside, Path.join(r.fx, "link.pem"))

    assert {:error, {:ca_file_symlink, "a"}} =
             Bootstrap.from_map(doc(r, f.(Path.join(r.fx, "link.pem"))), r.opts)

    File.mkdir_p!(Path.join(d, "realdir"))
    File.write!(Path.join([d, "realdir", "ca.pem"]), ca_pem())
    File.ln_s!(Path.join(d, "realdir"), Path.join(r.fx, "dirlink"))

    assert {:error, {:ca_file_symlink, "a"}} =
             Bootstrap.from_map(doc(r, f.(Path.join([r.fx, "dirlink", "ca.pem"]))), r.opts)

    assert {:error, {:ca_file_unreadable, "a"}} =
             Bootstrap.from_map(doc(r, f.(Path.join(r.fx, "missing.pem"))), r.opts)
  end

  test "bootstrap file location is policy-checked before any open", %{tmp_dir: d} do
    r = setup_roots(d)

    assert {:error, :bootstrap_path_outside_sandbox_root} =
             Bootstrap.from_file("/etc/telelem/production/config.json", r.opts)

    assert {:error, :bootstrap_path_outside_sandbox_root} =
             Bootstrap.from_file(Path.join(d, "x.json"), r.opts)

    p = Path.join(r.sb, "bad.json")
    File.write!(p, "[1]")
    assert {:error, :bootstrap_not_json_object} = Bootstrap.from_file(p, r.opts)
    File.write!(p, String.duplicate(" ", 70_000))
    assert {:error, :bootstrap_file_too_large} = Bootstrap.from_file(p, r.opts)
  end
end
