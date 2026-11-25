# Throwaway boot inputs for the boot-validated cases of integration/m2a/route-store/run.
# Runs inside the runner image (offline): `elixir boot_fixture.exs OUT_DIR TENANT`.
#
# Writes OUT_DIR/{bootstrap.json,fixture-ca.pem,downstream-cert.pem,downstream-key.pem,
# manifest.json} and prints the manifest's SHA-256 on stdout. Everything is generated
# in memory per run with :public_key test helpers: a fixture CA, a downstream (leaf)
# certificate under it and the leaf's private key. None of it is a secret and none of
# it leaves the run's temp directory. Paths inside the documents are the container
# paths the writer sees (/sbx/boot, /sbx/store).
[out, tenant] = System.argv()

data =
  :public_key.pkix_test_data(%{
    server_chain: %{root: [], intermediates: [], peer: [key: {:namedCurve, :secp256r1}]},
    client_chain: %{root: [], intermediates: [], peer: []}
  })

srv = data[:server_config]
leaf = srv[:cert]
{key_type, key_der} = srv[:key]
ca = List.last(srv[:cacerts])

pem = fn entries ->
  :public_key.pem_encode(Enum.map(entries, fn {t, d} -> {t, d, :not_encrypted} end))
end

files = %{
  "fixture_ca" => {"fixture-ca.pem", pem.([{:Certificate, ca}])},
  "downstream_cert" => {"downstream-cert.pem", pem.([{:Certificate, leaf}])},
  "downstream_key" => {"downstream-key.pem", pem.([{key_type, key_der}])},
  "bootstrap" =>
    {"bootstrap.json",
     JSON.encode!(%{
       "version" => 2,
       "tenant_id" => tenant,
       "mode" => "sandbox",
       "fixture_targets" => %{
         "a" => %{"host" => "127.0.0.1", "port" => 18443, "ca_file" => "/sbx/boot/fixture-ca.pem"},
         "b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_file" => "/sbx/boot/fixture-ca.pem"}
       },
       "sandbox_store_root" => "/sbx/store",
       "sandbox_signer_ref" => "sandbox:local-fixture"
     })}
}

hex = fn b -> Base.encode16(:crypto.hash(:sha256, b), case: :lower) end

entries =
  Map.new(files, fn {role, {name, bin}} ->
    path = Path.join(out, name)
    File.write!(path, bin)
    File.chmod!(path, 0o600)
    {role, %{"bytes" => byte_size(bin), "sha256" => hex.(bin)}}
  end)

manifest =
  JSON.encode!(%{
    "version" => "telelem.route_store.boot_manifest.v1",
    "tenant_id" => tenant,
    "store_root" => "/sbx/store",
    "files" => entries
  })

File.write!(Path.join(out, "manifest.json"), manifest)
File.chmod!(Path.join(out, "manifest.json"), 0o600)
IO.puts(hex.(manifest))
