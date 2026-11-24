defmodule Telelem.Sandbox.BootstrapTest do
  use ExUnit.Case, async: true

  alias Telelem.Sandbox.{Bindings, Bootstrap, PathPolicy, VerifiedTarget}

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
        "version" => 2,
        "tenant_id" => "local",
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

    assert {:ok, %Bindings{mode: :sandbox, tenant_id: "local"} = b} =
             Bootstrap.from_map(doc(r), r.opts)

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

    assert {:error, :unsupported_version} = Bootstrap.from_map(doc(r, %{"version" => 1}), r.opts)

    # A version-1 document (no tenant) is rejected by shape.
    assert {:error, :unknown_or_missing_keys} =
             Bootstrap.from_map(
               doc(r) |> Map.delete("tenant_id") |> Map.put("version", 1),
               r.opts
             )

    for t <- ["", "a b", "../x", 1, nil, String.duplicate("t", 65)] do
      assert {:error, :invalid_tenant} = Bootstrap.from_map(doc(r, %{"tenant_id" => t}), r.opts)
    end

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

  # ── store root: filesystem checks after the pure policy ──────────────────

  test "store root symlink escape is rejected, leaf or intermediate", %{tmp_dir: d} do
    r = setup_roots(d)
    s = &doc(r, %{"sandbox_store_root" => &1})
    escape = Path.join(d, "escape")
    File.mkdir_p!(escape)

    File.ln_s!(escape, Path.join(r.sb, "link"))

    assert {:error, :store_root_symlink} =
             Bootstrap.from_map(s.(Path.join(r.sb, "link")), r.opts)

    assert {:error, :store_root_symlink} =
             Bootstrap.from_map(s.(Path.join([r.sb, "link", "inner"])), r.opts)

    # Points at a would-be production path: still only a symlink error, and
    # the string policy never saw a denied path, so nothing denied was walked.
    File.ln_s!("/var/lib/telelem/production/store", Path.join(r.sb, "prod"))

    assert {:error, :store_root_symlink} =
             Bootstrap.from_map(s.(Path.join(r.sb, "prod")), r.opts)

    # Dangling symlink is also a symlink, not "missing".
    File.ln_s!(Path.join(d, "nowhere"), Path.join(r.sb, "dangling"))

    assert {:error, :store_root_symlink} =
             Bootstrap.from_map(s.(Path.join(r.sb, "dangling")), r.opts)
  end

  test "store root must be an existing real directory strictly below the sandbox root",
       %{tmp_dir: d} do
    r = setup_roots(d)
    s = &doc(r, %{"sandbox_store_root" => &1})

    assert {:error, :store_root_unreadable} =
             Bootstrap.from_map(s.(Path.join(r.sb, "missing")), r.opts)

    File.write!(Path.join(r.sb, "file"), "x")

    assert {:error, :store_root_not_a_directory} =
             Bootstrap.from_map(s.(Path.join(r.sb, "file")), r.opts)

    assert {:error, :store_root_not_a_directory} =
             Bootstrap.from_map(s.(Path.join([r.sb, "file", "x"])), r.opts)

    assert {:error, :store_root_is_sandbox_root} = Bootstrap.from_map(s.(r.sb), r.opts)

    ww = Path.join(r.sb, "ww")
    File.mkdir_p!(Path.join(ww, "store"))
    # Sticky does not excuse a world-writable directory below the root.
    {_, 0} = System.cmd("chmod", ["1777", ww])

    assert {:error, :store_root_world_writable} =
             Bootstrap.from_map(s.(Path.join(ww, "store")), r.opts)

    File.chmod!(ww, 0o755)
    File.chmod!(Path.join(ww, "store"), 0o777)

    assert {:error, :store_root_world_writable} =
             Bootstrap.from_map(s.(Path.join(ww, "store")), r.opts)

    File.chmod!(Path.join(ww, "store"), 0o755)
    assert {:ok, _} = Bootstrap.from_map(s.(Path.join(ww, "store")), r.opts)
  end

  test "store root may not overlap the fixture root", %{tmp_dir: d} do
    r = setup_roots(d)
    # fixture root nested in the sandbox root
    fx = Path.join([r.sb, "store", "fx"])
    File.mkdir_p!(fx)
    File.write!(Path.join(fx, "ca.pem"), ca_pem())
    opts = [fixture_root: fx, sandbox_root: r.sb]
    dd = doc(%{r | fx: fx})

    assert {:error, :store_root_overlaps_fixture_root} = Bootstrap.from_map(dd, opts)

    assert {:error, :store_root_overlaps_fixture_root} =
             Bootstrap.from_map(Map.put(dd, "sandbox_store_root", Path.join(fx, "s")), opts)
  end

  test "from_file rejects a bootstrap inside the store root", %{tmp_dir: d} do
    r = setup_roots(d)
    p = Path.join([r.sb, "store", "bootstrap.json"])
    File.write!(p, JSON.encode!(doc(r)))
    assert {:error, :bootstrap_path_inside_store_root} = Bootstrap.from_file(p, r.opts)
  end

  test "from_file rejects symlinked or hard-linked bootstrap files", %{tmp_dir: d} do
    r = setup_roots(d)
    real = Path.join(d, "real.json")
    File.write!(real, JSON.encode!(doc(r)))

    File.ln_s!(real, Path.join(r.sb, "link.json"))

    assert {:error, :bootstrap_file_symlink} =
             Bootstrap.from_file(Path.join(r.sb, "link.json"), r.opts)

    File.ln!(real, Path.join(r.sb, "hard.json"))

    assert {:error, :bootstrap_file_hard_linked} =
             Bootstrap.from_file(Path.join(r.sb, "hard.json"), r.opts)

    assert {:error, :bootstrap_path_outside_sandbox_root} = Bootstrap.from_file(r.sb, r.opts)
  end

  # ── launcher roots ────────────────────────────────────────────────────────

  test "launcher roots must be real, non-world-writable directories", %{tmp_dir: d} do
    r = setup_roots(d)

    File.ln_s!(r.sb, Path.join(d, "sb-link"))

    assert {:error, :sandbox_root_symlink} =
             Bootstrap.from_map(
               doc(%{r | sb: Path.join(d, "sb-link")}),
               fixture_root: r.fx,
               sandbox_root: Path.join(d, "sb-link")
             )

    # A symlinked ancestor of the root is rejected as well.
    File.ln_s!(d, Path.join(d, "d-link"))
    fx2 = Path.join([d, "d-link", "fixture"])

    assert {:error, :fixture_root_symlink} =
             Bootstrap.from_map(doc(%{r | fx: fx2}), fixture_root: fx2, sandbox_root: r.sb)

    assert {:error, :fixture_root_unreadable} =
             Bootstrap.from_map(doc(r, %{"fixture_targets" => only_pem_target()}),
               fixture_root: Path.join(d, "nofx"),
               sandbox_root: r.sb
             )

    File.chmod!(r.fx, 0o777)
    assert {:error, :fixture_root_world_writable} = Bootstrap.from_map(doc(r), r.opts)
    File.chmod!(r.fx, 0o755)

    # World-writable ancestor: allowed only with the sticky bit (like /tmp).
    up = Path.join(d, "up")
    sb2 = Path.join(up, "sandbox")
    File.mkdir_p!(Path.join(sb2, "store"))
    File.chmod!(up, 0o777)
    opts2 = [fixture_root: r.fx, sandbox_root: sb2]
    assert {:error, :sandbox_root_world_writable} = Bootstrap.from_map(doc(%{r | sb: sb2}), opts2)
    # File.chmod/2 drops the sticky bit on this OTP; use chmod(1).
    {_, 0} = System.cmd("chmod", ["1777", up])
    assert {:ok, %File.Stat{mode: m}} = File.lstat(up)
    assert Bitwise.band(m, 0o1777) == 0o1777
    assert {:ok, _} = Bootstrap.from_map(doc(%{r | sb: sb2}), opts2)
    File.chmod!(up, 0o755)
  end

  test "options must be a keyword list", %{tmp_dir: d} do
    r = setup_roots(d)
    assert {:error, :invalid_options} = Bootstrap.from_map(doc(r), %{fixture_root: r.fx})
    assert {:error, :invalid_options} = Bootstrap.from_map(doc(r), [r.fx])

    assert {:error, :invalid_denied_roots} =
             Bootstrap.from_map(doc(r), r.opts ++ [denied_roots: ["x"]])
  end

  # ── ill-formed targets ────────────────────────────────────────────────────

  test "host must be the canonical text of a loopback IP", %{tmp_dir: d} do
    r = setup_roots(d)
    pem = ca_pem()
    t = fn host -> target(r, "a", %{"host" => host, "port" => 1, "ca_pem" => pem}) end

    for h <- ["0:0:0:0:0:0:0:1", "::0001", "0::1"] do
      assert {:error, {:non_canonical_host, "a"}} = Bootstrap.from_map(doc(r, t.(h)), r.opts)
    end

    for h <- [
          "127.0.0.01",
          "127.1",
          "0177.0.0.1",
          "::1%lo",
          " 127.0.0.1",
          "127.0.0.1\n",
          2_130_706_433,
          nil,
          <<0xFF>>
        ] do
      assert {:error, {:host_not_ip_literal, "a"}} = Bootstrap.from_map(doc(r, t.(h)), r.opts)
    end

    assert {:error, {:non_loopback_host, "a"}} = Bootstrap.from_map(doc(r, t.("0.0.0.0")), r.opts)
    assert {:error, {:non_loopback_host, "a"}} = Bootstrap.from_map(doc(r, t.("::")), r.opts)

    assert {:error, {:invalid_target, "a"}} =
             Bootstrap.from_map(doc(r, target(r, "a", "127.0.0.1:1")), r.opts)

    assert {:error, {:invalid_port, "a"}} =
             Bootstrap.from_map(
               doc(r, target(r, "a", %{"host" => "127.0.0.1", "port" => 1.0, "ca_pem" => pem})),
               r.opts
             )

    assert {:error, {:invalid_port, "a"}} =
             Bootstrap.from_map(
               doc(
                 r,
                 target(r, "a", %{"host" => "127.0.0.1", "port" => 65_536, "ca_pem" => pem})
               ),
               r.opts
             )
  end

  test "fixture CA text: certificate blocks and whitespace only, distinct certs", %{tmp_dir: d} do
    r = setup_roots(d)
    pem = ca_pem()
    t = &target(r, "a", %{"host" => "127.0.0.1", "port" => 1, "ca_pem" => &1})

    assert {:error, {:ca_pem_has_extra_content, "a"}} =
             Bootstrap.from_map(doc(r, t.("note: trust me\n" <> pem)), r.opts)

    assert {:error, {:ca_pem_has_extra_content, "a"}} =
             Bootstrap.from_map(doc(r, t.(pem <> "trailer")), r.opts)

    assert {:error, {:invalid_fixture_ca, "a"}} =
             Bootstrap.from_map(doc(r, t.(pem <> pem)), r.opts)

    # Line-ending and wrapping differences are fine.
    assert {:ok, _} = Bootstrap.from_map(doc(r, t.(String.replace(pem, "\n", "\r\n"))), r.opts)
    assert {:ok, _} = Bootstrap.from_map(doc(r, t.(pem <> "\n" <> ca_pem())), r.opts)

    assert {:error, {:invalid_ca_pem, "a"}} = Bootstrap.from_map(doc(r, t.(1)), r.opts)
  end

  test "hard-linked CA files are rejected", %{tmp_dir: d} do
    r = setup_roots(d)
    outside = Path.join(d, "outside-ca.pem")
    File.write!(outside, ca_pem())
    File.ln!(outside, Path.join(r.fx, "hard.pem"))

    f =
      target(r, "a", %{
        "host" => "127.0.0.1",
        "port" => 1,
        "ca_file" => Path.join(r.fx, "hard.pem")
      })

    assert {:error, {:ca_file_hard_linked, "a"}} = Bootstrap.from_map(doc(r, f), r.opts)
  end

  test "non-canonical DER and duplicate CAs fail VerifiedTarget.build" do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"x", [])
    assert {:ok, _} = VerifiedTarget.build("a", "127.0.0.1", 1, [der])

    assert {:error, :invalid_fixture_ca} =
             VerifiedTarget.build("a", "127.0.0.1", 1, [der <> <<0>>])

    assert {:error, :invalid_fixture_ca} = VerifiedTarget.build("a", "127.0.0.1", 1, [der, der])
    assert {:error, :non_canonical_host} = VerifiedTarget.build("a", "0:0:0:0:0:0:0:1", 1, [der])
    assert {:error, :invalid_port} = VerifiedTarget.build("a", "127.0.0.1", 0, [der])
  end

  test "rejected documents never reach the filesystem checks", %{tmp_dir: d} do
    # Roots that do not exist: a pure-policy failure must be reported before
    # the root lstat would fail with :unreadable.
    r = setup_roots(d)
    opts = [fixture_root: Path.join(d, "nofx"), sandbox_root: Path.join(d, "nosb")]
    rr = %{r | fx: opts[:fixture_root], sb: opts[:sandbox_root]}

    assert {:error, :store_root_denied} =
             Bootstrap.from_map(doc(rr, %{"sandbox_store_root" => "/var/lib/telelem/x"}), opts)

    assert {:error, {:ca_file_denied, "a"}} =
             Bootstrap.from_map(
               doc(
                 rr,
                 target(rr, "a", %{
                   "host" => "127.0.0.1",
                   "port" => 1,
                   "ca_file" => "/run/secrets/k"
                 })
               ),
               opts
             )

    assert {:error, {:non_loopback_host, "a"}} =
             Bootstrap.from_map(
               doc(
                 rr,
                 target(rr, "a", %{"host" => "192.0.2.1", "port" => 1, "ca_pem" => ca_pem()})
               ),
               opts
             )

    assert {:error, :invalid_signer_ref} =
             Bootstrap.from_map(doc(rr, %{"sandbox_signer_ref" => "file:/run/secrets/k"}), opts)

    assert {:error, :fixture_root_unreadable} = Bootstrap.from_map(doc(rr), opts)
  end

  test "PathPolicy.same_file? requires identical singly-linked regular inode", %{tmp_dir: d} do
    a = Path.join(d, "a")
    b = Path.join(d, "b")
    File.write!(a, "1")
    File.write!(b, "1")
    {:ok, sa} = File.lstat(a)
    {:ok, sb} = File.lstat(b)
    assert PathPolicy.same_file?(sa, sa)
    refute PathPolicy.same_file?(sa, sb)
    refute PathPolicy.same_file?(sa, %{sa | links: 2})
    refute PathPolicy.same_file?(sa, %{sa | type: :directory})
  end

  defp only_pem_target,
    do: %{"b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_pem" => ca_pem()}}
end
