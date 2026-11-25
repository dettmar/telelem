Code.require_file("../../scripts/route-store/boot_check.ex", __DIR__)

defmodule RouteStore.BootCheckTest do
  # Listener-free checks of the boot-validated preflight (scripts/route-store/boot_check.ex).
  # Throwaway certificates and keys are generated in memory per test; none is a secret.
  # The in-container path (real env, resolver, mounts, lock, FileStore) is covered by
  # integration/m2a/route-store/run case 8.
  use ExUnit.Case, async: true

  import Bitwise

  alias RouteStore.BootCheck

  @boot_mounts_sh Path.expand("../../scripts/route-store/boot_mounts.sh", __DIR__)

  @moduletag :tmp_dir

  ## staging helpers

  defp chain(key \\ {:namedCurve, :secp256r1}) do
    data =
      :public_key.pkix_test_data(%{
        server_chain: %{root: [], intermediates: [], peer: [key: key]},
        client_chain: %{root: [], intermediates: [], peer: []}
      })

    s = data[:server_config]
    {kt, kd} = s[:key]
    %{ca: List.last(s[:cacerts]), leaf: s[:cert], key: {kt, kd}}
  end

  defp pem(entries),
    do: :public_key.pem_encode(Enum.map(entries, fn {t, d} -> {t, d, :not_encrypted} end))

  defp roots(d) do
    sb = Path.join(d, "sbx")
    boot = Path.join(sb, "boot")
    store = Path.join(sb, "store")
    File.mkdir_p!(boot)
    File.mkdir_p!(store)
    File.chmod!(sb, 0o755)
    File.chmod!(boot, 0o700)
    File.chmod!(store, 0o700)
    %{sb: sb, boot: boot, store: store}
  end

  defp bootstrap_doc(r, over \\ %{}) do
    ca = Path.join(r.boot, "fixture-ca.pem")

    Map.merge(
      %{
        "version" => 2,
        "tenant_id" => "local",
        "mode" => "sandbox",
        "fixture_targets" => %{
          "a" => %{"host" => "127.0.0.1", "port" => 18443, "ca_file" => ca},
          "b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_file" => ca}
        },
        "sandbox_store_root" => r.store,
        "sandbox_signer_ref" => "sandbox:local-fixture"
      },
      over
    )
  end

  # Stage a valid boot directory. `files` overrides role bytes; `man` edits the manifest
  # map before it is written. Returns {manifest_sha256, manifest_map}.
  defp stage(r, opts \\ []) do
    c = Keyword.get_lazy(opts, :chain, fn -> chain() end)

    bins =
      Map.merge(
        %{
          "bootstrap" => JSON.encode!(bootstrap_doc(r)),
          "fixture_ca" => pem([{:Certificate, c.ca}]),
          "downstream_cert" => pem([{:Certificate, c.leaf}]),
          "downstream_key" => pem([c.key])
        },
        Keyword.get(opts, :files, %{})
      )

    entries =
      for {role, bin} <- bins, into: %{} do
        path = Path.join(r.boot, BootCheck.roles()[role])
        File.write!(path, bin)
        File.chmod!(path, 0o600)
        {role, %{"bytes" => byte_size(bin), "sha256" => hex(bin)}}
      end

    man =
      %{
        "version" => BootCheck.manifest_version(),
        "tenant_id" => "local",
        "store_root" => r.store,
        "files" => entries
      }
      |> Keyword.get(opts, :man, & &1).()

    mbin = JSON.encode!(man)
    File.write!(Path.join(r.boot, "manifest.json"), mbin)
    File.chmod!(Path.join(r.boot, "manifest.json"), 0o600)
    {hex(mbin), man}
  end

  defp hex(b), do: Base.encode16(:crypto.hash(:sha256, b), case: :lower)

  defp check(r, sha, over \\ []) do
    BootCheck.inputs(
      Keyword.merge(
        [
          sandbox_root: r.sb,
          boot_dir: r.boot,
          store_dir: r.store,
          tenant: "local",
          manifest_sha256: sha
        ],
        over
      )
    )
  end

  defp refused(result) do
    assert {:error, code} = result
    assert code in BootCheck.codes()
    code
  end

  ## boot inputs

  test "positive: staged inputs verify; summary carries digests only", %{tmp_dir: d} do
    r = roots(d)
    c = chain()
    {sha, man} = stage(r, chain: c)

    assert {:ok, s} = check(r, sha)
    assert s.manifest_sha256 == sha
    assert s.tenant_id == "local"
    assert s.deployments == ["a", "b"]
    assert s.fixture_ca_sha256 == man["files"]["fixture_ca"]["sha256"]
    assert s.downstream_cert_sha256 == hex(c.leaf)
    {_kt, kd} = c.key
    refute inspect(s) =~ Base.encode16(kd, case: :lower)

    assert Map.keys(s) |> Enum.sort() ==
             [
               :deployments,
               :downstream_cert_sha256,
               :fixture_ca_sha256,
               :manifest_sha256,
               :tenant_id
             ]
  end

  test "positive: an RSA-2048 downstream key also matches in memory", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r, chain: chain({:rsa, 2048, 65537}))
    assert {:ok, _} = check(r, sha)
  end

  test "RSA size is the modulus's bit length: 2047 bits in 256 bytes is refused", %{tmp_dir: d} do
    c = chain({:rsa, 2047, 65537})
    {:RSAPrivateKey, kd} = c.key
    n = :public_key.der_decode(:RSAPrivateKey, kd) |> elem(2)
    # The boundary case a byte-width check would let through.
    assert byte_size(:binary.encode_unsigned(n)) == 256
    assert n >= 1 <<< 2046 and n < 1 <<< 2047

    r = roots(d)
    {sha, _} = stage(r, chain: c)
    assert refused(check(r, sha)) == :downstream_key_unsupported
  end

  test "manifest digest comes from the caller and must match", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    assert refused(check(r, String.duplicate("0", 64))) == :manifest_digest_mismatch
    assert refused(check(r, String.upcase(sha))) == :bad_manifest_digest_arg
    assert refused(check(r, nil)) == :bad_manifest_digest_arg
  end

  test "boot directory is closed: route.rec, stray or missing files are refused", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    File.write!(Path.join(r.boot, "route.rec"), "x")
    assert refused(check(r, sha)) == :boot_dir_not_closed
    File.rm!(Path.join(r.boot, "route.rec"))
    File.write!(Path.join(r.boot, ".hidden"), "x")
    assert refused(check(r, sha)) == :boot_dir_not_closed
    File.rm!(Path.join(r.boot, ".hidden"))
    File.rm!(Path.join(r.boot, "downstream-key.pem"))
    assert refused(check(r, sha)) == :boot_dir_not_closed
  end

  test "boot directory must be below the sandbox root and apart from the store", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    assert refused(check(r, sha, boot_dir: r.store)) == :boot_dir_policy
    assert refused(check(r, sha, boot_dir: r.sb)) == :boot_dir_policy
    assert refused(check(r, sha, boot_dir: Path.join(r.store, "boot"))) == :boot_dir_policy
    assert refused(check(r, sha, tenant: "bad tenant")) == :boot_dir_policy
  end

  test "manifest shape is closed and versioned", %{tmp_dir: d} do
    cases = [
      {&Map.put(&1, "extra", 1), :manifest_keys},
      {&Map.delete(&1, "store_root"), :manifest_keys},
      {&Map.put(&1, "version", "telelem.route_store.boot_manifest.v0"), :manifest_version},
      {&Map.put(&1, "tenant_id", "other"), :manifest_tenant},
      {&Map.put(&1, "store_root", "/sbx/store"), :manifest_store_root},
      {&put_in(&1, ["files", "route_record"], %{
         "bytes" => 1,
         "sha256" => String.duplicate("0", 64)
       }), :manifest_roles},
      {&update_in(&1, ["files"], fn f -> Map.delete(f, "downstream_key") end), :manifest_roles},
      {&put_in(&1, ["files", "bootstrap", "bytes"], 0), :manifest_entry},
      {&put_in(&1, ["files", "bootstrap", "sha256"], "ABC"), :manifest_entry},
      {&put_in(&1, ["files", "bootstrap", "path"], "/etc/passwd"), :manifest_entry}
    ]

    for {{edit, code}, i} <- Enum.with_index(cases) do
      r = roots(Path.join(d, "c#{i}"))
      {sha, _} = stage(r, man: edit)
      assert refused(check(r, sha)) == code, "case #{i}"
    end
  end

  test "manifest that is not JSON or too large", %{tmp_dir: d} do
    r = roots(d)
    stage(r)
    p = Path.join(r.boot, "manifest.json")
    File.write!(p, "not json")
    assert refused(check(r, hex("not json"))) == :manifest_not_json
    big = String.duplicate(" ", 5000)
    File.write!(p, big)
    assert refused(check(r, hex(big))) == :manifest_size
  end

  test "each file must have exactly the manifested length and digest", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    p = Path.join(r.boot, "bootstrap.json")
    orig = File.read!(p)
    File.write!(p, String.replace(orig, "18443", "18445"))
    assert refused(check(r, sha)) == :file_digest_mismatch
    File.write!(p, orig <> " ")
    assert refused(check(r, sha)) == :file_size_mismatch
    File.write!(p, binary_part(orig, 0, byte_size(orig) - 1))
    assert refused(check(r, sha)) == :file_size_mismatch
    File.write!(p, orig)
    assert {:ok, _} = check(r, sha)
  end

  test "symlinked or hard-linked inputs and a readable key are refused", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    key = Path.join(r.boot, "downstream-key.pem")
    File.chmod!(key, 0o640)
    assert refused(check(r, sha)) == :file_mode
    File.chmod!(key, 0o600)

    ca = Path.join(r.boot, "fixture-ca.pem")
    real = Path.join(d, "real-ca.pem")
    File.rename!(ca, real)
    File.ln_s!(real, ca)
    assert refused(check(r, sha)) == :file_unsafe
    File.rm!(ca)
    File.ln!(real, ca)
    assert refused(check(r, sha)) == :file_unsafe
  end

  test "unsafe boot directory, unreadable manifest or file", %{tmp_dir: d} do
    r = roots(d)
    {sha, _} = stage(r)
    File.chmod!(r.boot, 0o777)
    assert refused(check(r, sha)) == :boot_dir_unsafe
    File.chmod!(r.boot, 0o700)

    m = Path.join(r.boot, "manifest.json")
    File.chmod!(m, 0o000)
    assert refused(check(r, sha)) == :manifest_unreadable
    File.chmod!(m, 0o600)

    ca = Path.join(r.boot, "fixture-ca.pem")
    File.chmod!(ca, 0o000)
    assert refused(check(r, sha)) == :file_unreadable
    File.chmod!(ca, 0o600)
    assert {:ok, _} = check(r, sha)

    r2 = roots(Path.join(d, "linked"))
    {sha2, _} = stage(r2)
    File.rename!(r2.boot, r2.boot <> ".real")
    File.ln_s!(r2.boot <> ".real", r2.boot)
    assert refused(check(r2, sha2)) == :boot_dir_unsafe
  end

  test "bootstrap tenant, store root and CA source are bound", %{tmp_dir: d} do
    cases = [
      {%{"tenant_id" => "other"}, :bootstrap_tenant},
      {%{"version" => 1}, :bootstrap_rejected},
      {%{"sandbox_signer_ref" => "file:/k"}, :bootstrap_rejected}
    ]

    for {{over, code}, i} <- Enum.with_index(cases) do
      r = roots(Path.join(d, "t#{i}"))
      {sha, _} = stage(r, files: %{"bootstrap" => JSON.encode!(bootstrap_doc(r, over))})
      assert refused(check(r, sha)) == code, "case #{i}"
    end

    r = roots(Path.join(d, "store"))
    doc = bootstrap_doc(r, %{"sandbox_store_root" => Path.join(r.sb, "other")})
    File.mkdir_p!(Path.join(r.sb, "other"))
    {sha, _} = stage(r, files: %{"bootstrap" => JSON.encode!(doc)})
    assert refused(check(r, sha)) == :bootstrap_store_root

    c = chain()
    r = roots(Path.join(d, "inline"))
    inline = %{"host" => "127.0.0.1", "port" => 18444, "ca_pem" => pem([{:Certificate, c.ca}])}
    doc = bootstrap_doc(r) |> put_in(["fixture_targets", "b"], inline)
    {sha, _} = stage(r, chain: c, files: %{"bootstrap" => JSON.encode!(doc)})
    assert refused(check(r, sha)) == :bootstrap_ca_source

    r = roots(Path.join(d, "otherca"))

    doc =
      bootstrap_doc(r)
      |> put_in(["fixture_targets", "a", "ca_file"], Path.join(r.boot, "manifest.json"))

    {sha, _} = stage(r, files: %{"bootstrap" => JSON.encode!(doc)})
    assert refused(check(r, sha)) == :bootstrap_ca_source

    r = roots(Path.join(d, "array"))
    {sha, _} = stage(r, files: %{"bootstrap" => "[]"})
    assert refused(check(r, sha)) == :bootstrap_not_json
  end

  test "fixture CA role must hold a CA that Bootstrap accepts", %{tmp_dir: d} do
    r = roots(d)
    c = chain()
    {sha, _} = stage(r, chain: c, files: %{"fixture_ca" => pem([{:Certificate, c.leaf}])})
    assert refused(check(r, sha)) == :bootstrap_rejected
  end

  test "downstream cert and key: roles are strict and must match", %{tmp_dir: d} do
    c1 = chain()
    c2 = chain()

    cases = [
      {%{"downstream_key" => pem([c2.key])}, :downstream_key_mismatch},
      {%{"downstream_cert" => pem([{:Certificate, c1.ca}])}, :downstream_cert_is_ca},
      {%{"downstream_key" => pem([{:Certificate, c1.leaf}])}, :downstream_key_invalid},
      {%{"downstream_cert" => pem([c1.key])}, :downstream_cert_invalid},
      {%{"downstream_cert" => pem([{:Certificate, c1.leaf}, {:Certificate, c1.ca}])},
       :downstream_cert_invalid},
      {%{"downstream_cert" => pem([{:Certificate, c1.leaf}]) <> "junk"},
       :downstream_cert_invalid},
      {%{"downstream_key" => pem([c1.key]) <> "junk"}, :downstream_key_invalid},
      {%{"downstream_key" => pem([c1.key, c1.key])}, :downstream_key_invalid}
    ]

    for {{files, code}, i} <- Enum.with_index(cases) do
      r = roots(Path.join(d, "k#{i}"))
      {sha, _} = stage(r, chain: c1, files: files)
      assert refused(check(r, sha)) == code, "case #{i}"
    end

    r = roots(Path.join(d, "ed"))
    ed = :public_key.generate_key({:namedCurve, :ed25519})
    ed_pem = :public_key.pem_encode([:public_key.pem_entry_encode(:PrivateKeyInfo, ed)])
    {sha, _} = stage(r, chain: c1, files: %{"downstream_key" => ed_pem})
    assert refused(check(r, sha)) == :downstream_key_unsupported
  end

  ## environment

  defp good_env(root \\ "/usr/local/lib/erlang") do
    bindir = "#{root}/erts-#{:erlang.system_info(:version)}/bin"

    %{
      "BINDIR" => bindir,
      "EMU" => "beam",
      "ERL_CRASH_DUMP_SECONDS" => "0",
      "ERL_INETRC" => "/opt/route-store/inetrc",
      "HOME" => "/tmp",
      "LANG" => "C.UTF-8",
      "OLDPWD" => "/tmp/ebin",
      "PATH" => "#{bindir}:#{root}/bin:/usr/local/bin:/usr/bin:/bin",
      "PROGNAME" => "erl",
      "PWD" => "/",
      "ROOTDIR" => root,
      "SHLVL" => "0",
      "TELELEM_ROUTE_STORE_LOCK_DIR" => "/sbx/store",
      "TELELEM_ROUTE_STORE_LOCK_FD" => "11"
    }
  end

  test "env: exactly the fixed set" do
    root = "/usr/local/lib/erlang"
    assert BootCheck.env(good_env(), root) == :ok

    for bad <- [
          Map.put(good_env(), "ERL_LIBS", "/x"),
          Map.put(good_env(), "ELIXIR_ERL_OPTIONS", "-x"),
          Map.put(good_env(), "HOSTNAME", "h"),
          Map.delete(good_env(), "ERL_INETRC"),
          Map.put(good_env(), "ERL_INETRC", "/tmp/inetrc"),
          Map.put(good_env(), "ERL_CRASH_DUMP_SECONDS", "10"),
          Map.put(good_env(), "PATH", "/tmp:/usr/bin"),
          Map.put(good_env(), "TELELEM_ROUTE_STORE_LOCK_FD", "x"),
          Map.delete(good_env(), "TELELEM_ROUTE_STORE_LOCK_FD"),
          :not_a_map
        ] do
      assert BootCheck.env(bad, root) == {:error, :env_unexpected}
    end
  end

  test "resolver: this test VM has no pinned ERL_INETRC, so it is refused" do
    assert BootCheck.resolver(%{}) == {:error, :resolver_unexpected}
    assert BootCheck.resolver(%{"ERL_INETRC" => "/tmp/x"}) == {:error, :resolver_unexpected}
  end

  ## mounts

  @src ~w(boundary plan_digest file_store revision path_policy id bindings verified_target bootstrap)

  defp mi(point, opts, root \\ "/", fstype \\ "zfs"),
    do: "1 2 0:1 #{root} #{point} #{opts},relatime - #{fstype} x rw"

  defp good_mounts do
    [
      mi("/", "ro", "/", "overlay"),
      mi("/proc", "rw,nosuid", "/", "proc"),
      mi("/dev", "rw,nosuid", "/", "tmpfs"),
      mi("/dev/pts", "rw", "/", "devpts"),
      mi("/sys", "ro", "/", "sysfs"),
      mi("/sys/fs/cgroup", "ro", "/", "cgroup2"),
      mi("/dev/mqueue", "rw", "/", "mqueue"),
      mi("/dev/shm", "rw", "/", "tmpfs"),
      mi("/tmp", "rw,nosuid", "/", "tmpfs"),
      mi("/etc/hostname", "ro", "/c/hostname"),
      mi("/etc/hosts", "ro", "/c/hosts"),
      mi("/etc/resolv.conf", "ro", "/c/resolv.conf")
    ] ++
      Enum.map(@src, &mi("/opt/telelem/src/#{&1}.ex", "ro", "/h/#{&1}.ex")) ++
      [
        mi("/opt/route-store", "ro", "/h/route-store"),
        mi("/opt/lock/with-route-store-lock", "ro", "/h/wrap"),
        mi("/sbx/boot", "ro", "/h/boot"),
        mi("/sbx/store", "rw,nosuid,nodev", "/h/store", "tmpfs"),
        mi("/proc/bus", "ro", "/bus", "proc"),
        mi("/proc/kcore", "rw,nosuid", "/null", "tmpfs"),
        mi("/proc/acpi", "ro", "/", "tmpfs"),
        mi("/sys/firmware", "ro", "/", "tmpfs")
      ]
  end

  defp mounts(lines), do: BootCheck.mounts(Enum.join(lines, "\n") <> "\n")

  defp swap(lines, point, new),
    do: Enum.map(lines, &if(String.split(&1, " ") |> Enum.at(4) == point, do: new, else: &1))

  test "mounts: closed set, rootfs and boot read-only, store the only rw app bind" do
    g = good_mounts()
    assert mounts(g) == :ok

    assert mounts(swap(g, "/", mi("/", "rw"))) == {:error, :mount_rootfs_writable}

    assert mounts(swap(g, "/sbx/boot", mi("/sbx/boot", "rw", "/h/boot"))) ==
             {:error, :mount_app_mode}

    assert mounts(swap(g, "/sbx/store", mi("/sbx/store", "ro", "/h/store"))) ==
             {:error, :mount_app_mode}

    assert mounts(swap(g, "/opt/route-store", mi("/opt/route-store", "rw", "/h/rs"))) ==
             {:error, :mount_app_mode}

    assert mounts(Enum.reject(g, &String.contains?(&1, " /sbx/boot "))) ==
             {:error, :mount_app_set_mismatch}

    # An extra mount anywhere, /opt and /sbx included, is not in the closed set.
    assert mounts(g ++ [mi("/opt/extra", "ro", "/h/x")]) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/sbx/store/route.rec", "ro", "/h/r")]) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/data", "rw", "/h/data")]) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/run/secrets", "ro", "/h/s")]) == {:error, :mount_unexpected}

    assert mounts(swap(g, "/etc/hosts", mi("/etc/hosts", "rw", "/c/hosts"))) ==
             {:error, :mount_unexpected}

    assert mounts(swap(g, "/sys", mi("/sys", "rw"))) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/proc/sys", "rw", "/sys", "proc")]) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/sbx/boot", "ro", "/h/boot2")]) == {:error, :mount_unexpected}
    assert mounts(g ++ [mi("/opt/a\\040b", "ro", "/h/x")]) == {:error, :mount_unexpected}
    assert mounts([]) == {:error, :mountinfo_unreadable}
    assert BootCheck.mounts("garbage") == {:error, :mountinfo_unreadable}
  end

  test "mounts: propagation tags and malformed fields are refused" do
    g = good_mounts()

    for tag <- ["shared:5", "master:3", "master:3 propagate_from:2", "unbindable"] do
      tagged = "1 2 0:1 /h/store /sbx/store rw,nosuid #{tag} - tmpfs x rw"
      assert mounts(swap(g, "/sbx/store", tagged)) == {:error, :mount_propagation}, tag
    end

    assert mounts(swap(g, "/tmp", "1 2 0:1 / /tmp  rw - tmpfs x rw")) ==
             {:error, :mountinfo_unreadable}

    assert mounts(swap(g, "/tmp", " 1 2 0:1 / /tmp rw - tmpfs x rw")) ==
             {:error, :mountinfo_unreadable}

    assert mounts(swap(g, "/tmp", "1 2 0:1 / /tmp rw")) == {:error, :mount_unexpected}
    assert mounts(swap(g, "/tmp", "1 2 0:1 / /tmp")) == {:error, :mountinfo_unreadable}
    assert mounts(g ++ ["1 2 0:1 / / ro - overlay x rw"]) == {:error, :mount_unexpected}
  end

  ## entry.sh's pre-BEAM mount check (boot_mounts.sh) is the same policy

  # Mountinfo as docker 29.10 / runc shows it for `launch boot-validated` (host paths
  # replaced), the shape the positive container run produces.
  defp docker_mounts do
    srcs =
      Enum.map(
        @src,
        &"30 10 0:29 /c/lib/telelem/x/#{&1}.ex /opt/telelem/src/#{&1}.ex ro,nodev,relatime - zfs rpool/ROOT rw,xattr,posixacl,casesensitive"
      )

    [
      "10 9 0:137 / / ro,relatime - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/A:/var/lib/docker/overlay2/l/B,upperdir=/var/lib/docker/overlay2/u/diff,workdir=/var/lib/docker/overlay2/u/work,nouserxattr",
      "11 10 0:146 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw",
      "12 10 0:147 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64",
      "13 12 0:148 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666",
      "14 10 0:149 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro",
      "15 14 0:44 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw,nsdelegate,memory_recursiveprot",
      "16 12 0:144 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw",
      "17 12 0:150 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k,inode64",
      "18 10 0:151 / /tmp rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,inode64",
      "19 10 0:33 /docker/containers/i/hostname /etc/hostname ro,nodev,relatime - zfs rpool/var/lib rw,xattr",
      "20 10 0:33 /docker/containers/i/hosts /etc/hosts ro,nodev,relatime - zfs rpool/var/lib rw,xattr",
      "21 10 0:33 /docker/containers/i/resolv.conf /etc/resolv.conf ro,nodev,relatime - zfs rpool/var/lib rw,xattr"
    ] ++
      srcs ++
      [
        "31 10 0:29 /c/scripts/route-store /opt/route-store ro,nodev,relatime - zfs rpool/ROOT rw,xattr",
        "32 10 0:29 /c/scripts/with-route-store-lock /opt/lock/with-route-store-lock ro,nodev,relatime - zfs rpool/ROOT rw,xattr",
        "33 10 0:64 /t/b1 /sbx/boot ro,nosuid,nodev - tmpfs tmpfs rw,nr_inodes=1048576,inode64",
        "34 10 0:64 /t/s1 /sbx/store rw,nosuid,nodev - tmpfs tmpfs rw,nr_inodes=1048576,inode64",
        "40 11 0:146 /bus /proc/bus ro,nosuid,nodev,noexec,relatime - proc proc rw",
        "41 11 0:146 /fs /proc/fs ro,nosuid,nodev,noexec,relatime - proc proc rw",
        "42 11 0:146 /irq /proc/irq ro,nosuid,nodev,noexec,relatime - proc proc rw",
        "43 11 0:146 /sys /proc/sys ro,nosuid,nodev,noexec,relatime - proc proc rw",
        "44 11 0:146 /sysrq-trigger /proc/sysrq-trigger ro,nosuid,nodev,noexec,relatime - proc proc rw",
        "45 11 0:152 / /proc/acpi ro,relatime - tmpfs tmpfs ro,size=4k,nr_inodes=1,inode64",
        "46 11 0:147 /null /proc/interrupts rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64",
        "47 11 0:147 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64",
        "48 11 0:147 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64",
        "49 11 0:152 / /proc/scsi ro,relatime - tmpfs tmpfs ro,size=4k,nr_inodes=1,inode64",
        "50 11 0:147 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64",
        "51 14 0:152 / /sys/devices/virtual/powercap ro,relatime - tmpfs tmpfs ro,size=4k,nr_inodes=1,inode64",
        "52 14 0:152 / /sys/firmware ro,relatime - tmpfs tmpfs ro,size=4k,nr_inodes=1,inode64"
      ]
  end

  defp sh_mounts(text, dir) do
    file = Path.join(dir, "mountinfo-#{System.unique_integer([:positive])}")
    File.write!(file, text)

    {out, 0} =
      System.cmd("/bin/sh", [
        "-c",
        ~S|. "$1"; rs_mounts "$2"; s=$?; printf '%s %s' "$s" "$RS_MOUNTS_CODE"|,
        "sh",
        @boot_mounts_sh,
        file
      ])

    case String.split(out, " ") do
      ["0", ""] -> :ok
      ["1", code] -> {:error, String.to_atom(code)}
    end
  end

  test "boot_mounts.sh has BootCheck's tables" do
    {out, 0} =
      System.cmd("/bin/sh", ["-c", ~S|. "$1"; rs_mounts_table|, "sh", @boot_mounts_sh])

    rows = out |> String.split("\n", trim: true) |> Enum.map(&String.split(&1, " "))
    app = for [k, p] <- rows, k in ["app_ro", "app_rw"], into: %{}, do: {p, k == "app_rw"}
    assert app == BootCheck.app_mounts()
    assert length(for [k, _] <- rows, k in ["app_ro", "app_rw"], do: k) == map_size(app)
    std = BootCheck.std_mounts()
    assert for([k, p] <- rows, k == "std_any", do: p) == std.any
    assert for([k, p] <- rows, k == "std_ro", do: p) == std.ro
  end

  test "boot_mounts.sh and mounts/1 give the same verdict, ok and refused", %{tmp_dir: d} do
    g = good_mounts()
    dm = docker_mounts()
    j = &(Enum.join(&1, "\n") <> "\n")

    cases = [
      {:ok, j.(g)},
      {:ok, j.(dm)},
      {:ok, Enum.join(dm, "\n")},
      {:ok, j.(Enum.reverse(dm))},
      {:ok, "\n" <> Enum.join(dm, "\n\n") <> "\n"},
      {:mount_rootfs_writable, j.(swap(g, "/", mi("/", "rw")))},
      {:mount_rootfs_writable, j.(Enum.reject(dm, &String.contains?(&1, " / / ")))},
      {:mount_app_mode, j.(swap(g, "/sbx/boot", mi("/sbx/boot", "rw", "/h/boot")))},
      {:mount_app_mode, j.(swap(g, "/sbx/store", mi("/sbx/store", "ro", "/h/store")))},
      {:mount_app_mode, j.(swap(dm, "/opt/route-store", mi("/opt/route-store", "rw", "/h/rs")))},
      {:mount_app_set_mismatch, j.(Enum.reject(dm, &String.contains?(&1, " /sbx/boot ")))},
      {:mount_app_set_mismatch,
       j.(Enum.reject(dm, &String.contains?(&1, " /opt/telelem/src/id.ex ")))},
      {:mount_unexpected, j.(dm ++ [mi("/opt/extra", "ro", "/h/x")])},
      {:mount_unexpected, j.(dm ++ [mi("/opt/route-store/boot_check.ex", "ro", "/h/x")])},
      {:mount_unexpected, j.(dm ++ [mi("/sbx/store/route.rec", "ro", "/h/r")])},
      {:mount_unexpected, j.(dm ++ [mi("/opt", "ro", "/h/o")])},
      {:mount_unexpected, j.(dm ++ [mi("/sbx", "ro", "/h/s")])},
      {:mount_unexpected, j.(dm ++ [mi("/usr/local/bin/elixirc", "ro", "/h/e")])},
      {:mount_unexpected, j.(dm ++ [mi("/data", "rw", "/h/data")])},
      {:mount_unexpected, j.(swap(dm, "/etc/hosts", mi("/etc/hosts", "rw", "/c/hosts")))},
      {:mount_unexpected, j.(swap(dm, "/sys", mi("/sys", "rw")))},
      {:mount_unexpected, j.(dm ++ [mi("/sysfoo", "ro")])},
      {:mount_unexpected, j.(dm ++ [mi("/proc/sys/x", "rw", "/sys", "proc")])},
      {:ok, j.(dm ++ [mi("/proc/x", "rw", "/null", "tmpfs")])},
      {:mount_unexpected, j.(dm ++ [mi("/sbx/boot", "ro", "/h/boot2")])},
      {:mount_unexpected, j.(dm ++ [mi("/opt/a\\040b", "ro", "/h/x")])},
      {:mount_unexpected, j.(dm ++ [mi("/opt/*", "ro", "/h/x")])},
      {:mount_unexpected, j.(swap(dm, "/tmp", "1 2 0:1 / /tmp rw"))},
      {:mount_propagation,
       j.(swap(dm, "/sbx/store", "34 10 0:64 /t/s1 /sbx/store rw,nosuid shared:7 - tmpfs t rw"))},
      {:mount_propagation, j.(swap(dm, "/", "10 9 0:137 / / ro master:1 - overlay o rw"))},
      {:mountinfo_unreadable, j.(swap(dm, "/tmp", "1 2 0:1 / /tmp  rw - tmpfs x rw"))},
      {:mountinfo_unreadable, j.(swap(dm, "/tmp", "1 2 0:1 / /tmp rw - tmpfs x rw "))},
      {:mountinfo_unreadable, j.(swap(dm, "/tmp", "1 2 0:1 / /tmp"))},
      {:mountinfo_unreadable, j.(dm ++ ["   "])},
      {:mountinfo_unreadable, ""},
      {:mountinfo_unreadable, "\n\n"},
      {:mountinfo_unreadable, "garbage"}
    ]

    for {{want, text}, i} <- Enum.with_index(cases) do
      want = if want == :ok, do: :ok, else: {:error, want}
      assert BootCheck.mounts(text) == want, "case #{i}: mounts/1"
      assert sh_mounts(text, d) == want, "case #{i}: boot_mounts.sh"
    end

    missing = Path.join(d, "no-such-mountinfo")

    {out, 0} =
      System.cmd(
        "/bin/sh",
        [
          "-c",
          ~S|. "$1"; rs_mounts "$2"; printf '%s %s' "$?" "$RS_MOUNTS_CODE"|,
          "sh",
          @boot_mounts_sh,
          missing
        ],
        stderr_to_stdout: true
      )

    assert out == "1 mountinfo_unreadable"
  end
end
