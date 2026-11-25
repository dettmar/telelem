# Boot-input preflight for `scripts/route-store/launch boot-validated` (M2a, TEL-014/015
# local slice). Compiled by entry.sh, in the writer container only, for that mode;
# test/route_store/boot_check_test.exs loads this file directly.
#
# Every check returns :ok / {:ok, _} or {:error, code}, where code is an atom from the
# closed set in @codes. No error ever carries file contents, paths, key material or a
# raw exception: main.exs prints only the code.
#
# What is checked (main.exs runs these after the lock check and before any FileStore
# call; see scripts/route-store/launch for the trust assumptions):
#
#   env/1       the BEAM's actual environment is exactly the fixed set entry.sh and the
#               lock wrapper produce (nothing inherited from the image or the host).
#   resolver/1  the resolver option the booted VM actually uses is hosts-file only,
#               from the pinned ERL_INETRC path (entry.sh pinned its bytes pre-boot).
#   mounts/2    /proc/self/mountinfo: a read-only rootfs, the exact closed set of
#               application mounts below /opt and /sbx (all read-only except the store
#               at /sbx/store), and only docker's standard mounts elsewhere.
#   inputs/1    the staged boot directory against the versioned closed manifest:
#               manifest digest supplied independently (argv), exact roles, exact byte
#               lengths and SHA-256 per file, manifest/bootstrap/start tenant equal,
#               store root equal to the locked store, the fixture CA through the
#               existing Sandbox.Bootstrap/VerifiedTarget path and pinned to the
#               manifested CA bytes, and an in-memory downstream cert/key match
#               (sign + verify; no TLS, no socket).
#
# Not established here: provenance or immutability of anything on the host. The
# staging directory is a trusted local input; the read-only bind only stops this
# container from writing it. Each file is read once (handle inode = walked inode) and
# hashed, and only the bytes that were hashed are used.

defmodule RouteStore.BootCheck do
  @moduledoc false

  require Record

  Record.defrecordp(
    :otp_cert,
    :OTPCertificate,
    Record.extract(:OTPCertificate, from_lib: "public_key/include/public_key.hrl")
  )

  Record.defrecordp(
    :otp_tbs,
    :OTPTBSCertificate,
    Record.extract(:OTPTBSCertificate, from_lib: "public_key/include/public_key.hrl")
  )

  Record.defrecordp(
    :otp_spki,
    :OTPSubjectPublicKeyInfo,
    Record.extract(:OTPSubjectPublicKeyInfo, from_lib: "public_key/include/public_key.hrl")
  )

  Record.defrecordp(
    :pk_alg,
    :PublicKeyAlgorithm,
    Record.extract(:PublicKeyAlgorithm, from_lib: "public_key/include/public_key.hrl")
  )

  alias Telelem.Sandbox.{Bootstrap, Id, PathPolicy, VerifiedTarget}

  import Bitwise

  @manifest_version "telelem.route_store.boot_manifest.v1"
  @manifest_name "manifest.json"
  @manifest_max 4096
  @file_max 65_536

  # role => fixed file name in the boot directory. Names never come from the manifest.
  @roles %{
    "bootstrap" => "bootstrap.json",
    "fixture_ca" => "fixture-ca.pem",
    "downstream_cert" => "downstream-cert.pem",
    "downstream_key" => "downstream-key.pem"
  }
  @role_set MapSet.new(Map.keys(@roles))
  @dir_listing Enum.sort([@manifest_name | Map.values(@roles)])
  @manifest_keys MapSet.new(~w(version tenant_id store_root files))
  @entry_keys MapSet.new(~w(bytes sha256))

  @inetrc "/opt/route-store/inetrc"
  @lock_dir "/sbx/store"

  @src ~w(boundary plan_digest file_store revision path_policy id bindings verified_target bootstrap)
  # Application mounts of this mode: exact set; true = must be rw, false = must be ro.
  @app_mounts Map.merge(
                Map.new(@src, &{"/opt/telelem/src/#{&1}.ex", false}),
                %{
                  "/opt/route-store" => false,
                  "/opt/lock/with-route-store-lock" => false,
                  "/sbx/boot" => false,
                  "/sbx/store" => true
                }
              )
  @std_ro ~w(/etc/hostname /etc/hosts /etc/resolv.conf)
  @std_any ~w(/proc /dev /dev/pts /dev/mqueue /dev/shm /tmp)

  @codes ~w(env_unexpected resolver_unexpected mountinfo_unreadable mount_unexpected
            mount_rootfs_writable mount_app_set_mismatch mount_app_mode
            bad_manifest_digest_arg boot_dir_policy boot_dir_unsafe boot_dir_not_closed
            manifest_unreadable manifest_size manifest_digest_mismatch manifest_not_json
            manifest_keys manifest_version manifest_tenant manifest_store_root
            manifest_roles manifest_entry
            file_unsafe file_unreadable file_changed file_mode file_size_mismatch
            file_digest_mismatch
            bootstrap_not_json bootstrap_tenant bootstrap_store_root bootstrap_ca_source
            bootstrap_rejected bootstrap_ca_not_pinned
            downstream_cert_invalid downstream_cert_is_ca downstream_key_invalid
            downstream_key_unsupported downstream_key_mismatch)a

  @doc false
  def codes, do: @codes

  @doc false
  def roles, do: @roles

  @doc false
  def manifest_version, do: @manifest_version

  ## environment ----------------------------------------------------------------------

  @doc """
  `env` is the BEAM's actual environment (`System.get_env/0`). It must be exactly the
  set produced by the launcher's `env -i` list, the `cd`s in entry.sh, the lock
  wrapper and `erl`, with these values. Only the lock fd number varies.
  """
  def env(env, root \\ :code.root_dir() |> List.to_string()) do
    bindir = "#{root}/erts-#{:erlang.system_info(:version)}/bin"

    want = %{
      "BINDIR" => bindir,
      "EMU" => "beam",
      "ERL_CRASH_DUMP_SECONDS" => "0",
      "ERL_INETRC" => @inetrc,
      "HOME" => "/tmp",
      "LANG" => "C.UTF-8",
      "OLDPWD" => "/tmp/ebin",
      "PATH" => "#{bindir}:#{root}/bin:/usr/local/bin:/usr/bin:/bin",
      "PROGNAME" => "erl",
      "PWD" => "/",
      "ROOTDIR" => root,
      "SHLVL" => "0",
      "TELELEM_ROUTE_STORE_LOCK_DIR" => @lock_dir
    }

    with true <- is_map(env),
         {fd, rest} <- Map.pop(env, "TELELEM_ROUTE_STORE_LOCK_FD"),
         true <- is_binary(fd) and Regex.match?(~r/\A[1-9][0-9]{0,3}\z/, fd),
         true <- rest == want do
      :ok
    else
      _ -> {:error, :env_unexpected}
    end
  end

  ## resolver -------------------------------------------------------------------------

  @doc "The VM's resolver after boot: lookup is `[:file]`, loaded from the pinned path."
  def resolver(env) do
    if Map.get(env, "ERL_INETRC") == @inetrc and :inet_db.res_option(:lookup) == [:file],
      do: :ok,
      else: {:error, :resolver_unexpected}
  end

  ## mounts ---------------------------------------------------------------------------

  @doc "Check `/proc/self/mountinfo` text (see module header)."
  def mounts(text) when is_binary(text) do
    with {:ok, ms} <- parse_mountinfo(text),
         :ok <- unique_points(ms),
         :ok <- rootfs_ro(ms),
         :ok <- every_mount_known(ms),
         :ok <- app_set(ms) do
      :ok
    end
  end

  def mounts(_), do: {:error, :mountinfo_unreadable}

  defp parse_mountinfo(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.reduce_while({:ok, []}, fn line, {:ok, acc} ->
      case String.split(line, " ") do
        [_id, _parent, _dev, root, point, opts | _rest] ->
          if String.contains?(line, " - ") and not String.contains?(point, "\\"),
            do: {:cont, {:ok, [%{root: root, point: point, rw: rw?(opts)} | acc]}},
            else: {:halt, {:error, :mount_unexpected}}

        _ ->
          {:halt, {:error, :mountinfo_unreadable}}
      end
    end)
    |> case do
      {:ok, []} -> {:error, :mountinfo_unreadable}
      {:ok, ms} -> {:ok, Enum.reverse(ms)}
      err -> err
    end
  end

  defp rw?(opts), do: "rw" in String.split(opts, ",")

  defp unique_points(ms) do
    points = Enum.map(ms, & &1.point)
    if length(Enum.uniq(points)) == length(points), do: :ok, else: {:error, :mount_unexpected}
  end

  defp rootfs_ro(ms) do
    case Enum.find(ms, &(&1.point == "/")) do
      %{rw: false} -> :ok
      _ -> {:error, :mount_rootfs_writable}
    end
  end

  defp every_mount_known(ms) do
    if Enum.all?(ms, &known?/1), do: :ok, else: {:error, :mount_unexpected}
  end

  defp known?(%{point: "/"}), do: true
  defp known?(%{point: p}) when is_map_key(@app_mounts, p), do: true
  defp known?(%{point: p}) when p in @std_any, do: true
  defp known?(%{point: p, rw: rw}) when p in @std_ro, do: not rw
  defp known?(%{point: "/sys" <> _ = p, rw: rw}), do: under_sys?(p) and not rw

  # Below /proc: read-only masks, or docker's masked paths bound from /dev/null.
  defp known?(%{point: "/proc/" <> _, root: root, rw: rw}), do: not rw or root == "/null"
  defp known?(_), do: false

  defp under_sys?(p), do: p == "/sys" or String.starts_with?(p, "/sys/")

  defp app_set(ms) do
    app = for m <- ms, app_point?(m.point), into: %{}, do: {m.point, m.rw}

    cond do
      Map.keys(app) |> Enum.sort() != Map.keys(@app_mounts) |> Enum.sort() ->
        {:error, :mount_app_set_mismatch}

      app != @app_mounts ->
        {:error, :mount_app_mode}

      true ->
        :ok
    end
  end

  defp app_point?(p),
    do: Enum.any?(["/opt", "/sbx"], &(p == &1 or String.starts_with?(p, &1 <> "/")))

  ## boot inputs ------------------------------------------------------------------------

  @doc """
  Verify the staged boot inputs. Options (all required; main.exs passes the fixed
  container paths): `:sandbox_root`, `:boot_dir`, `:store_dir`, `:tenant`,
  `:manifest_sha256`. Returns `{:ok, summary}` with digests only, never key material.
  """
  def inputs(opts) do
    sb = Keyword.fetch!(opts, :sandbox_root)
    boot = Keyword.fetch!(opts, :boot_dir)
    store = Keyword.fetch!(opts, :store_dir)
    tenant = Keyword.fetch!(opts, :tenant)
    want = Keyword.fetch!(opts, :manifest_sha256)

    with :ok <- expect(hex64?(want), :bad_manifest_digest_arg),
         :ok <- boot_dir_policy(sb, boot, store, tenant),
         :ok <- expect(PathPolicy.safe_root(sb) == :ok, :boot_dir_unsafe),
         :ok <-
           expect(match?({:ok, _}, PathPolicy.walk_below(sb, boot, :directory)), :boot_dir_unsafe),
         :ok <- closed_listing(boot),
         {:ok, mbin} <- read_pinned(boot, @manifest_name, @manifest_max, :manifest),
         :ok <- expect(sha256(mbin) == want, :manifest_digest_mismatch),
         {:ok, man} <- manifest(mbin, tenant, store),
         {:ok, files} <- read_roles(boot, man["files"]),
         {:ok, bindings} <- bootstrap(files, sb, boot, store, tenant),
         {:ok, cert_sha} <- downstream(files["downstream_cert"], files["downstream_key"]) do
      {:ok,
       %{
         manifest_sha256: want,
         tenant_id: tenant,
         deployments: bindings.fixture_targets |> Map.keys() |> Enum.sort(),
         fixture_ca_sha256: sha256(files["fixture_ca"]),
         downstream_cert_sha256: cert_sha
       }}
    end
  end

  defp boot_dir_policy(sb, boot, store, tenant) do
    ok =
      Id.valid?(tenant) and Enum.all?([sb, boot, store], &PathPolicy.normalized_absolute?/1) and
        boot != sb and PathPolicy.under?(boot, sb) and store != sb and
        PathPolicy.under?(store, sb) and
        not PathPolicy.overlap?(boot, store) and
        not Enum.any?([boot, store], &PathPolicy.denied?(&1, PathPolicy.default_denied_roots()))

    expect(ok, :boot_dir_policy)
  end

  # Exactly the manifest and one file per role: no route.rec, no stray or hidden files.
  defp closed_listing(boot) do
    case File.ls(boot) do
      {:ok, names} -> expect(Enum.sort(names) == @dir_listing, :boot_dir_not_closed)
      _ -> {:error, :boot_dir_not_closed}
    end
  end

  defp manifest(bin, tenant, store) do
    case JSON.decode(bin) do
      {:ok, %{} = m} ->
        with :ok <- expect(keys(m) == @manifest_keys, :manifest_keys),
             :ok <- expect(m["version"] == @manifest_version, :manifest_version),
             :ok <-
               expect(Id.valid?(m["tenant_id"]) and m["tenant_id"] == tenant, :manifest_tenant),
             :ok <- expect(m["store_root"] == store, :manifest_store_root),
             :ok <- expect(is_map(m["files"]) and keys(m["files"]) == @role_set, :manifest_roles),
             :ok <- expect(Enum.all?(Map.values(m["files"]), &entry?/1), :manifest_entry),
             do: {:ok, m}

      _ ->
        {:error, :manifest_not_json}
    end
  end

  defp entry?(%{} = e) do
    keys(e) == @entry_keys and is_integer(e["bytes"]) and e["bytes"] in 1..@file_max and
      hex64?(e["sha256"])
  end

  defp entry?(_), do: false

  defp read_roles(boot, entries) do
    @roles
    |> Enum.sort()
    |> Enum.reduce_while({:ok, %{}}, fn {role, name}, {:ok, acc} ->
      %{"bytes" => n, "sha256" => h} = entries[role]

      with {:ok, bin} <- read_pinned(boot, name, n, role),
           :ok <- expect(byte_size(bin) == n, :file_size_mismatch),
           :ok <- expect(sha256(bin) == h, :file_digest_mismatch) do
        {:cont, {:ok, Map.put(acc, role, bin)}}
      else
        err -> {:halt, err}
      end
    end)
  end

  # One open per file: walk without symlinks, open raw, the handle must be the walked
  # inode, then read at most max + 1 bytes. The key file must not be group/other
  # readable. Returns the bytes; the caller hashes exactly these.
  defp read_pinned(boot, name, max, what) do
    path = Path.join(boot, name)
    err = if what == :manifest, do: :manifest_unreadable, else: :file_unreadable

    with {:ok, st} <- PathPolicy.walk_below(boot, path, :regular) |> or_error(:file_unsafe),
         :ok <- expect(what != "downstream_key" or band(st.mode, 0o077) == 0, :file_mode),
         {:ok, io} <- File.open(path, [:read, :binary, :raw]) |> or_error(err) do
      try do
        with {:ok, fi} <- :file.read_file_info(io, time: :posix) |> or_error(err),
             :ok <- expect(PathPolicy.same_file?(st, File.Stat.from_record(fi)), :file_changed) do
          case :file.read(io, max + 1) do
            {:ok, bin} when byte_size(bin) <= max -> {:ok, bin}
            {:ok, _} when what == :manifest -> {:error, :manifest_size}
            {:ok, _} -> {:error, :file_size_mismatch}
            :eof when what == :manifest -> {:error, :manifest_size}
            :eof -> {:error, :file_size_mismatch}
            _ -> {:error, err}
          end
        end
      after
        File.close(io)
      end
    end
  end

  # The bootstrap document is decoded from the hashed bytes (never re-read from disk),
  # must name the start tenant and the locked store, and every target's CA must be the
  # manifested fixture CA file. Sandbox.Bootstrap then builds the bindings (it reads
  # that CA file itself), and the resulting VerifiedTargets must carry exactly the
  # certificates in the hashed CA bytes.
  defp bootstrap(files, sb, boot, store, tenant) do
    ca_path = Path.join(boot, @roles["fixture_ca"])

    with {:ok, %{} = doc} <- JSON.decode(files["bootstrap"]) |> or_error(:bootstrap_not_json),
         :ok <- expect(doc["tenant_id"] == tenant, :bootstrap_tenant),
         :ok <- expect(doc["sandbox_store_root"] == store, :bootstrap_store_root),
         :ok <- expect(ca_sources_pinned?(doc["fixture_targets"], ca_path), :bootstrap_ca_source),
         {:ok, b} <-
           Bootstrap.from_map(doc, fixture_root: boot, sandbox_root: sb)
           |> or_error(:bootstrap_rejected),
         {:ok, ders} <- pem_ders(files["fixture_ca"]),
         :ok <-
           expect(
             b.tenant_id == tenant and b.sandbox_store_root == store and
               Enum.all?(b.fixture_targets, fn {id, t} ->
                 VerifiedTarget.check(t, id) == :ok and t.cacerts == ders
               end),
             :bootstrap_ca_not_pinned
           ) do
      {:ok, b}
    else
      {:ok, _not_a_map} -> {:error, :bootstrap_not_json}
      err -> err
    end
  end

  defp ca_sources_pinned?(%{} = ts, ca_path) when map_size(ts) > 0 do
    Enum.all?(ts, fn
      {_id, %{"ca_file" => ^ca_path} = t} -> not Map.has_key?(t, "ca_pem")
      _ -> false
    end)
  end

  defp ca_sources_pinned?(_, _), do: false

  defp pem_ders(pem) do
    entries = safe(fn -> :public_key.pem_decode(pem) end)

    if is_list(entries) and entries != [] and
         Enum.all?(entries, &match?({:Certificate, _, :not_encrypted}, &1)),
       do: {:ok, Enum.map(entries, &elem(&1, 1))},
       else: {:error, :bootstrap_ca_not_pinned}
  end

  ## downstream cert/key (in memory only) ----------------------------------------------

  @match_msg "telelem.route_store.boot_key_match.v1"

  defp downstream(cert_pem, key_pem) do
    with {:ok, der} <- one_cert(cert_pem),
         :ok <- expect(not VerifiedTarget.ca_cert?(der), :downstream_cert_is_ca),
         {:ok, pub} <- cert_public_key(der),
         {:ok, priv} <- one_key(key_pem),
         :ok <- key_matches(priv, pub) do
      {:ok, sha256(der)}
    end
  end

  defp one_cert(pem) do
    case safe(fn -> :public_key.pem_decode(pem) end) do
      [{:Certificate, der, :not_encrypted}] = e ->
        canonical =
          safe(fn ->
            :public_key.der_encode(:Certificate, :public_key.pkix_decode_cert(der, :plain))
          end)

        if canonical == der and squash(:public_key.pem_encode(e)) == squash(pem),
          do: {:ok, der},
          else: {:error, :downstream_cert_invalid}

      _ ->
        {:error, :downstream_cert_invalid}
    end
  end

  defp cert_public_key(der) do
    case safe(fn ->
           otp_cert(tbsCertificate: tbs) = :public_key.pkix_decode_cert(der, :otp)
           otp_spki(algorithm: alg, subjectPublicKey: key) = otp_tbs(tbs, :subjectPublicKeyInfo)
           {pk_alg(alg, :algorithm), pk_alg(alg, :parameters), key}
         end) do
      {{1, 2, 840, 10_045, 2, 1}, {:namedCurve, _} = curve, {:ECPoint, _} = point} ->
        {:ok, {:ec, {point, curve}}}

      {{1, 2, 840, 113_549, 1, 1, 1}, _, {:RSAPublicKey, _, _} = rsa} ->
        {:ok, {:rsa, rsa}}

      _ ->
        {:error, :downstream_cert_invalid}
    end
  end

  @key_types [:ECPrivateKey, :RSAPrivateKey, :PrivateKeyInfo]
  @curves [{1, 2, 840, 10_045, 3, 1, 7}, {1, 3, 132, 0, 34}]

  defp one_key(pem) do
    with [{type, _, :not_encrypted} = e] when type in @key_types <-
           safe(fn -> :public_key.pem_decode(pem) end),
         true <- squash(:public_key.pem_encode([e])) == squash(pem),
         key when is_tuple(key) <- safe(fn -> :public_key.pem_entry_decode(e) end) do
      case key do
        {:ECPrivateKey, _, _, {:namedCurve, c}, _, _} when c in @curves -> {:ok, {:ec, key}}
        {:RSAPrivateKey, _, n, _, _, _, _, _, _, _, _} when n > 0 -> rsa_size(key, n)
        _ -> {:error, :downstream_key_unsupported}
      end
    else
      _ -> {:error, :downstream_key_invalid}
    end
  end

  defp rsa_size(key, n) do
    if bit_size(:binary.encode_unsigned(n)) >= 2048,
      do: {:ok, {:rsa, key}},
      else: {:error, :downstream_key_unsupported}
  end

  # Prove possession: sign a fixed message with the private key and verify it with the
  # certificate's public key. Nothing leaves this process.
  defp key_matches({alg, priv}, {alg, pub}) do
    ok =
      safe(fn ->
        sig = :public_key.sign(@match_msg, :sha256, priv)
        :public_key.verify(@match_msg, :sha256, sig, pub)
      end)

    expect(ok == true, :downstream_key_mismatch)
  end

  defp key_matches(_, _), do: {:error, :downstream_key_mismatch}

  ## helpers ----------------------------------------------------------------------------

  defp safe(fun) do
    fun.()
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp keys(m), do: m |> Map.keys() |> MapSet.new()
  defp squash(s), do: String.replace(s, ~r/\s+/, "")
  defp hex64?(h), do: is_binary(h) and Regex.match?(~r/\A[0-9a-f]{64}\z/, h)
  defp sha256(bin), do: Base.encode16(:crypto.hash(:sha256, bin), case: :lower)

  defp or_error({:ok, _} = ok, _), do: ok
  defp or_error(_, code), do: {:error, code}

  defp expect(true, _), do: :ok
  defp expect(_, code), do: {:error, code}
end
