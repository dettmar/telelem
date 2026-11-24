defmodule Telelem.Sandbox.Bootstrap do
  @moduledoc """
  Scoped sandbox bootstrap: explicit document in, `Bindings` or a bounded
  error out. No ambient environment, no OS trust store, no DNS, no default
  paths. Every path passes the pure policy (normalized, inside its launcher
  root, outside every denied root) **before** any filesystem call, so a
  rejected bootstrap never touches a production-shaped path (the isolation
  harness audits this with strace).

  Document (JSON or the equivalent string-keyed map), exact key sets:

      {"version": 2, "tenant_id": "local", "mode": "sandbox",
       "fixture_targets": {"a": {"host": "127.0.0.1", "port": 18443, "ca_file": "/fx/ca.pem"},
                           "b": {"host": "127.0.0.1", "port": 18444, "ca_pem": "-----BEGIN…"}},
       "sandbox_store_root": "/sbx/store",
       "sandbox_signer_ref": "sandbox:local-fixture"}

  Version 2 added `tenant_id`; a version-1 document is rejected.

  Required launcher options (not taken from the document):

    * `:fixture_root` — every `ca_file` must lie strictly below it.
    * `:sandbox_root` — `sandbox_store_root` and the bootstrap file itself
      (for `from_file/2`) must lie strictly below it.
    * `:denied_roots` (optional) — added to `PathPolicy.default_denied_roots/0`.

  ## Filesystem checks (after the pure policy passes)

    * Launcher roots (`PathPolicy.safe_root/1`): each is a real directory
      reached through real directories from `/`, not world-writable, with no
      world-writable non-sticky ancestor.
    * `sandbox_store_root` is an existing directory strictly below
      `sandbox_root`, reached without any symlink or world-writable directory
      below the root, and it neither contains nor lies inside `fixture_root`.
      A `from_file/2` bootstrap may not lie inside it. Nothing here opens it.
    * `ca_file` and the bootstrap file are regular, singly-linked files
      reached without any symlink below their root. After opening, the
      handle's inode/device must equal the walked file's.

  ## Launcher preconditions (not enforced here)

  These checks inspect the tree; they do not make it safe against a
  concurrent writer (see `Telelem.Sandbox.PathPolicy`). The launcher must
  guarantee that `fixture_root` and `sandbox_root` — everything below them
  except the contents of `sandbox_store_root` — are immutable to the sandbox
  process and anything untrusted for its lifetime (e.g. read-only bind
  mounts), and that the store opener re-validates `sandbox_store_root`
  before use.

  Errors are atoms or `{atom, deployment_id | :invalid}`; never file contents.
  """

  alias Telelem.Sandbox.{Bindings, Id, PathPolicy, VerifiedTarget}

  @version 2
  @max_doc 65_536
  @max_ca 65_536
  @top_keys MapSet.new(
              ~w(version tenant_id mode fixture_targets sandbox_store_root sandbox_signer_ref)
            )

  @type error :: atom() | {atom(), String.t() | :invalid}

  @spec from_file(String.t(), keyword()) :: {:ok, Bindings.t()} | {:error, error()}
  def from_file(path, opts) do
    with {:ok, pol} <- policy(opts),
         :ok <- sandbox_path(path, pol, :bootstrap_path_outside_sandbox_root),
         :ok <- roots_safe(pol),
         {:ok, st} <-
           PathPolicy.walk_below(pol.sandbox_root, path, :regular) |> tag(:bootstrap_file),
         {:ok, bin} <- read_bounded(path, st, @max_doc, :bootstrap_file),
         {:ok, doc} <- decode(bin),
         {:ok, b} <- build(doc, pol) do
      if PathPolicy.under?(path, b.sandbox_store_root),
        do: {:error, :bootstrap_path_inside_store_root},
        else: {:ok, b}
    end
  end

  @spec from_map(map(), keyword()) :: {:ok, Bindings.t()} | {:error, error()}
  def from_map(doc, opts) do
    with {:ok, pol} <- policy(opts), do: build(doc, pol)
  end

  # ── policy ────────────────────────────────────────────────────────────────

  defp policy(opts) do
    if Keyword.keyword?(opts), do: policy_kw(opts), else: {:error, :invalid_options}
  end

  defp policy_kw(opts) do
    extra = Keyword.get(opts, :denied_roots, [])
    fx = Keyword.get(opts, :fixture_root)
    sb = Keyword.get(opts, :sandbox_root)

    cond do
      not (is_list(extra) and Enum.all?(extra, &PathPolicy.normalized_absolute?/1)) ->
        {:error, :invalid_denied_roots}

      not PathPolicy.normalized_absolute?(fx) ->
        {:error, :missing_fixture_root}

      not PathPolicy.normalized_absolute?(sb) ->
        {:error, :missing_sandbox_root}

      true ->
        denied = PathPolicy.default_denied_roots() ++ extra

        cond do
          PathPolicy.denied?(fx, denied) -> {:error, :fixture_root_denied}
          PathPolicy.denied?(sb, denied) -> {:error, :sandbox_root_denied}
          true -> {:ok, %{fixture_root: fx, sandbox_root: sb, denied: denied}}
        end
    end
  end

  # Filesystem precondition check of both launcher roots; only lstat.
  defp roots_safe(pol) do
    with :ok <- PathPolicy.safe_root(pol.fixture_root) |> tag(:fixture_root),
         do: PathPolicy.safe_root(pol.sandbox_root) |> tag(:sandbox_root)
  end

  defp sandbox_path(p, pol, err) do
    if PathPolicy.normalized_absolute?(p) and p != pol.sandbox_root and
         PathPolicy.under?(p, pol.sandbox_root) and not PathPolicy.denied?(p, pol.denied),
       do: :ok,
       else: {:error, err}
  end

  # ── document ──────────────────────────────────────────────────────────────

  defp decode(bin) do
    case JSON.decode(bin) do
      {:ok, %{} = doc} -> {:ok, doc}
      _ -> {:error, :bootstrap_not_json_object}
    end
  end

  # Pure document checks first, so a rejected document never reaches the
  # filesystem; then the launcher roots, the store root and the CA files.
  defp build(%{} = doc, pol) do
    with :ok <- exact_keys(doc, @top_keys, :unknown_or_missing_keys),
         :ok <- expect(doc["version"] == @version, :unsupported_version),
         :ok <- expect(Id.valid?(doc["tenant_id"]), :invalid_tenant),
         :ok <- expect(doc["mode"] == "sandbox", :mode_not_sandbox),
         :ok <- store_root_policy(doc["sandbox_store_root"], pol),
         :ok <- expect(Bindings.signer_ref_ok?(doc["sandbox_signer_ref"]), :invalid_signer_ref),
         {:ok, specs} <- target_specs(doc["fixture_targets"], pol),
         :ok <- roots_safe(pol),
         :ok <- store_root_fs(doc["sandbox_store_root"], pol),
         {:ok, targets} <- targets(specs, pol) do
      b = %Bindings{
        tenant_id: doc["tenant_id"],
        mode: :sandbox,
        fixture_targets: targets,
        sandbox_store_root: doc["sandbox_store_root"],
        sandbox_signer_ref: doc["sandbox_signer_ref"]
      }

      with :ok <- Bindings.check(b), do: {:ok, b}
    end
  end

  defp build(_, _), do: {:error, :bootstrap_not_json_object}

  defp store_root_policy(p, pol) do
    cond do
      not PathPolicy.normalized_absolute?(p) -> {:error, :invalid_store_root}
      PathPolicy.denied?(p, pol.denied) -> {:error, :store_root_denied}
      p == pol.sandbox_root -> {:error, :store_root_is_sandbox_root}
      not PathPolicy.under?(p, pol.sandbox_root) -> {:error, :store_root_outside_sandbox_root}
      PathPolicy.overlap?(p, pol.fixture_root) -> {:error, :store_root_overlaps_fixture_root}
      true -> :ok
    end
  end

  defp store_root_fs(p, pol) do
    case PathPolicy.walk_below(pol.sandbox_root, p, :directory) do
      {:ok, _} -> :ok
      {:error, r} -> {:error, :"store_root_#{r}"}
    end
  end

  # ── targets: pure pass, then CA loading ───────────────────────────────────

  defp target_specs(%{} = m, pol) when map_size(m) in 1..16 do
    ids = Map.keys(m)

    if Enum.all?(ids, &Id.valid?/1) do
      # Sorted so the first reported error is deterministic.
      ids
      |> Enum.sort()
      |> Enum.reduce_while({:ok, []}, fn id, {:ok, acc} ->
        case target_spec(m[id], pol) do
          {:ok, s} -> {:cont, {:ok, [{id, s} | acc]}}
          {:error, r} -> {:halt, {:error, {r, id}}}
        end
      end)
      |> case do
        {:ok, specs} ->
          specs = Enum.reverse(specs)
          eps = Enum.map(specs, fn {_, s} -> {s.address, s.port} end)

          if length(Enum.uniq(eps)) == length(eps),
            do: {:ok, specs},
            else: {:error, :duplicate_fixture_endpoint}

        err ->
          err
      end
    else
      {:error, {:invalid_deployment_id, :invalid}}
    end
  end

  defp target_specs(_, _), do: {:error, :invalid_fixture_targets}

  defp target_spec(%{} = t, pol) do
    keys = t |> Map.keys() |> MapSet.new()

    ca =
      cond do
        keys == MapSet.new(~w(host port ca_file)) -> {:file, t["ca_file"]}
        keys == MapSet.new(~w(host port ca_pem)) -> {:pem, t["ca_pem"]}
        true -> :bad
      end

    with :ok <- expect(ca != :bad, :unknown_or_missing_target_keys),
         {:ok, addr} <- VerifiedTarget.loopback(t["host"]),
         :ok <- expect(is_integer(t["port"]) and t["port"] in 1..65535, :invalid_port),
         :ok <- ca_policy(ca, pol) do
      {:ok, %{host: t["host"], address: addr, port: t["port"], ca: ca}}
    end
  end

  defp target_spec(_, _), do: {:error, :invalid_target}

  defp ca_policy({:pem, pem}, _pol) when is_binary(pem) and byte_size(pem) <= @max_ca, do: :ok
  defp ca_policy({:pem, _}, _pol), do: {:error, :invalid_ca_pem}

  defp ca_policy({:file, p}, pol) do
    cond do
      not PathPolicy.normalized_absolute?(p) -> {:error, :invalid_ca_file}
      PathPolicy.denied?(p, pol.denied) -> {:error, :ca_file_denied}
      p == pol.fixture_root -> {:error, :ca_file_outside_fixture_root}
      not PathPolicy.under?(p, pol.fixture_root) -> {:error, :ca_file_outside_fixture_root}
      true -> :ok
    end
  end

  defp targets(specs, pol) do
    Enum.reduce_while(specs, {:ok, %{}}, fn {id, s}, {:ok, acc} ->
      with {:ok, pem} <- ca_source(s.ca, pol),
           {:ok, ders} <- ca_ders(pem),
           {:ok, t} <- VerifiedTarget.build(id, s.host, s.port, ders) do
        {:cont, {:ok, Map.put(acc, id, t)}}
      else
        {:error, r} -> {:halt, {:error, {r, id}}}
      end
    end)
  end

  defp ca_source({:pem, pem}, _pol), do: {:ok, pem}

  defp ca_source({:file, p}, pol) do
    with {:ok, st} <- PathPolicy.walk_below(pol.fixture_root, p, :regular) |> tag(:ca_file),
         do: read_bounded(p, st, @max_ca, :ca_file)
  end

  # Only CERTIFICATE blocks and whitespace; any key, other block or stray
  # text rejects the whole input.
  defp ca_ders(pem) do
    entries =
      try do
        :public_key.pem_decode(pem)
      rescue
        _ -> :error
      end

    cond do
      entries == :error or entries == [] ->
        {:error, :invalid_fixture_ca}

      Enum.any?(entries, &(elem(&1, 0) != :Certificate)) ->
        {:error, :ca_file_has_non_certificate}

      squash(:public_key.pem_encode(entries)) != squash(pem) ->
        {:error, :ca_pem_has_extra_content}

      true ->
        ders = Enum.map(entries, &elem(&1, 1))
        if VerifiedTarget.ca_set?(ders), do: {:ok, ders}, else: {:error, :invalid_fixture_ca}
    end
  end

  defp squash(s), do: String.replace(s, ~r/\s+/, "")

  # ── helpers ───────────────────────────────────────────────────────────────

  # Open, then require the handle to be the very inode the walk saw (a
  # symlink or rename swapped in between is rejected). This narrows, but does
  # not close, the window the launcher precondition covers.
  defp read_bounded(path, %File.Stat{} = walked, max, what) do
    case File.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        try do
          with {:ok, fi} <- :file.read_file_info(io, time: :posix),
               true <- PathPolicy.same_file?(walked, File.Stat.from_record(fi)) do
            case :file.read(io, max + 1) do
              {:ok, bin} when byte_size(bin) <= max -> {:ok, bin}
              {:ok, _} -> {:error, :"#{what}_too_large"}
              _ -> {:error, :"#{what}_unreadable"}
            end
          else
            false -> {:error, :"#{what}_changed"}
            _ -> {:error, :"#{what}_unreadable"}
          end
        after
          File.close(io)
        end

      {:error, _} ->
        {:error, :"#{what}_unreadable"}
    end
  end

  defp tag(:ok, _), do: :ok
  defp tag({:ok, _} = ok, _), do: ok
  defp tag({:error, r}, what), do: {:error, :"#{what}_#{r}"}

  defp exact_keys(m, keys, err),
    do: if(MapSet.new(Map.keys(m)) == keys, do: :ok, else: {:error, err})

  defp expect(true, _), do: :ok
  defp expect(_, err), do: {:error, err}
end
