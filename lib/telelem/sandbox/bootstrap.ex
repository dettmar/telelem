defmodule Telelem.Sandbox.Bootstrap do
  @moduledoc """
  Scoped sandbox bootstrap: explicit document in, `Bindings` or a bounded
  error out. No ambient environment, no OS trust store, no DNS, no default
  paths. Every path is checked against policy **before** it is opened, so a
  rejected bootstrap never touches a production-shaped file (the isolation
  harness audits this with strace).

  Document (JSON or the equivalent string-keyed map), exact key sets:

      {"version": 1, "mode": "sandbox",
       "fixture_targets": {"a": {"host": "127.0.0.1", "port": 18443, "ca_file": "/fx/ca.pem"},
                           "b": {"host": "127.0.0.1", "port": 18444, "ca_pem": "-----BEGIN…"}},
       "sandbox_store_root": "/sbx/store",
       "sandbox_signer_ref": "sandbox:local-fixture"}

  Required launcher options (not taken from the document):

    * `:fixture_root` — every `ca_file` must lie below it, with no symlink
      component below the root.
    * `:sandbox_root` — `sandbox_store_root` and the bootstrap file itself
      (for `from_file/2`) must lie below it.
    * `:denied_roots` (optional) — added to `PathPolicy.default_denied_roots/0`.

  Errors are atoms or `{atom, deployment_id | :invalid}`; never file contents.
  """

  alias Telelem.Sandbox.{Bindings, Id, PathPolicy, VerifiedTarget}

  @max_doc 65_536
  @max_ca 65_536
  @top_keys MapSet.new(~w(version mode fixture_targets sandbox_store_root sandbox_signer_ref))

  @type error :: atom() | {atom(), String.t() | :invalid}

  @spec from_file(String.t(), keyword()) :: {:ok, Bindings.t()} | {:error, error()}
  def from_file(path, opts) do
    with {:ok, pol} <- policy(opts),
         :ok <- sandbox_path(path, pol, :bootstrap_path_outside_sandbox_root),
         :ok <- PathPolicy.no_symlinks_below(pol.sandbox_root, path) |> tag(:bootstrap_file),
         {:ok, bin} <- read_bounded(path, @max_doc, :bootstrap_file),
         {:ok, doc} <- decode(bin) do
      build(doc, pol)
    end
  end

  @spec from_map(map(), keyword()) :: {:ok, Bindings.t()} | {:error, error()}
  def from_map(doc, opts) do
    with {:ok, pol} <- policy(opts), do: build(doc, pol)
  end

  # ── policy ────────────────────────────────────────────────────────────────

  defp policy(opts) when is_list(opts) do
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

  defp policy(_), do: {:error, :invalid_options}

  defp sandbox_path(p, pol, err) do
    if PathPolicy.normalized_absolute?(p) and PathPolicy.under?(p, pol.sandbox_root) and
         not PathPolicy.denied?(p, pol.denied),
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

  defp build(%{} = doc, pol) do
    with :ok <- exact_keys(doc, @top_keys, :unknown_or_missing_keys),
         :ok <- expect(doc["version"] == 1, :unsupported_version),
         :ok <- expect(doc["mode"] == "sandbox", :mode_not_sandbox),
         :ok <- store_root(doc["sandbox_store_root"], pol),
         :ok <- expect(Bindings.signer_ref_ok?(doc["sandbox_signer_ref"]), :invalid_signer_ref),
         {:ok, targets} <- targets(doc["fixture_targets"], pol) do
      b = %Bindings{
        mode: :sandbox,
        fixture_targets: targets,
        sandbox_store_root: doc["sandbox_store_root"],
        sandbox_signer_ref: doc["sandbox_signer_ref"]
      }

      with :ok <- Bindings.check(b), do: {:ok, b}
    end
  end

  defp build(_, _), do: {:error, :bootstrap_not_json_object}

  defp store_root(p, pol) do
    cond do
      not PathPolicy.normalized_absolute?(p) -> {:error, :invalid_store_root}
      PathPolicy.denied?(p, pol.denied) -> {:error, :store_root_denied}
      not PathPolicy.under?(p, pol.sandbox_root) -> {:error, :store_root_outside_sandbox_root}
      true -> :ok
    end
  end

  defp targets(%{} = m, pol) when map_size(m) in 1..16 do
    ids = Map.keys(m)

    cond do
      not Enum.all?(ids, &Id.valid?/1) ->
        {:error, {:invalid_deployment_id, :invalid}}

      true ->
        # Sorted so the first reported error is deterministic.
        ids
        |> Enum.sort()
        |> Enum.reduce_while({:ok, %{}}, fn id, {:ok, acc} ->
          case target(id, m[id], pol) do
            {:ok, t} -> {:cont, {:ok, Map.put(acc, id, t)}}
            {:error, r} -> {:halt, {:error, {r, id}}}
          end
        end)
        |> distinct_endpoints()
    end
  end

  defp targets(_, _), do: {:error, :invalid_fixture_targets}

  defp distinct_endpoints({:ok, ts} = ok) do
    eps = Enum.map(ts, fn {_, t} -> {t.address, t.port} end)
    if length(Enum.uniq(eps)) == length(eps), do: ok, else: {:error, :duplicate_fixture_endpoint}
  end

  defp distinct_endpoints(err), do: err

  defp target(id, %{} = t, pol) do
    keys = t |> Map.keys() |> MapSet.new()

    ca_key =
      cond do
        keys == MapSet.new(~w(host port ca_file)) -> :file
        keys == MapSet.new(~w(host port ca_pem)) -> :pem
        true -> :bad
      end

    with :ok <- expect(ca_key != :bad, :unknown_or_missing_target_keys),
         {:ok, _} <- VerifiedTarget.loopback(t["host"]),
         :ok <- expect(is_integer(t["port"]) and t["port"] in 1..65535, :invalid_port),
         {:ok, pem} <- ca_source(ca_key, t, pol),
         {:ok, ders} <- ca_ders(pem) do
      VerifiedTarget.build(id, t["host"], t["port"], ders)
    end
  end

  defp target(_, _, _), do: {:error, :invalid_target}

  defp ca_source(:pem, %{"ca_pem" => pem}, _pol)
       when is_binary(pem) and byte_size(pem) <= @max_ca,
       do: {:ok, pem}

  defp ca_source(:pem, _, _), do: {:error, :invalid_ca_pem}

  defp ca_source(:file, %{"ca_file" => p}, pol) do
    cond do
      not PathPolicy.normalized_absolute?(p) ->
        {:error, :invalid_ca_file}

      PathPolicy.denied?(p, pol.denied) ->
        {:error, :ca_file_denied}

      not PathPolicy.under?(p, pol.fixture_root) or p == pol.fixture_root ->
        {:error, :ca_file_outside_fixture_root}

      true ->
        with :ok <- PathPolicy.no_symlinks_below(pol.fixture_root, p) |> tag(:ca_file),
             do: read_bounded(p, @max_ca, :ca_file)
    end
  end

  # Only CERTIFICATE blocks; any key or other block rejects the whole file.
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

      true ->
        ders = Enum.map(entries, &elem(&1, 1))
        if VerifiedTarget.ca_set?(ders), do: {:ok, ders}, else: {:error, :invalid_fixture_ca}
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp read_bounded(path, max, what) do
    case File.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        try do
          case :file.read(io, max + 1) do
            {:ok, bin} when byte_size(bin) <= max -> {:ok, bin}
            {:ok, _} -> {:error, :"#{what}_too_large"}
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
  defp tag({:error, r}, what), do: {:error, :"#{what}_#{r}"}

  defp exact_keys(m, keys, err),
    do: if(MapSet.new(Map.keys(m)) == keys, do: :ok, else: {:error, err})

  defp expect(true, _), do: :ok
  defp expect(_, err), do: {:error, err}
end
