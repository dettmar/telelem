defmodule Telelem.Sandbox.PathPolicy do
  @moduledoc """
  Pure path checks used before any sandbox file access. Nothing here touches
  the filesystem except `no_symlinks_below/2`, which only `lstat`s components
  strictly below an already-approved root.

  `default_denied_roots/0` names the production-shaped locations a sandbox
  must never read or write. Callers may add roots; they cannot remove these.
  """

  @default_denied ["/etc/telelem", "/var/lib/telelem", "/run/telelem", "/run/secrets"]
  @max_path 4096

  @spec default_denied_roots() :: [String.t()]
  def default_denied_roots, do: @default_denied

  @doc "Absolute, already normalized (no `.`, `..`, `//`, trailing `/`), no NUL, not `/`."
  @spec normalized_absolute?(term()) :: boolean()
  def normalized_absolute?(p) when is_binary(p) and byte_size(p) in 2..@max_path do
    String.starts_with?(p, "/") and String.valid?(p) and not String.contains?(p, <<0>>) and
      Path.expand(p) == p
  end

  def normalized_absolute?(_), do: false

  @doc "`p` equals `root` or lies below it. Both must be normalized absolute."
  @spec under?(String.t(), String.t()) :: boolean()
  def under?(p, root), do: p == root or String.starts_with?(p, root <> "/")

  @doc "True if `p` is inside a denied root or would contain one."
  @spec denied?(String.t(), [String.t()]) :: boolean()
  def denied?(p, denied_roots) do
    Enum.any?(denied_roots, fn d -> under?(p, d) or under?(d, p) end)
  end

  @doc """
  `lstat` each component of `p` strictly below `root`; every intermediate one
  must be a directory and the last a regular file, none a symlink. `root`
  itself is trusted as given by the launcher.
  """
  @spec no_symlinks_below(String.t(), String.t()) :: :ok | {:error, atom()}
  def no_symlinks_below(root, p) do
    rel = String.trim_leading(String.replace_prefix(p, root, ""), "/")
    parts = Path.split(rel)
    walk(root, parts)
  end

  defp walk(_dir, []), do: {:error, :not_a_file}

  defp walk(dir, [last]) do
    case File.lstat(Path.join(dir, last)) do
      {:ok, %File.Stat{type: :regular}} -> :ok
      {:ok, %File.Stat{type: :symlink}} -> {:error, :symlink}
      {:ok, _} -> {:error, :not_a_file}
      {:error, _} -> {:error, :unreadable}
    end
  end

  defp walk(dir, [part | rest]) do
    next = Path.join(dir, part)

    case File.lstat(next) do
      {:ok, %File.Stat{type: :directory}} -> walk(next, rest)
      {:ok, %File.Stat{type: :symlink}} -> {:error, :symlink}
      {:ok, _} -> {:error, :not_a_file}
      {:error, _} -> {:error, :unreadable}
    end
  end
end
