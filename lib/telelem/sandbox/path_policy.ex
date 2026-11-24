defmodule Telelem.Sandbox.PathPolicy do
  @moduledoc """
  Path checks used before any sandbox file access.

  `normalized_absolute?/1`, `under?/2`, `overlap?/2` and `denied?/2` are pure.
  `safe_root/1` and `walk_below/3` only `lstat` paths that already passed the
  pure checks; neither opens anything.

  `default_denied_roots/0` names the production-shaped locations a sandbox
  must never read or write. Callers may add roots; they cannot remove these.

  ## What the lstat checks do and do not establish

  They reject a tree that, **at the moment it is inspected**, has a symlink,
  a non-directory component, a hard-linked file or a world-writable
  directory where the policy forbids one. They are not a TOCTOU defence:
  `lstat` resolves every component but the last, and nothing here holds a
  directory handle (`openat`/`O_NOFOLLOW` are not available through the
  BEAM's file API), so a concurrent writer can swap a component between the
  walk and a later open. `Telelem.Sandbox.Bootstrap` narrows the window for
  the files it reads by comparing the opened handle's inode with the walked
  one, but the safety of every path below a launcher root rests on the
  launcher precondition that the roots are immutable to anything untrusted
  (for example read-only bind mounts) for the lifetime of the process.
  """

  import Bitwise

  @default_denied ["/etc/telelem", "/var/lib/telelem", "/run/telelem", "/run/secrets"]
  @max_path 4096
  @other_write 0o002
  @sticky 0o1000

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

  @doc "Either path equals or contains the other."
  @spec overlap?(String.t(), String.t()) :: boolean()
  def overlap?(a, b), do: under?(a, b) or under?(b, a)

  @doc "True if `p` is inside a denied root or would contain one."
  @spec denied?(String.t(), [String.t()]) :: boolean()
  def denied?(p, denied_roots), do: Enum.any?(denied_roots, &overlap?(p, &1))

  @doc """
  Launcher-root precondition check. `lstat` every component from `/` to
  `root`: each must be a directory (no symlink anywhere in the path), no
  ancestor may be world-writable without the sticky bit, and `root` itself
  may not be world-writable at all. `root` must already be normalized and
  policy-checked.
  """
  @spec safe_root(String.t()) :: :ok | {:error, atom()}
  def safe_root(root) do
    with :ok <- dir(lstat("/"), :ancestor) do
      root
      |> Path.split()
      |> tl()
      |> Enum.reduce_while({:ok, "/"}, fn part, {:ok, dir} ->
        next = Path.join(dir, part)
        role = if next == root, do: :root, else: :ancestor

        case dir(lstat(next), role) do
          :ok -> {:cont, {:ok, next}}
          err -> {:halt, err}
        end
      end)
      |> case do
        {:ok, _} -> :ok
        err -> err
      end
    end
  end

  @doc """
  `lstat` each component of `p` strictly below `root`. Intermediate
  components must be non-world-writable directories; the last must be of
  `type` (`:regular` with exactly one link, or a non-world-writable
  `:directory`). No component may be a symlink. Returns the leaf's stat so a
  caller that opens the leaf can compare identities afterwards.
  `root` itself is checked separately by `safe_root/1`.
  """
  @spec walk_below(String.t(), String.t(), :regular | :directory) ::
          {:ok, File.Stat.t()} | {:error, atom()}
  def walk_below(root, p, type) when type in [:regular, :directory] do
    if p != root and under?(p, root) do
      parts = p |> String.replace_prefix(root <> "/", "") |> Path.split()
      walk(root, parts, type)
    else
      {:error, :outside_root}
    end
  end

  @doc "`walk_below(root, p, :regular)` without the stat."
  @spec no_symlinks_below(String.t(), String.t()) :: :ok | {:error, atom()}
  def no_symlinks_below(root, p) do
    with {:ok, _} <- walk_below(root, p, :regular), do: :ok
  end

  @doc false
  @spec same_file?(File.Stat.t(), File.Stat.t()) :: boolean()
  def same_file?(%File.Stat{} = a, %File.Stat{} = b) do
    a.type == :regular and b.type == :regular and a.links == 1 and b.links == 1 and
      a.inode == b.inode and a.major_device == b.major_device and
      a.minor_device == b.minor_device
  end

  defp walk(dir, [last], type) do
    case lstat(Path.join(dir, last)) do
      {:ok, %File.Stat{type: :symlink}} -> {:error, :symlink}
      {:ok, %File.Stat{type: ^type} = st} -> leaf(st, type)
      {:ok, _} when type == :regular -> {:error, :not_a_file}
      {:ok, _} -> {:error, :not_a_directory}
      {:error, _} -> {:error, :unreadable}
    end
  end

  defp walk(dir, [part | rest], type) do
    next = Path.join(dir, part)

    case dir(lstat(next), :below) do
      :ok -> walk(next, rest, type)
      err -> err
    end
  end

  defp leaf(%File.Stat{links: 1} = st, :regular), do: {:ok, st}
  defp leaf(%File.Stat{}, :regular), do: {:error, :hard_linked}

  defp leaf(%File.Stat{} = st, :directory) do
    if world_writable?(st), do: {:error, :world_writable}, else: {:ok, st}
  end

  defp dir({:ok, %File.Stat{type: :directory} = st}, role) do
    cond do
      not world_writable?(st) -> :ok
      role == :ancestor and sticky?(st) -> :ok
      true -> {:error, :world_writable}
    end
  end

  defp dir({:ok, %File.Stat{type: :symlink}}, _), do: {:error, :symlink}
  defp dir({:ok, _}, _), do: {:error, :not_a_directory}
  defp dir({:error, _}, _), do: {:error, :unreadable}

  defp lstat(p), do: File.lstat(p, time: :posix)

  defp world_writable?(%File.Stat{mode: m}) when is_integer(m), do: (m &&& @other_write) != 0
  defp world_writable?(_), do: true

  defp sticky?(%File.Stat{mode: m}) when is_integer(m), do: (m &&& @sticky) != 0
  defp sticky?(_), do: false
end
