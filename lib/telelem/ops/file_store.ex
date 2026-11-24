defmodule Telelem.Ops.FileStore do
  @moduledoc """
  File-backed `Telelem.Ops.RevisionStore` for exactly one tenant and one route
  (TEL-015, "`up` twice preserves state").

  One registered GenServer owns one directory. The directory holds two fixed
  names:

    * `route.rec`     — the committed record (the only file ever read as state)
    * `route.rec.tmp` — the single, exclusive staging file for the next write

  ## Caller contract (not enforced here)

  The launcher must hold a **crash-released, cross-VM single-writer lock** for
  the directory (for example an exclusive `flock(2)` on the store directory
  itself, held by the BEAM OS process) **before** calling `provision/3` or
  `start_link/1`, and keep holding it for as long as any FileStore or
  provisioning call for that directory can run in this OS process. Every
  no-overwrite and cleanup decision below assumes no other writer touches the
  directory. This module never takes that lock itself; the integration with a
  launcher that does is not tested here.

  Within one VM the module adds a guard that does not replace that lock. It
  is keyed only by the directory's filesystem identity (device and inode from
  `lstat` of `:dir`), never by the path string or the registered name, so a
  renamed or bind-mounted path to the same directory shares it. It has two
  parts:

    * exclusion: a live owner (a store process for its whole life, or a
      `provision/3` caller for the whole provisioning call) holds a `:global`
      lock on that identity. A concurrent open gets `{:error, :already_open}`,
      a concurrent provision `{:error, :busy}`; neither touches any file. Open
      and provision take the guard before their first `lstat` of `route.rec`
      or `route.rec.tmp` and hold it through temp cleanup or the rename.
    * abandonment: an owner that may have died with file I/O in flight
      leaves the identity marked, and every later open or provision of that
      directory in this VM returns `{:error, :abandoned_in_this_vm}` (see
      *I/O quiescence*).

  Before touching the files, the owner re-`lstat`s `:dir` and refuses
  `{:error, {:dir, :changed}}` if it no longer has the guarded identity.

  ## Record schema

  Bounded (`max_bytes/0`) and checksummed. Before anything is staged, the
  exact canonical bytes are checked against what a later open accepts: size
  within the bound (`:too_large`), and the read validator below must return
  the same revision and plan digest (`:not_decodable`; for example an
  integer of more than 310 digits, which `JSON.decode/1` refuses). A
  refusal is `{:error, reason}` from provisioning and `{:error, {:rejected,
  reason}}` from a compare-and-write, with nothing written. A growing `rev`
  can therefore stop a route from advancing. It never leaves a main that the
  store cannot read. The format is:

      <payload> "\\n" <lowercase hex SHA-256 of payload> "\\n"

  where `payload` is the compact UTF-8 JSON array

      ["telelem.ops.file_store/1", tenant_id, route_id,
       [route_id, rev, [dep_a, dep_b], preferred, fallback_on_refusal,
        deadline_ms, max_attempts, revision_digest],
       last_plan_digest | null]

  A read accepts a file only if the checksum matches, the payload is exactly
  the canonical encoding of what it decodes to, the bound tenant and route
  match (outer and inner route ID), `Telelem.Route.Revision.new/1` accepts the
  revision including its §1 digest, and `last_plan_digest` is `null` for
  `rev` 1 and otherwise equals `Telelem.Ops.PlanDigest.compute(tenant, route,
  rev - 1, preferred, revision_digest)`. The file must be a regular file with
  one link, owned by this process's effective uid and not group/other
  writable; it is read through a handle whose inode must match the `lstat`.

  The store validates revisions with `Telelem.Route.Revision` itself, so a
  context using it must inject `Telelem.Route.Revision` as `revisions`.

  ## Provisioning (`provision/3`)

  Explicit and only into a directory with neither name present. Under the
  in-VM guard for the whole call, it checks both names, writes the `rev` 1
  record to an exclusively created temp file, syncs and closes it, re-checks
  that no main exists and renames it into place. The no-overwrite guarantee
  across OS processes rests on the launcher lock. Inside one VM the guard
  serializes provisions with each other and with open. On any failure the
  directory is left as it is.

  ## Open (`start_link/1`)

    * main valid, no temp: open.
    * main valid, temp is a regular single-link file owned by this euid: the
      temp is an uncommitted write (the rename never happened); it is removed
      **after** main validated, then open.
    * main valid, temp anything else (symlink, directory, hard link, foreign
      owner): refuse, temp untouched.
    * no main, temp present: `{:error, :interrupted_provision}`. Never
      promoted, never removed; recover with a fresh directory.
    * no main, no temp: `{:error, :not_provisioned}`. Never seeded.
    * main unsafe or invalid: refuse, nothing touched.

  ## Compare-and-write

  Within the GenServer, which holds the in-VM guard: if `expected` is not exactly the current `rev` and
  revision digest, the reply is `{:conflict, current}` only when `current` is
  strictly newer than `expected.rev`; otherwise `{:error, {:rejected,
  :expected_not_current}}`. The new revision must be accepted by
  `Revision.new/1` with its own digest, have `rev` = current + 1 and the same
  `route_id`, `deployments` (order included), `fallback_on_refusal`,
  `deadline_ms` and `max_attempts`; re-preferring the current deployment is a
  valid successor. The plan digest must equal the recomputed one, and the
  encoded record must pass the pre-staging check (`:too_large`,
  `:not_decodable`; see *Record schema*). A refused
  successor gets `{:error, {:rejected, reason}}` (nothing written; outside the
  `RevisionStore` reply set, so `Telelem.Ops.Route` reports it as
  outcome-unknown/`store_integrity`, which is conservative).

  The write is: exclusive create temp, `chmod 0600`, write, `fsync`, close,
  `rename` onto main. State is updated and `{:ok, record}` replied only after
  the rename returned `:ok`. Any failure from the exclusive create onwards
  stops the store **without a reply** (the caller's call exits, which ops
  reports as outcome-unknown/`store_unavailable`); nothing is retried. A
  restart re-reads the disk, which is the only truth about a failed write.

  ## Durability limits

  No directory `fsync` is available through the BEAM file API, so a power
  loss or kernel crash may lose a completed rename (state rolls back to an
  earlier record or, after provisioning, to an empty directory) or leave a
  temp. No power-loss, rollback-resistance or freshness claim is made.

  ## I/O quiescence

  BEAM file operations run on dirty I/O schedulers. A process killed with
  `:kill` (or any untrapped exit) can be observed dead (monitors fire, names
  and `:global` locks are released) while a `write`, `fsync` or `rename` it
  started is still executing, and that operation may still land afterwards.
  The store traps exits, so ordinary shutdown and callback crashes run
  `terminate/2` after its last file operation returned; only then is the
  directory released for a reopen in the same VM. If the store dies any other
  way, or crashes during `init/1`, the directory stays marked.
  `provision/3` runs in the caller's process and releases the guard when it
  returns or raises; a caller killed by an exit signal during the call leaves
  the directory marked, whatever stage it reached. Either way, every later
  `start_link/1` or `provision/3` for that directory in this VM returns
  `{:error, :abandoned_in_this_vm}`, nothing is reseeded or cleaned, and the
  only recovery is to restart the OS process (whose exit ends all its
  syscalls before the kernel releases the launcher's lock). The new process
  then sees whatever reached the disk: main, a leftover temp, or an
  interrupted provision.

  The marker is per VM and per device/inode. It is not persisted. If a
  marked directory is deleted and its inode is reused by a new directory on
  the same device, the new directory is refused too (fail closed).

  ## Calls

  `fetch/3` and `compare_and_write/6` use the default 5 s `GenServer.call`
  timeout. A timed-out compare-and-write may still commit; read back and
  compare `last_plan_digest` (m2a-contracts §4).
  """

  use GenServer
  @behaviour Telelem.Ops.RevisionStore

  import Bitwise

  alias Telelem.Ops.PlanDigest
  alias Telelem.Route.Revision

  @version "telelem.ops.file_store/1"
  @main "route.rec"
  @temp "route.rec.tmp"
  @max_bytes 1024
  @file_mode 0o600
  @group_other_write 0o022
  @fixed [:route_id, :deployments, :fallback_on_refusal, :deadline_ms, :max_attempts]

  @type barrier_stage ::
          :provision_synced | :provision_renamed | :open_validated | :cas_synced | :cas_renamed
  @type opt ::
          {:name, atom()}
          | {:dir, String.t()}
          | {:tenant_id, String.t()}
          | {:route_id, String.t()}
          | {:barrier, (barrier_stage() -> any())}

  @doc "Record format version (first element of the payload)."
  def version, do: @version
  @doc "Committed record file name inside the store directory."
  def main_name, do: @main
  @doc "Staging file name inside the store directory."
  def temp_name, do: @temp
  @doc "Upper bound on a record file's size in bytes."
  def max_bytes, do: @max_bytes

  @doc """
  Canonical file bytes for a record. Exposed for golden and tamper tests.
  `revision` must be a valid `%Revision{}`.
  """
  @spec encode(String.t(), Revision.t(), String.t() | nil) :: binary()
  def encode(tenant_id, %Revision{} = r, last_plan_digest) do
    payload =
      JSON.encode!([
        @version,
        tenant_id,
        r.route_id,
        [
          r.route_id,
          r.rev,
          r.deployments,
          r.preferred,
          r.fallback_on_refusal,
          r.deadline_ms,
          r.max_attempts,
          r.digest
        ],
        last_plan_digest
      ])

    payload <> "\n" <> sha256(payload) <> "\n"
  end

  ## Provisioning

  @doc """
  Writes the initial `rev` 1 record into a directory that contains neither
  `route.rec` nor `route.rec.tmp`. Caller contract: the launcher lock is held
  (see moduledoc). Options: `:barrier` (test hook, called with
  `:provision_synced` before and `:provision_renamed` after the rename).

  Errors: `{:error, {:dir, reason}}`, `{:error, {:invalid, :tenant_id}}`,
  `{:error, {:invalid_revision, reason}}` (not a valid `rev` 1 revision;
  `reason` is `:rev`, `:digest` or a `Revision.new/1` error),
  `{:error, :too_large}` / `{:error, :not_decodable}` (the encoded record
  would be refused by a later open; nothing written), `{:error, :busy}` (another live owner holds the in-VM guard),
  `{:error, :abandoned_in_this_vm}`, `{:error, {:dir, :changed}}`,
  `{:error, :already_provisioned}`, `{:error, :interrupted_provision}` (temp
  present, left untouched), `{:error, {:io, stage, reason}}` (directory left
  as is; if a temp remains it needs fresh-directory recovery).
  """
  @spec provision(String.t(), String.t(), Revision.t(), keyword()) :: :ok | {:error, term()}
  def provision(dir, tenant_id, revision, opts \\ [])

  def provision(dir, tenant_id, %Revision{} = revision, opts) do
    barrier = Keyword.get(opts, :barrier, fn _ -> :ok end)

    with :ok <- valid_id(tenant_id, :tenant_id),
         {:ok, revision} <- invalid_revision(revalidate(revision)),
         :ok <- if(revision.rev == 1, do: :ok, else: {:error, {:invalid_revision, :rev}}),
         {:ok, bytes} <- storable(encode(tenant_id, revision, nil), tenant_id, revision, nil),
         {:ok, dir_stat} <- check_dir(dir) do
      case guarded(dir, dir_stat, fn -> provision_locked(dir, bytes, barrier) end) do
        {:error, :held} -> {:error, :busy}
        other -> other
      end
    end
  end

  def provision(_dir, _tenant_id, _revision, _opts), do: {:error, {:invalid_revision, :revision}}

  # Runs entirely under the in-VM directory guard, from the first lstat of the
  # two names to the rename.
  defp provision_locked(dir, bytes, barrier) do
    with :ok <- absent(dir),
         :ok <- stage_temp(dir, bytes) |> io(:temp) do
      barrier.(:provision_synced)

      case lstat(Path.join(dir, @main)) do
        {:error, :enoent} ->
          with :ok <- :file.rename(Path.join(dir, @temp), Path.join(dir, @main)) |> io(:rename) do
            barrier.(:provision_renamed)
            :ok
          end

        _ ->
          {:error, :already_provisioned}
      end
    end
  end

  # Runs `fun` in the calling process under the in-VM guard. Any return or
  # exception from `fun` releases the guard (the caller's own file I/O has
  # returned by then); an exit signal that kills the caller does not, so the
  # directory stays marked abandoned in this VM.
  defp guarded(dir, dir_stat, fun) do
    key = dir_key(dir_stat)

    with :ok <- claim(key) do
      try do
        with :ok <- same_dir(dir, dir_stat), do: fun.()
      after
        release(key)
      end
    end
  end

  defp invalid_revision({:ok, _} = ok), do: ok
  defp invalid_revision({:error, reason}), do: {:error, {:invalid_revision, reason}}

  defp absent(dir) do
    case {lstat(Path.join(dir, @main)), lstat(Path.join(dir, @temp))} do
      {{:error, :enoent}, {:error, :enoent}} -> :ok
      {{:error, :enoent}, _} -> {:error, :interrupted_provision}
      _ -> {:error, :already_provisioned}
    end
  end

  ## Server API

  @doc """
  Opens the store. Required: `:name` (atom; the store is one registered
  process), `:dir` (normalized absolute path), `:tenant_id`, `:route_id`.
  Optional `:barrier` test hook, run inside the store process:
  `:open_validated` (in `init/1`, main validated, before temp cleanup),
  `:cas_synced` before and `:cas_renamed` after a write's rename. Caller
  contract: the launcher lock is held (see moduledoc).

  Errors besides the validation ones listed in the moduledoc:
  `{:error, :already_open}` (another live owner, a store or a provisioning
  call, holds the in-VM guard), `{:error, :abandoned_in_this_vm}` and
  `{:error, {:dir, :changed}}`.
  """
  @spec start_link([opt()]) :: GenServer.on_start()
  def start_link(opts), do: start(opts, :start_link)

  @doc "Like `start_link/1` but not linked."
  @spec start([opt()]) :: GenServer.on_start()
  def start(opts), do: start(opts, :start)

  defp start(opts, how) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} when is_atom(name) and name not in [nil, true, false] ->
        apply(GenServer, how, [__MODULE__, opts, [name: name]])

      _ ->
        {:error, {:invalid, :name}}
    end
  end

  def child_spec(opts),
    do: %{
      id: {__MODULE__, opts[:name]},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }

  @doc "Stops the store; the directory is released once `terminate/2` ran."
  def stop(server, timeout \\ :infinity), do: GenServer.stop(server, :normal, timeout)

  @impl Telelem.Ops.RevisionStore
  def fetch(server, tenant_id, route_id),
    do: GenServer.call(server, {:fetch, tenant_id, route_id})

  @impl Telelem.Ops.RevisionStore
  def compare_and_write(server, tenant_id, route_id, expected, new_revision, plan_digest) do
    GenServer.call(server, {:cas, tenant_id, route_id, expected, new_revision, plan_digest})
  end

  ## GenServer

  @impl GenServer
  def init(opts) do
    with :ok <- valid_id(opts[:tenant_id], :tenant_id),
         :ok <- valid_id(opts[:route_id], :route_id),
         {:ok, dir_stat} <- check_dir(opts[:dir]) do
      key = dir_key(dir_stat)

      case claim(key) do
        :ok ->
          # Released here only for an error *returned* by open/2. A crash in
          # init leaves the marker set (fail closed).
          case open(opts, dir_stat, key) do
            {:ok, state} ->
              {:ok, state}

            {:error, _} = err ->
              release(key)
              err
          end

        {:error, :held} ->
          {:error, :already_open}

        {:error, _} = err ->
          err
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # In-VM guard, keyed only by the directory's filesystem identity (device
  # and inode from `lstat`), so two paths that reach the same directory (a
  # rename, a bind mount) share it and the registered name plays no part.
  # Two parts:
  #
  #   * a `:global` lock on `{key, owner}`: exclusion between live owners
  #     (a store process or a provisioning caller), released by `:global`
  #     as soon as the owner is observed dead;
  #   * a `:persistent_term` marker holding the owner pid, set before any
  #     file I/O beyond the directory lstat and erased (before the lock) only
  #     after the owner's own I/O returned.
  #
  # Holding the lock while the marker is still set therefore means the
  # previous owner died without releasing: its I/O may still be in flight.
  @doc false
  def dir_key(%File.Stat{major_device: major, minor_device: minor, inode: inode}),
    do: {__MODULE__, :dir, major, minor, inode}

  defp claim(key) do
    me = self()

    case :persistent_term.get(key, nil) do
      # Re-entrant call by the current owner (`:global` would grant it).
      ^me ->
        {:error, :held}

      _ ->
        if :global.set_lock({key, me}, [node()], 0) do
          case :persistent_term.get(key, nil) do
            nil ->
              :persistent_term.put(key, me)
              :ok

            _dead_owner ->
              :global.del_lock({key, me}, [node()])
              {:error, :abandoned_in_this_vm}
          end
        else
          {:error, :held}
        end
    end
  end

  defp release(key) do
    :persistent_term.erase(key)
    :global.del_lock({key, self()}, [node()])
  end

  # The guard was taken for the identity seen by the first lstat; the path
  # must still name that directory.
  defp same_dir(dir, %File.Stat{} = before) do
    case lstat(dir) do
      {:ok, %File.Stat{type: :directory} = now} ->
        if dir_key(now) == dir_key(before), do: :ok, else: {:error, {:dir, :changed}}

      _ ->
        {:error, {:dir, :changed}}
    end
  end

  defp open(opts, dir_stat, key) do
    Process.flag(:trap_exit, true)
    dir = opts[:dir]
    tenant = opts[:tenant_id]
    route = opts[:route_id]
    barrier = Keyword.get(opts, :barrier, fn _ -> :ok end)

    with :ok <- same_dir(dir, dir_stat),
         {:ok, euid} <- euid(),
         {:ok, record} <- read_main(dir, tenant, route, euid),
         _ = barrier.(:open_validated),
         :ok <- clear_temp(dir, euid) do
      {:ok,
       %{
         key: key,
         dir: dir,
         tenant: tenant,
         route: route,
         record: record,
         barrier: barrier
       }}
    end
  end

  @impl GenServer
  def handle_call({:fetch, tenant, route}, _from, %{tenant: tenant, route: route} = s),
    do: {:reply, {:ok, s.record}, s}

  def handle_call({:fetch, _, _}, _from, s), do: {:reply, :not_found, s}

  def handle_call({:cas, tenant, route, expected, new, plan_digest}, _from, s)
      when tenant == s.tenant and route == s.route do
    current = s.record.revision

    case check_expected(expected, current) do
      :current ->
        with {:ok, revision} <- successor(current, new, plan_digest, s),
             bytes = encode(s.tenant, revision, plan_digest),
             {:ok, bytes} <- storable(bytes, s.tenant, revision, plan_digest) do
          commit(revision, plan_digest, bytes, s)
        else
          {:error, reason} -> {:reply, {:error, {:rejected, reason}}, s}
        end

      :stale ->
        {:reply, {:conflict, s.record}, s}

      :not_current ->
        {:reply, {:error, {:rejected, :expected_not_current}}, s}
    end
  end

  def handle_call({:cas, _, _, _, _, _}, _from, s), do: {:reply, {:conflict, :not_found}, s}

  @impl GenServer
  def handle_info(_msg, s), do: {:noreply, s}

  @impl GenServer
  def terminate(_reason, s), do: release(s.key)

  ## Compare-and-write

  defp check_expected(%{rev: rev, digest: digest}, current)
       when is_integer(rev) and rev > 0 and is_binary(digest) do
    cond do
      rev == current.rev and PlanDigest.equal?(digest, current.digest) -> :current
      current.rev > rev -> :stale
      true -> :not_current
    end
  end

  defp check_expected(_, _), do: :not_current

  defp successor(current, new, plan_digest, s) do
    with {:ok, revision} <- revalidate(new),
         :ok <- same_fixed(current, revision),
         :ok <- if(revision.rev == current.rev + 1, do: :ok, else: {:error, :rev}),
         recomputed =
           PlanDigest.compute(s.tenant, s.route, current.rev, revision.preferred, revision.digest),
         :ok <-
           if(PlanDigest.equal?(recomputed, plan_digest), do: :ok, else: {:error, :plan_digest}) do
      {:ok, revision}
    end
  end

  defp same_fixed(current, revision) do
    case Enum.find(@fixed, &(Map.fetch!(current, &1) != Map.fetch!(revision, &1))) do
      nil -> :ok
      field -> {:error, field}
    end
  end

  defp commit(revision, plan_digest, bytes, s) do
    with :ok <- stage_temp(s.dir, bytes) |> io(:temp),
         _ = s.barrier.(:cas_synced),
         :ok <- :file.rename(Path.join(s.dir, @temp), Path.join(s.dir, @main)) |> io(:rename) do
      s.barrier.(:cas_renamed)
      record = %{revision: revision, last_plan_digest: plan_digest}
      {:reply, {:ok, record}, %{s | record: record}}
    else
      # Outcome on disk unknown to this process: no reply, no retry, no
      # cleanup; a reopen reads the disk.
      {:error, reason} -> {:stop, {:shutdown, {:write_failed, reason}}, s}
    end
  end

  ## Files

  # Bytes are staged only if a later open would accept exactly them: within
  # the size bound, and `decode/3` (the read validator) returns the same
  # revision and plan digest. `JSON.decode/1` refuses integers longer than
  # 310 digits, so a huge `rev` or `deadline_ms` can fail here even within
  # the size bound.
  defp storable(bytes, _tenant, _revision, _last) when byte_size(bytes) > @max_bytes,
    do: {:error, :too_large}

  defp storable(bytes, tenant, %Revision{} = revision, last) do
    case decode(bytes, tenant, revision.route_id) do
      {:ok, %{revision: ^revision, last_plan_digest: ^last}} -> {:ok, bytes}
      _ -> {:error, :not_decodable}
    end
  end

  # Exclusive create, restrict mode, write, fsync, close. Leaves the temp in
  # place on failure (only an open with a valid main may remove it).
  defp stage_temp(dir, bytes) do
    temp = Path.join(dir, @temp)

    case :file.open(temp, [:write, :exclusive, :binary, :raw]) do
      {:ok, io} ->
        result =
          with :ok <- tag(:file.change_mode(temp, @file_mode), :chmod),
               :ok <- tag(:file.write(io, bytes), :write) do
            tag(:file.sync(io), :sync)
          end

        closed = tag(:file.close(io), :close)
        if result == :ok, do: closed, else: result

      {:error, reason} ->
        {:error, {:create, reason}}
    end
  end

  defp tag(:ok, _), do: :ok
  defp tag({:error, reason}, stage), do: {:error, {stage, reason}}

  defp io(:ok, _), do: :ok
  defp io({:error, {stage, reason}}, :temp), do: {:error, {:io, stage, reason}}
  defp io({:error, reason}, stage), do: {:error, {:io, stage, reason}}

  defp read_main(dir, tenant, route, euid) do
    path = Path.join(dir, @main)

    case lstat(path) do
      {:error, :enoent} ->
        case lstat(Path.join(dir, @temp)) do
          {:error, :enoent} -> {:error, :not_provisioned}
          _ -> {:error, :interrupted_provision}
        end

      {:ok, st} ->
        with :ok <- owned_file(st, euid, :main),
             {:ok, bin} <- read_bounded(path, st) do
          decode(bin, tenant, route)
        end

      {:error, _} ->
        {:error, {:main, :unreadable}}
    end
  end

  defp clear_temp(dir, euid) do
    path = Path.join(dir, @temp)

    case lstat(path) do
      {:error, :enoent} ->
        :ok

      {:ok, st} ->
        with :ok <- owned_file(st, euid, :temp) do
          case :file.delete(path) do
            :ok -> :ok
            {:error, reason} -> {:error, {:temp, {:delete, reason}}}
          end
        end

      {:error, _} ->
        {:error, {:temp, :unreadable}}
    end
  end

  defp owned_file(%File.Stat{} = st, euid, what) do
    cond do
      st.type == :symlink -> {:error, {what, :symlink}}
      st.type != :regular -> {:error, {what, :not_a_file}}
      st.links != 1 -> {:error, {what, :hard_linked}}
      st.uid != euid -> {:error, {what, :not_owned}}
      (st.mode &&& @group_other_write) != 0 -> {:error, {what, :writable_by_others}}
      true -> :ok
    end
  end

  defp read_bounded(path, %File.Stat{} = walked) do
    case :file.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        try do
          with {:ok, fi} <- :file.read_file_info(io, time: :posix),
               true <- same_file?(walked, File.Stat.from_record(fi)) do
            case :file.read(io, @max_bytes + 1) do
              {:ok, bin} when byte_size(bin) <= @max_bytes -> {:ok, bin}
              {:ok, _} -> {:error, {:main, :too_large}}
              :eof -> {:error, {:main, :empty}}
              _ -> {:error, {:main, :unreadable}}
            end
          else
            false -> {:error, {:main, :changed}}
            _ -> {:error, {:main, :unreadable}}
          end
        after
          :file.close(io)
        end

      {:error, _} ->
        {:error, {:main, :unreadable}}
    end
  end

  defp same_file?(a, b),
    do:
      b.type == :regular and b.links == 1 and a.inode == b.inode and
        a.major_device == b.major_device and a.minor_device == b.minor_device

  @doc false
  # Full validation of record bytes against the bound tenant and route.
  @spec decode(binary(), String.t(), String.t()) ::
          {:ok, Telelem.Ops.RevisionStore.route_record()} | {:error, {:main, atom()}}
  def decode(bin, tenant, route) when is_binary(bin) do
    with {:ok, payload} <- split(bin),
         {:ok, term} <- json(payload),
         {:ok, attrs, last} <- shape(term, tenant, route),
         {:ok, revision} <- main_error(Revision.new(attrs), :revision),
         :ok <- last_plan(tenant, route, revision, last),
         :ok <-
           if(encode(tenant, revision, last) == bin, do: :ok, else: {:error, {:main, :encoding}}) do
      {:ok, %{revision: revision, last_plan_digest: last}}
    end
  end

  defp split(bin) when byte_size(bin) > 66 and byte_size(bin) <= @max_bytes do
    size = byte_size(bin) - 66

    case bin do
      <<payload::binary-size(^size), ?\n, sum::binary-size(64), ?\n>> ->
        if :crypto.hash_equals(sha256(payload), sum),
          do: {:ok, payload},
          else: {:error, {:main, :checksum}}

      _ ->
        {:error, {:main, :framing}}
    end
  end

  defp split(_), do: {:error, {:main, :framing}}

  defp json(payload) do
    case JSON.decode(payload) do
      {:ok, term} -> {:ok, term}
      {:error, _} -> {:error, {:main, :encoding}}
    end
  end

  defp shape([@version, tenant, route, [inner, rev, deps, pref, fb, dl, ma, dig], last], t, r) do
    cond do
      tenant != t ->
        {:error, {:main, :wrong_tenant}}

      route != r or inner != r ->
        {:error, {:main, :wrong_route}}

      true ->
        {:ok,
         %{
           route_id: inner,
           rev: rev,
           deployments: deps,
           preferred: pref,
           fallback_on_refusal: fb,
           deadline_ms: dl,
           max_attempts: ma,
           digest: dig
         }, last}
    end
  end

  defp shape([v | _], _, _) when v != @version, do: {:error, {:main, :version}}
  defp shape(_, _, _), do: {:error, {:main, :schema}}

  defp last_plan(_tenant, _route, %Revision{rev: 1}, nil), do: :ok

  defp last_plan(tenant, route, %Revision{rev: rev} = r, last) when rev > 1 and is_binary(last) do
    expected = PlanDigest.compute(tenant, route, rev - 1, r.preferred, r.digest)
    if PlanDigest.equal?(expected, last), do: :ok, else: {:error, {:main, :plan_digest}}
  end

  defp last_plan(_, _, _, _), do: {:error, {:main, :plan_digest}}

  defp main_error({:ok, _} = ok, _), do: ok
  defp main_error({:error, _}, what), do: {:error, {:main, what}}

  ## Helpers

  defp revalidate(%Revision{} = r), do: revalidate(Map.from_struct(r))

  defp revalidate(%{} = attrs) when not is_struct(attrs) do
    if Map.has_key?(attrs, :digest) do
      case Revision.new(attrs) do
        {:ok, r} -> {:ok, r}
        {:error, :digest_mismatch} -> {:error, :digest}
        {:error, other} -> {:error, other}
      end
    else
      {:error, {:invalid, :digest}}
    end
  end

  defp revalidate(_), do: {:error, :revision}

  defp check_dir(dir) do
    with true <- Telelem.Sandbox.PathPolicy.normalized_absolute?(dir) || {:error, {:dir, :path}},
         :ok <- dir_error(Telelem.Sandbox.PathPolicy.safe_root(dir)),
         {:ok, st} <- dir_error(lstat(dir)) do
      {:ok, st}
    end
  end

  defp dir_error(:ok), do: :ok
  defp dir_error({:ok, _} = ok), do: ok
  defp dir_error({:error, reason}), do: {:error, {:dir, reason}}

  defp valid_id(id, field), do: if(Revision.id?(id), do: :ok, else: {:error, {:invalid, field}})

  defp lstat(path), do: File.lstat(path, time: :posix)

  defp euid do
    with {:ok, status} <- File.read("/proc/self/status"),
         [_, uid] <- Regex.run(~r/^Uid:\s+\d+\s+(\d+)/m, status) do
      {:ok, String.to_integer(uid)}
    else
      _ -> {:error, :euid_unavailable}
    end
  end

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)
end
