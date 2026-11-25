# Route-store writer entrypoint (M2a, TEL-015 "`up` twice preserves state").
#
# Runs INSIDE the writer environment that scripts/route-store/launch builds: the
# runner container, after scripts/with-route-store-lock has taken the store lock and
# exec'd `elixir` into this script. Do not run it any other way; it refuses to touch
# the store unless it can see that this OS process holds that lock.
#
#   elixir -pa EBIN main.exs provision --dir D --tenant T --route R --deployments A,B \
#       --preferred A --fallback-on-refusal true|false --deadline-ms N --max-attempts 1|2
#   elixir -pa EBIN main.exs start --dir D --tenant T --route R [--harness-control]
#   elixir -pa EBIN main.exs boot-validated --dir /sbx/store --tenant T --route R \
#       --manifest-sha256 HEX
#
# stdout carries exactly one compact JSON object per line (events below); stderr
# carries "route-store: ..." diagnostics. Exit status:
#   0  provisioned / stopped normally
#   2  usage, or the store directory fails the trusted-directory contract
#   3  the store refused (reason in the "error" event, e.g. not_provisioned)
#   4  this OS process does not hold the store lock; nothing was touched
#   5  boot-validated only: a boot check refused (fixed code in the "error" event,
#      op "boot"); no FileStore call was made
#   1  anything else (unexpected)
#
# `boot-validated` is `start` (no harness control) with a preflight between the lock
# check and the first FileStore call: RouteStore.BootCheck (boot_check.ex) checks the
# BEAM's actual environment, its resolver option, its mounts and the staged boot
# inputs at /sbx/boot against the manifest digest given on argv. Its output is fixed:
# events carry only codes from closed sets and digests, stderr carries fixed lines,
# the OTP logger is silenced (no crash reports) and any unexpected exception becomes
# {"event":"error","op":"boot","reason":"internal"} with exit 1. It opens no listener
# and dials nothing; the bindings it validates are discarded after the check.
#
# `start` opens the real Telelem.Ops.FileStore, prints {"event":"ready",...} only after
# FileStore.start/1 returned {:ok, pid}, and then stays in the foreground until SIGTERM
# (or, with --harness-control, a "stop" line or stdin EOF). It stops the store with
# FileStore.stop/1 (terminate/2 runs after its last file operation) before exiting 0.
#
# --harness-control is a TEST HARNESS channel, not an operations interface: it reads
# "apply PREFERRED", "read" and "stop" lines from stdin so the local lifecycle suite
# (integration/m2a/route-store/run) can make an acknowledged change through the store.
# Route operations for real callers go through Telelem.Ops.Route; nothing here is a
# product CLI, MCP or UI surface.

defmodule RouteStore.Signals do
  @moduledoc false
  # Replaces erl_signal_handler for `start`: SIGTERM -> orderly stop request to the
  # main loop; SIGQUIT and SIGUSR1 keep OTP's default (halt) behavior.
  @behaviour :gen_event

  @impl true
  def init({main, _old_handler_state}), do: {:ok, main}

  @impl true
  def handle_event(:sigterm, main) do
    send(main, {:stop, :sigterm})
    {:ok, main}
  end

  def handle_event(:sigquit, _main), do: :erlang.halt()
  def handle_event(:sigusr1, _main), do: :erlang.halt(~c"Received SIGUSR1")
  def handle_event(_other, main), do: {:ok, main}

  @impl true
  def handle_call(_req, main), do: {:ok, :ok, main}

  @impl true
  def handle_info(_msg, main), do: {:ok, main}
end

defmodule RouteStore.Main do
  alias Telelem.Ops.{FileStore, PlanDigest}
  alias Telelem.Route.Revision
  alias Telelem.Sandbox.PathPolicy
  alias RouteStore.BootCheck

  @name :telelem_route_store
  # Trusted-directory contract (m2a-contracts §3, Telelem.Sandbox.Bootstrap): the
  # launcher-owned sandbox root is immutable to the writer (read-only rootfs); the
  # store is a separate writable mount strictly below it.
  @sandbox_root "/sbx"

  def main(argv) do
    case argv do
      ["provision" | rest] -> provision(rest)
      ["start" | rest] -> start(rest)
      ["boot-validated" | rest] -> boot_validated(rest)
      _ -> usage("expected provision, start or boot-validated")
    end
  end

  ## provision

  @provision_opts [
    dir: :string,
    tenant: :string,
    route: :string,
    deployments: :string,
    preferred: :string,
    fallback_on_refusal: :string,
    deadline_ms: :integer,
    max_attempts: :integer
  ]

  defp provision(argv) do
    opts = parse(argv, @provision_opts)
    dir = opts[:dir]
    trusted_dir!(dir)
    lock_held!(dir)

    fallback =
      case opts[:fallback_on_refusal] do
        "true" -> true
        "false" -> false
        _ -> usage("--fallback-on-refusal must be true or false")
      end

    attrs = %{
      route_id: opts[:route],
      rev: 1,
      deployments: String.split(opts[:deployments], ","),
      preferred: opts[:preferred],
      fallback_on_refusal: fallback,
      deadline_ms: opts[:deadline_ms],
      max_attempts: opts[:max_attempts]
    }

    with {:ok, revision} <- Revision.new(attrs) |> tag(:invalid_revision),
         :ok <- FileStore.provision(dir, opts[:tenant], revision) do
      emit(%{event: "provisioned", tenant_id: opts[:tenant], record: record_json(revision, nil)})
      halt(0)
    else
      {:error, reason} -> refused("provision", reason)
    end
  end

  ## start

  @start_opts [dir: :string, tenant: :string, route: :string, harness_control: :boolean]

  defp start(argv) do
    opts = parse(argv, @start_opts, [:dir, :tenant, :route])
    dir = opts[:dir]
    trusted_dir!(dir)
    lock_held!(dir)
    main = self()
    install_signals(main)

    case FileStore.start(name: @name, dir: dir, tenant_id: opts[:tenant], route_id: opts[:route]) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        {:ok, rec} = FileStore.fetch(@name, opts[:tenant], opts[:route])

        emit(%{
          event: "ready",
          tenant_id: opts[:tenant],
          os_pid: System.pid(),
          record: record_json(rec.revision, rec.last_plan_digest)
        })

        if opts[:harness_control], do: spawn_link(fn -> read_stdin(main) end)
        loop(%{tenant: opts[:tenant], route: opts[:route], ref: ref, sanitized: false})

      {:error, reason} ->
        refused("start", reason)
    end
  end

  # Replace the default SIGTERM action. OTP's erl_signal_handler runs init:stop on
  # SIGTERM, which kills every non-system process (the store included, with :kill and
  # without terminate/2) and still runs after System.trap_signal/3 traps. Swap that
  # handler for RouteStore.Signals, so SIGTERM only asks this loop for an orderly stop.
  defp install_signals(main) do
    :ok =
      :gen_event.swap_handler(
        :erl_signal_server,
        {:erl_signal_handler, :swap},
        {RouteStore.Signals, main}
      )

    true = :erl_signal_handler not in :gen_event.which_handlers(:erl_signal_server)
    :ok
  end

  ## boot-validated (see header)

  @boot_opts [dir: :string, tenant: :string, route: :string, manifest_sha256: :string]
  @store_dir "/sbx/store"
  @boot_dir "/sbx/boot"

  defp boot_validated(argv) do
    # No OTP log output in this mode (crash and supervisor reports carry raw terms).
    :ok = :logger.set_primary_config(:level, :none)

    try do
      boot_validated!(argv)
    rescue
      _ -> boot_fail(:internal, 1)
    catch
      _, _ -> boot_fail(:internal, 1)
    end
  end

  defp boot_validated!(argv) do
    opts =
      case OptionParser.parse(argv, strict: @boot_opts) do
        {opts, [], []} ->
          if Enum.all?(Keyword.keys(@boot_opts), &Keyword.has_key?(opts, &1)), do: opts

        _ ->
          nil
      end

    if opts == nil or opts[:dir] != @store_dir,
      do:
        fail(
          2,
          "usage: boot-validated --dir #{@store_dir} --tenant T --route R --manifest-sha256 HEX"
        )

    # The shared checks of every writer mode, in the same order: trusted directory,
    # then the kernel's view of this process's lock. Both print fixed text here, since
    # dir is the fixed store path.
    trusted_dir!(@store_dir)
    lock_held!(@store_dir)

    env = System.get_env()

    summary =
      with :ok <- BootCheck.env(env),
           :ok <- BootCheck.resolver(env),
           {:ok, mi} <- read_mountinfo(),
           :ok <- BootCheck.mounts(mi),
           {:ok, summary} <-
             BootCheck.inputs(
               sandbox_root: @sandbox_root,
               boot_dir: @boot_dir,
               store_dir: @store_dir,
               tenant: opts[:tenant],
               manifest_sha256: opts[:manifest_sha256]
             ) do
        summary
      else
        {:error, code} when is_atom(code) -> boot_fail(code, 5)
        _ -> boot_fail(:internal, 1)
      end

    install_signals(self())

    case FileStore.start(
           name: @name,
           dir: @store_dir,
           tenant_id: opts[:tenant],
           route_id: opts[:route]
         ) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        {:ok, rec} = FileStore.fetch(@name, opts[:tenant], opts[:route])

        emit(%{
          event: "ready",
          mode: "boot_validated",
          tenant_id: opts[:tenant],
          os_pid: System.pid(),
          record: record_json(rec.revision, rec.last_plan_digest),
          boot: summary
        })

        loop(%{tenant: opts[:tenant], route: opts[:route], ref: ref, sanitized: true})

      {:error, reason} ->
        code = sanitized_reason(reason)
        emit(%{event: "error", op: "start", reason: code})
        IO.puts(:stderr, "route-store: start refused: " <> code)
        halt(3)
    end
  end

  defp read_mountinfo do
    case File.read("/proc/self/mountinfo") do
      {:ok, text} -> {:ok, text}
      _ -> {:error, :mountinfo_unreadable}
    end
  end

  defp boot_fail(code, status) do
    text = if code in BootCheck.codes(), do: Atom.to_string(code), else: "internal"
    emit(%{event: "error", op: "boot", reason: text})
    IO.puts(:stderr, "route-store: boot refused: " <> text)
    halt(status)
  end

  # Store refusals as fixed text: an atom, or a tuple of atoms joined by "_"; anything
  # else (it could carry terms from the store) collapses to "store_refused".
  defp sanitized_reason(r) when is_atom(r), do: Atom.to_string(r)

  defp sanitized_reason(r) when is_tuple(r) and tuple_size(r) in 1..4 do
    parts = Tuple.to_list(r)

    if Enum.all?(parts, &is_atom/1),
      do: Enum.map_join(parts, "_", &Atom.to_string/1),
      else: "store_refused"
  end

  defp sanitized_reason(_), do: "store_refused"

  defp loop(s) do
    receive do
      {:stop, why} ->
        FileStore.stop(@name)
        emit(%{event: "stopped", reason: Atom.to_string(why)})
        halt(0)

      {:line, line} ->
        handle_line(String.trim_trailing(line, "\n"), s)
        loop(s)

      {:DOWN, ref, :process, _, reason} when ref == s.ref ->
        # The store stopped by itself (for example a failed write): the disk is the
        # only truth now. Report and exit non-zero; never retry or reopen here.
        emit(%{event: "store_down", reason: if(s.sanitized, do: "exited", else: inspect(reason))})
        halt(1)
    end
  end

  # Harness-only control lines (see header).
  defp handle_line("stop", _s), do: send(self(), {:stop, :harness})

  defp handle_line("read", s) do
    {:ok, rec} = FileStore.fetch(@name, s.tenant, s.route)
    emit(%{event: "record", record: record_json(rec.revision, rec.last_plan_digest)})
  end

  defp handle_line("apply " <> preferred, s) do
    {:ok, %{revision: current}} = FileStore.fetch(@name, s.tenant, s.route)

    with {:ok, proposed} <- Revision.with_preferred(current, preferred),
         plan = PlanDigest.compute(s.tenant, s.route, current.rev, preferred, proposed.digest),
         expected = %{rev: current.rev, digest: current.digest},
         {:ok, rec} <-
           FileStore.compare_and_write(@name, s.tenant, s.route, expected, proposed, plan) do
      emit(%{
        event: "applied",
        plan_digest: plan,
        record: record_json(rec.revision, rec.last_plan_digest)
      })
    else
      other -> emit(%{event: "apply_refused", reason: inspect(other)})
    end
  end

  defp handle_line(other, _s), do: emit(%{event: "harness_error", line: other})

  defp read_stdin(main) do
    case IO.read(:stdio, :line) do
      line when is_binary(line) ->
        send(main, {:line, line})
        read_stdin(main)

      _eof_or_error ->
        send(main, {:stop, :stdin_eof})
    end
  end

  ## checks

  # The store path must follow the launcher's trusted-directory contract before any
  # FileStore call. FileStore re-checks the path itself (PathPolicy.safe_root).
  defp trusted_dir!(dir) do
    checks = [
      {PathPolicy.normalized_absolute?(dir), :path},
      {dir != @sandbox_root and PathPolicy.under?(dir, @sandbox_root), :outside_sandbox_root},
      {not PathPolicy.denied?(dir, PathPolicy.default_denied_roots()), :denied_root},
      {PathPolicy.safe_root(@sandbox_root) == :ok, :sandbox_root_unsafe},
      {match?({:ok, _}, PathPolicy.walk_below(@sandbox_root, dir, :directory)), :store_dir_unsafe}
    ]

    case Enum.find(checks, fn {ok, _} -> not ok end) do
      nil ->
        :ok

      {_, why} ->
        fail(2, "store directory #{inspect(dir)} fails the trusted-directory contract: #{why}")
    end
  end

  # The lock wrapper exec'd into this OS process with the locked descriptor. Verify,
  # from the kernel's view, that this process still has that descriptor, that it is
  # the store directory's inode and that an exclusive flock is attached to it.
  defp lock_held!(dir) do
    with {:ok, fd_s} <- System.fetch_env("TELELEM_ROUTE_STORE_LOCK_FD"),
         {fd, ""} when fd >= 0 <- Integer.parse(fd_s),
         {:ok, ^dir} <- System.fetch_env("TELELEM_ROUTE_STORE_LOCK_DIR"),
         {:ok, held} <- File.stat("/proc/self/fd/#{fd}", time: :posix),
         {:ok, now} <- File.lstat(dir, time: :posix),
         true <- held.type == :directory and now.type == :directory,
         true <- {held.major_device, held.inode} == {now.major_device, now.inode},
         {:ok, info} <- File.read("/proc/self/fdinfo/#{fd}"),
         true <- flock_attached?(info, now) do
      :ok
    else
      _ ->
        fail(
          4,
          "this process does not hold the store lock on #{inspect(dir)}; run it through scripts/route-store"
        )
    end
  end

  defp flock_attached?(info, %File.Stat{major_device: dev, inode: ino}) do
    key = lock_key(dev, ino)

    info
    |> String.split("\n")
    |> Enum.any?(fn line ->
      case Regex.run(~r/^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s+\S+\s+(\S+)\s/, line) do
        [_, ^key] -> true
        _ -> false
      end
    end)
  end

  # /proc/locks and fdinfo print "%02x:%02x:%lu" of MAJOR(st_dev), MINOR(st_dev),
  # inode, using the kernel's dev_t; File.Stat.major_device is the raw st_dev.
  defp lock_key(dev, ino) do
    major = bor(band(Bitwise.bsr(dev, 8), 0xFFF), band(Bitwise.bsr(dev, 32), bnot(0xFFF)))
    minor = bor(band(dev, 0xFF), band(Bitwise.bsr(dev, 12), bnot(0xFF)))
    hex2(major) <> ":" <> hex2(minor) <> ":" <> Integer.to_string(ino)
  end

  defp hex2(n), do: n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(2, "0")
  defp band(a, b), do: Bitwise.band(a, b)
  defp bor(a, b), do: Bitwise.bor(a, b)
  defp bnot(a), do: Bitwise.bnot(a)

  ## helpers

  defp parse(argv, spec, required \\ nil) do
    required = required || Keyword.keys(spec)

    case OptionParser.parse(argv, strict: spec) do
      {opts, [], []} ->
        missing = Enum.reject(required, &Keyword.has_key?(opts, &1))
        if missing != [], do: usage("missing #{Enum.map_join(missing, ", ", &flag/1)}")
        opts

      {_, rest, bad} ->
        usage("unexpected arguments #{inspect(rest ++ Enum.map(bad, &elem(&1, 0)))}")
    end
  end

  defp flag(key), do: "--" <> String.replace(Atom.to_string(key), "_", "-")

  defp record_json(%Revision{} = r, last) do
    %{
      route_id: r.route_id,
      rev: r.rev,
      deployments: r.deployments,
      preferred: r.preferred,
      fallback_on_refusal: r.fallback_on_refusal,
      deadline_ms: r.deadline_ms,
      max_attempts: r.max_attempts,
      digest: r.digest,
      last_plan_digest: last
    }
  end

  defp tag({:ok, _} = ok, _), do: ok
  defp tag({:error, reason}, what), do: {:error, {what, reason}}

  defp refused(op, reason) do
    emit(%{event: "error", op: op, reason: reason_text(reason)})
    IO.puts(:stderr, "route-store: #{op} refused: #{inspect(reason)}")
    halt(3)
  end

  defp reason_text(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_text(reason), do: inspect(reason)

  defp usage(msg), do: fail(2, "usage: #{msg}")

  defp fail(code, msg) do
    IO.puts(:stderr, "route-store: " <> msg)
    halt(code)
  end

  defp emit(map), do: IO.puts(JSON.encode!(map))

  defp halt(code), do: System.halt(code)
end

RouteStore.Main.main(System.argv())
