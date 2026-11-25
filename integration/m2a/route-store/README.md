# Route-store provision and start (M2a, TEL-015 local slice)

```sh
scripts/route-store/provision --store HOST_DIR --tenant local --route chat \
    --deployments a,b --preferred a --fallback-on-refusal true --deadline-ms 5000 --max-attempts 2
scripts/route-store/start --store HOST_DIR --tenant local --route chat   # foreground; SIGTERM stops
scripts/route-store/boot-validated --store HOST_DIR --boot BOOT_DIR \
    --image-id sha256:HEX64 --manifest-sha256 HEX64 --tenant local --route chat   # start after a boot preflight
integration/m2a/route-store/run      # lifecycle suite: exit 0 PASS · 1 FAIL · 2 prerequisite missing
integration/m2a/route-store/run --cleanup-selftest   # case 0 only: stubbed docker, no daemon needed
```

These are the first commands that put `Telelem.Ops.FileStore` and `scripts/with-route-store-lock` together. They are minimal "store up" commands. There is no listener, no gateway, no serving path, no route-operation CLI, MCP or UI, and nothing is wired into `mix`, a release or a supervisor.

## Supported execution platform

- A Linux host with a local docker daemon on the **same kernel** (not Docker Desktop's VM, a remote daemon or rootless/userns-remapped docker; the lock and the FileStore uid checks need the host's inode and uid).
- The pinned runner image from `scripts/setup` (`telelem-mix:elixir-1.20.5-otp-28.5.0.8-hex-2.7.0`, override with `TELELEM_IMAGE`). Nothing is fetched; containers run `--network none`.
- `HOST_DIR` on a local filesystem the wrapper allowlists: ext2/3/4, xfs, btrfs, zfs or tmpfs.
- Verified here: Linux 7.0.0-44-generic, Docker 29.10.0, bash 5.3.9, stores on tmpfs (`/tmp`). The other allowlisted filesystems were not exercised by this suite.

## What the commands do

`scripts/route-store/{provision,start}` → `launch` (host) → one foreground `docker run --rm` → `entry.sh` (container) → `with-route-store-lock /sbx/store` → `elixir` → `erl` → `beam.smp` → `main.exs` → `Telelem.Ops.FileStore`.

| Step | Where | What |
| --- | --- | --- |
| Host checks | host, before docker | `HOST_DIR` absolute, canonical, a real directory owned by the caller, not group/other-writable; no world-writable non-sticky ancestor; does not overlap `/etc/telelem`, `/var/lib/telelem`, `/run/telelem`, `/run/secrets` or the checkout. These are defense in depth only. |
| Writer environment | container | Runner image, `--network none`, `--read-only` rootfs, `--cap-drop ALL`, no-new-privileges, the caller's uid/gid, private `/tmp` tmpfs, `ERL_INETRC` hosts-only. Read-only mounts: the five store sources, `scripts/route-store`, the wrapper. The only writable host path is `HOST_DIR`, at `/sbx/store`. |
| Compile | container, before the lock | `elixirc --warnings-as-errors` of `ops/boundary.ex`, `ops/plan_digest.ex`, `ops/file_store.ex`, `route/revision.ex`, `sandbox/path_policy.ex` into `/tmp`, so the writer always runs the checkout's current code (about 1 s). |
| Lock | container | `exec` of the wrapper on `/sbx/store`, which locks the directory and execs into the BEAM. The BEAM is the container's PID 1 and holds the locked descriptor. |
| Trusted directory | BEAM | Before any FileStore call: `/sbx/store` must be normalized, strictly below the sandbox root `/sbx` (the read-only rootfs), outside the denied roots, and reached without symlinks or world-writable directories (`PathPolicy`). This is the m2a-contracts §3 / `Sandbox.Bootstrap` contract: trusted roots immutable to the writer, with a separate writable store mount. |
| Lock check | BEAM | Before any FileStore call: `TELELEM_ROUTE_STORE_LOCK_FD/DIR` are present, `/proc/self/fd/FD` is the store's inode, and `/proc/self/fdinfo/FD` shows an exclusive `FLOCK` on that inode. Otherwise the BEAM exits 4 and touches nothing. |
| provision | BEAM | `Revision.new/1` (rev 1), then `FileStore.provision/3`: create-only. `already_provisioned` and `interrupted_provision` are refusals (exit 3). |
| start | BEAM | `FileStore.start/1`, which never provisions. Prints the `ready` event only after it returned `{:ok, pid}`, then stays in the foreground. SIGTERM, forwarded by the attached client (`--sig-proxy`), results in `FileStore.stop/1` (its `terminate/2` runs after its last file operation), a `stopped` event and exit 0. OTP's default SIGTERM handler (`init:stop`, which would kill the store without `terminate/2`) is swapped out. |

### boot-validated (offline preflight; listener-free)

`boot-validated` runs `start` (no harness control) through the same launch → entry → lock → foreground BEAM path, with a boot preflight before the first FileStore call. It opens no listener and dials nothing. It does not call `Primary.start` or any serving, network or provider code, and it changes no schema.

| Step | Where | What |
| --- | --- | --- |
| Closed argv | host | Only `--store --boot --image-id --manifest-sha256 --tenant --route [--container-name]`, each once. `TELELEM_IMAGE` is refused. `--image-id` is a full local image ID that the operator approved in advance. It must exist locally under exactly that ID, and the container runs by ID with `--pull never`. `BOOT_DIR` gets the same host checks as `HOST_DIR` and must not overlap it. |
| Container | host | As for `start`, plus `--log-driver none`, `--workdir /`, BOOT_DIR read-only at `/sbx/boot`, and four more read-only sources (`sandbox/{id,bindings,verified_target,bootstrap}.ex`). The command is `env -i` with a fixed list: `PATH HOME LANG ERL_INETRC ERL_CRASH_DUMP_SECONDS=0`. |
| Before any BEAM | entry.sh | The environment is exactly that list. The resolver file is exactly the pinned bytes (181 bytes, SHA-256 `37ff36d2…f8704a8`; hosts-file lookup only). If either fails, the result is exit 5 with a fixed stderr line, and no compiler or VM runs. The compiler's output is discarded. |
| Shared checks | BEAM | Trusted directory, then the kernel's view of the BEAM-held flock: the same `trusted_dir!`/`lock_held!` that `start` uses. |
| Boot checks | BEAM, `boot_check.ex` | The actual env is the exact expected set (only the lock fd varies). The VM's `:inet_db` lookup is `[file]`, from the pinned path. `/proc/self/mountinfo` shows a read-only rootfs, the exact closed application mount set (read-only except `/sbx/store`) and only docker's standard mounts elsewhere. Then the boot inputs (below). |
| Store | BEAM | `FileStore.start/1` (never provisions). `ready` carries `mode: "boot_validated"` and a `boot` summary that holds digests only. SIGTERM stops it as for `start`. |

**Boot inputs.** BOOT_DIR holds exactly `manifest.json`, `bootstrap.json`, `fixture-ca.pem`, `downstream-cert.pem` and `downstream-key.pem`. Anything else is refused, `route.rec` included, and the manifest has no role for it. The manifest has the closed shape `{"version":"telelem.route_store.boot_manifest.v1","tenant_id":T,"store_root":"/sbx/store","files":{ROLE:{"bytes":N,"sha256":H}}}`, with exactly the four roles `bootstrap`, `fixture_ca`, `downstream_cert` and `downstream_key`. File names are fixed per role and never taken from the manifest. Its own SHA-256 is given independently on argv. Each file is checked without following symlinks: it must be singly linked and opened once (the handle's inode must equal the walked inode). Its length and digest must match the manifest, and only the hashed bytes are used. The key file must not be group- or other-readable. The manifest tenant, the bootstrap tenant and `--tenant` must all be equal. `sandbox_store_root` must be `/sbx/store`. Every fixture target must use `ca_file: /sbx/boot/fixture-ca.pem`, never an inline `ca_pem`. `Sandbox.Bootstrap.from_map/2` then builds the bindings, and every resulting `VerifiedTarget` must carry exactly the certificates in the hashed CA bytes. The downstream certificate must be one canonical non-CA certificate, and the key one unencrypted EC P-256/P-384 or RSA ≥ 2048 key. The match is in memory: sign a fixed message with the key and verify it with the certificate's public key. No TLS, no socket. The bindings are discarded after the check.

**Output and exit.** A boot refusal is exit 5: exactly one `{"event":"error","op":"boot","reason":CODE}` event, where CODE comes from the closed set in `boot_check.ex` (entry.sh refusals use `entry_*` codes on stderr only), plus one fixed `route-store: boot refused: CODE` line on stderr. Store refusals are reduced to atom text or `store_refused`, and `store_down` carries `exited`. The OTP logger is set to `:none`, any exception becomes `internal` (exit 1), crash dumps are disabled, and docker keeps no log file.

**Trust assumptions (not established here).** The local docker daemon and the approved image are trusted; the launcher does not establish the image's provenance. BOOT_DIR is a trusted local staging directory. The read-only bind only stops the container from writing it, and nothing makes the host copy immutable. Hashing each file once narrows the window between check and use; it does not protect against a host writer. The certificate's validity period, its chain to any CA and its suitability for a real TLS handshake are not checked.

**Output.** stdout carries one compact JSON object per line: `provisioned`, `ready`, `stopped`, `error` (with `reason`), plus the harness events below. Diagnostics go to stderr with a `route-store:` prefix, or `with-route-store-lock:` for wrapper refusals.

**Exit status.**

| Status | Meaning |
| --- | --- |
| 0 | ok, or a normal stop |
| 2 | usage, trusted-directory contract failure, host-side precondition or missing prerequisite |
| 3 | the store refused (`not_provisioned`, `interrupted_provision`, `already_provisioned`, a validation error, ...) |
| 4 | the writer does not hold the lock |
| 5 | boot-validated: a boot check refused; no FileStore call was made |
| 75 | busy: another wrapped writer holds the store; nothing ran |
| 64, 69, 70, 78 | other wrapper refusals |
| 125–127 | docker or exec failure |
| 128+N | killed by signal N, e.g. 137 |

## Lock lifetime

The lock is taken inside the container after the compile step and before `main.exs` runs. It is held by the container's PID 1 (the BEAM) until that process exits, and the kernel releases it on exit, including SIGKILL. A normal stop holds it through `FileStore.stop/1`.

**The docker client is not the lock holder.** If the client is SIGKILLed, the container keeps running and keeps the store locked. Every other writer then gets 75 (fail safe) until the container is stopped with `docker stop NAME` (`--container-name`, label `telelem.route-store`). A supervisor must therefore manage the container by name, not only the client PID.

**Harness mode.** With `--harness-control`, stdin is attached. Losing the client (and so stdin) makes the BEAM stop the store and exit (main.exs treats stdin EOF like `stop`). The suite observes the container exit and the lock release, not the `stopped` event (case 6b).

## Harness control (tests only)

`start --harness-control` reads `apply PREFERRED`, `read` and `stop` lines on stdin, and answers `applied` (with `plan_digest` and the stored record), `apply_refused`, `record` or `stopped`. `apply` does exactly one `FileStore.compare_and_write/6`. It builds the successor with `Revision.with_preferred/2` and the plan digest with `PlanDigest.compute/5`, expecting the current `rev` and digest. `applied` is printed only after the store replied `{:ok, record}`, which happens after the rename.

This is how the suite makes an **acknowledged change**. It is a test harness, not a route-operation interface: it has no principal or authority checks, and real callers go through `Telelem.Ops.Route` (not wired here).

## What the suite shows

`run` uses fresh `mktemp -d` stores (mode 700, under `TMPDIR`) that are removed on exit. Every container gets a per-run nonce name, registered with the role label and the store it is created with. Readiness, acks and process exits are lines read through FIFOs with bounded `read -t`. A timeout counts as FAIL.

**Host processes and signals.** Each docker client and the case-3 holder runs under `supervise` (in this directory). It is a suite-owned Python supervisor using only the standard library. Neither the suite nor the supervisor ever signals a process by PID, and bash's `$!` is not kept.

- The supervisor spawns the child itself (`subprocess.Popen`) and immediately opens a pidfd on it. The child cannot have been reaped at that point: the supervisor is its only parent, it resets `SIGCHLD` to the default so the kernel does not auto-reap, and it calls `waitpid` only after the pidfd reports the exit. The pidfd therefore names that child and no other process.
- The suite asks for `TERM` or `KILL` over the supervisor's control FIFO. The supervisor sends the signal only through the pidfd and answers `sent SIG` or `gone SIG` on its status FIFO, followed by exactly one `exit RC` (128+N for signal N). If there is no `sent`, the run aborts before relying on the signal.
- The supervisor closes every inherited descriptor except stdio and the ones passed explicitly (the case-3 holder's FIFOs). A later supervisor therefore never keeps an earlier one's control FIFO open.
- When the suite closes a control FIFO, the supervisor SIGKILLs its child through the pidfd and reports the exit. The FIFO also closes if the suite's shell dies, even by SIGKILL. Cleanup and a `run_cmd` timeout both use this path.
- **Writer check before the spawn.** The suite opens the control FIFO before it starts the supervisor, but it can die before the supervisor opens it. Linux gives a non-blocking FIFO reader that was opened with no writer no `POLLHUP` until some writer opens, so a supervisor that relied on `POLLHUP` alone would then start its child and never release it. Before it spawns, the supervisor therefore reads the control FIFO (non-blocking) until it would block. EOF there means that no writer is left: the child is never started, every complete request already read is answered (`gone SIG`, since no child runs, or `refused LINE`), and the last line is `exit 125`. Without a `started` line, `spawn` fails. A read that would block means that a writer is open, and Linux then reports `POLLHUP` once the last writer closes. Requests read by the check are kept and handled in order right after `started`.
- The case-3 holder is released cooperatively: it reads `go` from its FIFO and exits 0.

No host signal ever goes to a BEAM. A BEAM is killed or stopped only through `docker kill` or `docker stop`, using the full 64-hex container ID. Before that, `verify_writer` checks all of the following:

- The ID passes `owned_cid` (below) for a name registered as `start` on this store.
- `docker inspect ID` shows the same ID and name, the container is running, and this run's store is mounted at `/sbx/store`.
- Its `State.Pid` is `beam.smp`, is PID 1 in its own namespace (`NSpid`), and sits in a cgroup that contains the ID.
- That PID has exactly one fd on the store, with an exclusive flock (`fdinfo`), and no other process holds the store.
- Right before the action, the container still shows the same `State.Pid` with the same start time.

If any check fails, the run counts a FAIL and exits 1 before it acts on the writer.

**Cleanup** first releases every supervisor that is still open, and waits (bounded) for each to report its child's exit. A supervisor that reports nothing is reported and fails the run. Cleanup then makes two passes over the registered names. It runs `docker rm -f FULL_ID` only for an ID that `owned_cid` accepts:

- the name was registered by this run, with a role and a store inside this run's `mktemp` dir;
- exactly one container has that exact name, and its full ID equals the ID already pinned for that name, if there is one;
- `docker inspect ID` shows that Id and Name, the registered `telelem.route-store` label value, and exactly one mount at `/sbx/store`, a bind whose source is the store registered for that name.

The first ID accepted for a name (at `verify_writer`, at the case-4 pin, or in cleanup's first pass) is pinned. Later lookups, cleanup included, never fall back to name and label matching: a different ID under that name is left alone. A container that fails any check is left alone, reported on stderr (`cleanup: left NAME alone: WHY`), and the run exits 1.

**Gone** means only one thing: a `docker ps -a` exited 0 and listed nothing, either for the exact name or, after a failed inspect or rm, for the full ID. Nothing else is treated as gone, and each of these fails the run:

- a failed `docker ps` (even one with empty output);
- a failed `docker inspect` whose follow-up lookup by ID fails or still lists the container;
- a failed `docker rm -f` of an accepted ID that is still listed (or cannot be looked up).

Each is reported with docker's exit status and stderr (`cleanup: left NAME alone: …` or `cleanup: could not remove NAME (ID): docker rm failed (…); …`). Uncertain attribution never leads to a removal: `rm` runs only after `owned_cid` accepts. The same rule decides case 4's "container is gone" and case 6b's `docker wait` failure. A name with no container (a successful empty lookup) is not reported. The same check guards the in-run removal after a `run_cmd` timeout. SIGINT and SIGTERM abort with 130 and 143, and cleanup runs. Run time is about 28 s.

| # | Case | Observed |
| --- | --- | --- |
| 0 | Cleanup paths, supervisor | (a) The real `owned_cid`, `rm_owned` and `reap_containers`, run in a subshell where `docker` is a stub. The stub answers `ps` (by name or by ID) and `inspect` from fixed tables, can be told to fail a lookup or an `rm`, and only records `rm`. `docker rm` is called only for accepted IDs: a fresh match and a pinned match once each, a "rm fails, container went away" case once, and a "rm fails, still listed" case in both passes. It is never called for a wrong mount source, a mount at another destination, a volume instead of a bind, a wrong or missing role label, a wrong inspected name, an inspect Id differing from the listed ID, two containers under one name, a short ID, a registered store outside the run's dir, a pinned name now showing another ID, an unregistered name, a **failed `ps` by name with empty output**, a failed inspect followed by a **failed `ps` by ID with empty output**, or a failed inspect whose ID is still listed. Each of those, and the failed `rm`, is reported in both passes with docker's stderr. Only the successful-empty-lookup cases count as gone and go unreported: no container under the name, inspect failed and `ps` by ID exited 0 empty, rm failed and `ps` by ID exited 0 empty. A rejected ID is never pinned. (b) The real `spawn`, `sup_sig`, `sup_wait`, `sup_end` and `supervise`, on the subshell's own `sh`, `true` and `sleep` children. These checks cover the reported exit code, stdin redirected on the `spawn` call, the child holding only stdio plus the passed fd, `TERM` → `sent TERM` and 143, `KILL` → `sent KILL` and 137, nothing sent after a reported exit, no signal other than TERM or KILL, any other request refused by the supervisor, release → SIGKILL and 137, releasing one supervisor while a later one runs, a command that cannot run → spawn fails with 127, no key reuse, and no supervisor left open. (c) The writer check, running `supervise` directly because `spawn` always opens the control FIFO first. The child would create a file and then wait for EOF on a suite-held FIFO. A FIFO whose only writer the supervisor keeps (and any child would inherit) shows by EOF, bounded, that both have exited. If the control writer was closed before the supervisor opened the FIFO, only `exit 125` is printed, no child is started (no file), and the supervisor exits. If `TERM` and `HUP` were buffered before that close (the suite keeps a read-only fd so the bytes survive), the result is `gone TERM`, `refused HUP`, `exit 125`, with no child. Control: a `KILL` written before the open, with the writer still held, gives `started`, `sent KILL`, `exit 137`. No daemon or container is involved. The only signals are TERM and KILL from each supervisor to its own child. |
| 1 | Missing provision | `start` on an empty store → exit 3 `not_provisioned`, twice; the store stays empty |
| 2 | Interrupted provision | A store holding only `route.rec.tmp` (the exact bytes a real provision stages): `start` and `provision` → exit 3 `interrupted_provision`; the temp is untouched (inode, size, mtime, hash) and no `route.rec` appears |
| 3 | Contention before mutation | A host-side wrapped holder holds an empty store: real `provision` and `start` → 75 busy from the wrapper inside the container, no writer event, the store stays empty. The holder is released over its FIFO (no signal) and exits 0. Control: `provision` succeeds after the holder exits. |
| 4 | Lifecycle and normal stop | `provision` gives rev 1 with no plan digest; a second `provision` gives `already_provisioned` with the record unchanged. `start`: `ready` at rev 1. On the host, the writer is `beam.smp`, the container's PID 1, with exactly one fd on the store inode, an exclusive flock attached to it (`fdinfo`), and no other process holding the store. A real `provision` and a second `start` → 75 with the store unchanged. `apply b` → acked rev 2 (`last_plan_digest` = plan digest), and `route.rec` on disk holds exactly that revision and plan digest with no temp. Contention after the change is still 75. SIGTERM to the start command (through its supervisor's pidfd, to the docker client, which forwards it to the BEAM) → `stopped sigterm`, exit 0, the container is gone (`docker ps` exits 0 and lists nothing for the name), nothing holds the store, record unchanged. |
| 5 | Restart after normal stop, then abrupt exit | `start` → `ready` with the acked rev 2, digest and plan digest, and the record is not rewritten. `apply a` gives acked rev 3. `docker kill --signal KILL FULL_ID` of this run's container, after `verify_writer` → 137, the lock is gone, the record is untouched. `start` → `ready` with the acked rev 3, digest and plan digest. `read` agrees, and `stop` exits 0. |
| 6 | Client death | (a) Product form: `verify_writer` after `ready` pins the container ID and BEAM PID. The client is SIGKILLed; the container still runs and its BEAM still holds the store. `start` and `provision` → 75, store unchanged. `verify_writer` again (same pinned ID, same BEAM PID), then `docker stop -t 30 FULL_ID` returns the ID. No client is attached, so neither the `stopped` event nor the exit status of that stop is observed. What is observed is that the lock is released: the next `start` acquires and reopens rev 3 with the acked plan digest. (b) Harness form: the client is SIGKILLed, then the container exits without any docker action. `docker wait` returns 0, or fails while a lookup by full ID exits 0 and lists nothing (already removed by `--rm`). In the latter case the exit status is not observed; any other `docker wait` failure is a FAIL, shown with its output. Nothing holds the store, the record is unchanged, and a further `start` reopens rev 3 / plan digest and stops with `stopped` and exit 0. Orderly stop on stdin EOF is the design (main.exs), not something this case proves. |
| 7 | No lock, no touch | `main.exs` run in the same container without the wrapper → exit 4, and an interrupted-provision temp is not removed or promoted |
| 8 | boot-validated | Throwaway boot inputs are generated offline in the runner image (`boot_fixture.exs`). The approved ID is the suite's runner image ID. (a) Launcher refusals give exit 2, no container and the store unchanged: a tag instead of an ID, an absent ID, `--harness-control`, a repeated flag, `TELELEM_IMAGE` set, a boot dir inside the store. (b) Positive: `ready` with `mode boot_validated`, rev 1, the manifest digest, the manifested CA digest and no key material. `verify_writer` shows beam.smp as PID 1 holding the flock. `docker inspect` shows the run by approved ID, network none, log driver none, a read-only rootfs, the exact mount set (only `/sbx/store` rw), `env -i`, and no key in env or argv. A second boot-validated and a `start` get 75. SIGTERM gives `stopped`, exit 0, empty stderr, the lock released and the record unchanged. (c) Fail closed, each exit 5 with one fixed event, only `route-store:` stderr lines, no key bytes and the store unchanged: wrong manifest digest, `--tenant` mismatch, `route.rec` in the boot dir, a tampered bootstrap byte, a bootstrap tenant mismatch (re-manifested), a mismatched key (re-manifested), a group-readable key. (d) Valid boot inputs on an unprovisioned store → exit 3 `not_provisioned`, store still empty. (e) Bypassing the launcher with the same `docker run`: the unchanged command passes every boot check (control, exit 3). An extra env var and a modified resolver file are refused by entry.sh before any BEAM. An extra read-only mount and a writable `/sbx/boot` are refused by the VM's mountinfo check. |

Mutation check, done by hand during authoring and not shipped. The suite failed for each broken copy of the commands:
- wrapper around the host-side docker client instead of inside the container: case 6a lets a second writer in next to the orphaned one
- OTP default SIGTERM handler kept (`init:stop`): no orderly `stopped` event
- `start` seeds a missing store: cases 1 and 2
- the writer's own lock check removed: case 7

## Not established (unverified limits)

- **In-flight file calls.** Every SIGKILL in the suite lands at quiescence, after an ack. A kill during the temp write, `fsync` or `rename` is not injected (that needs FileStore's barrier hooks inside the real command). FileStore's documented behavior applies: the outcome is whatever reached the disk, the next start opens main or refuses `interrupted_provision`, and an un-acked write may or may not have landed. The kernel finishes the dead BEAM's syscalls before it releases the lock, but that is not tested here.
- **Power loss or kernel crash.** Not tested. There is no directory `fsync`, so a completed rename can be lost. No durability, rollback-resistance or freshness claim is made (FileStore moduledoc).
- **Interrupted provision** is reproduced by planting the staged bytes, not by killing a real `provision` between `fsync` and `rename`.
- **Two concurrent real provisions.** Provisioning is too short to hold open without a hook. Contention against a provision is shown with the wrapped stand-in holder (case 3) and against a running `start` (case 4).
- **Container actions and cleanup.** These limits remain:
  - The container ID is found by name, because `launch` takes no `--cidfile`, so no creation ID is captured. Ownership therefore rests on the per-run nonce name, the label, the bind of this run's `mktemp` store at `/sbx/store`, and the pinned ID once one is accepted. Docker labels, names and mounts are not access control: any process with access to the daemon could create a container that matches all of them before this run pins an ID, and cleanup would then remove it.
  - Between `still_same` and `docker kill`, the BEAM can still exit. A full ID never refers to a later container, and docker/containerd/runc deliver the signal to that container's init, not to a recycled host PID. The suite trusts that runtime behavior and does not test it.
  - `verify_writer`'s abort path has not been exercised against real containers, since triggering it would mean tampering with a container. `client_sig`'s refusal path (no `sent` from the supervisor) is exercised only in case 0, on the suite's own children.
  - Cleanup's rejection and docker-failure paths are exercised only against the stubbed docker in case 0. The stub's failure messages are made up; only the exit status decides. Its JSON fixtures copy the fields of real `docker inspect` output that `owned_cid` reads (`Id`, `Name`, `Config.Labels`, `Mounts[].Type/Destination/Source`). The live cases exercise the accepting path on real output only when a run aborts or times out with a container left behind, which a passing run does not do. (During authoring, a buggy draft did time out in case 4: the supervisor SIGKILLed that client and cleanup removed its container on real output. That is not part of a passing run.)
  - The docker CLI calls made by cleanup and `owned_cid` are not individually bounded. A hung daemon can hang cleanup.
  - Cleanup uses `docker rm -f FULL_ID`, which sends SIGKILL through docker. Cleanup does not run `verify_writer` (BEAM, cgroup or lock checks) on these containers, since they may be created, stopped or mid-boot.
  - The supervisor's guarantee rests on Linux semantics: an unreaped child keeps its PID, `pidfd_open` on it names that child, and `pidfd_send_signal` never reaches another process. It also assumes no other code in the supervisor reaps the child (it calls `waitpid` only once, after the pidfd reports the exit). Requires Linux ≥ 5.3 and Python ≥ 3.9. A race where the child exits just as a signal is requested is handled (`gone`, or `exit` before the answer, are both treated as "not sent"), but it is not injected.
  - The writer check relies on Linux FIFO behavior: a read with no writer returns EOF, and a reader that saw a writer (open at its own open, or opened later) gets `POLLHUP` when the last writer closes. Case 0 tests "closed before the open" and "open at the open". It does not inject a writer that closes between the check and the poll loop, because that needs a hook inside the supervisor; the second rule covers that window. A partial last line (no newline) before EOF is dropped, as it is after the spawn. If the supervisor gets SIGTERM or SIGHUP before the spawn, it dies with no child. If it gets one between the spawn and installing its handlers, it dies and orphans the child, as it did before this check was added.
  - If a supervisor itself is SIGKILLed by something outside the suite, its child is orphaned and keeps running. The suite then sees no `exit` line, so a bounded read fails (FAIL), and cleanup reports it. The container is still subject to the normal cleanup checks. Nothing in the suite kills a supervisor.
  - A `docker run` already in flight when cleanup's two passes run can still create a container after both passes. That container keeps a name with this run's nonce and must be removed by hand.
  - Case 7's container is registered like the others (role `test`, its store) and is removed by `--rm` or by cleanup's checks. The `timeout` around `docker` commands signals only the docker client it started itself.
- **Docker itself.** Not tested: daemon restart, `live-restore`, or `docker kill` of a container whose client is attached elsewhere. Stopping an orphaned writer is the operator's job, by container name.
- **Immutable path.** The wrapper's assumption holds: whoever controls `HOST_DIR`'s ancestors can swap the directory (store-lock README, test 9). The host-side ancestor check is a snapshot, not a guarantee.
- **Wrapper exit-code collisions.** The writer's own codes (2/3/4) do not collide with the wrapper's, but a signal status (128+N) can come from either the BEAM or the client. Use stdout events and stderr prefixes when the difference matters.
- **boot-validated.** These limits apply to the boot-validated mode and case 8:
  - The negative cases are representative, not exhaustive. `test/route_store/boot_check_test.exs` runs on the host BEAM with tmp roots, not in the container. It covers every refusal code except `file_changed` and `bootstrap_ca_not_pinned`, which need a file swapped between the walk and the open (or between hashing and `Bootstrap`'s own CA read), and `internal`. None of these three is injected.
  - The resolver's post-boot positive path is shown only by case 8's `ready`. In ExUnit, only its refusal is shown.
  - The in-VM env check's refusal is not reached through a container, because entry.sh refuses first. The ExUnit test covers it.
  - The expected env and mount sets are tied to this image, docker 29.10.0 and runc. Another runtime may add mounts or variables and is then refused.
  - Case 8 approves the image by reading its ID from the tag the suite already uses, which shows only that the by-ID path works.
  - A host-side change to BOOT_DIR during the check is not injected.
  - Mutation check, done by hand and not shipped: with the VM's mount check removed, both case 8e mount cases fail.
- **Not a gateway.** The serving path, `Telelem.Ops.Route`/CLI wiring against a running store, the isolation harness rerun against a gateway, and supervision/release integration all belong to other owners.
