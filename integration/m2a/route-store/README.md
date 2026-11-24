# Route-store provision and start (M2a, TEL-015 local slice)

```sh
scripts/route-store/provision --store HOST_DIR --tenant local --route chat \
    --deployments a,b --preferred a --fallback-on-refusal true --deadline-ms 5000 --max-attempts 2
scripts/route-store/start --store HOST_DIR --tenant local --route chat   # foreground; SIGTERM stops
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

**Output.** stdout carries one compact JSON object per line: `provisioned`, `ready`, `stopped`, `error` (with `reason`), plus the harness events below. Diagnostics go to stderr with a `route-store:` prefix, or `with-route-store-lock:` for wrapper refusals.

**Exit status.**

| Status | Meaning |
| --- | --- |
| 0 | ok, or a normal stop |
| 2 | usage, trusted-directory contract failure, host-side precondition or missing prerequisite |
| 3 | the store refused (`not_provisioned`, `interrupted_provision`, `already_provisioned`, a validation error, ...) |
| 4 | the writer does not hold the lock |
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

`run` uses fresh `mktemp -d` stores (mode 700, under `TMPDIR`) that are removed on exit. Every container gets a per-run nonce name, registered with the role label and the store it is created with. Readiness and acks are JSON lines read through FIFOs with bounded `read -t`. Process exit is observed by a bounded `/proc` poll. A timeout counts as FAIL.

**Host signals** go only to processes this run started (the docker clients and the case-3 holder), and only through `sig_ours`: it opens a pidfd on the PID, rechecks the kernel start time recorded at spawn, and sends the signal to the pidfd, so a PID recycled between check and send cannot be hit. If the check fails, nothing is sent; in a test step the run aborts. The suite never sends a signal by host PID to a BEAM. A BEAM is killed or stopped only through `docker kill` or `docker stop`, using the full 64-hex container ID. Before that, `verify_writer` checks all of the following:

- The ID passes `owned_cid` (below) for a name registered as `start` on this store.
- `docker inspect ID` shows the same ID and name, the container is running, and this run's store is mounted at `/sbx/store`.
- Its `State.Pid` is `beam.smp`, is PID 1 in its own namespace (`NSpid`), and sits in a cgroup that contains the ID.
- That PID has exactly one fd on the store, with an exclusive flock (`fdinfo`), and no other process holds the store.
- Right before the action, the container still shows the same `State.Pid` with the same start time.

If any check fails, the run counts a FAIL and exits 1 before it acts on the writer.

**Cleanup** kills the remaining clients this run started (through `sig_ours`), then makes two passes over the registered names. It runs `docker rm -f FULL_ID` only for an ID that `owned_cid` accepts:

- the name was registered by this run, with a role and a store inside this run's `mktemp` dir;
- exactly one container has that exact name, and its full ID equals the ID already pinned for that name, if there is one;
- `docker inspect ID` shows that Id and Name, the registered `telelem.route-store` label value, and exactly one mount at `/sbx/store`, a bind whose source is the store registered for that name.

The first ID accepted for a name (at `verify_writer`, at the case-4 pin, or in cleanup's first pass) is pinned. Later lookups, cleanup included, never fall back to name and label matching: a different ID under that name is left alone. A container that fails any check is left alone, reported on stderr (`cleanup: left NAME alone: WHY`), and the run exits 1. A name with no container is not reported. The same check guards the in-run removal after a `run_cmd` timeout. SIGINT and SIGTERM abort with 130 and 143, and cleanup runs. Run time is about 28 s.

| # | Case | Observed |
| --- | --- | --- |
| 0 | Cleanup rejection path, signal gate | The real `owned_cid`, `rm_owned`, `reap_containers` and `sig_ours`, run in a subshell where `docker` is a stub that answers `ps` and `inspect` from fixed tables and only records `rm`. `docker rm` is called exactly once each for the two accepted IDs (a fresh match and a pinned match). It is never called for a wrong mount source, a mount at another destination, a volume instead of a bind, a wrong or missing role label, a wrong inspected name, an inspect Id differing from the listed ID, two containers under one name, a short ID, a registered store outside the run's dir, a pinned name now showing another ID, or an unregistered name. Each of those is reported in both passes, a missing container is not, and a rejected ID is never pinned. `sig_ours` with the null signal 0 only: an unregistered PID, a wrong start time and a gone PID get nothing, and the control (this subshell, correct start time) is reachable. No daemon, container or delivered signal is involved. |
| 1 | Missing provision | `start` on an empty store → exit 3 `not_provisioned`, twice; the store stays empty |
| 2 | Interrupted provision | A store holding only `route.rec.tmp` (the exact bytes a real provision stages): `start` and `provision` → exit 3 `interrupted_provision`; the temp is untouched (inode, size, mtime, hash) and no `route.rec` appears |
| 3 | Contention before mutation | A host-side wrapped holder holds an empty store: real `provision` and `start` → 75 busy from the wrapper inside the container, no writer event, the store stays empty. Control: `provision` succeeds after the holder exits. |
| 4 | Lifecycle and normal stop | `provision` gives rev 1 with no plan digest; a second `provision` gives `already_provisioned` with the record unchanged. `start`: `ready` at rev 1. On the host, the writer is `beam.smp`, the container's PID 1, with exactly one fd on the store inode, an exclusive flock attached to it (`fdinfo`), and no other process holding the store. A real `provision` and a second `start` → 75 with the store unchanged. `apply b` → acked rev 2 (`last_plan_digest` = plan digest), and `route.rec` on disk holds exactly that revision and plan digest with no temp. Contention after the change is still 75. SIGTERM to the start command → `stopped sigterm`, exit 0, container gone, nothing holds the store, record unchanged. |
| 5 | Restart after normal stop, then abrupt exit | `start` → `ready` with the acked rev 2, digest and plan digest, and the record is not rewritten. `apply a` gives acked rev 3. `docker kill --signal KILL FULL_ID` of this run's container, after `verify_writer` → 137, the lock is gone, the record is untouched. `start` → `ready` with the acked rev 3, digest and plan digest. `read` agrees, and `stop` exits 0. |
| 6 | Client death | (a) Product form: `verify_writer` after `ready` pins the container ID and BEAM PID. The client is SIGKILLed; the container still runs and its BEAM still holds the store. `start` and `provision` → 75, store unchanged. `verify_writer` again (same pinned ID, same BEAM PID), then `docker stop -t 30 FULL_ID` returns the ID. No client is attached, so neither the `stopped` event nor the exit status of that stop is observed. What is observed is that the lock is released: the next `start` acquires and reopens rev 3 with the acked plan digest. (b) Harness form: the client is SIGKILLed, then the container exits without any docker action. `docker wait` returns 0 or finds it already removed (`--rm`), and in the latter case the exit status is not observed. Nothing holds the store, the record is unchanged, and a further `start` reopens rev 3 / plan digest and stops with `stopped` and exit 0. Orderly stop on stdin EOF is the design (main.exs), not something this case proves. |
| 7 | No lock, no touch | `main.exs` run in the same container without the wrapper → exit 4, and an interrupted-provision temp is not removed or promoted |

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
  - `verify_writer`'s abort path and `client_sig`'s refusal path have not been exercised against real processes or containers. Triggering them would mean tampering with a container or a process, which this suite does not do.
  - Cleanup's rejection path is exercised only against the stubbed docker in case 0. Its JSON fixtures copy the fields of real `docker inspect` output that `owned_cid` reads (`Id`, `Name`, `Config.Labels`, `Mounts[].Type/Destination/Source`); the live cases exercise the accepting path on real output only when a run aborts or times out with a container left behind, which a passing run does not do.
  - Cleanup uses `docker rm -f FULL_ID`, which sends SIGKILL through docker. Cleanup does not run `verify_writer` (BEAM, cgroup or lock checks) on these containers, since they may be created, stopped or mid-boot.
  - `sig_ours` identifies a process by PID plus kernel start time (clock ticks). A PID reused by another process started in the same tick would pass. The pidfd closes only the window between the check and the send.
  - A `docker run` already in flight when cleanup's two passes run can still create a container after both passes. That container keeps a name with this run's nonce and must be removed by hand.
  - Case 7's container is registered like the others (role `test`, its store) and is removed by `--rm` or by cleanup's checks. The `timeout` around `docker` commands signals only the docker client it started itself.
- **Docker itself.** Not tested: daemon restart, `live-restore`, or `docker kill` of a container whose client is attached elsewhere. Stopping an orphaned writer is the operator's job, by container name.
- **Immutable path.** The wrapper's assumption holds: whoever controls `HOST_DIR`'s ancestors can swap the directory (store-lock README, test 9). The host-side ancestor check is a snapshot, not a guarantee.
- **Wrapper exit-code collisions.** The writer's own codes (2/3/4) do not collide with the wrapper's, but a signal status (128+N) can come from either the BEAM or the client. Use stdout events and stderr prefixes when the difference matters.
- **Not a gateway.** The serving path, `Telelem.Ops.Route`/CLI wiring against a running store, the isolation harness rerun against a gateway, and supervision/release integration all belong to other owners.
