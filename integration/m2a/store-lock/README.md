# Route-store writer lock (M2a, TEL-015 local prerequisite)

```sh
scripts/with-route-store-lock STORE_DIR -- COMMAND [ARG...]   # the guard
integration/m2a/store-lock/run                                # its tests: exit 0 PASS · 1 FAIL · 2 prerequisite missing
```

Use one wrapper for **explicit provisioning** and for **startup**. While any wrapped writer is running against a store, every other wrapped writer is refused before its command runs, whether that writer is a provisioner or a startup.

This is a standalone guard. Nothing in the gateway, the launchers or startup calls it yet; wiring it in belongs to the serving/release owner. It adds no project dependencies. It needs only bash, coreutils, findutils, `/proc` and the installed util-linux `flock`.

## Interface

| Item | Behavior |
| --- | --- |
| Lock target | `STORE_DIR` itself. The wrapper opens the directory and takes `flock(2)` on that descriptor. There is no lock file that someone could delete, replace or recreate. The lock belongs to the directory's inode, not to its path. |
| Waiting | None. It uses `flock --nonblock --exclusive`. A held lock gives exit 75 at once, and COMMAND is not run. |
| Handover | `exec`, no fork. COMMAND gets the wrapper's PID and inherits the locked descriptor. Bash does not set close-on-exec on it, and the tests confirm the descriptor has no `O_CLOEXEC`. |
| Release | The kernel drops the lock when the last descriptor that refers to the locked open file description closes. Normally that is when COMMAND exits, by any route: normal exit, signal or `SIGKILL`. A stale lock cannot survive the death of every holder. |
| Environment | `TELELEM_ROUTE_STORE_LOCK_FD` (descriptor number) and `TELELEM_ROUTE_STORE_LOCK_DIR` (the store path). The caller's `PATH` is restored before `exec`. The wrapper uses `/usr/bin:/bin` for its own tools. |
| Trusted `flock` | Only `/usr/bin/flock` or `/bin/flock`, and only if it is root-owned and not group/other-writable. It is never looked up through `PATH`. |

**Exit status.** After the `exec`, the status is COMMAND's own, or 126/127 if the `exec` fails. Before that, the wrapper uses its own codes and prints a `with-route-store-lock: …` line on stderr:

- 64: usage error
- 69: no trusted `flock`, no `/proc`, or another `flock` failure
- 70: the path no longer names the locked inode
- 75: **busy**
- 78: a `STORE_DIR` precondition failed

A COMMAND can exit with these same numbers. A launcher that needs to tell them apart must use the stderr prefix or its own readiness signal.

**`STORE_DIR` preconditions** (exit 78):
- absolute, exactly canonical (`realpath -e` must return the same string), and not `/`
- not a symlink, with no symlink, `.` or `..` component and no trailing slash
- an existing directory, owned by the writer's effective uid, not group- or other-writable
- on a local filesystem whose `stat -f` type is `ext2/ext3` (also reported for ext4), `xfs`, `btrfs`, `zfs` or `tmpfs`

After locking, the wrapper checks that the locked descriptor, the path and the pre-lock `stat` all show the same device and inode, and that the path is still canonical. Otherwise it exits 70.

## Writer contract

The guarantee holds only while COMMAND keeps to all of these rules:

1. **Stay in the foreground and don't daemonize.** The writer must be COMMAND itself, or something COMMAND `exec`s into, so it keeps the wrapper's PID. Don't use `erl -detached`, a release `daemon` mode, double-fork or `setsid`-and-exit. A supervisor that watches the wrapper's PID is then watching the writer.
2. **Don't close or unlock the descriptor.** Don't close fd `$TELELEM_ROUTE_STORE_LOCK_FD`, call `flock(fd, LOCK_UN)` on it, or close every inherited descriptor (`close_fds`-style). Test 8 shows that a writer which closes it loses the lock while it is still running.
3. **Don't open and `flock` the store directory a second time from the writer.** A second open is a separate open file description. Its lock conflicts with the writer's own lock.
4. **Inheritance runs one way.** A child that inherits the descriptor keeps the store locked until that child exits too, even if the writer has died (test 7). That fails safe, because it blocks the next writer and never lets two in. A child should get the descriptor close-on-exec, or be bounded by the writer's lifetime, unless keeping the store locked is intended.
5. **Every mutating path must use the wrapper.** `flock` is advisory. Provisioning, startup, migration, repair and restore tools are excluded from each other only when they all go through this wrapper, or `flock` the same directory inode. Readers are not excluded.
6. **Run the wrapper in the writer's own process lineage.** If COMMAND is a client that proxies a process somewhere else (for example `docker run …`, which is how `scripts/mix` uses docker), the lock lives in the client. The real writer can outlive a killed client. For a containerized writer, run the wrapper inside the container as the writer's entry point, with the store bind-mounted. Tests 10 and 11 cover this.

## Assumptions (not enforced, or enforced only as defense in depth)

- **Immutable path.** The launcher owns every ancestor of `STORE_DIR` and fixes it. No untrusted principal, including the sandboxed writer, can rename, replace or remount any component. If someone moves the directory aside and creates a new one at the same path, a new writer locks the new inode and gets in while the old writer still runs. Test 9 shows this limit. The post-lock inode check catches a swap only between the wrapper's own `stat` and lock calls. No test injects that race, so treat it as defense in depth, not proof.
- **Private store mount.** `STORE_DIR` is a dedicated writable mount, or directory, for exactly one store. It is owned by the writer's uid and is not shared with other tenants or other stores. It matches the separate writable store mount in [contract §3](../../../docs/m2a-contracts.md).
- **Local filesystem.** `flock(2)` semantics are reliable here only on a local filesystem on one kernel. NFS emulates `flock` with byte-range locks. CIFS, FUSE, 9p and cluster filesystems differ, and overlay directories can split across layers. All of these are rejected by type allowlist. Extend the allowlist only after testing.
- **Same inode.** Every writer must reach the same directory inode on the same kernel. Bind mounts share the inode, so a container that sees the store under a different path is still excluded (test 11). Separate kernels are not covered: VMs, Docker Desktop's VM against the host, remote hosts and copies of the directory.

## What the tests show

`run` uses fresh `mktemp -d` directories that are removed on exit, and harmless stand-in writers: `holder.sh` and `sh -c 'printf x > file'`. Handshakes go through FIFOs. There are no timing sleeps, and a safety timeout counts as FAIL. Kernel state is read from `/proc/locks`, `/proc/<pid>/fdinfo/<fd>` (`lock:` lines) and `/proc/*/fd`. A `0 left` check always follows a `1 held` check on the same key.

| # | Case | Observed |
| --- | --- | --- |
| 1 | Preconditions | usage, relative, empty, missing, file, symlink (store or parent), non-canonical, group/other-writable, `/` → 64/78, command never run |
| 2 | exec, no fork | writer PID == `$!` of the wrapper. Its fd points to the store inode and has no `O_CLOEXEC`. It is the only process holding the store open. The kernel shows one exclusive flock, attached to that fd. |
| 3 | Concurrency | startup vs startup, provision vs a running startup, startup vs a running provision → 75. The store tree (names, inodes, sizes, mtimes, modes) is unchanged. A `PATH`-shadowed fake `flock` is ignored. |
| 4 | Normal exit | exit 7 passes through. No flock is left right after `wait`. The next writer runs. The caller's `PATH` reaches COMMAND. A failed `exec` gives 127 and leaves no lock. |
| 5 | `SIGKILL` | status 137. No flock is left right after `wait`. The next writer acquires. |
| 6 | Shutdown | while the writer is in its `SIGTERM` handler, a contender gets 75. It acquires after the writer exits. |
| 7 | Inherited child | after the writer is SIGKILLed, the orphan child still holds the flock on its inherited fd and contenders get 75. Once the child exits, a contender acquires. |
| 8 | Control | a writer that closes its fd loses the lock while alive. This shows the tests detect lock loss. |
| 9 | Limit | replacing the path lets a second writer in; the original inode stays locked |
| 10 | Real BEAM (runner image) | wrapper → `erl` → `erlexec` → `beam.smp` keeps one PID. The BEAM's inherited fd holds the flock. `erl_child_setup` does not hold the fd, so port programs it spawns cannot inherit it from there. A contender gets 75. The lock is released after `init:stop/0` and after `SIGKILL`. |
| 11 | Same inode across namespaces | a host writer holds the store. A container sees it as `/store` through a bind mount, in different mount and PID namespaces. Its wrapped writer gets 75, and the store is unchanged. The control run acquires after the host writer exits. |

Tests 10 and 11 need docker and the runner image from `scripts/setup`. They run with `--network none`, a read-only rootfs, `--cap-drop ALL`, no-new-privileges and the host uid. Without docker or the image they are reported as `skipped`, and in that case they prove nothing. Set `TELELEM_STORE_LOCK_BEAM=0` to skip them on purpose.

Mutation check, done by hand during authoring and not shipped. Each broken copy of the wrapper failed the suite:
- fork instead of `exec`
- `--shared`
- canonical check removed (caught by the post-lock check)
- mode check removed
- `PATH` lookup of `flock`
- descriptor closed before `exec`

## Not established here

- **The real gateway's startup and provisioning commands.** None exist yet. Test 10 covers a bare `erl -noshell -eval`, not `mix`, the `elixir` launcher script, a release script or the gateway's own shutdown. Before integrating, rerun tests 2/6/10-style checks against the actual command: PID equality, fd holders, `fdinfo` lock, a contender during shutdown, and release after `SIGKILL`.
- **Contention during BEAM `init:stop/0` shutdown.** Test 6 covers it for the generic writer. For the BEAM, only release after the stop is tested.
- **The pre-exec swap race.** It is not injected, as described above.
- **Non-allowlisted filesystems and multi-kernel setups.** They are rejected or out of scope, not tested.

## Diagnostics

`/proc/locks` lists the PID of the short-lived `flock(1)` helper that took the lock, not the writer. Outside the initial PID namespace it hides such locks entirely. To find the holder, use `/proc/<pid>/fdinfo/<fd>` (`lock:` lines) or `find /proc/[0-9]*/fd -lname STORE_DIR`.
