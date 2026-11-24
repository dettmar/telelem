# M2a isolation harness (A2, contract §3)

```sh
integration/m2a/isolation/run      # exit 0 PASS · 1 FAIL · 2 prerequisite missing
```

Takes about a minute. It is offline after `scripts/setup`. Results go to `integration/m2a/isolation/_run/<stamp>/`, which git ignores. The verdict is in `verdict/verdict.json`. It lists one check per line, and the run passes only if every check passes.

## Layout

| Container (runner image, host uid, `--cap-drop ALL`, no-new-privileges, read-only rootfs) | Network | Role |
| --- | --- | --- |
| `boundary` | `none`: loopback only, no routes | TLS fixture lanes `a` 127.0.0.1:18443 and `b` :18444 with a throwaway CA. DNS sink on 127.0.0.53:53 (udp and tcp) that logs each query and answers NXDOMAIN. |
| `canary` | existing default `bridge` (beyond the boundary) | TCP sink on :443 and :80. It logs each accepted connection, then acks. |
| `probe` | joins `boundary` | Verifier facts from inside the boundary: interfaces, routes, listeners, canary reachability (must be `ENETUNREACH`). Also sends the DNS positive-control query. |
| `outside` | `bridge` | Verifier control request to the canary (positive control). |
| `good` / `red_boundary` | joins `boundary` | The subject under strace. |
| `red_bridge` | `bridge` | The misbehaving subject with the boundary removed. |

The subject is `subject.exs`. It runs the real `Telelem.Sandbox` bootstrap and resolve code plus a real Mint TLS client. It stands in for the gateway until A1 wires bindings into serving. The harness judges it only from the outside, so the real gateway can replace it later without changing the observers.

The subject sees these production-shaped, non-secret sentinel files, writable and with a fresh set for each phase:
- `/etc/telelem/production/telelem-prod-config.sentinel.json`
- `/var/lib/telelem/production/store/telelem-prod-receipts.sentinel.db`
- `/run/secrets/telelem-production/telelem-prod-signer.sentinel.key`

It also sees the fixture CA certificate in `/fx`, its bootstrap files in `/sbx` (read-only) and the fixture CA only. It never sees the CA key, the checkout or the run directory.

## Observers and controls

| Observer | Positive control (must be seen) | Red control (must be detected) |
| --- | --- | --- |
| Canary log | The outside verifier connects once, from its own IP, with a nonce marker. | `red_bridge`: the misbehaving subject reaches the canary from its container IP. |
| DNS sink log | The probe's `control-<nonce>.isolation.invalid` query is logged and gets NXDOMAIN. | `red_boundary`: the subject's `prod-provider-<nonce>.isolation.invalid` lookup is logged. |
| Lane log | The subject's allowed requests produce exactly one TLS POST on `a` and one on `b`. | (none) |
| strace (`-f -ff -yy`, `%file,%network`), per process | `fs-control` runs in the same container under the same strace config before the subject. It reads one sentinel, stats another and writes the scratch path. All three accesses must appear in the trace. | `red_boundary`: sentinel read and write, and the `connect()` attempt to the canary IP. |
| Host-side sentinel inventory (`stat` and `sha256sum`, before and after) | (none) | `red_boundary`: the store sentinel hash changes. |

Acceptance (`good` phase) checks:
- Inside the boundary, the only listeners are the two lanes and the DNS sink. There is only `lo` and no route, and the canary is unreachable.
- The subject's only `connect()` calls are one to each registered lane. It makes no unix-socket connects, no DNS queries and no canary arrivals.
- It never touches sentinel paths, opens no OS trust store files and writes nothing outside `/audit` and `/tmp`.
- Sentinel hashes and inventory are unchanged.
- It is denied for a missing binding (`c`), a serving principal, missing `:dispatch` and absent bindings.
- Bootstrap is rejected for a disallowed target (the canary IP), a bootstrap path at the production config, a `ca_file` pointing at the production signer key, a production store root and a `file:` signer ref. strace shows that none of these opened anything.

Arrivals outside their expected phase fail the run. So does any missing or unparseable observation.

## Rerun and inspect

```sh
integration/m2a/isolation/run
R=$(ls -d integration/m2a/isolation/_run/* | tail -1)
python3 -m json.tool "$R/verdict/verdict.json" | less   # per-check evidence
ls "$R"/audit/good/subject/                             # raw per-pid strace
cat "$R"/obs/boundary/{lanes,dns}.jsonl "$R"/obs/canary/canary.jsonl
```

## Finding: BEAM resolves its own hostname at boot

Without `sandbox.inetrc` the subject fails. Before any application code runs, the BEAM resolves the node's hostname through glibc. strace shows two `connect()` calls to `/var/run/nscd/socket` and one to 127.0.0.53:53, and the DNS sink logs the query.

The harness therefore starts the subject with `ERL_INETRC=sandbox.inetrc` (`{lookup, [file]}`). Sandbox targets are IP literals, so they need no resolver.

Any sandbox launch of the real gateway needs the same setting or an equivalent one. That launch configuration belongs to the serving/release owner, not this lane. To reproduce the finding (expected verdict FAIL on the DNS and connect checks):

```sh
TELELEM_ISOLATION_NO_INETRC=1 integration/m2a/isolation/run
```

## Constraints and limits

- **Host changes:** none. No docker network is created, and nothing touches host firewall or sysctl settings. The run uses Docker's existing `none` and `bridge` networks, and its containers are removed on exit.
- **Privileges:** no docker socket mount, no added capabilities and no `--privileged`.
- **No external targets and no real secrets:** the only addresses dialled are the canary's bridge IP and loopback. Sentinel contents are marked `NOT A SECRET`.
- **Prerequisites (exit 2):**
  - a docker daemon usable by the invoking user
  - the runner image from `scripts/setup`
  - `_build/test/lib/{mint,hpax}/ebin` from `scripts/setup`
  - host `strace`, which is copied with its libraries and loader into the run directory and runs inside the container through that loader
- **User namespaces:** unprivileged ones are blocked on this host (`apparmor_restrict_unprivileged_userns=1`). That is why the boundary is a docker `--network none` namespace and not `unshare`.
- **The audit can be tampered with:** strace runs as the same uid in the same container as the subject. It is independent of the subject's code but not protected from a hostile subject. Syscalls outside strace's view, such as io_uring file I/O, are not covered. BEAM does not use io_uring here.
- **The DNS sink is reachable:** it is an observer inside the boundary that the subject could reach. Any query that reaches it fails the run.
- **Canary placement:** the canary is "beyond the boundary" only in the sense that it sits on another network namespace (the default bridge). The `red_bridge` control shows that a process without the boundary does reach it.
- **The subject is a stand-in:** it exercises `Telelem.Sandbox` and Mint, not the serving gateway. Re-run the harness against the serving path once A1 owns that wiring.
