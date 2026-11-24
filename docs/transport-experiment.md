# M1 transport experiment (TEL-005)

This is a test gateway, not a product. It repeats the TEL-003 handoff over real sockets: the pinned OpenAI Python SDK talks to Bandit (TLS), then `Telelem.Handoff`, then Mint (TLS), then the independent TEL-004 fixture's two lanes. All endpoints are on loopback and no credentials are used.

```
openai 3.52.0 (host, max_retries=0, dummy key, trusts only the gateway CA)
  --TLS 1.3--> Bandit 1.14.1 handler  = Handoff requester / downstream controller
                 |  Telelem.Handoff (TEL-003, unchanged; under the gateway's DynamicSupervisor)
                 +- Telelem.Wire.Upstream :primary --Mint 1.11.0 TLS--> fixture lane a
                 +- Telelem.Wire.Upstream :backup  --Mint 1.11.0 TLS--> fixture lane b  (started only on refusal)
```

## Reader choice: a bounded, credit-driven reader

The PM may choose either "proven passive handoff" or "proven credit-driven reader". This experiment uses a **credit-driven reader**. The reader still owns its socket passively, which keeps ownership simple. A blocking passive `recv` would make an attempt deaf to `{:cancel, _}` for as long as the recv lasts. It would also leave the coordinator → requester path unbounded, as TEL-003 warns.

- **Ownership.** One `Telelem.Wire.Upstream` process owns one Mint connection and its TLS socket for the socket's whole life. Mint runs in `:passive` mode, so `Mint.HTTP.stream/2` never re-arms the socket. The attempt arms it itself with `:ssl.setopts(sock, active: :once)` from its `receive` loop. It is never blocked in I/O, and it sees cancel, credit and the coordinator's `:DOWN` immediately. When the owner is killed, the socket is closed. This is tested by `wire_upstream_test`'s "killing the attempt" case.
- **Read side.** The attempt arms only while its *retained* bytes are below `high_water`. Retained bytes are Mint's undecoded buffer plus the partial SSE frame plus complete events not yet transferred. At most one TLS delivery is in flight to its mailbox. When it stops arming, it stops reading, and TCP flow control then reaches the fixture.
- **Write side.** `Handoff.transfer/2` (one whole SSE event) runs only while the attempt holds byte credit. The initial credit is `window`. The Bandit handler returns `n` bytes of credit only after `Plug.Conn.chunk/2` returned `:ok` for those bytes. So bytes accepted by the coordinator but not yet written downstream are always ≤ `window`.
- **Overflow.** One event larger than `max_event`, or retained bytes above `hard_cap` after one delivery, causes an **abort**. Before commit the result is `failed` (502); after commit it is `incomplete`. Nothing is dropped silently and nothing resumes.

### Declared budget (`Telelem.Wire.Limits.new/0`, written to `slow_consumer.declared.json` before the run starts)

| Limit | Value | Meaning |
| --- | --- | --- |
| `window` | 65,536 B | max coordinator-accepted, not-yet-written bytes (byte credit) |
| `max_event` | 65,536 B | max one SSE event / partial frame; larger triggers abort |
| `high_water` | 131,072 B | reader stops arming at or above this retained level |
| `hard_cap` | 262,144 B | retained bound after any delivery; larger triggers abort |
| `overflow` | `:abort` | only action |
| `cleanup_timeout_ms` | 500 | Handoff force-kills an attempt that has not closed |
| `cleanup_deadline_ms` | 1,000 | declared bound: detected downstream close → `{:closed, report}` |
| `upstream_recbuf` | 16,384 | gateway's own upstream kernel receive buffer (Linux doubles it) |
| `downstream_sndbuf` | 65,536 | gateway's own downstream kernel send buffer |
| `response_timeout_ms` | 30,000 | upstream idle timeout while armed, and downstream `send_timeout` |

`Limits.new/1` rejects inconsistent budgets, for example `max_event > window` or `max_event ≥ high_water` (a split event would deadlock).

## Truthful termination rules

- Only **complete** SSE events are forwarded, byte for byte. A partial frame left at a break is discarded and counted (`partial_discarded`).
- `Handoff.finish/2` is called only when the upstream sent `data: [DONE]` **and** Mint saw the end of the HTTP body. A clean HTTP end without `[DONE]` is `incomplete`. This matters because the SDK treats a clean end with no `[DONE]` as silent success.
- On `incomplete`, or a downstream write error, the handler raises `Gateway.Aborted`. Bandit then closes the connection **without** the chunked terminator. It never writes `[DONE]` or `0\r\n\r\n` on its own.
- An attempt becomes `ready` on its first content, tool-call, finish, usage, error or `[DONE]` event. Role-only preamble and heartbeats do not make it ready, and pre-election heartbeats are discarded.
- **Dispatch boundary.** After `connect` succeeds, the attempt calls `Handoff.begin_dispatch/2` and writes the request only on `:ok`. It does not dispatch on a cancel that is already in its mailbox or already recorded by the coordinator. A `connect` that got `ECONNREFUSED` never reached that boundary, so it is provably pre-send (`charge: :none`).
- **Failover** to `:backup` happens only on `{:connect_failed, :refused, :econnrefused}` from the primary, before any request byte was written. Any other connect/TLS failure is `:ambiguous`: the controller abstains and ends with 502. After dispatch there is no failover. Once a byte is committed there is no reselection, because Handoff never reopens the election.

## Commands (clean checkout)

```sh
scripts/setup                                   # online, once: runner image + Hex deps
(cd fixtures/wire && uv sync --locked)          # online, once: fixture + pinned SDK venv
scripts/mix test --seed 0 test/telelem/wire_sse_test.exs test/telelem/wire_upstream_test.exs   # offline
integration/m1/run                              # offline: fixture + SDK agent on host, ExUnit in runner
```

`integration/m1/run` starts these on the host, loopback only:

- `telelem-wire serve`. Its state dir sits under `integration/m1/_run/<stamp>/`, so the runner sees the fixture CA at `/work/...`.
- `integration/m1/sdk_agent.py`, the real SDK behind a small JSON control server. It drops `OPENAI_*` variables.

It then runs `TELELEM_NETWORK=host scripts/mix test test/telelem/wire_m1_test.exs --seed 0`. Without the launcher those six tests are **skipped**, never passed. Evidence goes to `integration/m1/_run/<stamp>/evidence/*.json`: per-request gateway event log, Handoff report, SDK ordered log, fixture counts and records. Before reading any fixture count, the test waits for gateway quiescence (no live attempt under the supervisor), then calls the fixture's `POST /wait {settled: true}`.

Versions observed in a run (`evidence/versions.json`):

- gateway: Elixir 1.20.5 / OTP 28, ssl 11.6.0.7, Mint 1.11.0, Bandit 1.14.1, ThousandIsland 1.6.0
- SDK agent: Python 3.13.9, openai 3.52.0, httpx2/httpcore2 2.16.2, h11 0.16.0, OpenSSL 3.5.4

## Cases and raw outcomes

All numbers below come from 5 consecutive `integration/m1/run` runs on 2026-11-24 (Linux 7.0, 32 cores). Every run was `6 passed`.

| Case | Observation (asserted) |
| --- | --- |
| Refusal before connection | Fixture lane a is bound but not listening (`listening: false`). Gateway: `connect_attempt a` then `connect_failed a :refused :econnrefused`, then `connect_attempt b` and `dispatched b`. Fixture a: `tcp_accepts 0, requests 0`. Fixture b: `tcp_accepts 1, chat_completions 1, outcomes {complete: 1}`. One gateway ingress. SDK: status 200, `content "Hello from lane b."`, `finish_reason "stop"`, iterator ended, no exception. Report: `{:done, :upstream_done}`, primary `charge: :none`, backup `:possible`. Downstream ended with the chunked terminator. |
| Reset after commit (fixture `hold: true`, released only after the gateway wrote the fragment) | SDK ordered log: `chunk` (role), `chunk` (tool name), `chunk` with `tool_args '{"city": "Sto'`, then `openai.APIConnectionError` caused by `httpx2.RemoteProtocolError: peer closed connection without sending complete message body (incomplete chunked read)`. `finish_reason nil`, status 200 (recorded, not the pass condition). Fixture a outcome `reset`. Fixture b: `tcp_accepts 0, requests 0`. Report: `{:incomplete, :winner_closed}`, committed, backup `never_registered`, `possible_charges [:primary]`. No downstream write contains `[DONE]`. Downstream `:aborted`. |
| Reset after commit, immediate RST | 5/5 runs took the `committed_then_incomplete` path, with the same SDK observation as above. The test would also accept a truthful `failed_before_commit` (502) path, which applies if the RST destroys unread bytes in the gateway socket. That path was **not observed**. |
| Slow consumer, pressure, disconnect | 400 events × 30,000 B queued on lane a. The SDK reads 3 chunks, then stops iterating. **Pressure:** downstream `chunk/2` blocked ≥ 1,008–1,071 ms (write #11–13 of 30,232 B). The attempt is paused and not armed, and its `deliveries` count is unchanged over 500 ms. The fixture's `body_bytes_sent` was frozen at 3.57–4.23 MB of ≥ 12 MB in both samples. **Bounds:** peak retained 151,160 B (≤ 262,144). In flight 60,464 B (≤ 65,536). Max single TLS delivery 30,240 B. **Disconnect:** the SDK calls `stream.close()`. SDK close → gateway detect is ≈ 1.3–1.5 ms (wall clock, cross-process, approximate). Detected close → local `{:closed, report}` took 0.25–0.41 ms (declared ≤ 1,000). Afterwards the attempt, the coordinator and the Bandit handler are dead. The gateway supervisor has 0 children, and there are 0 TCP ports to lane a and 0 connected ports on the gateway port. Report: `{:incomplete, {:downstream_closed, :closed}}`, primary cleanup `:ack`, `possible_charges [:primary]`. Fixture request outcome `client_closed`. Lane b: `tcp_accepts 0`. |
| Trust | Primary configured with the wrong CA: `connect_failed a :ambiguous {:tls_alert, {:unknown_ca, _}}`. No failover. Fixture a: `tcp_accepts 1, tls_failed 1, requests 0`. Fixture b: 0. SDK: `openai.InternalServerError` with status 502. An SDK that trusts only the fixture CA is refused by the gateway (`APIConnectionError`, `CERTIFICATE_VERIFY_FAILED`), and the gateway sees no extra ingress. |
| Baseline | Valid lane a: complete SDK stream "Hello from lane a.", lane b untouched. |

The offline `wire_upstream_test.exs` uses real loopback TLS with an in-BEAM Bandit as upstream. It is transport logic only and is **not** M1 evidence. It covers:

- credit gating and pause at `high_water` with no further reads;
- cancel while paused closes the socket in < `cleanup_timeout`;
- killing the owner closes the socket (`{:down, :killed}`, `:possible`);
- a clean end without `[DONE]` gives `incomplete`;
- an abrupt close mid-event forwards complete events only and discards the partial frame;
- an oversized event aborts before commit;
- a non-200 status fails before commit;
- an untrusted CA is `:ambiguous`;
- a refused port is `:refused` / `:none`.

## Measured vs. not measured

- **Measured (application):** attempt retained bytes (Mint buffer + partial frame + pending events), in-flight credit, the size of each TLS delivery handed to the attempt, blocked-write duration, the cleanup interval in BEAM monotonic time, and live processes and ports after cleanup.
- **Sampled, not bounded:** memory of the OTP ssl processes behind the upstream socket (≈ 8.8 KB + 55 KB in one run). Their internal buffering is an OTP implementation detail outside the declared bound.
- **Not application bytes:** the 3.5–4.2 MB the fixture managed to send before stalling. It sits in kernel buffers: the SDK's receive buffer (autotuned, not controlled), the gateway's pinned sndbuf and recbuf, and the fixture's send buffer.
- **Not controlled by the gateway:** one TLS delivery's size. `buffer:` did not cap it. The pinned `recbuf` did bound it in practice: 120,960 B max with recbuf 64 KiB, 30,240 B with 16 KiB. `hard_cap` enforces the bound, with abort if exceeded.
- **Unknown:** whether the remote side stopped work or billing. The fixture reporting `client_closed` is the fixture's own observation. A local close proves neither, so the report keeps the charge `:possible`.

## Limitations and gaps

- If the downstream client disconnects while the upstream is silent, the gateway notices only on its **next write**, or after the upstream idle timeout (30 s) or the send timeout. Bandit's HTTP/1 handler does not read the socket during a plug call. The tested disconnect happens during a blocked write.
- A downstream abort is Bandit's normal TLS close (close_notify + FIN), not an RST. Truncation is detected by HTTP/1.1 chunked framing (h11 "incomplete chunked read"). An HTTP/1.0 or close-delimited client could not tell the difference. Only chunked HTTP/1.1 is supported.
- Each SSE event is one downstream chunk, with no coalescing. Credit is bytes per event, so `max_event ≤ window` is required.
- The reference reset case orders the RST after the gateway forwarded the fragment (fixture `hold`). The immediate-RST variant passed via commit in 5/5 runs, but the race is not eliminated.
- No authority/admission is wired in (`authority: nil`). Spend settlement, two-instance fencing and the admission model are TEL-002/003/006.
- Cross-process timings use wall clocks and are approximate. In-gateway timings use BEAM monotonic µs.
- This is not a provider compatibility or performance claim.

## Code

- `lib/telelem/wire/limits.ex`: the declared budget.
- `lib/telelem/wire/sse.ex`: bounded SSE splitter and classifier.
- `lib/telelem/wire/upstream.ex`: attempt process (owner, reader, credit, termination rules).
- `lib/telelem/wire/gateway.ex`: supervisor, in-memory downstream CA, Bandit plug, downstream controller.
- `lib/telelem/wire/recorder.ex`: evidence log, including the Handoff trace.
- `integration/m1/run`, `integration/m1/sdk_agent.py`: host launcher and SDK agent.
