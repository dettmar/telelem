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
- **Read side.** The attempt arms only while its *retained* bytes are below `high_water`. Retained bytes are Mint's undecoded buffer plus the partial SSE frame plus complete events not yet transferred plus a held `[DONE]`. Arming is the only *preventive* control. At most one TLS delivery is in flight to its mailbox. When it stops arming, it stops reading, and TCP flow control then reaches the fixture.
- **Write side.** `Handoff.transfer/2` (one whole SSE event) runs only while the attempt holds byte credit. The initial credit is `window`. The Bandit handler returns `n` bytes of credit only after `Plug.Conn.chunk/2` returned `:ok` for those bytes. So bytes accepted by the coordinator but not yet written downstream are always ≤ `window`.
- **Overflow.** `max_event` and `hard_cap` are **abort thresholds checked after** a delivery has been appended and parsed. They are not a memory ceiling. Before the check, the attempt already holds its previous retained bytes (< `high_water`) plus that delivery, plus transient parse copies. The gateway does not control the size of one delivery. Crossing either threshold causes an **abort**. Before commit the result is `failed` (502); after commit it is `incomplete`. Nothing is dropped silently and nothing resumes.

### Declared budget (`Telelem.Wire.Limits.new/0`, written to `slow_consumer.declared.json` before the run starts)

| Limit | Value | Meaning |
| --- | --- | --- |
| `window` | 65,536 B | max coordinator-accepted, not-yet-written bytes (byte credit) |
| `max_event` | 65,536 B | abort threshold for one SSE event / partial frame (checked after a delivery) |
| `high_water` | 131,072 B | reader stops arming at or above this retained level |
| `hard_cap` | 262,144 B | abort threshold on retained bytes, checked after each delivery; not a memory maximum |
| `overflow` | `:abort` | only action |
| `cleanup_timeout_ms` | 500 | Handoff force-kills an attempt that has not closed |
| `cleanup_deadline_ms` | 1,000 | declared bound: detected downstream close → `{:closed, report}` |
| `upstream_recbuf` | 16,384 | gateway's own upstream kernel receive buffer (Linux doubles it) |
| `downstream_sndbuf` | 65,536 | gateway's own downstream kernel send buffer |
| `response_timeout_ms` | 30,000 | upstream idle timeout while armed, and downstream `send_timeout` |

`Limits.new/1` rejects inconsistent budgets, for example `max_event > window` or `max_event ≥ high_water` (a split event would deadlock).

## Truthful termination rules (narrow M1 grammar)

The pinned SDK (`openai/_streaming.py`) ends iteration **cleanly** on the first event whose data *starts with* `[DONE]`, whatever happens on the wire after it. It also ends cleanly on an EOF with no `[DONE]` at all. It raises `APIError` on a JSON object with a truthy `"error"`. So whoever forwards a `[DONE]`-prefixed event declares success to the client.

- Only **complete** SSE events are forwarded, byte for byte. A partial frame left at a break is discarded and counted (`partial_discarded`).
- Events are parsed the way the SDK's decoder does. The M1 grammar is only: comment-only events, or `data:`-only events carrying `[DONE]` or a JSON object. Anything else (`event:`/`id:`/`retry:` fields, non-JSON data, mixed comment and data, a `[DONE]`-prefixed but non-exact sentinel, or any non-comment event after `[DONE]`) is a **protocol violation**. It is not forwarded, reading stops, and earlier complete events are flushed.
- The exact `data: [DONE]` event is **held**, not forwarded, until Mint reports the HTTP body complete (the chunked terminator arrived). The attempt then forwards it and calls `Handoff.finish(h, {:upstream_done, reason})`, and only if every one of these holds:
  - it is the released winner;
  - every earlier event has been transferred;
  - the finish_reasons seen were all in `stop`/`tool_calls`;
  - there was no error event and no violation.

  Anything else drops the held `[DONE]` and ends without `finish`: the result is `incomplete` after commit, `failed` before. `length`, `content_filter` and other reasons are outside the M1 grammar and are not claimed as success.
- The upstream's own finish chunk is forwarded unchanged when it arrives. The gateway never synthesizes or rewrites a finish chunk, `[DONE]` or the chunked terminator.
- **Error event.** Reading stops. With content already held, the attempt is released and the error event forwarded, so the SDK raises `APIError` with the provider message. The handoff then ends `incomplete`, and the attempt records `{:upstream_error_event, msg}`. With no content, the attempt fails before commit, nothing is forwarded, and the client gets 502.
- On `incomplete`, or a downstream write error, the handler raises `Gateway.Aborted`. Bandit then closes the connection **without** the chunked terminator.
- An attempt becomes `ready` only on its first `:semantic` event: content, tool call, refusal, finish_reason or usage. Role-only preamble, heartbeats, `[DONE]` and error events do not make it ready. Pre-election and post-`[DONE]` heartbeats are discarded.
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

It then runs `TELELEM_NETWORK=host scripts/mix test test/telelem/wire_m1_test.exs --seed 0`. Without the launcher those twelve tests are **skipped**, never passed. Evidence goes to `integration/m1/_run/<stamp>/evidence/*.json`: per-request gateway event log, Handoff report, SDK ordered log, fixture counts and records. Before reading any fixture count, the test waits for gateway quiescence (no live attempt under the supervisor), then calls the fixture's `POST /wait {settled: true}`.

Versions observed in a run (`evidence/versions.json`):

- gateway: Elixir 1.20.5 / OTP 28, ssl 11.6.0.7, Mint 1.11.0, Bandit 1.14.1, ThousandIsland 1.6.0
- SDK agent: Python 3.13.9, openai 3.52.0, httpx2/httpcore2 2.16.2, h11 0.16.0, OpenSSL 3.5.4

## Cases and raw outcomes

All numbers below come from 5 consecutive `integration/m1/run` runs on 2026-11-24 (Linux 7.0, 32 cores). Every run was `6 passed` at fe483cf. After the terminal-grammar fix the file has 12 tests (6 grammar cases added); re-run counts are given below. After the fix: 5 consecutive runs, each `12 passed`. Slow consumer: blocked 1,008–1,071 ms, peak retained 137,306–151,160 B, max delivery 30,240 B, in flight 60,464 B, cleanup 0.25–1.75 ms. Immediate RST: committed path 5/5.

| Case | Observation (asserted) |
| --- | --- |
| Refusal before connection | Fixture lane a is bound but not listening (`listening: false`). Gateway: `connect_attempt a` then `connect_failed a :refused :econnrefused`, then `connect_attempt b` and `dispatched b`. Fixture a: `tcp_accepts 0, requests 0`. Fixture b: `tcp_accepts 1, chat_completions 1, outcomes {complete: 1}`. One gateway ingress. SDK: status 200, `content "Hello from lane b."`, `finish_reason "stop"`, iterator ended, no exception. Report: `{:done, :upstream_done}`, primary `charge: :none`, backup `:possible`. Downstream ended with the chunked terminator. |
| Reset after commit (fixture `hold: true`, released only after the gateway wrote the fragment) | SDK ordered log: `chunk` (role), `chunk` (tool name), `chunk` with `tool_args '{"city": "Sto'`, then `openai.APIConnectionError` caused by `httpx2.RemoteProtocolError: peer closed connection without sending complete message body (incomplete chunked read)`. `finish_reason nil`, status 200 (recorded, not the pass condition). Fixture a outcome `reset`. Fixture b: `tcp_accepts 0, requests 0`. Report: `{:incomplete, :winner_closed}`, committed, backup `never_registered`, `possible_charges [:primary]`. No downstream write contains `[DONE]`. Downstream `:aborted`. |
| Reset after commit, immediate RST | 5/5 runs took the `committed_then_incomplete` path, with the same SDK observation as above. The test would also accept a truthful `failed_before_commit` (502) path, which applies if the RST destroys unread bytes in the gateway socket. That path was **not observed**. |
| Slow consumer, pressure, disconnect | 400 events × 30,000 B queued on lane a. The SDK reads 3 chunks, then stops iterating. **Pressure:** downstream `chunk/2` blocked ≥ 1,008–1,071 ms (write #11–13 of 30,232 B). The attempt is paused and not armed, and its `deliveries` count is unchanged over 500 ms. The fixture's `body_bytes_sent` was frozen at 3.57–4.23 MB of ≥ 12 MB in both samples. **Bounds:** peak retained 151,160 B (measured at the after-delivery check points; ≤ the 262,144 abort threshold). In flight 60,464 B (≤ 65,536). Max single TLS delivery 30,240 B. **Disconnect:** the SDK calls `stream.close()`. SDK close → gateway detect is ≈ 1.3–1.5 ms (wall clock, cross-process, approximate). Detected close → local `{:closed, report}` took 0.25–0.41 ms (declared ≤ 1,000). Afterwards the attempt, the coordinator and the Bandit handler are dead. The gateway supervisor has 0 children, and there are 0 TCP ports to lane a and 0 connected ports on the gateway port. Report: `{:incomplete, {:downstream_closed, :closed}}`, primary cleanup `:ack`, `possible_charges [:primary]`. Fixture request outcome `client_closed`. Lane b: `tcp_accepts 0`. |
| Trust | Primary configured with the wrong CA: `connect_failed a :ambiguous {:tls_alert, {:unknown_ca, _}}`. No failover. Fixture a: `tcp_accepts 1, tls_failed 1, requests 0`. Fixture b: 0. SDK: `openai.InternalServerError` with status 502. An SDK that trusts only the fixture CA is refused by the gateway (`APIConnectionError`, `CERTIFICATE_VERIFY_FAILED`), and the gateway sees no extra ingress. |
| Baseline | Valid lane a: complete SDK stream "Hello from lane a.", lane b untouched. |

### Terminal grammar through the real SDK (scripted upstream)

The independent fixture has no fault for these byte sequences. In these cases the primary lane is an in-BEAM Bandit TLS upstream that writes exact, test-authored bytes; the client is still the real SDK through the real gateway, and lane b (the fixture) must show `tcp_accepts 0`. These are **not** independent-fixture evidence. The table distinguishes the valid control from the cases where the SDK or the gateway reported false success at `fe483cf`:

| Upstream bytes (after the role chunk) | fe483cf (before) | now |
| --- | --- | --- |
| content, finish `stop`, `[DONE]`, complete body (control) | SDK clean, gateway `:done` | same: SDK clean, `{:done, {:upstream_done, "stop"}}`, terminator |
| content, `stop`, `[DONE]`, then close **without** chunked terminator | **SDK clean success** (`[DONE]` already forwarded) while the gateway said `incomplete` | SDK `APIConnectionError` after the forwarded `stop` chunk; attempt `{:upstream_broken, :closed, %{done_held: true}}`; no `[DONE]` written; aborted |
| content, `stop`, `data: [DONE]junk`, complete body | **SDK clean, gateway `:done`** | SDK `APIConnectionError`; `{:protocol_violation, {:bad_sentinel, 18}}`; no `[DONE]` written |
| content, `[DONE]` (no finish_reason), complete body | **SDK clean, gateway `:done`** | SDK `APIConnectionError`; `{:done_without_finish_reason, :no_finish}` |
| content, error event, `[DONE]`, complete body | SDK `APIError`, **gateway `:done`** | SDK `APIError` ("scripted upstream error"); handoff `incomplete`; attempt `{:upstream_error_event, _}`; no `[DONE]` written |
| error event, `[DONE]`, complete body | SDK `APIError` (forwarded), **gateway `:done`** | SDK `InternalServerError` 502; `{:failed, :no_winner}`; nothing written |

The offline `wire_upstream_test.exs` uses real loopback TLS with an in-BEAM Bandit as upstream. It is transport logic only and is **not** M1 evidence. It also repeats the terminal grammar: `[DONE]` held across an abort; exact `[DONE]` after `stop`/`tool_calls` only; `[DONE] `, `data:[DONE]x`, `length`, a missing finish, an event after `[DONE]`, and an `event:` field never become done; error events. It covers:

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

- **Measured (application):** attempt retained bytes (Mint buffer + partial frame + pending events + held `[DONE]`, sampled after each delivery), in-flight credit, the size of each TLS delivery handed to the attempt, blocked-write duration, the cleanup interval in BEAM monotonic time, and live processes and ports after cleanup.
- **Sampled, not bounded:** memory of the OTP ssl processes behind the upstream socket (≈ 8.8 KB + 55 KB in one run). Their internal buffering is an OTP implementation detail outside the declared bound.
- **Not application bytes:** the 3.5–4.2 MB the fixture managed to send before stalling. It sits in kernel buffers: the SDK's receive buffer (autotuned, not controlled), the gateway's pinned sndbuf and recbuf, and the fixture's send buffer.
- **Not controlled by the gateway:** one TLS delivery's size. `buffer:` did not cap it. The pinned `recbuf` did bound it in practice: 120,960 B max with recbuf 64 KiB, 30,240 B with 16 KiB. `hard_cap` is checked after each delivery and aborts if exceeded. It cannot stop one oversized delivery from being held briefly, so the transient worst case is < `high_water` + one delivery + parse copies.
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
