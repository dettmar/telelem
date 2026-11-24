# M1 · What the stream experiment establishes

**Candidate decision: keep the credit-driven reader.** Each attempt owns its upstream TLS socket for the socket’s lifetime. The downstream writer returns byte credit only after a successful write. Deployment fallback therefore needs no transfer of a live socket to a new process. This is an HTTP/1.1 experiment against local scripted deployments.

### Replay the experiment

From the repo root on Linux with Docker and `uv`:

```sh
# One-time dependency setup (network needed if uncached).
scripts/setup
(cd fixtures/wire && uv sync --locked)

# No provider keys or inference calls. The wire launcher needs host loopback.
MIX_ENV=test scripts/mix compile --force --warnings-as-errors
scripts/mix test --seed 424242
(cd fixtures/wire && uv run --locked --offline pytest -q)
integration/m1/run
```

The default `mix test` suite **skips** the real-SDK cases. `integration/m1/run` runs them with seed `0` and prints the evidence directory. Start there with `refusal_before_connection.json`, `reset_after_commit.json`, `slow_consumer.declared.json` and `slow_consumer.json`. For terminal edge cases, open `grammar_*.json`. The `grammar` cases use a scripted TLS primary, the real gateway and the real SDK; the basic failover cases use the independent Python fixture.

### What a reviewer should look for

| Question | Decisive observation |
| --- | --- |
| Did fallback rescue a call that could not have reached the primary? | One SDK ingress, primary connect refused and zero primary arrivals; one backup arrival and a valid SDK completion. |
| Did the gateway try to rescue a call that had already committed? | The SDK yields a partial tool argument, **then raises**. The gateway is quiescent, the fixture reports one primary arrival and zero backup arrivals. HTTP 200 alone says nothing about completion. |
| Whose definition of success wins? | The pinned SDK can stop at `[DONE]` without reading the HTTP terminator. Hold the exact marker until a supported finish reason **and** complete HTTP framing are observed. A finish chunk by itself can legitimately be followed by an exception. A provider error must not become a successful gateway outcome. |
| What did the reader bound? | Outstanding downstream credit is 65,536 bytes. When writes block and credit is exhausted, reads stop. The 262,144-byte threshold aborts **after** an upstream delivery; it is not a cap on process memory. |
| What survives a crash? | On authority loss an open committed stream becomes incomplete; an earlier terminal stays terminal. The other instance continues. Fenced, undispatched holds release; dispatched work stays possibly billable. Whole-instance replacement invalidates old grants; its empty ledger does not establish zero historical exposure. |

### Limits to carry forward

- The native success grammar tested here accepts `stop` and `tool_calls`. Other finish reasons and endpoints need their own conformance evidence.
- The wire gateway exercises the reader and handoff. The admission-authority crash is tested separately with the real instance supervisor and a loopback transport. M2 must join these in the ordinary serving path.
- The diagnostic recorder retains event history and payload samples for the experiment. Its memory grows with the trace. Replace it with bounded, payload-free instrumentation before reusing this path for serving. Kernel and OTP memory, temporary parse copies and recorder allocations are outside the reader's credit measurement.
- Local cleanup is timed from **detection**, not from the instant the client disconnected. The end timestamp is when the recorder sees the close report; the test samples processes and sockets separately after a further 200 ms. An upstream that's silent can delay detection until a write or timeout. Local close is not proof of remote cancellation or a zero bill.

See the [blind acceptance card](m1-gate.md) for the gate and the [transport experiment](transport-experiment.md) for versions, limits and raw evidence definitions.
