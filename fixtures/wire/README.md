# telelem-wire — TEL-004 wire fixture

A small, separate (non-Elixir) **two-lane loopback TLS HTTP/1.1 provider** that speaks the
OpenAI-compatible chat-completions SSE grammar, counts every arrival per lane itself, and
plays scripted faults byte-for-byte. Lanes `a` and `b` stand for two deployments of the same
protocol. Everything stays on loopback: it uses no live provider and no external network after setup.

* Raw sockets + `ssl` only (no HTTP framework), so the fixture owns chunk framing, missing
  terminators, TLS `close_notify` and TCP RST.
* **Ephemeral trust:** a fresh CA + leaf (ECDSA P-256 by default; SAN `127.0.0.1`, `::1`, `localhost`)
  is generated at start in a private `0700` state dir and deleted at stop. No keys are committed
  (`*.pem`/`*.key` are git-ignored).
* Reference client: the **pinned `openai` Python SDK** with `max_retries=0`, `trust_env=False`
  and the fixture CA as its only trust anchor (`telelem_wire.client.sdk_client`).

## Setup and run

Initial dependency setup (network, once):

```sh
cd fixtures/wire
uv sync --locked            # creates the isolated fixtures/wire/.venv from uv.lock
```

Offline test run (no network needed):

```sh
uv run --locked --offline pytest -q            # direct-wire + CLI/control tests
uv run --locked --offline telelem-wire scenarios   # JSON evidence for each reference case; exit 1 on mismatch
```

### Pins (from `uv.lock`; `exclude-newer = 2026-11-17` cooldown)

| Component | Version |
| --- | --- |
| CPython (uv-managed) | 3.13.9 (`requires-python >=3.13,<3.14`) |
| openai (SDK under test) | 3.52.0 → httpx2 2.16.2 / httpcore2 2.16.2 / h11 0.16.0 |
| cryptography | 50.0.2 |
| pytest | 9.2.0 |
| OpenSSL linked by Python | 3.5.4 |

`telelem-wire scenarios` and the `serve` ready line both report the versions they actually load.

## Faults

The lane default applies to every request. `queue` adds one-shot faults, used first-in, first-out, before the default.
`refuse` applies to the whole lane. Every other fault applies to one request.

| kind | params (defaults) | wire behaviour | what the pinned SDK surfaces |
| --- | --- | --- | --- |
| `valid` | `text=[…]`, `tool_call=false`, `interval=0` | role chunk, content (or tool-call) deltas, `finish_reason`, optional usage chunk (`stream_options.include_usage`), `data: [DONE]`, `0\r\n\r\n` | chunks, `finish_reason` `stop`/`tool_calls`, no exception |
| `heartbeat` | `count=3`, `interval=0.05`, `end="clean"` | only `: keep-alive` SSE comments. With `count=null` it repeats until the client closes, `release()` or `max_hold`. `clean` sends the `0`-chunk with no data and no `[DONE]` | **0 chunks, no `finish_reason`, no exception**. The iterator ends silently |
| `reset_after_tool_fragment` | `fragment='{"city": "Sto'`, `close="rst"`, `hold=false` | role chunk, tool-call header (`get_weather`, empty arguments), one **nonempty** argument fragment, then an abrupt close mid chunked body: no `finish_reason`, no `[DONE]`, no `0`-chunk, no TLS `close_notify`. `rst` uses `SO_LINGER 0` to send a TCP RST. `fin` sends a plain FIN. `hold=true` waits for `release()` before closing | fragment visible in `delta.tool_calls[0].function.arguments`, then `openai.APIConnectionError` caused by `httpx2.ReadError` / `ConnectionResetError` (rst), or `RemoteProtocolError … incomplete chunked read` (fin) |
| `stall` | `after_events=1`, `on_release="complete"` | headers, then N data events, then holds (phase `holding`). After `release()` it finishes the valid stream (or sends `rst`/`fin`). A client close is detected immediately with `select` (no polling) | blocks; the SDK read timeout gives `openai.APITimeoutError` |
| `slow` | `events=20`, `interval=0.2` | valid stream with a pause between events. A client close during a pause is detected and the producer stops | complete stream, slowly |
| `status` | `code=503`, `message` | JSON error with Content-Length; the connection stays open | `openai.InternalServerError` / `RateLimitError` … (one arrival: no retries) |
| `refuse` | — | the lane port stays **bound but not listening**, so the kernel answers SYN with RST (`ECONNREFUSED`). Nothing is accepted and no other process can take the port. Switching back to any other kind listens again on the **same port** | `openai.APIConnectionError` ← `httpx2.ConnectError` ← `ConnectionRefusedError` |

Non-streaming requests (`stream` false/absent) get these responses: `valid`/`slow` return a full `chat.completion` JSON. `status` returns the error.
`reset_after_tool_fragment` sends the full Content-Length, then a body prefix and an RST/FIN. `stall`/`heartbeat` send headers, hold, then RST.
`GET /v1/models` lists `telelem-wire-model`. Other routes return a 404, which is counted.

`max_hold` (default 60 s) caps every hold. When it expires the fixture sends an RST and records outcome `max_hold_rst`, so a broken test never hangs.

## Counting (fixture-side, independent of any gateway)

Per lane: `tcp_accepts`, `tls_ok`, `tls_failed`, `requests`, `chat_completions`, `completed`,
`client_closed`, `by_path{"POST /v1/chat/completions": n}`, `outcomes{complete|reset|fin_truncated|client_closed|max_hold_rst|shutdown|error: n}`.
A refused connect is **never accepted, so it is never counted**: `tcp_accepts` stays 0. That zero is the evidence.

### Observation ordering: when to call the settle barrier

The client and the fixture run on separate threads, so the fixture defines which records it commits
before the client can see anything:

* **Before the first response byte:** `tcp_accepts`, `tls_ok`, `requests`, `chat_completions`, `by_path`,
  creation of the request record and the selected `fault`. These are safe to read as soon as the client has
  seen any response or error from the lane.
* **Before the abrupt close:** the outcomes `reset`, `fin_truncated` and `max_hold_rst`. Once the SDK has raised
  for a reset, the record already says `reset`.
* **After the last byte, so the client can be ahead:** `complete` (the SDK returns at `[DONE]`), a unary or
  `status` `complete`, `client_closed`, `phase="done"`, `completed`, `outcomes`, and the final `events_sent` and
  byte counts.

Read the third group only behind the ack barrier `fx.wait_settled(lane=None, timeout=5)`, or
`POST /wait {"settled": true[, "lane": "a"]}` on the control plane. The barrier blocks until every request
record on that lane (or on all lanes) has an outcome, and returns False (408 on the control plane) on timeout.
It covers only requests the fixture has already parsed. An *absence* claim such as "lane b got no
replacement dispatch" needs its own causal end point on the gateway side.

Each request record (`requests()`, `GET /requests[/<id>]`) contains:

* `id` (`a-1`, `a-2`, …, also sent as the `x-request-id` response header) and `conn_id`
* `method`, `path`
* `headers`: lower-cased. Authorization is redacted, and the record adds `_tls_version` / `_alpn` and keeps `x-stainless-retry-count`
* parsed `body`, the selected `fault`, `status`
* `phase`: `received` → `streaming` → `holding` → `done`
* `outcome`
* `events_sent`: the exact payload of each chunk, as sent
* `body_bytes_sent` and `wire_bytes_sent` (the latter includes chunk framing)
* `released`, `client_closed`, `duration_ms`

## Python API (how TEL-005 should drive it)

```python
from telelem_wire import WireFixture, sdk_client, run_stream

with WireFixture(faults={"a": "refuse"}, max_hold=10) as fx:      # lanes ("a", "b"), 127.0.0.1, ephemeral ports
    env = fx.gateway_env()      # TELELEM_WIRE_CA_FILE, TELELEM_WIRE_LANES=a,b, TELELEM_WIRE_LANE_A_BASE_URL, _HOST, _PORT …
    gw = subprocess.Popen(["mix", "run", "--no-halt"], cwd=..., env={**os.environ, **env}, stdin=subprocess.PIPE)
    ...
    fx.set_fault("a", "reset_after_tool_fragment")                 # or "stall", after_events=2 …
    fx.queue_faults("b", [{"kind": "status", "code": 429}])
    rec = fx.wait_request("a", 1, phase="holding", timeout=5)     # condition variable, no sleeps
    assert fx.wait_settled()                                       # ack before reading outcome/outcomes
    fx.release(lane="a")                                           # a release after "holding" is never lost
    fx.wait_counter("b", "requests", 1, timeout=5)
    assert fx.counts("b")["requests"] == 0                         # "no replacement dispatch"
```

* `fx.lane(n).base_url` → `https://127.0.0.1:<port>/v1`. `fx.ca_file` is a PEM CA file.
  `info()["lanes"][n]["base_url_localhost"]` gives the same listener by DNS name.
* `sdk_client(base_url, ca_file, timeout=…)` returns the reference SDK client. Point it at the gateway's
  public URL (and the gateway's CA) to run the same observation through the gateway.
  `run_stream(client, **create_overrides)` never raises. It returns `chunks`, `content`, `tool_name`, `tool_args`,
  `finish_reason`, `usage`, `request_id` and `exception{type, mro, message, cause_chain}`.
* To use it from another uv project, add `telelem-wire = { path = "../wire", editable = true }`
  (extra `[sdk]` for openai). You can also run it in place with `uv run --project fixtures/wire …`.

## CLI / process API (for a BEAM-side harness)

```sh
uv run telelem-wire serve [--lanes a,b] [--fault 'a=refuse'] [--fault 'b=stall:{"after_events":1}'] \
    [--control-port 0] [--state-dir DIR] [--key-type ec|rsa] [--max-hold 60] \
    [--ready-file PATH] [--exit-on-stdin-eof]
```

The command prints **one JSON line** on stdout when it is ready (schema `telelem-wire/1`). Logs go to stderr.

```json
{"schema":"telelem-wire/1","pid":123,"ca_file":"/tmp/telelem-wire-x/ca.pem","ca_sha256":"…","state_dir":"…",
 "lanes":{"a":{"name":"a","host":"127.0.0.1","port":41001,"base_url":"https://127.0.0.1:41001/v1",
               "base_url_localhost":"https://localhost:41001/v1","chat_completions_url":"…/v1/chat/completions",
               "listening":false,"fault":{"kind":"refuse"},"queued":[]}, "b":{…}},
 "fault_kinds":[…],"control_url":"http://127.0.0.1:41003",
 "env":{"TELELEM_WIRE_CA_FILE":"…","TELELEM_WIRE_LANE_A_BASE_URL":"…","TELELEM_WIRE_CONTROL_URL":"…",…},
 "versions":{"python":"3.13.9","openai":"3.52.0",…}}
```

The process stops on SIGINT/SIGTERM, on `POST /shutdown`, or (with `--exit-on-stdin-eof`) when stdin closes,
which is what happens when an Erlang `Port` closes. On stop it deletes the trust files and the ready file.

Control plane: plain HTTP with JSON bodies, bound to loopback only, no auth. Any local process can reach it, so use it only on a dev or CI host.

| Request | Effect |
| --- | --- |
| `GET /info`, `GET /state`, `GET /counts[?lane=a]`, `GET /requests[?lane=a]`, `GET /requests/a-1`, `GET /env` | read |
| `POST /lanes/<lane>/fault` `{"kind":"stall","after_events":2}` | set the lane default (400 on an unknown kind or param) |
| `POST /lanes/<lane>/queue` `{"faults":[{"kind":"reset_after_tool_fragment"}]}` | add one-shot faults |
| `POST /release` `{"lane":"a"}` or `{"request_id":"a-1"}` | release held requests → `{"released":[ids]}` |
| `POST /wait` `{"lane":"a","counter":"requests","at_least":1,"timeout":5}`, `{"lane":"a","nth":1,"phase":"holding","timeout":5}` or `{"settled":true[,"lane":"a"],"timeout":5}` | blocks until true → 200, or 408 on timeout |
| `POST /reset` | zero the counters and drop finished records |
| `POST /shutdown` | stop the process |

`telelem-wire probe --control URL --lane a` (or `--base-url … --ca-file …`) runs one SDK stream and prints the observation.

## Observed SDK behaviour worth knowing (openai 3.52.0, sync)

* Errors raised mid-stream are wrapped: a reset after a fragment surfaces as `openai.APIConnectionError`, not as a raw httpx exception.
  The cause chain separates RST (`ConnectionResetError`) from FIN (`incomplete chunked read`).
* A heartbeat-only stream with a clean end looks like an **empty successful stream**: 0 chunks and no exception.
  Only the missing `finish_reason` and `[DONE]` tell it apart.
* The sync `Stream` stops at `data: [DONE]` and closes the response without reading the `0`-chunk, so a
  **streamed request's connection is not reused**: 30 sequential streams on one client made 30 TCP accepts.
  Unary requests do reuse a single kept-alive connection.
* `max_retries=0` is visible on the wire as `x-stainless-retry-count: 0`. A 503 gives exactly one arrival.

## Limitations

* HTTP/1.1 only (ALPN `http/1.1`). No HTTP/2, no pipelining and no request trailers. Request bodies up to 8 MiB.
* `refuse` cannot count SYNs. It proves zero accepts, not how many attempts there were.
* Thread per connection. It is meant for correctness experiments, not load.
* Byte transcripts record what the fixture *wrote to TLS*. What the client actually received must be observed on the client side
  (`run_stream`, or a raw `ssl` socket as in `tests/test_direct_wire.py`).
