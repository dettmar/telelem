"""Direct-wire tests: pinned openai SDK (max_retries=0) -> fixture lanes. No gateway, no external network."""

import json
import socket
import ssl
import threading
import time

import openai
import pytest

from telelem_wire import TOOL_ARGS, TOOL_FRAGMENT, WireFixture
from telelem_wire.client import CHAT_ARGS, run_stream, sdk_client


def client(fx, lane="a", **kw):
    return sdk_client(fx.lane(lane).base_url, fx.ca_file, **kw)


def causes(obs):
    return " | ".join(obs["exception"]["cause_chain"])


# --- valid terminal -----------------------------------------------------------------------------

def test_valid_terminal_stream_and_keepalive_counts(fx):
    c = client(fx)
    r1 = run_stream(c, stream_options={"include_usage": True})
    r2 = run_stream(c)
    for r in (r1, r2):
        assert r["exception"] is None
        assert r["finish_reason"] == "stop"
        assert r["content"] == "Hello from lane a."
    assert r1["usage"] == {"completion_tokens": 5, "prompt_tokens": 7, "total_tokens": 12,
                           "completion_tokens_details": None, "prompt_tokens_details": None}
    assert fx.wait_settled("a")  # ack: SDK returns at [DONE], before the fixture commits "complete"
    a = fx.counts("a")
    assert (a["requests"], a["chat_completions"]) == (2, 2)
    # The pinned sync SDK closes the response at `data: [DONE]` without reading the 0-chunk, so the
    # stream's connection is normally discarded rather than pooled: count requests, not connections.
    assert 1 <= a["tcp_accepts"] == a["tls_ok"] <= 2
    assert a["outcomes"] == {"complete": 2}
    assert fx.counts("b")["tcp_accepts"] == 0
    recs = fx.requests("a")
    assert [r["id"] for r in recs] == ["a-1", "a-2"]
    assert recs[0]["events_sent"][-1] == "data: [DONE]\n\n"
    assert recs[0]["headers"]["x-stainless-retry-count"] == "0"
    assert recs[0]["headers"]["_tls_version"] in ("TLSv1.3", "TLSv1.2")
    assert recs[0]["headers"]["authorization"].startswith("<redacted")


def test_unary_keepalive_reuses_one_connection(fx):
    c = client(fx)
    for _ in range(3):
        assert c.chat.completions.create(**CHAT_ARGS).choices[0].finish_reason == "stop"
    a = fx.counts("a")
    assert (a["tcp_accepts"], a["requests"]) == (1, 3)
    assert {r["conn_id"] for r in fx.requests("a")} == {"a-c1"}


def test_valid_terminal_tool_call(fx):
    fx.set_fault("a", "valid", tool_call=True)
    r = run_stream(client(fx))
    assert r["exception"] is None
    assert (r["tool_name"], r["tool_args"], r["finish_reason"]) == ("get_weather", TOOL_ARGS, "tool_calls")


def test_lanes_are_independent_deployments(fx):
    ra, rb = run_stream(client(fx, "a")), run_stream(client(fx, "b"))
    assert ra["content"] == "Hello from lane a." and rb["content"] == "Hello from lane b."
    assert fx.lane("a").port != fx.lane("b").port
    assert fx.counts("a")["requests"] == 1 and fx.counts("b")["requests"] == 1


# --- connect refused ------------------------------------------------------------------------------

def test_connect_refused_is_genuine_and_never_accepted(fx):
    port = fx.lane("a").port
    fx.set_fault("a", "refuse")
    assert not fx.lane("a").listening
    # Kernel-level refusal: bound but not listening -> RST to SYN -> ECONNREFUSED.
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("127.0.0.1", port), timeout=2)
    r = run_stream(client(fx))
    assert r["exception"]["type"] == "openai.APIConnectionError"
    assert "ConnectionRefusedError" in causes(r)
    assert r["chunks"] == 0 and r["request_id"] is None
    assert fx.counts("a")["tcp_accepts"] == 0 and fx.counts("a")["requests"] == 0
    # The other deployment is unaffected.
    assert run_stream(client(fx, "b"))["finish_reason"] == "stop"
    # Port is held (not released to the OS) and the lane comes back on the same address.
    fx.set_fault("a", "valid")
    assert fx.lane("a").port == port
    assert run_stream(client(fx))["finish_reason"] == "stop"
    assert fx.counts("a")["tcp_accepts"] == 1


def test_refuse_from_start():
    with WireFixture(faults={"a": "refuse"}) as f:
        r = run_stream(sdk_client(f.lane("a").base_url, f.ca_file))
        assert "ConnectionRefusedError" in causes(r)
        assert f.counts("a")["tcp_accepts"] == 0


# --- heartbeat only -------------------------------------------------------------------------------

def test_heartbeat_only_stream_finite(fx):
    fx.set_fault("a", "heartbeat", count=4, interval=0.01)
    r = run_stream(client(fx))
    # Visible to the SDK as an empty, *successful* iteration: no chunk, no finish_reason, no exception.
    assert (r["exception"], r["chunks"], r["finish_reason"]) == (None, 0, None)
    assert fx.wait_settled("a")
    rec = fx.requests("a")[0]
    assert rec["events_sent"] == [": keep-alive\n\n"] * 4
    assert rec["outcome"] == "complete"


def test_heartbeat_only_indefinite_until_client_cancel(fx):
    fx.set_fault("a", "heartbeat", count=None, interval=0.02)
    stream = client(fx).chat.completions.create(**CHAT_ARGS, stream=True)
    assert fx.wait_request("a", 1, phase="holding", timeout=5)
    # SDK iterator blocks (heartbeats are SSE comments); read it from a thread and cancel.
    got = []
    t = threading.Thread(target=lambda: got.extend(_drain(stream)), daemon=True)
    t.start()
    assert fx.wait_for(lambda f: len(f.requests("a")[0]["events_sent"]) >= 5, timeout=5)
    stream.response.close()
    assert fx.wait_for(lambda f: f.requests("a")[0]["outcome"] == "client_closed", timeout=5)
    t.join(5)
    assert not t.is_alive() and got  # SDK iterator unblocked by the cancel
    assert fx.counts("a")["client_closed"] == 1


def _drain(stream):
    n = 0
    try:
        for _ in stream:
            n += 1
        return ["done", n]
    except Exception as e:
        return ["exception", type(e).__name__]


def test_heartbeat_indefinite_release_ends_cleanly(fx):
    fx.set_fault("a", "heartbeat", count=None, interval=0.02)
    result = {}
    t = threading.Thread(target=lambda: result.update(run_stream(client(fx))), daemon=True)
    t.start()
    assert fx.wait_request("a", 1, phase="holding", timeout=5)
    assert fx.release(lane="a") == ["a-1"]
    t.join(5)
    assert result["exception"] is None and result["chunks"] == 0
    assert fx.wait_settled("a")
    assert fx.requests("a")[0]["outcome"] == "complete"


# --- reset after a tool argument fragment ------------------------------------------------------------

def test_reset_after_tool_fragment_visible_incomplete(fx):
    fx.set_fault("a", "reset_after_tool_fragment")
    r = run_stream(client(fx))
    assert r["tool_name"] == "get_weather"
    assert r["tool_args"] == TOOL_FRAGMENT and TOOL_FRAGMENT  # nonempty fragment reached the SDK user
    assert r["finish_reason"] is None
    assert r["exception"]["type"] == "openai.APIConnectionError"
    assert "ConnectionResetError" in causes(r)
    rec = fx.requests("a")[0]  # no barrier: abrupt-close outcomes are committed before the close
    assert rec["outcome"] == "reset"
    assert all("[DONE]" not in e and "finish_reason\":\"" not in e for e in rec["events_sent"])
    a, b = fx.counts("a"), fx.counts("b")
    assert (a["tcp_accepts"], a["requests"], a["outcomes"]) == (1, 1, {"reset": 1})
    assert (b["tcp_accepts"], b["requests"]) == (0, 0)


def test_reset_after_tool_fragment_is_deterministic(fx):
    """The fragment must be visible before the error on every run (no RST-vs-data race)."""
    fx.set_fault("a", "reset_after_tool_fragment")
    for _ in range(50):
        r = run_stream(client(fx))
        assert r["tool_args"] == TOOL_FRAGMENT and r["exception"] is not None and r["finish_reason"] is None
    assert fx.counts("a")["outcomes"] == {"reset": 50}


def test_fin_truncation_variant(fx):
    fx.set_fault("a", "reset_after_tool_fragment", close="fin")
    r = run_stream(client(fx))
    assert r["tool_args"] == TOOL_FRAGMENT and r["finish_reason"] is None
    assert r["exception"]["type"] == "openai.APIConnectionError"
    assert "incomplete chunked read" in causes(r)
    assert fx.requests("a")[0]["outcome"] == "fin_truncated"


def test_reset_hold_until_release(fx):
    fx.set_fault("a", "reset_after_tool_fragment", hold=True)
    result = {}
    t = threading.Thread(target=lambda: result.update(run_stream(client(fx))), daemon=True)
    t.start()
    assert fx.wait_request("a", 1, phase="holding", timeout=5)
    fx.release("a-1")
    t.join(5)
    assert result["tool_args"] == TOOL_FRAGMENT and "ConnectionResetError" in causes(result)


def _raw_request(fx, lane="a", body=None):
    ctx = ssl.create_default_context(cafile=fx.ca_file)
    raw = socket.create_connection(("127.0.0.1", fx.lane(lane).port), timeout=5)
    s = ctx.wrap_socket(raw, server_hostname="127.0.0.1", suppress_ragged_eofs=False)
    data = json.dumps(body or {**CHAT_ARGS, "stream": True}).encode()
    s.sendall(b"POST /v1/chat/completions HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\n"
              + f"content-length: {len(data)}\r\n\r\n".encode() + data)
    buf, err = b"", None
    try:
        while True:
            d = s.recv(65536)
            if not d:
                break
            buf += d
    except Exception as e:
        err = e
    s.close()
    return buf, err


def test_wire_bytes_rst_has_no_terminator_and_no_close_notify(fx):
    fx.set_fault("a", "reset_after_tool_fragment")
    buf, err = _raw_request(fx)
    head, _, body = buf.partition(b"\r\n\r\n")
    assert b"transfer-encoding: chunked" in head.lower()
    assert body.endswith(b"\r\n") and not body.endswith(b"0\r\n\r\n")
    assert b"[DONE]" not in body
    assert TOOL_FRAGMENT.replace('"', '\\"').encode() in body
    assert isinstance(err, ConnectionResetError)


def test_wire_bytes_fin_is_ragged_eof(fx):
    fx.set_fault("a", "reset_after_tool_fragment", close="fin")
    buf, err = _raw_request(fx)
    assert not buf.endswith(b"0\r\n\r\n")
    assert isinstance(err, ssl.SSLEOFError)  # TCP FIN without TLS close_notify


def test_wire_bytes_valid_is_well_framed(fx):
    buf, err = _raw_request(fx)
    assert err is None or isinstance(err, TimeoutError)  # keep-alive: server waits for next request
    assert buf.endswith(b"data: [DONE]\n\n\r\n0\r\n\r\n")


# --- slow / stalled producers (for bounded/cancel integration) ------------------------------------------

def test_stall_then_release_completes(fx):
    fx.set_fault("a", "stall", after_events=2)
    result = {}
    t = threading.Thread(target=lambda: result.update(run_stream(client(fx))), daemon=True)
    t.start()
    rec = fx.wait_request("a", 1, phase="holding", timeout=5)
    assert rec and len(rec["events_sent"]) == 2
    assert fx.release(lane="a") == ["a-1"]
    t.join(5)
    assert result["exception"] is None and result["finish_reason"] == "stop"
    assert result["content"] == "Hello from lane a."


def test_stall_client_cancel_detected_by_fixture(fx):
    fx.set_fault("a", "stall", after_events=1)
    stream = client(fx).chat.completions.create(**CHAT_ARGS, stream=True)
    next(stream)
    assert fx.wait_request("a", 1, phase="holding", timeout=5)
    t0 = time.monotonic()
    stream.close()
    assert fx.wait_for(lambda f: f.requests("a")[0]["outcome"] == "client_closed", timeout=5)
    assert time.monotonic() - t0 < 1.0
    assert fx.counts("a")["client_closed"] == 1


def test_stall_sdk_read_timeout(fx):
    fx.set_fault("a", "stall", after_events=1)
    r = run_stream(client(fx, timeout=0.3))
    assert r["exception"]["type"] == "openai.APITimeoutError" and r["chunks"] == 1
    assert fx.wait_for(lambda f: f.requests("a")[0]["outcome"] == "client_closed", timeout=5)
    assert fx.counts("a")["requests"] == 1  # max_retries=0: no second arrival


def test_stall_max_hold_safety_cap():
    with WireFixture(max_hold=0.2) as f:
        f.set_fault("a", "stall", after_events=0)
        r = run_stream(sdk_client(f.lane("a").base_url, f.ca_file, timeout=5))
        assert "ConnectionResetError" in causes(r)
        assert f.requests("a")[0]["outcome"] == "max_hold_rst"


def test_slow_producer_pacing_and_partial_cancel(fx):
    fx.set_fault("a", "slow", events=40, interval=0.05)
    stream = client(fx).chat.completions.create(**CHAT_ARGS, stream=True)
    t0 = time.monotonic()
    for _ in range(4):
        next(stream)
    assert time.monotonic() - t0 >= 0.14  # 3 inter-event intervals
    stream.close()
    assert fx.wait_for(lambda f: f.requests("a")[0]["outcome"] == "client_closed", timeout=5)
    sent = len(fx.requests("a")[0]["events_sent"])
    assert 4 <= sent < 42  # producer stopped well before the end


# --- scripting, status, unary, trust ------------------------------------------------------------------

def test_queue_is_one_shot_then_default(fx):
    fx.queue_faults("a", ["reset_after_tool_fragment"])
    c = client(fx)
    assert run_stream(c)["exception"] is not None
    assert run_stream(c)["finish_reason"] == "stop"
    assert [r["fault"]["kind"] for r in fx.requests("a")] == ["reset_after_tool_fragment", "valid"]


def test_status_error_single_arrival_no_sdk_retry(fx):
    fx.set_fault("a", "status", code=503)
    r = run_stream(client(fx))
    assert r["exception"]["type"] == "openai.InternalServerError"
    assert fx.counts("a")["requests"] == 1


def test_unary_valid_and_reset(fx):
    c = client(fx)
    resp = c.chat.completions.create(**CHAT_ARGS)
    assert resp.choices[0].message.content == "Hello from lane a." and resp.choices[0].finish_reason == "stop"
    fx.set_fault("a", "reset_after_tool_fragment")
    with pytest.raises(openai.APIConnectionError):
        c.chat.completions.create(**CHAT_ARGS)
    assert fx.wait_settled("a")  # the unary "complete" is committed after its last byte
    assert fx.counts("a")["outcomes"] == {"complete": 1, "reset": 1}


def test_untrusted_client_fails_tls_and_is_counted(fx):
    import ssl as _ssl
    ctx = _ssl.create_default_context()  # system store only: the ephemeral CA is NOT in it
    c = openai.OpenAI(api_key="x", base_url=fx.lane("a").base_url, max_retries=0, timeout=5,
                      http_client=openai.DefaultHttpx2Client(verify=ctx, trust_env=False))
    with pytest.raises(openai.APIConnectionError) as ei:
        c.chat.completions.create(**CHAT_ARGS)
    assert "CERTIFICATE_VERIFY_FAILED" in repr(ei.value.__cause__) + repr(ei.value.__cause__.__cause__)
    assert fx.wait_counter("a", "tls_failed", 1, timeout=5)
    assert fx.counts("a")["requests"] == 0


def test_unknown_route_counted(fx):
    c = client(fx)
    assert [m.id for m in c.models.list()] == ["telelem-wire-model"]
    assert fx.counts("a")["by_path"] == {"GET /v1/models": 1}


def test_reset_counts(fx):
    run_stream(client(fx))
    assert fx.wait_settled()
    fx.reset_counts()
    assert fx.counts("a")["requests"] == 0 and fx.requests("a") == []


def test_bad_fault_rejected(fx):
    with pytest.raises(ValueError):
        fx.set_fault("a", "nope")
    with pytest.raises(ValueError):
        fx.set_fault("a", "stall", bogus=1)
    with pytest.raises(ValueError):
        fx.queue_faults("a", ["refuse"])
    with pytest.raises(ValueError):
        WireFixture(host="0.0.0.0")


def test_trust_material_is_ephemeral():
    f = WireFixture().start()
    paths = [f.trust.ca_file, f.trust.cert_file, f.trust.key_file]
    assert all(p.exists() for p in paths)
    assert (f.trust.key_file.stat().st_mode & 0o777) == 0o600
    sd = f._state_dir
    f.stop()
    assert not sd.exists()


# --- observation ordering (the TEL-004 gate race) --------------------------------------------------------

def test_abrupt_close_outcome_committed_before_close(fx, monkeypatch):
    """Structural barrier: at the moment the fixture closes abruptly, the record already holds its outcome,
    so no client can observe a reset that the fixture has not recorded."""
    seen = []
    orig = WireFixture._abort

    def checked_abort(self, conn, mode):
        with self._cond:
            seen.append([(r.id, r.outcome, r.phase) for r in self._records if r.outcome is None or r.phase != "done"])
        orig(self, conn, mode)

    monkeypatch.setattr(WireFixture, "_abort", checked_abort)
    for spec in ({"kind": "reset_after_tool_fragment"}, {"kind": "reset_after_tool_fragment", "close": "fin"}):
        fx.set_fault("a", spec)
        assert run_stream(client(fx))["exception"] is not None
    fx.set_fault("a", "reset_after_tool_fragment")
    with pytest.raises(openai.APIConnectionError):
        client(fx).chat.completions.create(**CHAT_ARGS)  # unary variant
    assert seen == [[], [], []]  # nothing unfinished at any of the three closes


def test_complete_outcome_needs_ack_and_ack_waits(fx, monkeypatch):
    """Forced interleaving of the original gate failure, without sleeps: hold the fixture's commit of
    "complete" until the client has observed the end of the stream. Reading the record without the
    barrier sees no outcome; wait_settled() then returns only after the commit."""
    client_observed = threading.Event()
    commit_reached = threading.Event()
    orig = WireFixture._finish

    def gated_finish(self, lane, rec, outcome):
        if outcome == "complete":
            commit_reached.set()
            assert client_observed.wait(5), "client never observed the stream end"
        orig(self, lane, rec, outcome)

    monkeypatch.setattr(WireFixture, "_finish", gated_finish)
    r = run_stream(client(fx))
    assert r["finish_reason"] == "stop" and r["exception"] is None
    assert commit_reached.wait(5)
    before = fx.requests("a")[0]["outcome"]
    assert not fx.wait_settled("a", timeout=0)  # not yet committed
    client_observed.set()
    assert fx.wait_settled("a", timeout=5)
    assert (before, fx.requests("a")[0]["outcome"]) == (None, "complete")
    assert fx.counts("a")["outcomes"] == {"complete": 1}


def test_wait_settled_blocks_while_held(fx):
    fx.set_fault("a", "stall", after_events=0)
    result = {}
    t = threading.Thread(target=lambda: result.update(run_stream(client(fx))), daemon=True)
    t.start()
    assert fx.wait_request("a", 1, phase="holding", timeout=5)
    assert not fx.wait_settled("a", timeout=0)
    assert fx.wait_settled("b", timeout=0)  # per-lane
    fx.release("a-1")
    assert fx.wait_settled("a", timeout=5)
    t.join(5)
    assert result["finish_reason"] == "stop"
