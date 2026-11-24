"""Reference run: each TEL-004 case driven directly with the pinned SDK (max_retries=0).

Every scenario starts a fresh fixture, so counts are per scenario. Output is the evidence:
what the SDK surfaced, what the fixture sent, and per-lane arrival counts.
"""

from __future__ import annotations

import time
from typing import Any, Callable

from .client import CHAT_ARGS, describe_exception, run_stream, sdk_client
from .server import TOOL_FRAGMENT, WireFixture


def _arrivals(fx: WireFixture) -> dict[str, dict[str, int]]:
    c = fx.counts()
    return {n: {"tcp_accepts": v["tcp_accepts"], "requests": v["requests"], "outcomes": v["outcomes"]} for n, v in c.items()}


def valid_terminal(fx: WireFixture) -> dict[str, Any]:
    obs = run_stream(sdk_client(fx.lane("a").base_url, fx.ca_file))
    settled = fx.wait_settled("a")  # ack: the SDK returns at [DONE], before the fixture records "complete"
    rec = fx.requests("a")[0]
    ok = (settled and obs["exception"] is None and obs["finish_reason"] == "stop" and obs["content"] == "Hello from lane a."
          and rec["events_sent"][-1] == "data: [DONE]\n\n" and rec["outcome"] == "complete")
    return {"sdk": obs, "fixture_outcome": rec["outcome"], "last_event_sent": rec["events_sent"][-1], "passed": ok}


def connect_refused(fx: WireFixture) -> dict[str, Any]:
    fx.set_fault("a", "refuse")
    obs = run_stream(sdk_client(fx.lane("a").base_url, fx.ca_file))
    cause = " | ".join(obs["exception"]["cause_chain"]) if obs["exception"] else ""
    b = run_stream(sdk_client(fx.lane("b").base_url, fx.ca_file))  # the other deployment still serves
    c = fx.counts()
    ok = (obs["exception"] is not None and obs["exception"]["type"] == "openai.APIConnectionError"
          and "ConnectionRefusedError" in cause and c["a"]["tcp_accepts"] == 0 and c["a"]["requests"] == 0
          and b["finish_reason"] == "stop" and c["b"]["requests"] == 1)
    return {"sdk_lane_a": obs, "sdk_lane_b": {k: b[k] for k in ("finish_reason", "content", "exception")},
            "lane_a_listening": fx.lane("a").listening, "passed": ok}


def heartbeat_only(fx: WireFixture) -> dict[str, Any]:
    fx.set_fault("a", "heartbeat", count=3, interval=0.01)
    obs = run_stream(sdk_client(fx.lane("a").base_url, fx.ca_file))
    settled = fx.wait_settled("a")
    rec = fx.requests("a")[0]
    ok = (settled and obs["exception"] is None and obs["chunks"] == 0 and obs["finish_reason"] is None
          and all(e.startswith(":") for e in rec["events_sent"]) and len(rec["events_sent"]) == 3)
    return {"sdk": obs, "events_sent": rec["events_sent"], "fixture_outcome": rec["outcome"],
            "note": "clean chunked end, zero data events, no [DONE]: SDK iterator ends silently with 0 chunks",
            "passed": ok}


def reset_after_tool_fragment(fx: WireFixture) -> dict[str, Any]:
    fx.set_fault("a", "reset_after_tool_fragment")
    obs = run_stream(sdk_client(fx.lane("a").base_url, fx.ca_file))
    # No barrier on purpose: the fixture commits an abrupt-close outcome BEFORE closing, so once the
    # SDK has seen the reset the record must already say "reset".
    rec = fx.requests("a")[0]
    cause = " | ".join(obs["exception"]["cause_chain"]) if obs["exception"] else ""
    c = fx.counts()
    ok = (obs["tool_args"] == TOOL_FRAGMENT and obs["finish_reason"] is None and obs["exception"] is not None
          and obs["exception"]["type"] == "openai.APIConnectionError" and "ConnectionResetError" in cause
          and rec["outcome"] == "reset" and c["a"]["requests"] == 1 and c["b"]["requests"] == 0)
    return {"sdk": obs, "fixture_outcome": rec["outcome"], "body_bytes_sent": rec["body_bytes_sent"],
            "wire_bytes_sent": rec["wire_bytes_sent"], "passed": ok}


def stalled_then_client_cancel(fx: WireFixture) -> dict[str, Any]:
    fx.set_fault("a", "stall", after_events=2)
    client = sdk_client(fx.lane("a").base_url, fx.ca_file)
    stream = client.chat.completions.create(**CHAT_ARGS, stream=True)
    got = [next(stream), next(stream)]
    held = fx.wait_request("a", 1, phase="holding", timeout=5)
    t0 = time.monotonic()
    stream.close()  # client-side cancel
    closed = fx.wait_for(lambda f: f.requests("a")[0]["outcome"] is not None, timeout=5)
    detect_ms = round((time.monotonic() - t0) * 1000, 2)
    rec = fx.requests("a")[0]
    ok = held is not None and closed and rec["outcome"] == "client_closed" and len(got) == 2
    return {"chunks_read_before_cancel": len(got), "fixture_outcome": rec["outcome"],
            "fixture_detected_close_ms": detect_ms, "passed": ok}


def slow_producer_partial_read(fx: WireFixture) -> dict[str, Any]:
    fx.set_fault("a", "slow", events=50, interval=0.05)
    client = sdk_client(fx.lane("a").base_url, fx.ca_file)
    stream = client.chat.completions.create(**CHAT_ARGS, stream=True)
    for _ in range(3):
        next(stream)
    stream.close()
    fx.wait_for(lambda f: f.requests("a")[0]["outcome"] is not None, timeout=5)
    rec = fx.requests("a")[0]
    ok = rec["outcome"] == "client_closed" and len(rec["events_sent"]) < 52
    return {"events_sent_before_close": len(rec["events_sent"]), "fixture_outcome": rec["outcome"], "passed": ok}


SCENARIOS: dict[str, Callable[[WireFixture], dict[str, Any]]] = {
    "valid_terminal": valid_terminal,
    "connect_refused": connect_refused,
    "heartbeat_only": heartbeat_only,
    "reset_after_tool_fragment": reset_after_tool_fragment,
    "stalled_then_client_cancel": stalled_then_client_cancel,
    "slow_producer_partial_read": slow_producer_partial_read,
}


def run_reference() -> dict[str, Any]:
    results: dict[str, Any] = {}
    for name, fn in SCENARIOS.items():
        with WireFixture() as fx:
            try:
                r = fn(fx)
            except Exception as e:
                r = {"passed": False, "error": describe_exception(e)}
            r["settled"] = fx.wait_settled()  # ack before reporting outcome counts
            r["arrivals"] = _arrivals(fx)
            r["passed"] = r["passed"] and r["settled"]
        results[name] = r
    return {"scenarios": results, "all_passed": all(r["passed"] for r in results.values())}
