"""Out-of-process contract: `telelem-wire serve` ready line + loopback control plane, as a BEAM harness would use it."""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

from telelem_wire import TOOL_FRAGMENT
from telelem_wire.client import run_stream, sdk_client


def _call(base, method, path, body=None, timeout=10):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def _serve(*args):
    p = subprocess.Popen([sys.executable, "-m", "telelem_wire.cli", "serve", "--exit-on-stdin-eof", *args],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    ready = json.loads(p.stdout.readline())
    return p, ready


def test_serve_ready_line_control_and_stdin_eof_exit(tmp_path):
    ready_file = tmp_path / "ready.json"
    p, info = _serve("--fault", "a=refuse", "--fault", 'b=stall:{"after_events":1}', "--ready-file", str(ready_file))
    try:
        assert info["schema"] == "telelem-wire/1"
        assert json.loads(ready_file.read_text()) == info
        assert set(info["lanes"]) == {"a", "b"}
        assert info["lanes"]["a"]["listening"] is False and info["lanes"]["b"]["fault"]["kind"] == "stall"
        env = info["env"]
        assert env["TELELEM_WIRE_CA_FILE"] == info["ca_file"] and os.path.exists(info["ca_file"])
        assert env["TELELEM_WIRE_LANE_B_BASE_URL"] == info["lanes"]["b"]["base_url"]
        ctl = info["control_url"]
        assert ctl.startswith("http://127.0.0.1:")
        assert info["versions"]["openai"] and info["versions"]["cryptography"]

        # Lane a refuses; lane b stalls -> blocking /wait for "holding", then release over control plane.
        r = run_stream(sdk_client(info["lanes"]["a"]["base_url"], info["ca_file"]))
        assert "ConnectionRefusedError" in " ".join(r["exception"]["cause_chain"])

        import threading
        res = {}
        t = threading.Thread(target=lambda: res.update(run_stream(sdk_client(info["lanes"]["b"]["base_url"], info["ca_file"]))))
        t.start()
        code, w = _call(ctl, "POST", "/wait", {"lane": "b", "nth": 1, "phase": "holding", "timeout": 5})
        assert code == 200 and w["request"]["id"] == "b-1"
        assert _call(ctl, "POST", "/release", {"lane": "b"}) == (200, {"released": ["b-1"]})
        t.join(5)
        assert res["finish_reason"] == "stop"

        # Switch faults at runtime; queue one-shots.
        assert _call(ctl, "POST", "/lanes/a/fault", {"kind": "valid"})[0] == 200
        assert _call(ctl, "POST", "/lanes/a/queue", {"faults": [{"kind": "reset_after_tool_fragment"}]})[0] == 200
        r = run_stream(sdk_client(info["lanes"]["a"]["base_url"], info["ca_file"]))
        assert r["tool_args"] == TOOL_FRAGMENT and r["exception"] is not None
        assert _call(ctl, "POST", "/wait", {"settled": True, "timeout": 5})[0] == 200  # ack before reading outcomes
        code, c = _call(ctl, "GET", "/counts")
        assert c["a"]["outcomes"] == {"reset": 1} and c["b"]["outcomes"] == {"complete": 1}
        assert c["a"]["tcp_accepts"] == 1  # the refused attempt was never accepted
        assert _call(ctl, "POST", "/wait", {"lane": "a", "counter": "requests", "at_least": 9, "timeout": 0.1})[0] == 408
        assert _call(ctl, "POST", "/lanes/a/fault", {"kind": "bogus"})[0] == 400
        assert _call(ctl, "GET", "/requests/a-1")[1]["outcome"] == "reset"
    finally:
        p.stdin.close()  # Erlang Port closing == stdin EOF
        assert p.wait(timeout=10) == 0
    assert not os.path.exists(info["ca_file"]) and not ready_file.exists()


def test_serve_shutdown_endpoint():
    p, info = _serve()
    assert _call(info["control_url"], "POST", "/shutdown", {}) == (200, {"ok": True})
    assert p.wait(timeout=10) == 0


def test_probe_cli_against_server():
    p, info = _serve("--fault", "a=reset_after_tool_fragment")
    try:
        out = subprocess.run([sys.executable, "-m", "telelem_wire.cli", "probe", "--control", info["control_url"],
                              "--lane", "a"], capture_output=True, text=True, timeout=30, check=True)
        r = json.loads(out.stdout)
        assert r["tool_args"] == TOOL_FRAGMENT and r["exception"]["type"] == "openai.APIConnectionError"
    finally:
        p.stdin.close()
        p.wait(timeout=10)


def test_scenarios_reference_run():
    out = subprocess.run([sys.executable, "-m", "telelem_wire.cli", "scenarios"], capture_output=True, text=True, timeout=120)
    report = json.loads(out.stdout)
    failed = {k: v for k, v in report["scenarios"].items() if not v["passed"]}
    assert out.returncode == 0 and report["all_passed"] and not failed, json.dumps(failed, indent=1) + out.stderr[-2000:]
