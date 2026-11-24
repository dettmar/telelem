"""Loopback-only JSON control plane (plain HTTP) for driving the fixture from another process,
e.g. an ExUnit test or a BEAM gateway harness. Same operations as the Python API."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .server import WireFixture


class ControlServer:
    def __init__(self, fx: WireFixture, host: str = "127.0.0.1", port: int = 0, on_shutdown: Callable[[], None] | None = None):
        if not host.startswith("127.") and host != "::1":
            raise ValueError("control plane binds loopback only")
        self.fx = fx
        self.on_shutdown = on_shutdown or (lambda: None)
        handler = _make_handler(self)
        self.httpd = ThreadingHTTPServer((host, port), handler)
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="wire-control", daemon=True)

    @property
    def url(self) -> str:
        h, p = self.httpd.server_address[:2]
        return f"http://{h}:{p}"

    def start(self) -> "ControlServer":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _make_handler(ctl: ControlServer):
    fx = ctl.fx

    class H(BaseHTTPRequestHandler):
        server_version = "telelem-wire-control"

        def log_message(self, *a: Any) -> None:  # quiet
            pass

        def _send(self, code: int, obj: Any) -> None:
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict[str, Any]:
            n = int(self.headers.get("content-length") or 0)
            if not n:
                return {}
            obj = json.loads(self.rfile.read(n))
            if not isinstance(obj, dict):
                raise ValueError("JSON object body required")
            return obj

        def do_GET(self) -> None:
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path == "/info":
                    return self._send(200, fx.info())
                if u.path == "/state":
                    return self._send(200, fx.state())
                if u.path == "/counts":
                    return self._send(200, fx.counts(q.get("lane")))
                if u.path == "/requests":
                    return self._send(200, fx.requests(q.get("lane")))
                if u.path.startswith("/requests/"):
                    return self._send(200, fx.request(u.path.split("/", 2)[2]))
                if u.path == "/env":
                    return self._send(200, fx.gateway_env(q.get("prefix", "TELELEM_WIRE")))
                return self._send(404, {"error": f"no route GET {u.path}"})
            except KeyError as e:
                return self._send(404, {"error": f"unknown {e}"})

        def do_POST(self) -> None:
            u = urlparse(self.path)
            parts = [p for p in u.path.split("/") if p]
            try:
                b = self._body()
                if len(parts) == 3 and parts[0] == "lanes" and parts[2] == "fault":
                    spec = dict(b)
                    kind = spec.pop("kind")
                    return self._send(200, fx.set_fault(parts[1], kind, **spec))
                if len(parts) == 3 and parts[0] == "lanes" and parts[2] == "queue":
                    return self._send(200, fx.queue_faults(parts[1], b.get("faults", [])))
                if parts == ["release"]:
                    return self._send(200, {"released": fx.release(b.get("request_id"), b.get("lane"))})
                if parts == ["reset"]:
                    fx.reset_counts()
                    return self._send(200, {"ok": True})
                if parts == ["wait"]:
                    timeout = float(b.get("timeout", 5.0))
                    if "counter" in b:
                        ok = fx.wait_counter(b["lane"], b["counter"], int(b.get("at_least", 1)), timeout)
                        return self._send(200 if ok else 408, {"ok": ok, "counts": fx.counts(b["lane"])})
                    rec = fx.wait_request(b["lane"], int(b.get("nth", 1)), b.get("phase"), timeout)
                    return self._send(200 if rec else 408, {"ok": rec is not None, "request": rec})
                if parts == ["shutdown"]:
                    self._send(200, {"ok": True})
                    threading.Thread(target=ctl.on_shutdown, daemon=True).start()
                    return None
                return self._send(404, {"error": f"no route POST {u.path}"})
            except KeyError as e:
                return self._send(404, {"error": f"unknown {e}"})
            except (ValueError, TypeError) as e:
                return self._send(400, {"error": str(e)})

    return H
