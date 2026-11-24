"""TEL-005 SDK agent: drives the real, pinned OpenAI Python SDK against the test gateway.

Runs on the host (the Elixir runner image has no Python). It is a small loopback-only
JSON control server; the ExUnit M1 test tells it what to do and gets back exactly what
the SDK surfaced, in order. It never talks to a provider: the API key is a fixed dummy,
`max_retries=0`, `trust_env=False` (no proxies / env CA), and the only trusted CA is the
one file the gateway wrote for this run.

    python sdk_agent.py --ready-file PATH     # prints one ready JSON line

Routes (POST JSON, reply JSON):
  /stream       {base_url, ca_file, timeout?, overrides?}  -> one full streaming call
  /slow/start   {base_url, ca_file, read_chunks, timeout?}   -> read N chunks, then STOP reading
  /slow/close   {id}                                         -> close that stream via the SDK
  GET /versions
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import metadata
from typing import Any

import openai

API_KEY = "telelem-m1-dummy-key"  # not a credential; the fixture accepts anything

CHAT_ARGS: dict[str, Any] = {
    "model": "telelem-wire-model",
    "messages": [{"role": "user", "content": "What is the weather in Stockholm?"}],
    "tools": [{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Weather for a city",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
        },
    }],
}


def versions() -> dict[str, Any]:
    out: dict[str, Any] = {"python": sys.version.split()[0], "openssl": ssl.OPENSSL_VERSION}
    for pkg in ("openai", "httpx2", "httpcore2", "h11"):
        try:
            out[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            out[pkg] = None
    return out


def client(base_url: str, ca_file: str, timeout: float) -> openai.OpenAI:
    ctx = ssl.create_default_context(cafile=ca_file)  # only this CA; default store not loaded
    http = openai.DefaultHttpx2Client(verify=ctx, trust_env=False)
    return openai.OpenAI(api_key=API_KEY, base_url=base_url, max_retries=0, timeout=timeout, http_client=http)


def describe(e: BaseException) -> dict[str, Any]:
    chain = []
    cur = e.__cause__ or e.__context__
    while cur is not None and len(chain) < 6:
        chain.append(f"{type(cur).__module__}.{type(cur).__qualname__}: {cur}")
        cur = cur.__cause__ or cur.__context__
    d: dict[str, Any] = {
        "type": f"{type(e).__module__}.{type(e).__qualname__}",
        "message": str(e),
        "mro": [f"{c.__module__}.{c.__qualname__}" for c in type(e).__mro__[:-1]],
        "cause_chain": chain,
    }
    if isinstance(e, openai.APIStatusError):
        d["status_code"] = e.status_code
    return d


def chunk_view(chunk: Any) -> dict[str, Any]:
    v: dict[str, Any] = {"content": "", "tool_name": None, "tool_args": "", "finish_reason": None}
    for ch in chunk.choices:
        d = ch.delta
        if d.content:
            v["content"] += d.content
        for tc in d.tool_calls or []:
            if tc.function and tc.function.name:
                v["tool_name"] = tc.function.name
            if tc.function and tc.function.arguments:
                v["tool_args"] += tc.function.arguments
        if ch.finish_reason:
            v["finish_reason"] = ch.finish_reason
    return v


class Run:
    """One SDK streaming call, observed in order. `stop_after` pauses iteration (slow consumer)."""

    def __init__(self, base_url: str, ca_file: str, timeout: float, overrides: dict[str, Any] | None,
                 stop_after: int | None = None):
        self.args = {**CHAT_ARGS, "stream": True, **(overrides or {})}
        self.base_url, self.ca_file, self.timeout = base_url, ca_file, timeout
        self.stop_after = stop_after
        self.log: list[dict[str, Any]] = []
        self.paused = threading.Event()
        self.resume = threading.Event()
        self.done = threading.Event()
        self.result: dict[str, Any] = {}
        self.close_info: dict[str, Any] = {}

    def ev(self, kind: str, **kw: Any) -> None:
        self.log.append({"kind": kind, "wall_ns": time.time_ns(), **kw})

    def run(self) -> None:
        c = client(self.base_url, self.ca_file, self.timeout)
        chunks, content, tool_args, finish, tool_name = 0, "", "", None, None
        exc = None
        status = headers = None
        iterator_ended = False
        try:
            self.ev("create")
            stream = c.chat.completions.create(**self.args)
            status = stream.response.status_code
            headers = {k: v for k, v in stream.response.headers.items() if k in ("content-type", "x-telelem-rid", "transfer-encoding")}
            self.ev("response", status=status, headers=headers)
            try:
                for chunk in stream:
                    chunks += 1
                    v = chunk_view(chunk)
                    content += v["content"]
                    tool_args += v["tool_args"]
                    tool_name = v["tool_name"] or tool_name
                    finish = v["finish_reason"] or finish
                    self.ev("chunk", n=chunks, **v)
                    if self.stop_after is not None and chunks >= self.stop_after:
                        self.ev("stop_reading", after=chunks)
                        self.paused.set()
                        self.resume.wait()
                        t0 = time.time_ns()
                        self.ev("sdk_close_start")
                        stream.close()  # public SDK API: closes the HTTP response / connection
                        self.close_info = {"close_start_wall_ns": t0, "close_done_wall_ns": time.time_ns()}
                        self.ev("sdk_close_done")
                        break
                else:
                    iterator_ended = True
                    self.ev("iterator_end")
            finally:
                stream.close()
        except Exception as e:  # the exception IS the observation
            exc = describe(e)
            self.ev("exception", **exc)
        finally:
            c.close()
            self.result = {
                "chunks": chunks, "content": content, "tool_name": tool_name, "tool_args": tool_args,
                "finish_reason": finish, "exception": exc, "status": status, "headers": headers,
                "iterator_ended": iterator_ended, "log": self.log, "close": self.close_info,
                "max_retries": 0, "versions": versions(),
            }
            self.paused.set()
            self.done.set()


RUNS: dict[str, Run] = {}
LOCK = threading.Lock()


class H(BaseHTTPRequestHandler):
    server_version = "telelem-m1-sdk-agent"

    def log_message(self, *a: Any) -> None:
        pass

    def _send(self, code: int, obj: Any) -> None:
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/versions":
            return self._send(200, versions())
        self._send(404, {"error": "no route"})

    def do_POST(self) -> None:
        n = int(self.headers.get("content-length") or 0)
        b = json.loads(self.rfile.read(n) or b"{}")
        try:
            if self.path == "/stream":
                r = Run(b["base_url"], b["ca_file"], float(b.get("timeout", 20)), b.get("overrides"))
                r.run()
                return self._send(200, r.result)
            if self.path == "/slow/start":
                r = Run(b["base_url"], b["ca_file"], float(b.get("timeout", 60)), b.get("overrides"),
                        stop_after=int(b["read_chunks"]))
                rid = f"slow-{time.time_ns()}"
                with LOCK:
                    RUNS[rid] = r
                threading.Thread(target=r.run, daemon=True).start()
                r.paused.wait(float(b.get("timeout", 60)))
                return self._send(200, {"id": rid, "paused": not r.done.is_set(), "log": list(r.log),
                                        "result": r.result if r.done.is_set() else None})
            if self.path == "/slow/close":
                with LOCK:
                    r = RUNS.pop(b["id"])
                r.resume.set()
                r.done.wait(30)
                return self._send(200, r.result)
            return self._send(404, {"error": "no route"})
        except Exception as e:  # agent bug: report, never hang
            return self._send(500, {"error": repr(e), "trace": traceback.format_exc()})


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--ready-file")
    a = p.parse_args()
    if not a.host.startswith("127."):
        raise SystemExit("loopback only")
    for k in list(os.environ):
        if k.startswith("OPENAI_"):
            del os.environ[k]  # never let ambient provider settings reach the SDK
    httpd = ThreadingHTTPServer((a.host, 0), H)
    httpd.daemon_threads = True
    line = json.dumps({"url": f"http://{a.host}:{httpd.server_address[1]}", "pid": os.getpid(), "versions": versions()})
    if a.ready_file:
        with open(a.ready_file + ".tmp", "w") as fh:
            fh.write(line + "\n")
        os.replace(a.ready_file + ".tmp", a.ready_file)
    print(line, flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
