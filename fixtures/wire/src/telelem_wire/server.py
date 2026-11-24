"""Two-lane loopback TLS HTTP/1.1 wire fixture with counted arrivals and scripted SSE faults.

Each lane is an independent "deployment": its own TCP port, listener state, fault
script and counters. Everything is hand-written over raw sockets so the fixture
controls every byte (chunk framing, missing terminators, TCP RST) and counts
arrivals itself, independent of whatever client or gateway sits in front of it.

Threading model: one acceptor thread per listening lane, one thread per accepted
connection. No asyncio, no third-party HTTP stack.
"""

from __future__ import annotations

import copy
import json
import os
import select
import shutil
import socket
import ssl
import struct
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from . import certs

SCHEMA = "telelem-wire/1"
DEFAULT_LANES = ("a", "b")
CREATED = 1_700_000_000  # fixed chunk "created" for byte-stable transcripts
TOOL_NAME = "get_weather"
TOOL_ARGS = '{"city": "Stockholm"}'
TOOL_FRAGMENT = '{"city": "Sto'
HEARTBEAT = b": keep-alive\n\n"

# Fault kinds. "refuse" is lane-level (listener state); the others are per request.
FAULT_KINDS = ("valid", "heartbeat", "reset_after_tool_fragment", "stall", "slow", "status", "refuse")

_DEFAULT_PARAMS: dict[str, dict[str, Any]] = {
    # Complete stream ending in finish_reason + `data: [DONE]` + chunk terminator.
    "valid": {"text": ["Hello", " from", " lane", " {lane}", "."], "tool_call": False, "interval": 0.0},
    # Only SSE comment heartbeats, never a data event. count=None -> until client closes / release / max_hold.
    "heartbeat": {"count": 3, "interval": 0.05, "end": "clean"},
    # role chunk, tool-call header (empty args), one NONEMPTY argument fragment, then abrupt close
    # mid chunked body: no finish_reason, no [DONE], no 0-chunk, no TLS close_notify.
    # close: "rst" (SO_LINGER 0 -> TCP RST) | "fin" (plain FIN). hold=True waits for release() first.
    "reset_after_tool_fragment": {"fragment": TOOL_FRAGMENT, "close": "rst", "hold": False},
    # Headers + `after_events` data events, then hold until release()/client close/max_hold.
    # on_release: "complete" (finish the valid stream) | "rst" | "fin".
    "stall": {"after_events": 1, "on_release": "complete"},
    # Valid stream, `events` content deltas, `interval` seconds between each (cancel detected in-between).
    "slow": {"events": 20, "interval": 0.2},
    # Non-2xx JSON error with Content-Length (e.g. 429/500/503), connection kept open.
    "status": {"code": 503, "message": "telelem-wire scripted error"},
    "refuse": {},
}

_REASONS = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 405: "Method Not Allowed",
            408: "Request Timeout", 411: "Length Required", 413: "Payload Too Large", 429: "Too Many Requests",
            500: "Internal Server Error", 502: "Bad Gateway", 503: "Service Unavailable", 504: "Gateway Timeout"}


def fault_spec(kind: str, **params: Any) -> dict[str, Any]:
    """Normalise a fault selection into ``{"kind": ..., **params}`` with defaults filled in."""
    if kind not in FAULT_KINDS:
        raise ValueError(f"unknown fault kind {kind!r}; expected one of {FAULT_KINDS}")
    defaults = _DEFAULT_PARAMS[kind]
    unknown = set(params) - set(defaults)
    if unknown:
        raise ValueError(f"unknown params for {kind!r}: {sorted(unknown)}; allowed {sorted(defaults)}")
    spec = {"kind": kind, **copy.deepcopy(defaults)}
    spec.update(params)
    return spec


def _coerce_spec(spec: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(spec, str):
        return fault_spec(spec)
    spec = dict(spec)
    return fault_spec(spec.pop("kind"), **spec)


class _ClientGone(Exception):
    """Peer closed or reset the connection while we were writing/holding."""


class _Shutdown(Exception):
    pass


@dataclass
class RequestRecord:
    id: str
    lane: str
    seq: int
    conn_id: str
    method: str
    path: str
    headers: dict[str, str]
    body: Any
    fault: dict[str, Any] | None = None
    status: int | None = None
    phase: str = "received"  # received -> streaming -> holding -> streaming -> done
    outcome: str | None = None  # complete | reset | fin_truncated | client_closed | max_hold_rst | shutdown | error
    events_sent: list[str] = field(default_factory=list)  # each SSE event / body piece as sent (utf-8)
    body_bytes_sent: int = 0  # HTTP body payload bytes (excl. chunk framing)
    wire_bytes_sent: int = 0  # bytes written to TLS after headers (incl. chunk framing)
    released: bool = False
    release_tokens: int = 0  # release() calls not yet consumed by a hold
    client_closed: bool = False
    started_ns: int = field(default_factory=time.monotonic_ns)
    ended_ns: int | None = None

    def as_dict(self) -> dict[str, Any]:
        d = {k: copy.deepcopy(v) for k, v in self.__dict__.items()}
        d["duration_ms"] = None if self.ended_ns is None else round((self.ended_ns - self.started_ns) / 1e6, 3)
        return d


@dataclass
class _Counters:
    tcp_accepts: int = 0
    tls_ok: int = 0
    tls_failed: int = 0
    requests: int = 0
    chat_completions: int = 0
    completed: int = 0
    client_closed: int = 0
    by_path: dict[str, int] = field(default_factory=dict)
    outcomes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.__dict__)


class _Conn:
    """Buffered reader/writer over an accepted TLS socket."""

    def __init__(self, sock: ssl.SSLSocket, raw_fd: int):
        self.sock = sock
        self.fd = raw_fd
        self.buf = b""

    def _recv(self) -> bytes:
        data = self.sock.recv(65536)
        if not data:
            raise _ClientGone("eof")
        return data

    def read_until(self, marker: bytes, limit: int) -> bytes:
        while marker not in self.buf:
            if len(self.buf) > limit:
                raise ValueError("request head too large")
            self.buf += self._recv()
        idx = self.buf.index(marker) + len(marker)
        out, self.buf = self.buf[:idx], self.buf[idx:]
        return out

    def read_exact(self, n: int) -> bytes:
        while len(self.buf) < n:
            self.buf += self._recv()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send(self, data: bytes) -> None:
        try:
            self.sock.sendall(data)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError, OSError) as e:
            raise _ClientGone(repr(e)) from e


class Lane:
    def __init__(self, fixture: "WireFixture", name: str, host: str, fault: dict[str, Any]):
        self.fx = fixture
        self.name = name
        self.host = host
        self.port: int = 0
        self.default_fault = fault
        self.queue: list[dict[str, Any]] = []
        self.counters = _Counters()
        self.seq = 0
        self.conn_seq = 0
        self.listening = False
        self._lsock: socket.socket | None = None  # listening socket, or bound-but-not-listening (refuse)
        self._acceptor: threading.Thread | None = None
        self._stop_r: int | None = None
        self._stop_w: int | None = None

    # -- listener state -------------------------------------------------------------------------
    def _bind(self) -> socket.socket:
        fam = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        s = socket.socket(fam, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        self.port = s.getsockname()[1]
        return s

    def apply_listener(self, listen: bool) -> None:
        """listen=False keeps the port BOUND but not listening: the kernel answers SYN with RST
        (ECONNREFUSED) and nothing is ever accepted, while no other process can take the port."""
        if listen == self.listening and self._lsock is not None:
            return
        self._close_listener()
        s = self._bind()
        if listen:
            s.listen(128)
            self._lsock = s
            self.listening = True
            self._stop_r, self._stop_w = os.pipe()
            self._acceptor = threading.Thread(target=self._accept_loop, args=(s, self._stop_r),
                                              name=f"wire-{self.name}-accept", daemon=True)
            self._acceptor.start()
        else:
            self._lsock = s
            self.listening = False

    def _close_listener(self) -> None:
        if self._acceptor is not None:
            os.write(self._stop_w, b"x")
            self._acceptor.join(timeout=5)
            self._acceptor = None
            for fd in (self._stop_r, self._stop_w):
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._stop_r = self._stop_w = None
        if self._lsock is not None:
            self._lsock.close()
            self._lsock = None
        self.listening = False

    def _accept_loop(self, lsock: socket.socket, stop_r: int) -> None:
        while True:
            r, _, _ = select.select([lsock, stop_r], [], [])
            if stop_r in r:
                return
            try:
                csock, addr = lsock.accept()
            except OSError:
                return
            with self.fx._cond:
                self.counters.tcp_accepts += 1
                self.conn_seq += 1
                conn_id = f"{self.name}-c{self.conn_seq}"
                self.fx._cond.notify_all()
            t = threading.Thread(target=self.fx._serve_conn, args=(self, csock, conn_id),
                                 name=f"wire-{conn_id}", daemon=True)
            t.start()

    @property
    def base_url(self) -> str:
        h = f"[{self.host}]" if ":" in self.host else self.host
        return f"https://{h}:{self.port}/v1"

    def info(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "base_url": self.base_url,
            # Same listener by DNS name (cert SAN has localhost); resolve to 127.0.0.1, not ::1.
            "base_url_localhost": self.base_url.replace(f"//{self.host}:", "//localhost:", 1) if ":" not in self.host else None,
            "chat_completions_url": self.base_url + "/chat/completions",
            "listening": self.listening,
            "fault": copy.deepcopy(self.default_fault),
            "queued": copy.deepcopy(self.queue),
        }


class WireFixture:
    """In-process fixture. Use as a context manager or call start()/stop().

    >>> with WireFixture(faults={"a": "refuse"}) as fx:
    ...     fx.lane("b").base_url, fx.ca_file
    """

    def __init__(
        self,
        lanes: Iterable[str] = DEFAULT_LANES,
        faults: dict[str, str | dict[str, Any]] | None = None,
        host: str = "127.0.0.1",
        state_dir: str | os.PathLike[str] | None = None,
        key_type: str = "ec",
        max_hold: float = 60.0,
        request_timeout: float = 30.0,
    ):
        names = list(lanes)
        if len(set(names)) != len(names) or not names:
            raise ValueError("lane names must be unique and non-empty")
        faults = faults or {}
        unknown = set(faults) - set(names)
        if unknown:
            raise ValueError(f"faults for unknown lanes: {sorted(unknown)}")
        if not (host.startswith("127.") or host in ("::1", "localhost")):
            raise ValueError("wire fixture binds loopback only")
        self.host = "127.0.0.1" if host == "localhost" else host
        self.key_type = key_type
        self.max_hold = max_hold
        self.request_timeout = request_timeout
        self._cond = threading.Condition()
        self._records: list[RequestRecord] = []
        self._by_id: dict[str, RequestRecord] = {}
        self._holds: dict[str, int] = {}  # request id -> wake pipe write fd
        self._conns: set[socket.socket] = set()
        self._cur_conn: dict[str, _Conn] = {}
        self._shutdown = False
        self._own_state_dir = state_dir is None
        self._state_dir = Path(state_dir) if state_dir else None
        self.trust: certs.TrustFiles | None = None
        self._ssl: ssl.SSLContext | None = None
        self.lanes: dict[str, Lane] = {
            n: Lane(self, n, self.host, _coerce_spec(faults.get(n, "valid"))) for n in names
        }
        self.started = False

    # -- lifecycle ----------------------------------------------------------------------------------
    def start(self) -> "WireFixture":
        if self.started:
            return self
        if self._state_dir is None:
            self._state_dir = Path(tempfile.mkdtemp(prefix="telelem-wire-"))
        os.chmod(self._state_dir, 0o700)
        self.trust = certs.generate(self._state_dir, self.key_type)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(self.trust.cert_file, self.trust.key_file)
        ctx.set_alpn_protocols(["http/1.1"])
        self._ssl = ctx
        for lane in self.lanes.values():
            lane.apply_listener(lane.default_fault["kind"] != "refuse")
        self.started = True
        return self

    def stop(self) -> None:
        if not self.started:
            return
        with self._cond:
            self._shutdown = True
            for wfd in list(self._holds.values()):
                try:
                    os.write(wfd, b"s")
                except OSError:
                    pass
            conns = list(self._conns)
            self._cond.notify_all()
        for lane in self.lanes.values():
            lane._close_listener()
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self.trust is not None:
            if self._own_state_dir:
                shutil.rmtree(self._state_dir, ignore_errors=True)
            else:
                for p in self.trust.paths():
                    p.unlink(missing_ok=True)
        self.started = False

    def __enter__(self) -> "WireFixture":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    # -- addresses / trust ----------------------------------------------------------------------------
    @property
    def ca_file(self) -> str:
        assert self.trust is not None, "fixture not started"
        return str(self.trust.ca_file)

    def lane(self, name: str) -> Lane:
        return self.lanes[name]

    def info(self) -> dict[str, Any]:
        with self._cond:
            return {
                "schema": SCHEMA,
                "pid": os.getpid(),
                "ca_file": self.ca_file,
                "ca_sha256": self.trust.ca_fingerprint_sha256 if self.trust else None,
                "state_dir": str(self._state_dir),
                "lanes": {n: l.info() for n, l in self.lanes.items()},
                "fault_kinds": list(FAULT_KINDS),
            }

    def gateway_env(self, prefix: str = "TELELEM_WIRE") -> dict[str, str]:
        """Environment for a gateway process under test (BEAM or otherwise)."""
        env = {f"{prefix}_CA_FILE": self.ca_file, f"{prefix}_LANES": ",".join(self.lanes)}
        for n, l in self.lanes.items():
            up = n.upper()
            env[f"{prefix}_LANE_{up}_BASE_URL"] = l.base_url
            env[f"{prefix}_LANE_{up}_HOST"] = l.host
            env[f"{prefix}_LANE_{up}_PORT"] = str(l.port)
        return env

    # -- fault selection ---------------------------------------------------------------------------------
    def set_fault(self, lane: str, kind: str | dict[str, Any], **params: Any) -> dict[str, Any]:
        """Set the lane's default fault for all subsequent requests (``refuse`` flips the listener)."""
        spec = _coerce_spec(kind) if isinstance(kind, dict) else fault_spec(kind, **params)
        ln = self.lanes[lane]
        with self._cond:
            ln.default_fault = spec
        if self.started:
            ln.apply_listener(spec["kind"] != "refuse")
        return spec

    def queue_faults(self, lane: str, specs: list[str | dict[str, Any]]) -> list[dict[str, Any]]:
        """One-shot per-request faults consumed FIFO before falling back to the lane default."""
        norm = [_coerce_spec(s) for s in specs]
        if any(s["kind"] == "refuse" for s in norm):
            raise ValueError("'refuse' is lane-level; use set_fault(lane, 'refuse')")
        with self._cond:
            self.lanes[lane].queue.extend(norm)
        return norm

    def release(self, request_id: str | None = None, lane: str | None = None) -> list[str]:
        """Release requests currently in phase "holding" (stall / heartbeat count=None / reset hold=True).

        A release issued after observing phase == "holding" is never lost, even if the server
        thread has not yet entered its wait. Returns the released request ids."""
        out = []
        with self._cond:
            for rec in self._records:
                if rec.outcome is not None or rec.phase != "holding":
                    continue
                if (request_id is None or rec.id == request_id) and (lane is None or rec.lane == lane):
                    rec.released = True
                    rec.release_tokens += 1
                    wfd = self._holds.get(rec.id)
                    if wfd is not None:
                        os.write(wfd, b"r")
                    out.append(rec.id)
        return out

    # -- observation ---------------------------------------------------------------------------------
    def counts(self, lane: str | None = None) -> dict[str, Any]:
        with self._cond:
            if lane is not None:
                return self.lanes[lane].counters.as_dict()
            return {n: l.counters.as_dict() for n, l in self.lanes.items()}

    def requests(self, lane: str | None = None) -> list[dict[str, Any]]:
        with self._cond:
            return [r.as_dict() for r in self._records if lane is None or r.lane == lane]

    def request(self, request_id: str) -> dict[str, Any]:
        with self._cond:
            return self._by_id[request_id].as_dict()

    def state(self) -> dict[str, Any]:
        d = self.info()
        d["counts"] = self.counts()
        d["requests"] = self.requests()
        return d

    def reset_counts(self) -> None:
        """Zero counters and drop request records (lane faults and ports are kept)."""
        with self._cond:
            for l in self.lanes.values():
                l.counters = _Counters()
            self._records = [r for r in self._records if r.outcome is None]
            self._by_id = {r.id: r for r in self._records}

    def wait_for(self, predicate: Callable[["WireFixture"], bool], timeout: float = 5.0) -> bool:
        """Block (condition variable, no polling) until predicate(fixture) is true. Returns False on timeout."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if predicate(self):
                    return True
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._cond.wait(left)

    def wait_counter(self, lane: str, counter: str, at_least: int, timeout: float = 5.0) -> bool:
        return self.wait_for(lambda fx: getattr(fx.lanes[lane].counters, counter) >= at_least, timeout)

    def wait_request(self, lane: str, nth: int = 1, phase: str | None = None, timeout: float = 5.0) -> dict[str, Any] | None:
        """Wait until the lane's nth request (1-based) exists and, if given, reaches ``phase``
        ("streaming", "holding", "done"). Returns its record dict, or None on timeout."""
        rid = f"{lane}-{nth}"

        def ok(fx: WireFixture) -> bool:
            r = fx._by_id.get(rid)
            return r is not None and (phase is None or r.phase == phase)

        if not self.wait_for(ok, timeout):
            return None
        return self.request(rid)

    # -- connection handling -----------------------------------------------------------------------------------
    def _note(self, rec: RequestRecord, **changes: Any) -> None:
        with self._cond:
            for k, v in changes.items():
                setattr(rec, k, v)
            self._cond.notify_all()

    def _finish(self, lane: Lane, rec: RequestRecord, outcome: str) -> None:
        with self._cond:
            if rec.outcome is not None:
                return
            rec.outcome = outcome
            rec.phase = "done"
            rec.ended_ns = time.monotonic_ns()
            c = lane.counters
            if rec.id in self._by_id:  # not dropped by reset_counts
                c.completed += 1
                c.outcomes[outcome] = c.outcomes.get(outcome, 0) + 1
                if outcome == "client_closed":
                    c.client_closed += 1
            self._cond.notify_all()

    def _serve_conn(self, lane: Lane, raw: socket.socket, conn_id: str) -> None:
        with self._cond:
            if self._shutdown:
                raw.close()
                return
            self._conns.add(raw)
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        raw_fd = raw.fileno()
        tls: ssl.SSLSocket | None = None
        try:
            raw.settimeout(10)
            try:
                tls = self._ssl.wrap_socket(raw, server_side=True)  # handshake happens here (detaches raw)
            except (ssl.SSLError, OSError):
                with self._cond:
                    lane.counters.tls_failed += 1
                    self._cond.notify_all()
                return
            with self._cond:
                lane.counters.tls_ok += 1
                self._conns.discard(raw)
                self._conns.add(tls)
                self._cond.notify_all()
            conn = _Conn(tls, raw_fd)
            tls_info = {"tls_version": tls.version(), "alpn": tls.selected_alpn_protocol()}
            while not self._shutdown:
                tls.settimeout(self.request_timeout)
                try:
                    head = conn.read_until(b"\r\n\r\n", 64 * 1024)
                except (_ClientGone, TimeoutError, ssl.SSLError, OSError, ValueError):
                    return
                tls.settimeout(None)
                try:
                    keep = self._handle_request(lane, conn, conn_id, head, tls_info)
                except (_ClientGone, ValueError, OSError):
                    return
                except Exception as e:  # pragma: no cover - fixture bug; surface it, never hang
                    import sys, traceback
                    print(f"telelem-wire: handler error on {conn_id}: {e!r}", file=sys.stderr)
                    traceback.print_exc()
                    return
                if not keep:
                    return
        finally:
            with self._cond:
                self._conns.discard(raw)
                if tls is not None:
                    self._conns.discard(tls)
            try:
                (tls or raw).close()
            except OSError:
                pass

    def _parse_head(self, head: bytes) -> tuple[str, str, str, dict[str, str]]:
        lines = head.decode("latin-1").split("\r\n")
        method, target, version = lines[0].split(" ", 2)
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line:
                continue
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
        return method, target, version, headers

    def _read_body(self, conn: _Conn, headers: dict[str, str]) -> bytes:
        if "chunked" in headers.get("transfer-encoding", "").lower():
            body = b""
            while True:
                size_line = conn.read_until(b"\r\n", 1024)
                size = int(size_line.split(b";")[0].strip(), 16)
                if size == 0:
                    conn.read_until(b"\r\n", 64 * 1024)  # (no trailers expected)
                    return body
                body += conn.read_exact(size)
                conn.read_exact(2)
        n = int(headers.get("content-length", "0") or 0)
        if n > 8 * 1024 * 1024:
            raise ValueError("body too large")
        return conn.read_exact(n)

    def _handle_request(self, lane: Lane, conn: _Conn, conn_id: str, head: bytes, tls_info: dict[str, Any]) -> bool:
        method, target, _version, headers = self._parse_head(head)
        path = target.split("?", 1)[0]
        body_raw = self._read_body(conn, headers)
        try:
            body: Any = json.loads(body_raw) if body_raw else None
        except ValueError:
            body = {"_unparsed": body_raw[:2048].decode("utf-8", "replace")}
        shown = dict(headers)
        if "authorization" in shown:
            shown["authorization"] = f"<redacted {len(shown['authorization'])} chars>"
        shown.update({f"_{k}": str(v) for k, v in tls_info.items()})
        with self._cond:
            lane.seq += 1
            rec = RequestRecord(id=f"{lane.name}-{lane.seq}", lane=lane.name, seq=lane.seq, conn_id=conn_id,
                                method=method, path=path, headers=shown, body=body)
            self._records.append(rec)
            self._by_id[rec.id] = rec
            c = lane.counters
            c.requests += 1
            c.by_path[f"{method} {path}"] = c.by_path.get(f"{method} {path}", 0) + 1
            is_chat = method == "POST" and path == "/v1/chat/completions"
            if is_chat:
                c.chat_completions += 1
                rec.fault = lane.queue.pop(0) if lane.queue else copy.deepcopy(lane.default_fault)
            self._cond.notify_all()

        try:
            if is_chat and isinstance(body, dict) and "_unparsed" not in body:
                return self._chat(lane, conn, rec, body)
            if method == "GET" and path == "/v1/models":
                return self._json(lane, conn, rec, 200, {"object": "list", "data": [
                    {"id": "telelem-wire-model", "object": "model", "created": CREATED, "owned_by": "telelem-wire"}]})
            if is_chat:
                return self._json(lane, conn, rec, 400, _err("request body must be a JSON object", "invalid_request_error"))
            return self._json(lane, conn, rec, 404, _err(f"no route {method} {path}", "not_found"))
        except _ClientGone:
            self._finish(lane, rec, "client_closed")
            self._note(rec, client_closed=True)
            return False
        except _Shutdown:
            self._finish(lane, rec, "shutdown")
            return False
        except Exception:
            self._finish(lane, rec, "error")
            raise

    # -- responses ------------------------------------------------------------------------------------
    def _resp_head(self, rec: RequestRecord, status: int, extra: list[tuple[str, str]]) -> bytes:
        hs = [f"HTTP/1.1 {status} {_REASONS.get(status, 'Status')}",
              f"x-request-id: {rec.id}", f"x-telelem-lane: {rec.lane}", "server: telelem-wire"] + \
             [f"{k}: {v}" for k, v in extra]
        return ("\r\n".join(hs) + "\r\n\r\n").encode("latin-1")

    def _json(self, lane: Lane, conn: _Conn, rec: RequestRecord, status: int, obj: Any) -> bool:
        data = json.dumps(obj, separators=(",", ":")).encode()
        self._note(rec, status=status, phase="streaming")
        conn.send(self._resp_head(rec, status, [("content-type", "application/json"),
                                               ("content-length", str(len(data)))]) + data)
        with self._cond:
            rec.events_sent.append(data.decode())
            rec.body_bytes_sent += len(data)
            rec.wire_bytes_sent += len(data)
        self._finish(lane, rec, "complete")
        return True

    def _chunk(self, conn: _Conn, rec: RequestRecord, payload: bytes) -> None:
        frame = f"{len(payload):x}\r\n".encode() + payload + b"\r\n"
        conn.send(frame)
        with self._cond:
            rec.events_sent.append(payload.decode())
            rec.body_bytes_sent += len(payload)
            rec.wire_bytes_sent += len(frame)
            self._cond.notify_all()

    def _end_chunked(self, conn: _Conn, rec: RequestRecord) -> None:
        conn.send(b"0\r\n\r\n")
        with self._cond:
            rec.wire_bytes_sent += 5

    def _abort(self, conn: _Conn, mode: str) -> None:
        """Abrupt close mid-body: never sends the 0-chunk or a TLS close_notify."""
        raw = socket.socket(fileno=os.dup(conn.fd))
        try:
            if mode == "rst":
                raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                raw.close()  # dup'd fd: real close happens with the SSLSocket close below
            else:
                raw.shutdown(socket.SHUT_RDWR)
                raw.close()
        finally:
            # Close the SSL object WITHOUT unwrap() -> no close_notify alert.
            conn.sock.close()

    def _hold(self, rec: RequestRecord, timeout: float | None) -> str:
        """Wait for release / client close / timeout without polling. -> 'released'|'client_closed'|'timeout'."""
        r_fd, w_fd = os.pipe()
        with self._cond:
            if self._shutdown:
                os.close(r_fd)
                os.close(w_fd)
                raise _Shutdown()
            if rec.release_tokens > 0:
                rec.release_tokens -= 1
                os.close(r_fd)
                os.close(w_fd)
                return "released"
            self._holds[rec.id] = w_fd
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                left = None if deadline is None else max(0.0, deadline - time.monotonic())
                r, _, _ = select.select([r_fd, self._conn_fd(rec)], [], [], left)
                if r_fd in r:
                    with self._cond:
                        if self._shutdown:
                            raise _Shutdown()
                        rec.release_tokens = max(0, rec.release_tokens - 1)
                    return "released"
                if r:  # client socket readable while we're responding => EOF/RST/close_notify
                    if self._peer_gone(rec):
                        return "client_closed"
                    continue
                return "timeout"
        finally:
            with self._cond:
                self._holds.pop(rec.id, None)
            os.close(r_fd)
            os.close(w_fd)

    # per-request connection lookup for _hold; set in _chat
    def _conn_fd(self, rec: RequestRecord) -> int:
        with self._cond:
            return self._cur_conn[rec.id].fd

    def _peer_gone(self, rec: RequestRecord) -> bool:
        with self._cond:
            conn = self._cur_conn[rec.id]
        if conn.sock.pending():
            return False
        try:
            conn.sock.setblocking(False)
            data = conn.sock.recv(65536)
        except (ssl.SSLWantReadError, BlockingIOError):
            return False
        except (ssl.SSLError, OSError):
            return True
        finally:
            try:
                conn.sock.setblocking(True)
            except OSError:
                pass
        if not data:
            return True
        conn.buf += data  # pipelined bytes; keep them
        return False

    def _chunk_obj(self, rec: RequestRecord, model: str, delta: dict[str, Any], finish: str | None = None) -> bytes:
        obj = {"id": f"chatcmpl-{rec.id}", "object": "chat.completion.chunk", "created": CREATED, "model": model,
               "system_fingerprint": "telelem-wire",
               "choices": [{"index": 0, "delta": delta, "logprobs": None, "finish_reason": finish}]}
        return b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"

    def _script(self, rec: RequestRecord, body: dict[str, Any], spec: dict[str, Any]) -> list[bytes]:
        """Data events of a complete valid stream (without [DONE])."""
        model = str(body.get("model", "telelem-wire-model"))
        evs = [self._chunk_obj(rec, model, {"role": "assistant", "content": ""})]
        if spec.get("tool_call"):
            evs.append(self._chunk_obj(rec, model, {"tool_calls": [{"index": 0, "id": f"call_{rec.id}", "type": "function",
                                                                     "function": {"name": TOOL_NAME, "arguments": ""}}]}))
            for piece in (TOOL_FRAGMENT, TOOL_ARGS[len(TOOL_FRAGMENT):]):
                evs.append(self._chunk_obj(rec, model, {"tool_calls": [{"index": 0, "function": {"arguments": piece}}]}))
            finish = "tool_calls"
        else:
            pieces = spec.get("text") or [f" chunk{i}" for i in range(spec.get("events", 5))]
            for p in pieces:
                evs.append(self._chunk_obj(rec, model, {"content": p.replace("{lane}", rec.lane)}))
            finish = "stop"
        evs.append(self._chunk_obj(rec, model, {}, finish))
        so = body.get("stream_options") or {}
        if isinstance(so, dict) and so.get("include_usage"):
            n_out = len(evs) - 2
            usage = {"id": f"chatcmpl-{rec.id}", "object": "chat.completion.chunk", "created": CREATED, "model": model,
                     "system_fingerprint": "telelem-wire", "choices": [],
                     "usage": {"prompt_tokens": 7, "completion_tokens": n_out, "total_tokens": 7 + n_out}}
            evs.append(b"data: " + json.dumps(usage, separators=(",", ":")).encode() + b"\n\n")
        return evs

    def _chat(self, lane: Lane, conn: _Conn, rec: RequestRecord, body: dict[str, Any]) -> bool:
        spec = rec.fault or fault_spec("valid")
        kind = spec["kind"]
        with self._cond:
            self._cur_conn[rec.id] = conn
        try:
            if kind == "status":
                return self._json(lane, conn, rec, int(spec["code"]), _err(spec["message"], "server_error"))
            if not body.get("stream"):
                return self._unary(lane, conn, rec, body, spec)
            return self._stream(lane, conn, rec, body, spec)
        finally:
            with self._cond:
                self._cur_conn.pop(rec.id, None)

    def _sse_head(self, conn: _Conn, rec: RequestRecord) -> None:
        self._note(rec, status=200, phase="streaming")
        conn.send(self._resp_head(rec, 200, [("content-type", "text/event-stream; charset=utf-8"),
                                             ("cache-control", "no-cache"), ("transfer-encoding", "chunked")]))

    def _pause(self, rec: RequestRecord, seconds: float) -> None:
        if seconds and seconds > 0:
            res = self._hold(rec, seconds)
            if res == "client_closed":
                raise _ClientGone("detected during pause")

    def _stream(self, lane: Lane, conn: _Conn, rec: RequestRecord, body: dict[str, Any], spec: dict[str, Any]) -> bool:
        kind = spec["kind"]
        model = str(body.get("model", "telelem-wire-model"))
        self._sse_head(conn, rec)

        if kind in ("valid", "slow"):
            interval = spec["interval"]
            evs = self._script(rec, body, spec if kind == "valid" else {"text": None, "events": spec["events"]})
            for i, ev in enumerate(evs):
                if i:
                    self._pause(rec, interval)
                self._chunk(conn, rec, ev)
            self._chunk(conn, rec, b"data: [DONE]\n\n")
            self._end_chunked(conn, rec)
            self._finish(lane, rec, "complete")
            return True

        if kind == "heartbeat":
            n, interval = spec["count"], spec["interval"]
            sent = 0
            held_since = time.monotonic()
            while n is None or sent < n:
                self._chunk(conn, rec, HEARTBEAT)
                sent += 1
                if n is None or sent < n:
                    self._note(rec, phase="holding" if n is None else "streaming")
                    res = self._hold(rec, interval)
                    if res == "client_closed":
                        raise _ClientGone("heartbeat")
                    if res == "released":
                        break
                    if n is None and time.monotonic() - held_since > self.max_hold:
                        self._abort(conn, "rst")
                        self._finish(lane, rec, "max_hold_rst")
                        return False
            if spec["end"] == "hold":
                return self._hold_then(lane, conn, rec, "rst")
            self._end_chunked(conn, rec)  # clean end: zero data events, no [DONE]
            self._finish(lane, rec, "complete")
            return True

        if kind == "reset_after_tool_fragment":
            if not spec["fragment"]:
                raise ValueError("fragment must be nonempty")
            self._chunk(conn, rec, self._chunk_obj(rec, model, {"role": "assistant", "content": None}))
            self._chunk(conn, rec, self._chunk_obj(rec, model, {"tool_calls": [{"index": 0, "id": f"call_{rec.id}",
                        "type": "function", "function": {"name": TOOL_NAME, "arguments": ""}}]}))
            self._chunk(conn, rec, self._chunk_obj(rec, model, {"tool_calls": [{"index": 0,
                        "function": {"arguments": spec["fragment"]}}]}))
            if spec["hold"]:
                return self._hold_then(lane, conn, rec, spec["close"])
            self._abort(conn, spec["close"])
            self._finish(lane, rec, "reset" if spec["close"] == "rst" else "fin_truncated")
            return False

        if kind == "stall":
            evs = self._script(rec, body, fault_spec("valid"))
            k = max(0, min(int(spec["after_events"]), len(evs)))
            for ev in evs[:k]:
                self._chunk(conn, rec, ev)
            on_release = spec["on_release"]
            if on_release == "complete":
                self._note(rec, phase="holding")
                res = self._hold(rec, self.max_hold)
                if res == "client_closed":
                    raise _ClientGone("stall")
                if res == "timeout":
                    self._abort(conn, "rst")
                    self._finish(lane, rec, "max_hold_rst")
                    return False
                self._note(rec, phase="streaming")
                for ev in evs[k:]:
                    self._chunk(conn, rec, ev)
                self._chunk(conn, rec, b"data: [DONE]\n\n")
                self._end_chunked(conn, rec)
                self._finish(lane, rec, "complete")
                return True
            return self._hold_then(lane, conn, rec, on_release)

        raise ValueError(f"unsupported stream fault {kind!r}")

    def _hold_then(self, lane: Lane, conn: _Conn, rec: RequestRecord, close: str) -> bool:
        self._note(rec, phase="holding")
        res = self._hold(rec, self.max_hold)
        if res == "client_closed":
            raise _ClientGone("hold")
        self._abort(conn, close)
        if res == "timeout":
            self._finish(lane, rec, "max_hold_rst")
        else:
            self._finish(lane, rec, "reset" if close == "rst" else "fin_truncated")
        return False

    def _unary(self, lane: Lane, conn: _Conn, rec: RequestRecord, body: dict[str, Any], spec: dict[str, Any]) -> bool:
        model = str(body.get("model", "telelem-wire-model"))
        kind = spec["kind"]
        tool = kind == "reset_after_tool_fragment" or spec.get("tool_call")
        msg: dict[str, Any] = {"role": "assistant", "content": None if tool else
                               "".join(p.replace("{lane}", rec.lane) for p in _DEFAULT_PARAMS["valid"]["text"]),
                               "refusal": None}
        if tool:
            msg["tool_calls"] = [{"id": f"call_{rec.id}", "type": "function",
                                  "function": {"name": TOOL_NAME, "arguments": TOOL_ARGS}}]
        obj = {"id": f"chatcmpl-{rec.id}", "object": "chat.completion", "created": CREATED, "model": model,
               "system_fingerprint": "telelem-wire",
               "choices": [{"index": 0, "message": msg, "logprobs": None, "finish_reason": "tool_calls" if tool else "stop"}],
               "usage": {"prompt_tokens": 7, "completion_tokens": 5, "total_tokens": 12}}
        data = json.dumps(obj, separators=(",", ":")).encode()
        if kind in ("valid", "slow"):
            return self._json(lane, conn, rec, 200, obj)
        # Faults on unary: declare full Content-Length, then deliver a prefix (or nothing).
        self._note(rec, status=200, phase="streaming")
        conn.send(self._resp_head(rec, 200, [("content-type", "application/json"), ("content-length", str(len(data)))]))
        if kind == "reset_after_tool_fragment":
            cut = data.index(b'"arguments"') + len(b'"arguments":"') + 5
            conn.send(data[:cut])
            with self._cond:
                rec.events_sent.append(data[:cut].decode())
                rec.body_bytes_sent += cut
                rec.wire_bytes_sent += cut
            self._abort(conn, spec["close"])
            self._finish(lane, rec, "reset" if spec["close"] == "rst" else "fin_truncated")
            return False
        # stall / heartbeat on unary: headers only, then hold.
        return self._hold_then(lane, conn, rec, "rst")


def _err(message: str, typ: str) -> dict[str, Any]:
    return {"error": {"message": message, "type": typ, "param": None, "code": None}}
