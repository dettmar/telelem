"""Independent observers for the M2a isolation harness (stdlib only).

Runs inside disposable runner-image containers, never on the host network:

  canary          TCP sink on the pre-existing default docker bridge, outside the
                  boundary. Logs every accepted connection, then acks.
  boundary        Services inside the --network none boundary namespace: the two
                  registered TLS fixture lanes (a, b) and a DNS sink on
                  127.0.0.53:53 (udp+tcp) that logs every query, answers NXDOMAIN.
  probe           Verifier facts from inside the boundary namespace: interfaces,
                  routes, listeners, canary reachability, DNS control query.
  outside-control Verifier control request to the canary from outside the boundary.
  fs-control      Deliberate, audited sentinel access proving the file audit sees it.

Observers log one JSON object per line, flushed before the client is answered,
so a finished client implies a logged arrival. No request bodies, headers or
sentinel contents are logged; only sizes, digests and fixed marker classes.
"""

import argparse
import hashlib
import json
import os
import socket
import ssl
import struct
import sys
import threading
import time

LANES = {"a": 18443, "b": 18444}
DNS_ADDR = ("127.0.0.53", 53)
CANARY_PORTS = (443, 80)
CONTROL_MARK = b"TELELEM-CANARY-CONTROL "
RED_MARK = b"TELELEM-RED-SUBJECT "


class Log:
    def __init__(self, path):
        self._f = open(path, "a", buffering=1)
        self._lock = threading.Lock()
        self._seq = 0

    def emit(self, **rec):
        with self._lock:
            self._seq += 1
            rec = {"seq": self._seq, "t_ms": int(time.monotonic() * 1000), **rec}
            self._f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")
            self._f.flush()
            os.fsync(self._f.fileno())


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, sort_keys=True, indent=1)
    os.replace(tmp, path)


def serve_forever(sock, handler):
    def loop():
        while True:
            try:
                conn, peer = sock.accept()
            except OSError:
                return
            threading.Thread(target=handler, args=(conn, peer), daemon=True).start()

    threading.Thread(target=loop, daemon=True).start()


def listen_tcp(addr):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(addr)
    s.listen(64)
    return s


# ── canary ───────────────────────────────────────────────────────────────────


def classify(first, nonce):
    if first.startswith(CONTROL_MARK):
        return "control" if first[len(CONTROL_MARK):].strip() == nonce.encode() else "control_bad_nonce"
    if first.startswith(RED_MARK):
        return "red" if first[len(RED_MARK):].strip() == nonce.encode() else "red_bad_nonce"
    return "other" if first else "empty"


def cmd_canary(a):
    log = Log(os.path.join(a.obs, "canary.jsonl"))

    def handle(port):
        def h(conn, peer):
            conn.settimeout(1.5)
            first = b""
            try:
                while b"\n" not in first and len(first) < 128:
                    chunk = conn.recv(128 - len(first))
                    if not chunk:
                        break
                    first += chunk
            except OSError:
                pass
            log.emit(event="arrival", port=port, peer=peer[0], peer_port=peer[1],
                     cls=classify(first.split(b"\n")[0], a.nonce),
                     first_len=len(first), first_sha256=hashlib.sha256(first).hexdigest())
            try:
                conn.sendall(b"OK\n")
            except OSError:
                pass
            conn.close()
        return h

    for p in CANARY_PORTS:
        serve_forever(listen_tcp(("0.0.0.0", p)), handle(p))
    write_json(os.path.join(a.obs, "ready.json"), {"role": "canary", "ports": list(CANARY_PORTS)})
    threading.Event().wait()


# ── boundary: DNS sink + fixture lanes ───────────────────────────────────────


def dns_parse(msg):
    if len(msg) < 12:
        return None
    qid, flags, qd = struct.unpack("!HHH", msg[:6])
    i, labels = 12, []
    while i < len(msg) and msg[i] != 0 and len(labels) < 64:
        n = msg[i]
        if n & 0xC0:
            return None
        labels.append(msg[i + 1:i + 1 + n].decode("ascii", "replace"))
        i += 1 + n
    i += 1
    if i + 4 > len(msg):
        return None
    qtype = struct.unpack("!H", msg[i:i + 2])[0]
    return {"id": qid, "flags": flags, "qname": ".".join(labels)[:253], "qtype": qtype,
            "question": msg[12:i + 4]}


def dns_nxdomain(q):
    flags = 0x8000 | (q["flags"] & 0x0100) | 0x0080 | 3  # QR, RD copied, RA, NXDOMAIN
    return struct.pack("!HHHHHH", q["id"], flags, 1, 0, 0, 0) + q["question"]


def start_dns(log):
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    u.bind(DNS_ADDR)

    def udp_loop():
        while True:
            msg, peer = u.recvfrom(4096)
            q = dns_parse(msg)
            log.emit(event="query", proto="udp", peer=peer[0],
                     qname=q["qname"] if q else None, qtype=q["qtype"] if q else None)
            if q:
                u.sendto(dns_nxdomain(q), peer)

    threading.Thread(target=udp_loop, daemon=True).start()

    def tcp_handler(conn, peer):
        conn.settimeout(2)
        q = None
        try:
            hdr = conn.recv(2)
            n = struct.unpack("!H", hdr)[0] if len(hdr) == 2 else 0
            msg = b""
            while len(msg) < n:
                c = conn.recv(n - len(msg))
                if not c:
                    break
                msg += c
            q = dns_parse(msg)
        except (OSError, struct.error):
            pass
        log.emit(event="query", proto="tcp", peer=peer[0],
                 qname=q["qname"] if q else None, qtype=q["qtype"] if q else None)
        try:
            if q:
                r = dns_nxdomain(q)
                conn.sendall(struct.pack("!H", len(r)) + r)
        except OSError:
            pass
        conn.close()

    serve_forever(listen_tcp(DNS_ADDR), tcp_handler)


SSE = (
    b'data: {"id":"fx","object":"chat.completion.chunk","created":0,"model":"fixture",'
    b'"choices":[{"index":0,"delta":{"role":"assistant","content":"ok"},"finish_reason":null}]}\n\n'
    b'data: {"id":"fx","object":"chat.completion.chunk","created":0,"model":"fixture",'
    b'"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
    b"data: [DONE]\n\n"
)


def lane_handler(lane, ctx, log):
    def h(raw, peer):
        raw.settimeout(5)
        rec = {"event": "arrival", "lane": lane, "peer": peer[0]}
        try:
            conn = ctx.wrap_socket(raw, server_side=True)
        except (ssl.SSLError, OSError) as e:
            log.emit(**rec, tls="error", error=type(e).__name__)
            raw.close()
            return
        try:
            buf = b""
            while b"\r\n\r\n" not in buf and len(buf) < 16384:
                c = conn.recv(4096)
                if not c:
                    break
                buf += c
            head, _, body = buf.partition(b"\r\n\r\n")
            lines = head.split(b"\r\n")
            method, path = (lines[0].split(b" ") + [b"", b""])[:2]
            clen = 0
            for ln in lines[1:]:
                k, _, v = ln.partition(b":")
                if k.strip().lower() == b"content-length":
                    clen = min(int(v.strip() or 0), 1 << 20)
            while len(body) < clen:
                c = conn.recv(clen - len(body))
                if not c:
                    break
                body += c
            log.emit(**rec, tls=conn.version(), method=method.decode("ascii", "replace")[:16],
                     path=path.decode("ascii", "replace")[:64], body_len=len(body),
                     body_sha256=hashlib.sha256(body).hexdigest())
            out = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n" \
                  b"transfer-encoding: chunked\r\nconnection: close\r\n\r\n"
            out += b"%x\r\n%s\r\n0\r\n\r\n" % (len(SSE), SSE)
            conn.sendall(out)
        except (OSError, ValueError) as e:
            log.emit(**rec, tls="ok", error=type(e).__name__)
        finally:
            try:
                conn.close()
            except OSError:
                pass
    return h


def cmd_boundary(a):
    dns_log = Log(os.path.join(a.obs, "dns.jsonl"))
    lane_log = Log(os.path.join(a.obs, "lanes.jsonl"))
    start_dns(dns_log)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(os.path.join(a.pki, "leaf.pem"), os.path.join(a.pki, "leaf.key"))
    for lane, port in LANES.items():
        serve_forever(listen_tcp(("127.0.0.1", port)), lane_handler(lane, ctx, lane_log))
    write_json(os.path.join(a.obs, "ready.json"),
               {"role": "boundary", "lanes": LANES, "dns": list(DNS_ADDR)})
    threading.Event().wait()


# ── probe (verifier, inside the boundary namespace) ──────────────────────────


def hex4(h):
    return socket.inet_ntop(socket.AF_INET, struct.pack("<I", int(h, 16)))


def hex6(h):
    words = [struct.pack("<I", int(h[i:i + 8], 16)) for i in range(0, 32, 8)]
    return socket.inet_ntop(socket.AF_INET6, b"".join(words))


def sockets(path, v6, want_state):
    out = []
    try:
        with open(path) as f:
            next(f)
            for line in f:
                cols = line.split()
                addr, port = cols[1].split(":")
                if want_state is None or cols[3] == want_state:
                    out.append("%s:%d" % ((hex6 if v6 else hex4)(addr), int(port, 16)))
    except FileNotFoundError:
        pass
    return sorted(out)


def dns_query(name, server, timeout=2.0):
    q = struct.pack("!HHHHHH", 0x5151, 0x0100, 1, 0, 0, 0)
    for label in name.split("."):
        q += bytes([len(label)]) + label.encode()
    q += b"\x00" + struct.pack("!HH", 1, 1)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(q, server)
        r, _ = s.recvfrom(4096)
        return {"answered": True, "rcode": r[3] & 0x0F}
    except OSError as e:
        return {"answered": False, "error": type(e).__name__}
    finally:
        s.close()


def try_connect(ip, port, timeout=2.0):
    try:
        c = socket.create_connection((ip, port), timeout=timeout)
        c.close()
        return {"connected": True}
    except OSError as e:
        return {"connected": False, "errno": e.errno and __import__("errno").errorcode.get(e.errno),
                "error": type(e).__name__}


def cmd_probe(a):
    with open("/proc/net/dev") as f:
        ifaces = sorted(l.split(":")[0].strip() for l in list(f)[2:])
    with open("/proc/net/route") as f:
        v4_routes = [l.split()[0] for l in list(f)[1:] if l.strip()]
    with open("/proc/net/ipv6_route") as f:
        v6_route_devs = sorted({l.split()[-1] for l in f if l.strip()})
    qname = "control-%s.isolation.invalid" % a.nonce
    facts = {
        "interfaces": ifaces,
        "ipv4_main_routes": v4_routes,
        "ipv6_route_devices": v6_route_devs,
        "tcp_listen": sorted(sockets("/proc/net/tcp", False, "0A") + sockets("/proc/net/tcp6", True, "0A")),
        "udp_bound": sorted(sockets("/proc/net/udp", False, None) + sockets("/proc/net/udp6", True, None)),
        "canary_connect": {str(p): try_connect(a.canary_ip, p) for p in CANARY_PORTS},
        "dns_control": {"qname": qname, **dns_query(qname, DNS_ADDR)},
    }
    write_json(a.out, facts)


def cmd_outside_control(a):
    res = {}
    try:
        c = socket.create_connection((a.canary_ip, 443), timeout=3)
        c.settimeout(3)
        res["local_ip"] = c.getsockname()[0]
        c.sendall(CONTROL_MARK + a.nonce.encode() + b"\n")
        res["ack"] = c.recv(16).decode("ascii", "replace")
        c.close()
    except OSError as e:
        res["error"] = type(e).__name__
    write_json(a.out, res)


def cmd_fs_control(a):
    done = []
    with open(a.read, "rb") as f:
        done.append({"op": "read", "path": a.read, "len": len(f.read())})
    os.stat(a.stat)
    done.append({"op": "stat", "path": a.stat})
    with open(a.write, "w") as f:
        f.write("telelem isolation fs-control write\n")
    done.append({"op": "write", "path": a.write})
    write_json(a.out, {"done": done})


def main():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("canary"); c.add_argument("--obs", required=True); c.add_argument("--nonce", required=True)
    b = sp.add_parser("boundary"); b.add_argument("--obs", required=True); b.add_argument("--pki", required=True)
    pr = sp.add_parser("probe")
    for x in (pr,):
        x.add_argument("--canary-ip", required=True); x.add_argument("--nonce", required=True); x.add_argument("--out", required=True)
    oc = sp.add_parser("outside-control")
    oc.add_argument("--canary-ip", required=True); oc.add_argument("--nonce", required=True); oc.add_argument("--out", required=True)
    fc = sp.add_parser("fs-control")
    for k in ("read", "stat", "write", "out"):
        fc.add_argument("--" + k, required=True)
    a = p.parse_args()
    {"canary": cmd_canary, "boundary": cmd_boundary, "probe": cmd_probe,
     "outside-control": cmd_outside_control, "fs-control": cmd_fs_control}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
