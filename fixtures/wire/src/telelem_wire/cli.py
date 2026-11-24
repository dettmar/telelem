"""telelem-wire CLI: `serve` (long-running fixture + control plane), `probe` (one SDK stream), `scenarios` (reference run)."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import urllib.request
from typing import Any

from .server import FAULT_KINDS, WireFixture, fault_spec


def versions() -> dict[str, Any]:
    import ssl
    from importlib import metadata

    out: dict[str, Any] = {"python": sys.version.split()[0], "openssl": ssl.OPENSSL_VERSION}
    for pkg in ("telelem-wire", "cryptography", "openai", "httpx2", "httpcore2", "h11"):
        try:
            out[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            out[pkg] = None
    return out


def _parse_fault(s: str) -> tuple[str, dict[str, Any]]:
    """LANE=KIND or LANE=KIND:{json params}"""
    lane, sep, rest = s.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"--fault expects LANE=KIND[:JSON], got {s!r}")
    kind, _, params = rest.partition(":")
    p = json.loads(params) if params else {}
    try:
        return lane, fault_spec(kind, **p)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from e


def cmd_serve(a: argparse.Namespace) -> int:
    from .control import ControlServer

    faults = dict(a.fault or [])
    fx = WireFixture(lanes=a.lanes.split(","), faults=faults, host=a.host, state_dir=a.state_dir,
                     key_type=a.key_type, max_hold=a.max_hold).start()
    done = threading.Event()
    ctl = ControlServer(fx, host=a.host, port=a.control_port, on_shutdown=done.set).start()
    info = fx.info()
    info["control_url"] = ctl.url
    info["env"] = fx.gateway_env()
    info["env"]["TELELEM_WIRE_CONTROL_URL"] = ctl.url
    info["versions"] = versions()
    line = json.dumps(info, separators=(",", ":"))
    if a.ready_file:
        tmp = a.ready_file + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(line + "\n")
        os.replace(tmp, a.ready_file)
    print(line, flush=True)  # the ONE stdout line; everything else goes to stderr

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: done.set())
    if a.exit_on_stdin_eof:
        def watch() -> None:
            fd = sys.stdin.fileno()  # raw fd reads: no BufferedReader lock held by a daemon thread at exit
            try:
                while os.read(fd, 4096):
                    pass
            except OSError:
                pass
            done.set()
        threading.Thread(target=watch, daemon=True).start()
    done.wait()
    ctl.stop()
    fx.stop()
    if a.ready_file:
        try:
            os.unlink(a.ready_file)
        except OSError:
            pass
    print("telelem-wire: stopped", file=sys.stderr)
    return 0


def cmd_probe(a: argparse.Namespace) -> int:
    from .client import run_stream, sdk_client

    if a.control:
        with urllib.request.urlopen(a.control.rstrip("/") + "/info", timeout=5) as r:
            info = json.load(r)
        base_url, ca = info["lanes"][a.lane]["base_url"], info["ca_file"]
    else:
        base_url, ca = a.base_url, a.ca_file
    if not (base_url and ca):
        print("probe needs --control URL or --base-url + --ca-file", file=sys.stderr)
        return 2
    client = sdk_client(base_url, ca, timeout=a.timeout)
    res = run_stream(client)
    print(json.dumps(res, indent=2))
    return 0


def cmd_scenarios(a: argparse.Namespace) -> int:
    from .scenarios import run_reference

    report = run_reference()
    report["versions"] = versions()
    print(json.dumps(report, indent=2))
    return 0 if report["all_passed"] else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="telelem-wire", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the two-lane fixture + loopback control plane; prints one ready JSON line")
    s.add_argument("--lanes", default="a,b", help="comma-separated lane names (default a,b)")
    s.add_argument("--fault", action="append", type=_parse_fault, metavar="LANE=KIND[:JSON]",
                   help=f"initial lane fault; kinds: {', '.join(FAULT_KINDS)}")
    s.add_argument("--host", default="127.0.0.1", help="loopback bind address (127.0.0.1 or ::1)")
    s.add_argument("--control-port", type=int, default=0, help="control plane port (default: ephemeral)")
    s.add_argument("--state-dir", default=None, help="where ca.pem/server.pem/server.key go (default: fresh temp dir)")
    s.add_argument("--key-type", choices=("ec", "rsa"), default="ec")
    s.add_argument("--max-hold", type=float, default=60.0, help="safety cap (s) for any held/stalled response")
    s.add_argument("--ready-file", default=None, help="also write the ready JSON here (atomic rename)")
    s.add_argument("--exit-on-stdin-eof", action="store_true",
                   help="exit when stdin closes (use when launched from an Erlang Port / supervisor)")
    s.set_defaults(func=cmd_serve)

    pr = sub.add_parser("probe", help="one streaming request with the pinned SDK (no retries); prints what the SDK surfaced")
    pr.add_argument("--control", help="control URL from the ready line")
    pr.add_argument("--lane", default="a")
    pr.add_argument("--base-url")
    pr.add_argument("--ca-file")
    pr.add_argument("--timeout", type=float, default=10.0)
    pr.set_defaults(func=cmd_probe)

    sc = sub.add_parser("scenarios", help="in-process reference run of the TEL-004 cases; JSON evidence, exit 1 on mismatch")
    sc.set_defaults(func=cmd_scenarios)

    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
