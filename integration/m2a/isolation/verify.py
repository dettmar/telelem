"""Verdict for one isolation-harness run. Usage: verify.py RUN_DIR VERDICT_JSON

Judges only external observations: probe facts, canary/lane/DNS-sink logs, strace
per-process traces and host-side sentinel inventories. The subject's own result
file is a consistency check, never evidence. Fails closed: a missing or
unparseable observation, an unattributed arrival, an unmet positive control or
an undetected red control is a FAIL. Exit 0 PASS, 1 FAIL.
"""

import collections
import glob
import json
import os
import re
import sys
import traceback

LANES = {"a": 18443, "b": 18444}
SENTINEL_TOKENS = ("/etc/telelem", "/var/lib/telelem", "/run/telelem", "/run/secrets",
                   "telelem-prod-config.sentinel", "telelem-prod-receipts.sentinel",
                   "telelem-prod-signer.sentinel", "control-scratch")
TRUST_STORE_TOKENS = ("/etc/ssl/certs", "ca-certificates", "/usr/lib/ssl/cert", "/etc/pki/tls")
WRITE_ALLOWED = ("/audit/", "/tmp/", "/dev/null")
WRITE_SYSCALLS = {"rename", "renameat", "renameat2", "unlink", "unlinkat", "mkdir", "mkdirat",
                  "rmdir", "link", "linkat", "symlink", "symlinkat", "chmod", "fchmodat",
                  "fchmodat2", "chown", "lchown", "fchownat", "truncate", "utime", "utimes",
                  "utimensat", "futimesat", "setxattr", "lsetxattr", "removexattr",
                  "lremovexattr", "mknod", "mknodat", "creat"}
OPEN_SYSCALLS = {"open", "openat", "openat2", "creat"}
NET_DEST_SYSCALLS = {"connect", "sendto", "sendmsg", "sendmmsg"}
WRITE_FLAGS = re.compile(r"O_(WRONLY|RDWR|CREAT|TRUNC|APPEND)")
LINE = re.compile(r"^(\w+)\((.*)$")
QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')
DIRFD = re.compile(r"^\s*(?:AT_FDCWD|\d+)<([^>]*)>")
INET = re.compile(r'sa_family=AF_INET, sin_port=htons\((\d+)\), sin_addr=inet_addr\("([^"]+)"\)')
INET6 = re.compile(r'sa_family=AF_INET6, sin6_port=htons\((\d+)\).*?inet_pton\(AF_INET6, "([^"]+)"')
UNIX = re.compile(r'sa_family=AF_UNIX, sun_path=(@?"[^"]*")')


class Missing(Exception):
    pass


def load_json(path):
    if not os.path.isfile(path):
        raise Missing(path)
    with open(path) as f:
        return json.load(f)


def load_jsonl(path):
    if not os.path.isfile(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


# ── strace ───────────────────────────────────────────────────────────────────


def parse_trace_dir(d):
    files = sorted(glob.glob(os.path.join(d, "t.*")))
    if not files:
        raise Missing(d + "/t.*")
    events = []
    for fn in files:
        pid = fn.rsplit(".", 1)[-1]
        with open(fn, errors="replace") as f:
            for raw in f:
                raw = raw.rstrip("\n")
                m = LINE.match(raw)
                if not m:
                    continue
                events.append(classify(pid, m.group(1), m.group(2), raw))
    return events


def paths_of(sc, args):
    strs = [bytes(s, "utf-8").decode("unicode_escape", "replace") for s in QUOTED.findall(args)]
    base = None
    m = DIRFD.match(args)
    if m and (sc.endswith("at") or sc.endswith("at2")):
        base = m.group(1)
    out = []
    for s in strs[:2]:
        if s.startswith("/"):
            out.append(s)
        elif base and base.startswith("/"):
            out.append(base.rstrip("/") + "/" + s)
        else:
            out.append(s)
    return out


def classify(pid, sc, args, raw):
    ev = {"pid": pid, "sc": sc, "raw": raw[:400]}
    if sc in NET_DEST_SYSCALLS:
        dests = [("inet", a, int(p)) for p, a in INET.findall(raw)]
        dests += [("inet6", a, int(p)) for p, a in INET6.findall(raw)]
        dests += [("unix", u, 0) for u in UNIX.findall(raw)]
        ev["dests"] = dests
    else:
        ev["paths"] = paths_of(sc, args)
        ev["write"] = sc in WRITE_SYSCALLS or (sc in OPEN_SYSCALLS and bool(WRITE_FLAGS.search(args)))
    ev["sentinel"] = any(t in raw for t in SENTINEL_TOKENS)
    ev["trust_store"] = any(t in raw for t in TRUST_STORE_TOKENS)
    return ev


def summarize(events):
    inet = collections.Counter()
    unix = []
    for e in events:
        for fam, a, p in e.get("dests", []):
            if fam == "unix":
                unix.append(a)
            else:
                inet["%s:%d" % (a, p)] += 1
    sentinel = [e for e in events if e["sentinel"]]
    return {
        "events": len(events),
        "inet_dests": dict(sorted(inet.items())),
        "unix_dests": unix,
        "sentinel_refs": [e["raw"] for e in sentinel],
        "sentinel_reads": sum(1 for e in sentinel if "paths" in e and not e["write"]),
        "sentinel_writes": sum(1 for e in sentinel if e.get("write")),
        "trust_store_refs": [e["raw"] for e in events if e["trust_store"]],
        "writes_outside_allowed": [e["raw"] for e in events if e.get("write") and
                                   any(not p.startswith(WRITE_ALLOWED) for p in e["paths"][:1])],
    }


# ── verdict ──────────────────────────────────────────────────────────────────


class Verdict:
    def __init__(self):
        self.checks = []

    def check(self, cid, ok, **detail):
        self.checks.append({"id": cid, "ok": bool(ok), **detail})

    @property
    def passed(self):
        return bool(self.checks) and all(c["ok"] for c in self.checks)


def slices(marks, log, key):
    """Entries of `log` per phase, using the line counts recorded after each phase."""
    out, prev = {}, 0
    for m in marks:
        out[m["phase"]] = log[prev:m[key]]
        prev = m[key]
    out["_after_last_mark"] = log[prev:]
    return out


def sentinel_changed(run, phase):
    b = open(os.path.join(run, "verifier", "sentinels-%s-before.txt" % phase)).read()
    a = open(os.path.join(run, "verifier", "sentinels-%s-after.txt" % phase)).read()
    if not b.strip() or not a.strip():
        raise Missing("sentinel inventory " + phase)
    return a != b


def judge(run, v):
    env = load_json(os.path.join(run, "verifier", "env.json"))
    nonce, canary_ip = env["nonce"], env["canary_ip"]
    marks = load_jsonl(os.path.join(run, "verifier", "marks.jsonl"))
    phases = [m["phase"] for m in marks]
    want = ["start", "probe", "outside", "good", "red_boundary", "red_bridge"]
    v.check("phases_complete", phases == want, phases=phases)
    if phases != want:
        return

    canary = slices(marks, load_jsonl(os.path.join(run, "obs", "canary", "canary.jsonl")), "canary")
    dns = slices(marks, load_jsonl(os.path.join(run, "obs", "boundary", "dns.jsonl")), "dns")
    lanes = slices(marks, load_jsonl(os.path.join(run, "obs", "boundary", "lanes.jsonl")), "lanes")

    # boundary facts from inside the namespace
    probe = load_json(os.path.join(run, "verifier", "probe.json"))
    v.check("boundary_interfaces_lo_only", probe["interfaces"] == ["lo"], seen=probe["interfaces"])
    v.check("boundary_no_ipv4_routes", probe["ipv4_main_routes"] == [], seen=probe["ipv4_main_routes"])
    v.check("boundary_ipv6_routes_lo_only", set(probe["ipv6_route_devices"]) <= {"lo"},
            seen=probe["ipv6_route_devices"])
    want_tcp = sorted(["127.0.0.1:%d" % p for p in LANES.values()] + ["127.0.0.53:53"])
    v.check("boundary_reachable_listeners_are_lanes_plus_dns_sink", probe["tcp_listen"] == want_tcp,
            seen=probe["tcp_listen"], want=want_tcp)
    v.check("boundary_udp_only_dns_sink", probe["udp_bound"] == ["127.0.0.53:53"], seen=probe["udp_bound"])
    cc = probe["canary_connect"]
    v.check("boundary_canary_unreachable_from_inside",
            all((not r["connected"]) and r.get("errno") == "ENETUNREACH" for r in cc.values()), seen=cc)

    # DNS sink positive control and attribution
    dc = probe["dns_control"]
    q_probe = [q for q in dns["probe"]]
    v.check("dns_sink_positive_control",
            dc.get("answered") and dc.get("rcode") == 3 and len(q_probe) == 1 and q_probe[0]["qname"] == dc["qname"],
            reply=dc, logged=q_probe)

    # canary positive control from outside the boundary
    out = load_json(os.path.join(run, "verifier", "outside.json"))
    c_out = canary["outside"]
    v.check("canary_positive_control",
            out.get("ack") == "OK\n" and len(c_out) == 1 and c_out[0]["cls"] == "control"
            and c_out[0]["peer"] == out.get("local_ip"), client=out, logged=c_out)

    # subject phases
    ph = {p: load_json(os.path.join(run, "verifier", p + ".json")) for p in ("good", "red_boundary", "red_bridge")}
    for p, info in ph.items():
        v.check(p + "_subject_exited_0", info["exit"] == "0", exit=info["exit"])

    # file-audit positive control (same container, same strace config, before the subject)
    ctrl = summarize(parse_trace_dir(os.path.join(run, "audit", "good", "control")))
    v.check("fs_audit_positive_control_read", ctrl["sentinel_reads"] >= 2, seen=ctrl["sentinel_refs"])
    v.check("fs_audit_positive_control_write",
            any("fs-control.written" in r and WRITE_FLAGS.search(r) for r in ctrl["sentinel_refs"]),
            seen=ctrl["sentinel_refs"])
    load_json(os.path.join(run, "audit", "good", "control-result.json"))

    # good subject: the acceptance itself
    g = summarize(parse_trace_dir(os.path.join(run, "audit", "good", "subject")))
    want_dests = {"127.0.0.1:%d" % p: 1 for p in sorted(LANES.values())}
    v.check("good_connects_exactly_one_per_registered_lane", g["inet_dests"] == want_dests,
            seen=g["inet_dests"], want=want_dests)
    v.check("good_no_unix_socket_connects", g["unix_dests"] == [], seen=g["unix_dests"])
    v.check("good_no_sentinel_access", g["sentinel_refs"] == [], seen=g["sentinel_refs"][:20])
    v.check("good_no_os_trust_store_access", g["trust_store_refs"] == [], seen=g["trust_store_refs"][:20])
    v.check("good_no_writes_outside_audit_tmp", g["writes_outside_allowed"] == [],
            seen=g["writes_outside_allowed"][:20])
    v.check("good_sentinels_unchanged", not sentinel_changed(run, "good"))
    gl = lanes["good"]
    per_lane = collections.Counter(e["lane"] for e in gl)
    v.check("good_lane_arrivals_a1_b1", dict(per_lane) == {"a": 1, "b": 1} and all(
        e.get("method") == "POST" and e.get("path") == "/v1/chat/completions" and e.get("tls", "").startswith("TLSv")
        for e in gl), seen=gl)
    v.check("good_zero_dns_queries", dns["good"] == [], seen=dns["good"])
    v.check("good_zero_canary_arrivals", canary["good"] == [], seen=canary["good"])
    v.check("good_trace_sees_its_lane_connects", sum(g["inet_dests"].values()) == len(gl))

    res = load_json(os.path.join(run, "audit", "good", "subject-result.json"))
    want_res = {
        "bootstrap": "ok",
        "resolve_missing_c": "denied:no_binding",
        "resolve_serving_principal": "denied:no_serving_resolver",
        "resolve_without_dispatch": "denied:permission_denied",
        "resolve_no_bindings": "denied:invalid_bindings",
        "bootstrap_disallowed_target": "error:non_loopback_host:a",
        "bootstrap_prod_config_path": "error:bootstrap_path_outside_sandbox_root",
        "bootstrap_ca_is_prod_key": "error:ca_file_denied:a",
        "bootstrap_prod_store_root": "error:store_root_denied",
        "bootstrap_prod_signer_ref": "error:invalid_signer_ref",
        "request_a": "done",
        "request_b": "done",
        "resolve_missing_c_after": "denied:no_binding",
    }
    v.check("good_subject_self_report_consistent", res == want_res,
            diff={k: (res.get(k), w) for k, w in want_res.items() if res.get(k) != w})

    # red control 1: misbehaving subject inside the boundary must be detected
    r = summarize(parse_trace_dir(os.path.join(run, "audit", "red_boundary", "subject")))
    v.check("red_boundary_fs_audit_detects_sentinel_read", r["sentinel_reads"] >= 1, seen=r["sentinel_refs"][:10])
    v.check("red_boundary_fs_audit_detects_sentinel_write", r["sentinel_writes"] >= 1, seen=r["sentinel_refs"][:10])
    v.check("red_boundary_hash_audit_detects_change", sentinel_changed(run, "red_boundary"))
    red_q = "prod-provider-%s.isolation.invalid" % nonce
    v.check("red_boundary_dns_sink_detects_lookup",
            len(dns["red_boundary"]) >= 1 and all(q["qname"] == red_q for q in dns["red_boundary"]),
            seen=dns["red_boundary"])
    v.check("red_boundary_trace_detects_canary_dial", r["inet_dests"].get("%s:443" % canary_ip, 0) >= 1,
            seen=r["inet_dests"])
    v.check("red_boundary_canary_unreached", canary["red_boundary"] == [], seen=canary["red_boundary"])

    # red control 2: same subject with the boundary removed must reach the canary
    rb = canary["red_bridge"]
    v.check("red_bridge_canary_detects_leak",
            len(rb) >= 1 and all(e["cls"] == "red" and e["peer"] == ph["red_bridge"]["container_ip"] for e in rb),
            seen=rb, subject_ip=ph["red_bridge"]["container_ip"])
    v.check("red_bridge_zero_boundary_dns_or_lane_arrivals",
            dns["red_bridge"] == [] and lanes["red_bridge"] == [])

    # nothing unattributed anywhere
    for name, sl, allowed in (("canary", canary, {"outside", "red_bridge"}),
                              ("dns", dns, {"probe", "red_boundary"}),
                              ("lanes", lanes, {"good"})):
        stray = {p: e for p, e in sl.items() if p not in allowed and e}
        v.check("no_unattributed_%s_arrivals" % name, not stray, seen=stray)


def main():
    run, out = sys.argv[1], sys.argv[2]
    v = Verdict()
    try:
        judge(run, v)
    except Exception as e:  # fail closed on any missing/garbled observation
        v.check("verifier_error", False, error="%s: %s" % (type(e).__name__, e),
                where=traceback.format_exc(limit=3)[-600:])
    verdict = {"verdict": "PASS" if v.passed else "FAIL", "checks": v.checks}
    with open(out, "w") as f:
        json.dump(verdict, f, indent=1, sort_keys=True)
    for c in v.checks:
        print("%s  %s" % ("ok  " if c["ok"] else "FAIL", c["id"]))
    print("verdict:", verdict["verdict"])
    return 0 if v.passed else 1


if __name__ == "__main__":
    sys.exit(main())
