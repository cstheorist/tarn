#!/usr/bin/env python3
"""Linux-only signal/replication-ingress broker for tests/kv.sh.

One JSON request on stdin, one JSON response on stdout. No service lifecycle,
arbitrary signal, host selection, relay target or shell command is exposed here.
"""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import stat
import sys
import time

from kv_relay import validate_flows

OWNER = "tarn-kv181-host-v1"


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def private_json(path):
    s = path.lstat()
    require(stat.S_ISREG(s.st_mode) and s.st_uid == os.getuid()
            and stat.S_IMODE(s.st_mode) == 0o600 and s.st_size <= 65536,
            "expected owned mode-0600 regular JSON file")
    return json.loads(path.read_text())


def validate_marker_record(m, n):
    if "owner_marker" not in n:
        return
    marker = n["owner_marker"]
    require(isinstance(marker, dict) and set(marker) == {"fd", "path", "dev", "ino"}
            and type(marker.get("fd")) is int and marker["fd"] == 9
            and marker.get("path") == str(Path(m["workdir"]) / "owner-nonce")
            and all(type(marker.get(k)) is int and marker[k] > 0 for k in ("dev", "ino")),
            "invalid inherited owner marker record")


def read_manifest(path, platform=None):
    require((platform or sys.platform) == "linux", "only Linux host backend is supported")
    path = Path(path)
    parent = path.parent
    s = parent.lstat()
    require(stat.S_ISDIR(s.st_mode) and s.st_uid == os.getuid()
            and stat.S_IMODE(s.st_mode) == 0o700
            and parent.name.startswith("tarn-kv181-"), "invalid private fixture directory")
    require(parent.resolve() == parent.absolute(), "fixture directory cannot use symlinks")
    m = private_json(path)
    require(m.get("version") == 1 and m.get("backend") == "host"
            and m.get("owner") == OWNER and m.get("workdir") == str(parent), "invalid host manifest")
    require(re.fullmatch(r"[0-9a-f]{32}", m.get("ownerNonce", "")), "invalid owner nonce")
    owner = private_json(parent / "owner.json")
    require(owner == {k: m[k] for k in ("version", "backend", "owner", "ownerNonce", "workdir",
                                       "launcher_pid", "launcher_starttime")}, "owner receipt differs")
    require(all(type(m.get(k)) is int and m[k] > 1
                for k in ("launcher_pid", "launcher_starttime")), "invalid launcher identity")
    nodes = m.get("nodes")
    require(isinstance(nodes, list) and len(nodes) == 6, "expected six storage nodes")
    require({n.get("node_id") for n in nodes} == {f"s{s}r{r}" for s in (1, 2) for r in (1, 2, 3)},
            "invalid node inventory")
    require(len({n.get("pid") for n in nodes}) == 6, "duplicate node PID")
    for n in nodes:
        require(all(type(n.get(k)) is int and n[k] > 0 for k in ("pid", "ppid", "starttime")),
                "invalid process identity")
        require(n["pid"] > 1 and n["ppid"] == m["launcher_pid"] and n["pid"] != n["ppid"],
                "node is not an original launcher child")
        require(n.get("ownerNonce") == m["ownerNonce"]
                and n.get("workdir") == str(parent / n["node_id"]), "node owner/workdir mismatch")
        require(n.get("shard_id") == int(n["node_id"][1])
                and n.get("replica") == int(n["node_id"][3]), "invalid logical node identity")
        require(re.fullmatch(r"127\.0\.0\.1:[0-9]{1,5}", n.get("endpoint", ""))
                and 0 < int(n["endpoint"].split(":")[1]) < 65536, "invalid endpoint")
        require(isinstance(n.get("exe"), str) and n["exe"].startswith("/"), "invalid executable identity")
        validate_marker_record(m, n)
    if "relay" in m:
        r = m["relay"]
        require(isinstance(r, dict) and r.get("node_id") == "relay"
                and all(type(r.get(k)) is int and r[k] > 0 for k in ("pid", "ppid", "starttime")),
                "invalid relay process identity")
        require(r["pid"] > 1 and r["ppid"] == m["launcher_pid"]
                and r["pid"] not in {m["launcher_pid"], *(n["pid"] for n in nodes)}
                and r.get("ownerNonce") == m["ownerNonce"]
                and r.get("workdir") == str(parent / "relay")
                and isinstance(r.get("exe"), str) and r["exe"].startswith("/"), "invalid owned relay")
        require(r.get("control_socket") == str(parent / "replication-control.sock"), "invalid relay socket path")
        validate_flows(r.get("flows"))
        validate_marker_record(m, r)
        physical = {n["node_id"]: n["endpoint"] for n in nodes}
        require(all(f["target"] == physical[f["server_node"]] for f in r["flows"]), "relay targets differ from owned nodes")
    return m


def parse_stat(raw):
    # Linux comm can itself contain spaces and right parentheses.
    lead, fields = raw.rsplit(") ", 1)
    pid = int(lead.split(" (", 1)[0])
    fields = fields.split()
    require(len(fields) >= 20, "truncated /proc stat")
    return {"pid": pid, "state": fields[0], "ppid": int(fields[1]), "starttime": int(fields[19])}


def proc_snapshot(pid):
    p = Path("/proc") / str(pid)
    info = parse_stat((p / "stat").read_text())
    info.update(uid=p.stat().st_uid, cwd=os.readlink(p / "cwd"), exe=os.readlink(p / "exe"))
    try:
        info["environ"] = (p / "environ").read_bytes().split(b"\0")
    except OSError:
        info["environ"] = None
    try:
        fd = p / "fd/9"
        s = fd.stat()
        info["owner_marker"] = {"fd": 9, "path": os.readlink(fd), "dev": s.st_dev, "ino": s.st_ino,
                                "uid": s.st_uid, "mode": stat.S_IMODE(s.st_mode), "regular": stat.S_ISREG(s.st_mode)}
    except OSError:
        info["owner_marker"] = None
    return info


def verify_process(m, n, snapshot=proc_snapshot):
    parent = snapshot(m["launcher_pid"])
    require(parent["pid"] == m["launcher_pid"] and parent["starttime"] == m["launcher_starttime"]
            and parent["uid"] == os.getuid(), "launcher identity changed")
    p = snapshot(n["pid"])
    require(p["pid"] == n["pid"] and p["ppid"] == n["ppid"] and p["starttime"] == n["starttime"]
            and p["uid"] == os.getuid() and p["state"] != "Z", "node process identity changed")
    require(p["cwd"] == n["workdir"] and p["exe"] == n["exe"], "node executable/workdir changed")
    if "owner_marker" in n:
        # Tarantool may rewrite its original argv/environ memory. A recorded
        # inherited FD is mandatory here; never fall back to mutable env bytes.
        validate_marker_record(m, n)
        marker = n["owner_marker"]
        observed = p.get("owner_marker")
        require(isinstance(observed, dict) and all(observed.get(k) == marker[k]
                for k in ("fd", "path", "dev", "ino")) and observed.get("uid") == os.getuid()
                and observed.get("mode") == 0o600 and observed.get("regular") is True,
                "inherited owner marker changed/missing")
        path = Path(marker["path"])
        s = path.lstat()
        require(stat.S_ISREG(s.st_mode) and s.st_uid == os.getuid() and stat.S_IMODE(s.st_mode) == 0o600
                and (s.st_dev, s.st_ino) == (marker["dev"], marker["ino"])
                and s.st_size in (32, 33) and path.read_text().rstrip("\n") == m["ownerNonce"],
                "owner nonce file identity/content changed")
    else:
        for key, value in (("TARN_KV_OWNER_NONCE", m["ownerNonce"]), ("TARN_KV_NODE_ID", n["node_id"]),
                           ("TARN_KV_WORKDIR", n["workdir"])):
            require((key + "=" + value).encode() in (p.get("environ") or []), "node environment owner changed")
    return {**n, "state": p["state"], "identity_verified": True}


def verify_socket(m, lstat=lambda p: p.lstat()):
    p = Path(m["relay"]["control_socket"])
    s = lstat(p)
    require(stat.S_ISSOCK(s.st_mode) and s.st_uid == os.getuid()
            and stat.S_IMODE(s.st_mode) == 0o600, "relay control socket identity/mode differs")
    return p


def relay_exchange(m, frame):
    p = verify_socket(m)
    deadline = time.monotonic() + 3
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        def budget():
            left = deadline - time.monotonic()
            require(left > 0, "relay control deadline elapsed")
            s.settimeout(left)
        budget()
        s.connect(str(p))
        budget()
        s.sendall((json.dumps(frame, sort_keys=True) + "\n").encode())
        data = bytearray()
        while b"\n" not in data:
            budget()
            chunk = s.recv(min(4096, 65537 - len(data)))
            require(bool(chunk), "relay response ended before frame")
            data.extend(chunk)
            require(len(data) <= 65536, "relay response exceeds bound")
    return json.loads(data)


def relay_request(m, value, snapshot=proc_snapshot, exchange=relay_exchange):
    require("relay" in m, "replication ingress capability unavailable")
    r = m["relay"]
    verify_process(m, r, snapshot)
    reply = exchange(m, {"version": 1, "ownerNonce": m["ownerNonce"], **value})
    verify_process(m, r, snapshot)
    require(isinstance(reply, dict) and reply.get("version") == 1 and reply.get("status") == "ok"
            and reply.get("ownerNonce") == m["ownerNonce"] and reply.get("pid") == r["pid"]
            and type(reply.get("epoch")) is int and reply["epoch"] >= 0, "invalid relay response owner/version")
    flows = reply.get("flows")
    require(isinstance(flows, list) and len(flows) == 12, "invalid relay flow snapshot")
    geometry = [{k: f[k] for k in ("id", "client_node", "server_node", "listen", "target")} for f in flows]
    validate_flows(geometry)
    require({f["id"]: f for f in geometry} == {f["id"]: f for f in r["flows"]}, "relay flow mapping changed")
    for f in flows:
        require(type(f.get("paused")) is bool and all(type(f.get(k)) is int and f[k] >= 0 for k in
                ("connections", "accepted", "refused", "errors", "connect_failures", "queued_s2c", "queued_c2s", "drained_s2c", "drained_c2s")),
                "invalid relay counters")
        require(f["drained_s2c"] <= f["queued_s2c"] and f["drained_c2s"] <= f["queued_c2s"], "invalid relay drain counters")
        if value["action"] == "replication_ingress" and f["client_node"] == value["node"]:
            require(f["paused"] == value["pause"], "incoming relay gates differ from requested state")
    return reply


def request(m, value, snapshot=proc_snapshot, send=os.kill, exchange=relay_exchange):
    require(isinstance(value, dict), "request must be an object")
    if value.get("action") == "inspect":
        require(set(value) == {"action"}, "unsupported inspect fields")
        result = {"version": 1, "status": "ok", "capabilities": ["signal"],
                  "nodes": [verify_process(m, n, snapshot) for n in m["nodes"]]}
        if "relay" in m:
            result["replication_ingress"] = relay_request(m, value, snapshot, exchange)
            result["capabilities"].append("replication_ingress")
        return result
    if value.get("action") == "replication_ingress":
        require(set(value) == {"action", "node", "pause"} and type(value["pause"]) is bool
                and value["node"] in {n["node_id"] for n in m["nodes"]}, "unsupported ingress request")
        return {"version": 1, "status": "ok", **value,
                "replication_ingress": relay_request(m, value, snapshot, exchange)}
    require(value.get("action") == "signal" and set(value) == {"action", "node", "signal"},
            "unsupported action/fields")
    require(value["signal"] in ("STOP", "CONT"), "only STOP and CONT are supported")
    matches = [n for n in m["nodes"] if n["node_id"] == value["node"]]
    require(len(matches) == 1, "unknown owned node")
    n = matches[0]
    verify_process(m, n, snapshot)  # Immediately before the only signal syscall.
    send(n["pid"], {"STOP": signal.SIGSTOP, "CONT": signal.SIGCONT}[value["signal"]])
    return {"version": 1, "status": "ok", "action": "signal", "signal": value["signal"],
            "node": verify_process(m, n, snapshot)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", required=True)
    args = ap.parse_args()
    try:
        raw = sys.stdin.buffer.read(65537)
        require(len(raw) <= 65536, "request exceeds byte bound")
        result = request(read_manifest(args.manifest), json.loads(raw))
    except Exception as error:
        print(json.dumps({"version": 1, "status": "error", "error": type(error).__name__,
                          "message": str(error) if isinstance(error, ValueError) else "owned broker operation failed"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
