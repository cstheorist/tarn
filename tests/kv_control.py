#!/usr/bin/env python3
"""Linux-only STOP/CONT broker for the fresh, owned tests/kv.sh fixture.

One JSON request on stdin, one JSON response on stdout. No service lifecycle,
arbitrary signal, host selection, relay or shell command is exposed here.
"""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys

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
    info.update(uid=p.stat().st_uid, cwd=os.readlink(p / "cwd"), exe=os.readlink(p / "exe"),
                environ=(p / "environ").read_bytes().split(b"\0"))
    return info


def verify_process(m, n, snapshot=proc_snapshot):
    parent = snapshot(m["launcher_pid"])
    require(parent["pid"] == m["launcher_pid"] and parent["starttime"] == m["launcher_starttime"]
            and parent["uid"] == os.getuid(), "launcher identity changed")
    p = snapshot(n["pid"])
    require(p["pid"] == n["pid"] and p["ppid"] == n["ppid"] and p["starttime"] == n["starttime"]
            and p["uid"] == os.getuid() and p["state"] != "Z", "node process identity changed")
    require(p["cwd"] == n["workdir"] and p["exe"] == n["exe"], "node executable/workdir changed")
    for key, value in (("TARN_KV_OWNER_NONCE", m["ownerNonce"]), ("TARN_KV_NODE_ID", n["node_id"]),
                       ("TARN_KV_WORKDIR", n["workdir"])):
        require((key + "=" + value).encode() in p["environ"], "node environment owner changed")
    return {**n, "state": p["state"], "identity_verified": True}


def request(m, value, snapshot=proc_snapshot, send=os.kill):
    require(isinstance(value, dict), "request must be an object")
    if value.get("action") == "inspect":
        require(set(value) == {"action"}, "unsupported inspect fields")
        return {"version": 1, "status": "ok", "capabilities": ["signal"],
                "nodes": [verify_process(m, n, snapshot) for n in m["nodes"]]}
    require(value.get("action") == "signal" and set(value) == {"action", "node", "signal"},
            "unsupported action/fields; relay is unavailable")
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
