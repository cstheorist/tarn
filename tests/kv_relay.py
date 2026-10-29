#!/usr/bin/env python3
"""Fixed-loopback replication-ingress relay for the owned KV test fixture.

Only server-to-client bytes can be gated. Counters contain no payload bytes.
This does not partition direct client, freshness, clock or recovery RPCs.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import signal
import stat

NODES = {f"s{s}r{r}" for s in (1, 2) for r in (1, 2, 3)}
CHUNK = 65536
MAX_SESSIONS_PER_FLOW = 4


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def endpoint(value):
    require(isinstance(value, str) and re.fullmatch(r"127\.0\.0\.1:[0-9]{1,5}", value),
            "relay endpoints must be fixed loopback TCP")
    host, port = value.rsplit(":", 1)
    require(0 < int(port) < 65536, "invalid relay port")
    return host, int(port)


def validate_flows(flows):
    require(isinstance(flows, list) and len(flows) == 12, "expected twelve directed replica flows")
    pairs, listeners, targets = set(), set(), {}
    for f in flows:
        require(isinstance(f, dict) and set(f) == {"id", "client_node", "server_node", "listen", "target"},
                "invalid fixed relay flow fields")
        client, server = f["client_node"], f["server_node"]
        require(client in NODES and server in NODES and client != server and client[1] == server[1],
                "relay flow must join distinct replicas of the same shard")
        require(f["id"] == client + "<-" + server and (client, server) not in pairs, "duplicate/invalid flow ID")
        endpoint(f["listen"]); endpoint(f["target"])
        require(f["listen"] not in listeners, "duplicate relay listener")
        require(server not in targets or targets[server] == f["target"], "inconsistent physical target")
        pairs.add((client, server));listeners.add(f["listen"]);targets[server] = f["target"]
    require(len(pairs) == 12 and len(targets) == len(set(targets.values())) == 6,
            "replica target identities are not distinct")
    require(not listeners.intersection(targets.values()), "relay listener collides with physical target")
    return flows


def load_config(path):
    p = Path(path)
    s = p.lstat()
    require(stat.S_ISREG(s.st_mode) and s.st_uid == os.getuid()
            and stat.S_IMODE(s.st_mode) == 0o600 and s.st_size <= 65536, "invalid private relay config")
    c = json.loads(p.read_text())
    require(set(c) == {"version", "ownerNonce", "workdir", "control_socket", "flows"}
            and c["version"] == 1, "invalid relay config fields/version")
    root = Path(c["workdir"]);s = root.lstat()
    require(root == p.parent and root.resolve() == root.absolute()
            and root.name.startswith("tarn-kv181-") and stat.S_ISDIR(s.st_mode)
            and s.st_uid == os.getuid() and stat.S_IMODE(s.st_mode) == 0o700, "invalid relay owner directory")
    require(re.fullmatch(r"[0-9a-f]{32}", c["ownerNonce"]), "invalid relay owner nonce")
    require(c["control_socket"] == str(root / "replication-control.sock"), "invalid relay control socket")
    require(os.environ.get("TARN_KV_OWNER_NONCE") == c["ownerNonce"]
            and os.environ.get("TARN_KV_NODE_ID") == "relay"
            and os.environ.get("TARN_KV_WORKDIR") == str(root / "relay")
            and Path.cwd() == root / "relay", "relay process owner markers differ")
    validate_flows(c["flows"])
    return c


class Relay:
    def __init__(self, config):
        self.config = config
        self.epoch = 0
        self.flows = {}
        self.sessions = set()
        self.controls = 0
        for f in config["flows"]:
            gate = asyncio.Event();gate.set()
            self.flows[f["id"]] = {**f, "gate": gate, "paused": False, "connections": 0,
                "accepted": 0, "refused": 0, "errors": 0, "connect_failures": 0,
                "queued_s2c": 0, "queued_c2s": 0, "drained_s2c": 0, "drained_c2s": 0,
                "last_error_type": None}

    def snapshot(self):
        return {"version": 1, "status": "ok", "ownerNonce": self.config["ownerNonce"],
                "pid": os.getpid(), "epoch": self.epoch,
                "flows": [{k: v for k, v in f.items() if k != "gate"} for f in self.flows.values()]}

    def control(self, value):
        require(isinstance(value, dict) and value.get("version") == 1
                and value.get("ownerNonce") == self.config["ownerNonce"], "invalid relay control owner")
        if value.get("action") == "inspect":
            require(set(value) == {"version", "ownerNonce", "action"}, "invalid relay inspect fields")
            return self.snapshot()
        require(set(value) == {"version", "ownerNonce", "action", "node", "pause"}
                and value["action"] == "replication_ingress" and value["node"] in NODES
                and type(value["pause"]) is bool, "invalid ingress control")
        flows = [f for f in self.flows.values() if f["client_node"] == value["node"]]
        require(len(flows) == 2, "missing incoming replica flows")
        # No await: gate changes, epoch and counter snapshot are one event-loop turn.
        for f in flows:
            f["paused"] = value["pause"]
            if value["pause"]: f["gate"].clear()
            else: f["gate"].set()
        self.epoch += 1
        return self.snapshot()

    async def pump(self, reader, writer, f, direction):
        while True:
            if direction == "s2c": await f["gate"].wait()
            data = await reader.read(CHUNK)
            if not data: return
            if direction == "s2c": await f["gate"].wait()
            # write and queued counter cannot be separated by a control callback.
            writer.write(data)
            f["queued_" + direction] += len(data)
            await writer.drain()
            f["drained_" + direction] += len(data)

    @staticmethod
    async def close(writer):
        if writer is None: return
        writer.close()
        try: await asyncio.wait_for(writer.wait_closed(), 1)
        except (OSError, asyncio.TimeoutError): pass

    async def session(self, reader, writer, flow_id):
        f = self.flows[flow_id]
        if f["connections"] >= MAX_SESSIONS_PER_FLOW:
            f["refused"] += 1;await self.close(writer);return
        f["connections"] += 1;f["accepted"] += 1
        task = asyncio.current_task();self.sessions.add(task)
        upstream, pumps = None, []
        try:
            try:
                remote, upstream = await asyncio.wait_for(asyncio.open_connection(*endpoint(f["target"]), limit=CHUNK), 3)
            except (OSError, asyncio.TimeoutError) as error:
                f["connect_failures"] += 1;f["last_error_type"] = type(error).__name__;return
            for w in (writer, upstream): w.transport.set_write_buffer_limits(high=CHUNK, low=CHUNK // 4)
            pumps = [asyncio.create_task(self.pump(reader, upstream, f, "c2s")),
                     asyncio.create_task(self.pump(remote, writer, f, "s2c"))]
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for p in done: p.result()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            f["errors"] += 1;f["last_error_type"] = type(error).__name__
        finally:
            for p in pumps:
                if not p.done(): p.cancel()
            if pumps: await asyncio.gather(*pumps, return_exceptions=True)
            await self.close(upstream);await self.close(writer)
            f["connections"] -= 1;self.sessions.discard(task)

    async def control_session(self, reader, writer):
        if self.controls >= 16:
            await self.close(writer);return
        self.controls += 1
        try:
            raw = await asyncio.wait_for(reader.readline(), 2)
            require(len(raw) <= 4096 and raw.endswith(b"\n"), "invalid relay control frame")
            result = self.control(json.loads(raw))
        except Exception as error:
            result = {"version": 1, "status": "error", "error": type(error).__name__,
                      "message": str(error) if isinstance(error, ValueError) else "relay control failed"}
        try:
            writer.write((json.dumps(result, sort_keys=True) + "\n").encode())
            await asyncio.wait_for(writer.drain(), 2)
        finally:
            self.controls -= 1;await self.close(writer)


async def serve(config):
    relay = Relay(config);servers = [];stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, stop.set)
    socket_path = Path(config["control_socket"]);socket_identity = None
    require(not socket_path.exists() and not socket_path.is_symlink(), "control socket already exists")
    try:
        for f in config["flows"]:
            async def accept(reader, writer, flow_id=f["id"]): await relay.session(reader, writer, flow_id)
            servers.append(await asyncio.start_server(accept, *endpoint(f["listen"]), limit=CHUNK))
        server = await asyncio.start_unix_server(relay.control_session, path=str(socket_path), limit=4096, start_serving=False)
        servers.append(server);socket_path.chmod(0o600)
        s = socket_path.lstat();socket_identity = (s.st_dev, s.st_ino)
        await server.start_serving()
        print(json.dumps({"version": 1, "status": "ready", "pid": os.getpid(), "flows": len(relay.flows)}), flush=True)
        await stop.wait()
    finally:
        for server in servers: server.close()
        for server in servers: await server.wait_closed()
        for task in list(relay.sessions): task.cancel()
        if relay.sessions: await asyncio.gather(*list(relay.sessions), return_exceptions=True)
        if socket_identity is not None and socket_path.exists():
            s = socket_path.lstat()
            if stat.S_ISSOCK(s.st_mode) and (s.st_dev, s.st_ino) == socket_identity: socket_path.unlink()
        print(json.dumps({"event": "relay_shutdown", "snapshot": relay.snapshot()}, sort_keys=True), flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    os.umask(0o077)
    asyncio.run(serve(load_config(args.config)))


if __name__ == "__main__":
    main()
