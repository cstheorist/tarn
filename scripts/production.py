#!/usr/bin/env python3
"""Render an explicit inventory, launch a storage node, or run deployment health.
No credentials are accepted in argv or generated into the public configuration.
See docs/production.md. Linux is the supported production host.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def private_dir(path):
    path = Path(path)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    s = path.lstat()
    if (
        not path.is_absolute()
        or not path.is_dir()
        or path.is_symlink()
        or s.st_uid != os.geteuid()
        or s.st_mode & 0o077
    ):
        raise ValueError(
            "service directory must be absolute, private, and owned by the service UID"
        )


def render(inventory, output):
    inv = json.loads(Path(inventory).read_text())
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    nodes = inv["nodes"]
    domains = {n["uri"]: n["failure_domain"] for n in nodes}
    if len(domains) != len(nodes):
        raise ValueError("duplicate TLS endpoint")
    peers = {}
    names = set()
    for n in nodes:
        if (
            n["name"] in names
            or not n["name"].replace("_", "").replace("-", "").isalnum()
        ):
            raise ValueError("node names must be distinct file-safe names")
        names.add(n["name"])
        peers.setdefault(str(n["shard_id"]), []).append(n["uri"])
    for replicas in peers.values():
        if (
            len(replicas) < 3
            or len(replicas) % 2 == 0
            or len({domains[r] for r in replicas}) != len(replicas)
        ):
            raise ValueError(
                "each shard needs odd RF >=3 and distinct declared failure domains"
            )
    tls = inv["tls"]
    for n in nodes:
        private = Path(n["socket_dir"])
        routes = {
            r["uri"]: str(private / ("storage.sock" if r is n else r["name"] + ".sock"))
            for r in nodes
        }
        config = dict(
            version=1,
            shard_id=n["shard_id"],
            workdir=n["workdir"],
            public_uri=n["uri"],
            failure_domain=n["failure_domain"],
            failure_domains=domains,
            routes=routes,
            listen_unix=routes[n["uri"]],
            replicas=peers[str(n["shard_id"])],
            peers=peers,
            service_user="tarn_service",
            replication_user="tarn_repl",
            service_password_file=inv["service_password_file"],
            replication_password_file=inv["replication_password_file"],
            memtx_mb=inv["memtx_mb"],
            vinyl_memory_mb=inv["vinyl_memory_mb"],
            vinyl_cache_mb=inv["vinyl_cache_mb"],
            host=inv["storage_host"],
        )
        dump(output / (n["name"] + ".storage.json"), config)
        client = dict(
            ca_file=tls["ca_file"], cert_file=n["cert_file"], key_file=n["key_file"]
        )
        bridge = dict(
            max_connections=inv["bridge_max_connections"],
            servers=[
                dict(
                    listen=n["uri"].removeprefix("tls://"),
                    upstream_unix=routes[n["uri"]],
                    tls=dict(
                        cert_file=n["cert_file"],
                        key_file=n["key_file"],
                        client_ca_file=tls["ca_file"],
                    ),
                )
            ],
            clients=[
                dict(listen_unix=routes[r["uri"]], upstream=r["uri"], tls=client)
                for r in nodes
                if r is not n
            ],
        )
        dump(output / (n["name"] + ".tls.json"), bridge)
    cfg = dict(
        shards=[
            dict(id=int(s), replicas=r)
            for s, r in sorted(peers.items(), key=lambda p: int(p[0]))
        ],
        user="tarn_service",
        password_file=inv["service_password_file"],
        storage_tls=tls,
        node_id=inv["gateway_node_id"],
        pool_size=inv["pool_size"],
    )
    dump(output / "cluster.json", cfg)
    dump(
        output / "gateway.production.json",
        dict(
            mysql_tls=inv["mysql_tls"],
            users_file=inv["users_file"],
            failure_domains=domains,
            max_connections=inv["gateway_max_connections"],
            host=inv["gateway_host"],
        ),
    )


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def storage(config, tarantool):
    c = json.loads(Path(config).read_text())
    private_dir(c["workdir"])
    private_dir(Path(c["listen_unix"]).parent)
    # Reject inherited development switches, rather than silently mask them.
    allowed = {"TARN_LOG", "TARN_LOG_LEVEL", "TARN_NATIVE"}
    unexpected = sorted(
        k for k in os.environ if k.startswith("TARN_") and k not in allowed
    )
    if unexpected:
        raise ValueError("unexpected inherited Tarn options: " + ", ".join(unexpected))
    env = dict(
        os.environ,
        TARN_PROFILE="production",
        TARN_PRODUCTION_CONFIG=str(Path(config).resolve()),
        TARN_SHARD_ID=str(c["shard_id"]),
        TARN_WORKDIR=c["workdir"],
        TARN_INSTANCE_NAME="shard"
        + str(c["shard_id"])
        + "_"
        + Path(config).stem.replace(".", "_"),
        TARN_REPLICAS=json.dumps(c["replicas"]),
        TARN_PEERS=json.dumps(c["peers"]),
    )
    os.umask(0o077)
    os.execvpe(tarantool, [tarantool, str(ROOT / "lua/storage.lua")], env)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    r = sub.add_parser("render")
    r.add_argument("inventory")
    r.add_argument("output")
    s = sub.add_parser("storage")
    s.add_argument("config")
    s.add_argument("--tarantool", default="tarantool")
    h = sub.add_parser("health")
    h.add_argument("config_dir")
    h.add_argument("--gateway", default=str(ROOT / "target/release/tarn-server"))
    a = p.parse_args()
    if a.action == "render":
        render(a.inventory, a.output)
    elif a.action == "storage":
        storage(a.config, a.tarantool)
    else:
        d = Path(a.config_dir)
        subprocess.run(
            [
                a.gateway,
                "--config",
                str(d / "cluster.json"),
                "--production",
                str(d / "gateway.production.json"),
                "--check-deployment",
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
