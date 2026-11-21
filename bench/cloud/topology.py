#!/usr/bin/env python3
"""Fleets for the multi-host comparison of Tarn, TiDB and YugabyteDB.

Both the GCP provisioning script (tarn-bench, on the operator's machine) and
the orchestrator (orchestrate.py, on the control node) read this file, so the
names, roles, zones and replica placement are defined once.

    python3 topology.py plan TOPOLOGY [--region R] [--zones a,b,c]   # JSON plan
    python3 topology.py quota                                       # quota needs

Topologies:
  az-rf3      one zone; 3 shards x 3 replicas; 3 SQL nodes; 2 load generators
  region-rf5  three zones; 3 shards x 5 replicas placed 2/2/1, the
              single-replica zone rotating across shards (5 storage nodes per
              zone); 3 SQL nodes and 3 load generators, one per zone

Roles:
  control  orchestration, package cache, results (the only node with an
           external IP and SSH ingress)
  storage  a Tarantool replica / a TiKV store / a YugabyteDB tserver
           (+ master on the first RF of them), local NVMe SSD at /data
  sql      tarn-server / tidb-server + PD (YugabyteDB has no separate tier)
  load     sysbench

Stdlib only: it also runs on the operator's Mac.
"""
import argparse
import json
import sys

SHARDS = 3
STORAGE_MACHINE = "n2d-standard-8"   # 8 vCPU, 32 GB; + one 375 GB local NVMe SSD
SQL_MACHINE = "n2d-standard-8"
LOAD_MACHINE = "n2d-standard-16"
CONTROL_MACHINE = "n2d-standard-8"
VCPUS = {"n2d-standard-8": 8, "n2d-standard-16": 16}
LOCAL_SSD_GB = 375

TOPOLOGIES = {
    "az-rf3": {"rf": 3, "zones": 1, "sql": 3, "load": 2},
    "region-rf5": {"rf": 5, "zones": 3, "sql": 3, "load": 3},
}
DEFAULT_REGION = "us-central1"
DEFAULT_ZONE_SUFFIXES = ["a", "b", "c"]


def replica_zones(rf, zones, shard):
    """Zone index of each replica of `shard` (1-based)."""
    if len(zones) == 1:
        return [0] * rf
    assert rf == 5 and len(zones) == 3, "only 2/2/1 placement over three zones is defined"
    # Shard 1: a a b b c (singleton c); shard 2: b b c c a; shard 3: c c a a b.
    k = (shard - 1) % 3
    return [k, k, (k + 1) % 3, (k + 1) % 3, (k + 2) % 3]


def plan(name, region=DEFAULT_REGION, zones=None):
    if name not in TOPOLOGIES:
        raise SystemExit(f"unknown topology {name!r}; known: {', '.join(TOPOLOGIES)}")
    t = TOPOLOGIES[name]
    if zones is None:
        zones = [f"{region}-{s}" for s in DEFAULT_ZONE_SUFFIXES[: t["zones"]]]
    if len(zones) != t["zones"]:
        raise SystemExit(f"{name} needs {t['zones']} zone(s), got {zones}")
    rf = t["rf"]
    hosts = [{"name": "control", "role": "control", "zone": zones[0], "machine": CONTROL_MACHINE}]
    for s in range(1, SHARDS + 1):
        for r, z in enumerate(replica_zones(rf, zones, s), start=1):
            hosts.append({"name": f"s{s}r{r}", "role": "storage", "zone": zones[z],
                          "machine": STORAGE_MACHINE, "local_ssd": 1, "shard": s, "replica": r})
    for k in range(1, t["sql"] + 1):
        # (200 GB boot disk: TiDB's DDL ingest stages index data on /data.)
        hosts.append({"name": f"sql{k}", "role": "sql", "zone": zones[(k - 1) % len(zones)],
                      "machine": SQL_MACHINE, "index": k, "boot_disk_gb": 200})
    for k in range(1, t["load"] + 1):
        hosts.append({"name": f"load{k}", "role": "load", "zone": zones[(k - 1) % len(zones)],
                      "machine": LOAD_MACHINE, "index": k})
    return {"topology": name, "region": region, "zones": zones, "rf": rf, "shards": SHARDS,
            "hosts": hosts}


def preferred_leaders(p):
    """Replica 1 of every shard: always in a zone that holds two replicas,
    so its quorum needs one acknowledgment from another zone (see README)."""
    return {s: f"s{s}r1" for s in range(1, p["shards"] + 1)}


def quota(p):
    cpus = sum(VCPUS[h["machine"]] for h in p["hosts"])
    ssd = sum(h.get("local_ssd", 0) for h in p["hosts"]) * LOCAL_SSD_GB
    return {"N2D_CPUS": cpus, "LOCAL_SSD_TOTAL_GB": ssd, "INSTANCES": len(p["hosts"]),
            "IN_USE_ADDRESSES": 1}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("plan")
    a.add_argument("topology")
    a.add_argument("--region", default=DEFAULT_REGION)
    a.add_argument("--zones")
    q = sub.add_parser("quota")
    q.add_argument("--plan", help="a plan.json: print its needs as JSON")
    args = ap.parse_args()
    if args.cmd == "plan":
        zones = args.zones.split(",") if args.zones else None
        json.dump(plan(args.topology, args.region, zones), sys.stdout, indent=1)
        print()
    elif args.plan:
        json.dump(quota(json.load(open(args.plan))), sys.stdout)
        print()
    else:
        both = {}
        for name in TOPOLOGIES:
            q = quota(plan(name))
            print(f"{name}: {q}")
            for k, v in q.items():
                both[k] = both.get(k, 0) + v
        print(f"both at once, in {DEFAULT_REGION}: {both}")


if __name__ == "__main__":
    main()
