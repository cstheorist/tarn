# Multi-host comparison on GCP: Tarn, TiDB, YugabyteDB

Two fleets, each brought up for one run of at most six hours and deleted by
Compute Engine itself at the deadline:

| Topology | Zones | Storage | SQL | Load | Replicas per shard |
|---|---|---|---|---|---|
| `az-rf3` | 1 | 9 × n2d-standard-8 + 375 GB NVMe | 3 × n2d-standard-8 | 2 × n2d-standard-16 | 3 |
| `region-rf5` | 3 | 15 (5 per zone) | 3 (one per zone) | 3 (one per zone) | 5, placed 2/2/1 |

Each has a control node (n2d-standard-8). Three shards for Tarn. In
`region-rf5` every shard has two replicas in two zones and one in the third,
and the single-replica zone rotates (shard 1: a a b b c; shard 2: b b c c a;
shard 3: c c a a b). `python3 topology.py plan region-rf5` prints a fleet.

## Who does what

1. **You, once:** a GCP project used only for this, with billing, the Compute
   Engine API enabled and a budget alert. Request quota in the region
   (`./tarn-bench quota`): N2D CPUs 136 (`az-rf3`), 200 (`region-rf5`), 336
   for both at once; local SSD 3,375 / 5,625 / 9,000 GB. Use a Google
   account whose sign-in asks for your passkey (Touch ID). Put
   `PROJECT=...` in `~/tarn-bench/config`.
2. **You, per run:** `./tarn-bench up az-rf3` (or `region-rf5`). It signs
   you in (browser; use a private window so the passkey is asked for), creates
   the network and VMs, writes `~/tarn-bench/runs/RUN/{hosts.json,ssh_config,
   id_ed25519}`, revokes the credentials, and copies the bundle to the control
   node. From then on only that SSH key exists, and it reaches only these VMs.
3. **Claude:** through your computer, `ssh -F .../ssh_config control` and
   `orchestrate.py all` (or step by step), copying results back after each
   phase. Claude never holds cloud credentials: the VMs have no service
   account, so nothing on them can call a cloud API.
4. **The deadline:** every VM has `--max-run-duration` (6 h) and
   `--instance-termination-action=DELETE`. Nothing has to be running for it
   to happen. `./tarn-bench down RUN` ends a run early; `./tarn-bench sweep`
   removes anything left from any run (networks and firewall rules, free).

## The bundle

`build-bundle.sh OUTDIR` builds what the nodes run, on Ubuntu 24.04 x86_64
(the VMs' image): Tarantool 3.9.0, tarn-server and `libtarn_storage.so`,
sysbench 1.0.20 and YugabyteDB's sysbench fork, the .debs sysbench needs
(workers have no internet), Tarn's Lua and scripts, and this directory. Its
`MANIFEST` lists every version and checksum. TiDB and YugabyteDB are not in
it: the control node downloads them at `prepare` at the versions pinned in
`versions.env` (tiup checks its mirror's signatures; the YugabyteDB tarball
is checked against the vendor's SHA-256) and copies them to their nodes.

## The run (`orchestrate.py`)

`prepare` installs everything; then `system RUN` for tarn, tidb, yb and
tarn-fsync: deploy, load (`TABLES` × `SIZE`, the load verified by counting
every table), the sysbench matrix with every load generator at once, a second
isolation variant, `node-kill` and (region) `zone-cut`, logs, teardown.
`tarn-fsync` is Tarn again with fsync on every commit, writing workloads
only. `orchestrate.py estimate` prints the time it takes: about 4.6 h with
the defaults (16 × 1,000,000 rows, 16/64/256/512 threads, 60 s per point).
`all` skips a run that would not finish 20 minutes before the deadline,
which is why the optional tarn-fsync pass comes last.

Results go to `/data/results/RUN/SYSTEM/` on the control node:
`bench.csv` (one row per workload and thread count; latency percentiles are
exact, from the merged histograms of all generators), `load.json`,
`fault-KIND.csv` (throughput and errors per second through the fault),
`faults.jsonl`, `raw/` (every sysbench output) and `logs/`.

### Choices that affect the comparison

- **Deployment.** Each system as its guides deploy it on such hosts: Tarn's
  storage on the storage nodes and tarn-server on the SQL nodes; TiKV on the
  storage nodes, PD and tidb-server on the SQL nodes; YugabyteDB's tservers
  (which serve SQL themselves) on the storage nodes and RF masters on shard 1's
  replica hosts, so the SQL nodes are idle in its turn. Compare per system
  and report the vCPUs each used.
- **Isolation.** Tarn strictly serializable, then READ COMMITTED; TiDB
  snapshot isolation (its default), then READ COMMITTED; YugabyteDB its
  default, then SERIALIZABLE.
- **Durability.** Each system's default: Tarn writes its WAL without fsync
  per commit; TiKV syncs its Raft log; YugabyteDB syncs on an interval. The
  `tarn-fsync` pass measures Tarn with fsync on every commit.
- **Leaders.** Tarn's leaders are pinned to replica 1 of each shard, in a
  zone with two of its replicas (a quorum then needs one other zone, as for
  RF 3). TiDB and YugabyteDB place leaders themselves; zone labels are set
  for both.
- **Loading.** One sysbench thread per table, except YugabyteDB, which
  loads with one thread: concurrent CREATE TABLE / INDEX fail there with
  40001 on its catalog.
- **Tarn's admission budgets** are raised (`query_bytes` 512 MiB) so that
  sysbench's multi-MB bulk INSERTs are admitted (docs/resources.md).
- **Faults** run `oltp_read_write` at 64 threads per generator with every
  error ignored and counted, so the timeline continues through the failure,
  and with plain (not prepared) statements, which survive reconnecting.
  YugabyteDB's sysbench fork has no such option: `patches/` adds
  `--pgsql-ignore-errors=all`. `node-kill` kills shard 1's leader for Tarn and
  the store on `s1r1` for the others. `zone-cut` drops all traffic between
  the first zone and the rest (control excluded) for a third of the run;
  the generators outside it use only the SQL endpoints outside it (as clients
  with health-checked endpoints would), so the timeline shows the database's
  failover rather than connections hung on unreachable hosts.

## Rehearsal

`rehearse/fleet.sh up DIR` builds a fake fleet on one Linux machine (network
namespaces with their own sshd, iptables, `/data` and `/tmp`), and
`REHEARSAL=1 orchestrate.py --hosts DIR/hosts.json --key DIR/id_ed25519 ...`
runs unchanged against it. It found, before any money was spent: Tarn's
admission budget refusing sysbench's load; sysbench exiting 0 when a loader
thread died; fault runs that ended at the first error; TiDB's DDL ingest
needing a temp-dir larger than the default boot disk; a health check that
passed while data was still unavailable; YugabyteDB's initdb needing the
en_US.UTF-8 locale; its concurrent loading failing on catalog conflicts.
