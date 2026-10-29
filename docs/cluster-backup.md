# Cluster-consistent backup and restore

`scripts/cluster_backup.py` supplies the initial **maintenance-window** workflow
for #137. It builds on the physical file copier from #56. It accepts a cut only
after all registered replicas of every active shard are reachable, fenced and
caught up, all distributed writes are physically finished, and DDL, movement
and lazy delivery are settled. It copies native snapshots/run files and retains
all persistent transaction, catalog, ownership and lazy-index metadata.

This is an offline workflow: stop SQL gateways, direct writers, schema jobs,
rebalancers and administration before acquisition. Recovery and lazy delivery
remain available during the initial drain. Restrict administrative credentials
to the controller throughout the operation. An administrator who changes
`read_only`, elections, membership, files or credentials concurrently can break
the maintenance boundary. No tool can protect a backup from a trusted operator
deliberately bypassing its fence.

## Supported scope

- Tarantool **3.9.0**, `wal_mode=fsync`, the same Tarn storage/native build and
  original timing/configuration settings on restore. No rolling format or ABI
  migration is performed.
- memtx and vinyl tables; all registered replicas, not a selected leader or
  quorum subset. Anonymous, offline and unlisted registered replicas are
  refused. Every active logical shard must be present in `TARN_PEERS` or the
  published membership, with complete explicit TCP replica endpoints.
- All serving buckets have exactly one owner. Catalog definitions and published
  membership agree. Partial membership changes, moving/held buckets, staged
  receive rows, DDL jobs/holds/fences, ALTER undo, unfinished lazy builds,
  outboxes/reservations, PREPARED/STAGING records and unsettled synchronous
  queues cause refusal. Drain these with the existing coordinator/recovery
  mechanisms; never delete obligations to make the check pass.
- Persistent local (`is_local`) spaces are refused because they remain writable
  in read-only mode. Temporary rows are excluded from the recovery contract.
- Up to 1,024 listed replicas and one million buckets, with explicit manifest
  and RPC size limits. The audit streams every persistent user/internal tuple
  through SHA-256 and compares every replica within a shard. It consumes time
  proportional to all data, and schema/manifest memory proportional to metadata.
  It is not a constant-time cut, online backup, PB-capacity result or bound on
  whole-process memory.

The controller compares nonlocal WAL vectors and exact row/schema digests
within each replica set. It binds each physical file manifest to the certified
cut and rechecks every fence/vector before publishing `cluster.json`. Checksums
detect accidental corruption; store the manifests and files together in trusted,
access-controlled backup storage. They are not cryptographic attestations of a
hostile administrator or a substitute for durable off-site retention.

## Prepare and capture

Install this storage version on **every** replica before relying on restart
fencing. Use a fresh output directory; existing directories are never overwritten.
Keep secrets in environment variables supplied by your secret manager, not in
the plan, CLI arguments or source control. The plan lists every registered node:

```json
{
  "format": "tarn-cluster-backup-v1",
  "nodes": [
    {"name":"s1r1","shard":1,"uri_env":"BACKUP_S1R1","source":"/srv/tarn/s1r1"},
    {"name":"s1r2","shard":1,"uri_env":"BACKUP_S1R2","source":"/srv/tarn/s1r2"},
    {"name":"s1r3","shard":1,"uri_env":"BACKUP_S1R3","source":"/srv/tarn/s1r3"},
    {"name":"s2r1","shard":2,"uri_env":"BACKUP_S2R1","source":"/srv/tarn/s2r1"},
    {"name":"s2r2","shard":2,"uri_env":"BACKUP_S2R2","source":"/srv/tarn/s2r2"},
    {"name":"s2r3","shard":2,"uri_env":"BACKUP_S2R3","source":"/srv/tarn/s2r3"}
  ]
}
```

Each variable contains a credentialed net.box TCP URI. Endpoint spellings must
match the configured inventory; aliases are not silently treated as one node.
For multiple hosts, include a distinct `host` label when source directory paths
repeat. This label is inventory only, not an SSH destination.

When the controller can access every source directory at its exact storage-host
path, perform the complete capture with:

```sh
python3 scripts/cluster_backup.py create --plan cluster-plan.json \
  --output /backups/cut-001 --timeout 60 --rate-mib 64
```

For separate storage hosts, use the same protocol in three phases:

```sh
# Controller: drain, fence, audit. Retain journal.json even on failure.
python3 scripts/cluster_backup.py fence --plan cluster-plan.json --output /backups/cut-001

# Each storage host: copy its node after receiving the exact cut.json.
# Use the matching URI environment variable and local source path from the plan.
python3 scripts/cluster_backup.py copy-node --cut cut.json --node s1r1 \
  --output /backup-stage/s1r1 --rate-mib 64

# Transfer every completed node directory intact beneath /backups/cut-001.
# Controller: verify copy receipts and the still-frozen live cluster.
python3 scripts/cluster_backup.py seal --cut /backups/cut-001/cut.json --output /backups/cut-001
```

The output contains `journal.json` before the first fence and `cut.json` only
after certification. Each node's `cluster-node.json` binds its physical manifest
to that cut. Only a complete `cluster.json` without the parent `INCOMPLETE`
marker is accepted for restore. Do not mix cuts or copy directories from different
attempts. Separate-host copies may run concurrently; enforce an aggregate I/O
budget outside the individual per-agent rate limits.

`--timeout` bounds each drain/catch-up polling phase, with a bounded RPC deadline
per observation. It does not bound total downtime: tuple auditing and physical
copying take additional time, and a crashed controller leaves the cluster fenced.
The file copier has a one-day maximum GC lease; expiry invalidates a copy. Size
the maintenance window from actual data/file bytes and measured audit/copy rates.
An inability to drain is a failed backup, not permission to omit work.

## Failures and source recovery

Every fence writes and fsyncs `tarn-maintenance.json` in that replica's data
directory before disabling writes and elections. The new startup script honors
that marker after a crash and skips application initialization. There is no TTL
that silently makes storage writable. Failed or lost administrative replies are
not retried as mutations. Observe the current state and use the original journal.

After successful publication, or after abandoning an incomplete attempt:

```sh
python3 scripts/cluster_backup.py release --journal /backups/cut-001/journal.json
```

Release checks the owning token and removes source markers. It **leaves all
running nodes read-only**. Restart every source replica with its original
configuration, then verify elected leaders, synchronized replicas, clock
readiness, empty protocol backlog and a checked read/write canary before
restarting gateways/writers. If release loses a reply, inspect again: a missing
marker is reported without repeating that mutation. A foreign token is refused.

A partially fenced cluster may have new unfinished work when the remaining
replicas stop. No cut is published in that case. Release/restart, allow normal
recovery to settle and begin a new attempt. A completed `cut.json` may resume
missing **file copies** while every original fence and WAL vector still matches;
it may not resume SQL mutations. Preserve original failure logs.

## Restore and activation

Restore to a **fresh isolated network** with no route to original storage and no
application admission. Preserve original instance/replicaset identities, endpoint
names, membership and configuration: this workflow restores the original cluster,
not a new seed replica. Do not connect restored identities to a running original.
Changing peers or silently rewriting persisted membership is unsupported.

The restore controller writes all node directories beneath a fresh parent on its
host. The initial automated drill uses two replica sets in one isolated namespace;
it does not establish independent-host deployment or disaster-recovery capacity.
Keep the parent and child guard files together. Moving individual directories or
distributing an activated tree is outside this automated activation contract.

```sh
python3 scripts/cluster_backup.py restore --backup /backups/cut-001 --output /restore/cut-001
python3 scripts/cluster_backup.py verify-restore --restore /restore/cut-001
python3 scripts/cluster_backup.py activate --restore /restore/cut-001 --isolated-network
```

`restore` checks every file hash and writes each quarantine marker **before**
removing that node's local incomplete marker. The parent stays incomplete until
every replica passes an offline logical audit. Each probe starts without a
listener, replication, elections, Tarn initialization or recovery/GC fibers;
it uses the source memtx memory allocation and may require significant RAM.
Every persistent user/internal space, schema digest, transaction tombstone,
catalog, bucket map, lazy incarnation/version/counter/frontier/floor and WAL
identity must match the cut. An audit failure leaves quarantine intact.

Activation repeats the logical audit before removing quarantine and publishes
`activated.json` only after all markers are removed. Startup refuses a restored
child without its marker until this whole-parent activation file exists. Thus an
interrupted activation cannot expose a partly admitted restore. Do not start
processes or change files concurrently with verification/activation. The command
does not start any service. Start the original configuration in the isolated
network, check all replicas/leaders and exact canaries, then admit gateways.
`--isolated-network` is an operator assertion about network placement, not a
network firewall created by the tool.

## Recovery objective and evidence

RPO is the **certified cut**, not the completion time of file transfer. All commits
acknowledged before the cut must have drained to their user tuples. Unknown client
outcomes may also be present; this tool does not change 40001/40003 or provide a
mutation retry signal. A write acknowledged after source release is outside that
backup and is deliberately absent after restore. RTO includes file copy, full
logical verification, activation, startup, election/clock readiness and canaries.

`tests/cluster_backup.py` records source hashes, settings, every outcome, per-space
row/schema digests, fence-to-seal and source-resume times, copy/verification time
and restore-to-write time. It exercises real pending distributed transactions,
DDL jobs, paused lazy delivery, movement, a mutation held over the fence, leader
crash, killed copy/restore controllers, corruption and a delayed lazy frame after
GC/movement/restore. CI runs memtx/vinyl with native procedures enabled/disabled
and retains failure logs as well as passes. The quiescent policy stops foreground
work during capture; timings from this fault-heavy small same-host fixture are
operational drill observations, not online overhead, production RTO or throughput
claims. See the [source-bound evidence index](../tests/evidence/cluster-backup/index.json).

## Incremental, WAL and PITR design boundary

Independent per-node WAL timestamps or "latest available" snapshots are not a
distributed recovery point. A future incremental scheme needs: an immutable base
cluster-cut identity; per-origin contiguous WAL ranges and hashes on every replica
set; persisted schema/membership/ownership transitions; retention leases spanning
all required origins; and a **cluster** restore frontier that includes every
participant and decision needed by an included distributed transaction. Pending
APPLY, lazy assignments and DDL/movement publication must be carried and resolved
under their original immutable identities before traffic is admitted.

A safe first extension would capture named quiescent end cuts and retain complete
WAL between them, replaying the entire cluster to a named validated end cut in
quarantine. Arbitrary wall-clock PITR requires an additional global admission/
decision frontier or coordinated barrier protocol; wall clocks alone do not
supply it. Missing segments, partial transaction inclusion, old source epochs or
unproven metadata migrations must fail closed. This version does not implement
incremental restore, continuous WAL archiving or arbitrary-time PITR.
