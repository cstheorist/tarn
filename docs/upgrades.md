# Storage format upgrades

This release introduces **storage format 2** for bounded bucket movement:
synchronous Vinyl staging, durable page receipts and resource reservations,
and non-serving install/reclaim/cancel states. User tuple layouts and native
layout **14/392** remain unchanged. This is a **maintenance upgrade**, with
all gateways and storage processes stopped at the transition. It does not
preserve the previous release's format-1 rolling upgrade/rollback promise.

The tested predecessor is exactly
`8b12c4525bd677499a91b973afbc86cbcbb0cafd`, which has the format-1 admission
guard. Keep Tarantool fixed at **3.9.0**, including a `3.9.0-…` build suffix.
Other releases and Tarantool upgrades require separate validation. In
particular, `2e8aaeb` predates the disk-format guard: never start it on a
format-2 directory, even if it appears to open the files.

The new gateway also rejects writes to predecessor-created tables by default:
old catalogs lack declaration provenance, including discarded CHECK/ENUM/SET
history. Inspect `SHOW TARN INTEGRITY` and follow [the migration contract](integrity.md)
before replacing gateways. A deliberate session-only `tarn_legacy_integrity='ALLOW'`
acknowledges the old unenforced behavior; it is never enabled automatically.
This SQL admission boundary is separate from format-2 disk admission; an old
gateway is refused against the upgraded storage.
The upgrade matrix checks default refusal without an effect before explicitly
acknowledging only its owned, known-schema legacy canary sessions. This is a
SQL admission change, not a data-validation certificate or format conversion.

## Compatibility matrix

| Gateway | Storage | Admission |
|---|---|---|
| Tested format-1 predecessor | Format 1 | Existing behavior |
| Tested format-1 predecessor | This release / format 2 | Refused before user RPCs |
| This release | Format 1 or missing compatibility RPC | Refused before user RPCs |
| This release | This release / format 2 | Protocol 1, format 2, native layout 14 |
| This release | This release with predecessor ABI 14 module | Supported by the unchanged native state/RPC layout; tested explicitly |
| This release | This release with native disabled, missing or incompatible | Explicit Lua fallback; same SQL contract |
| Any supported gateway | Unknown format, protocol, runtime, shard or expiry contract | Refused; no unsafe override |

Deploy Lua and its native module as an immutable release directory. Restart
to replace them. Hot reload and changing format markers manually are
unsupported. Physical pool connections cache compatibility admission for
their lifetime; every new or replacement connection negotiates before
carrying user work. Compatibility and role probes use the reserved
completion lane so overload cannot prevent discovery of decided work.

`tarn_compatibility()` returns the fixed eight-field array:

```text
["ok", protocol, storage_format, shard_id, tarantool_version,
 native_state_layout, native_status, max_txn_age]
```

It reports protocol **1**, persisted format **2**, native layout **14**, and
the configured transaction age (120 seconds by default). Native status is
`ready`, `disabled`, `load_failed`, or `abi_mismatch`; `pending` is refused
by gateways. Storage refuses the report until the new staging/progress
spaces exist. The report is neither a quorum/freshness barrier nor a
mutation receipt. Confirmed conflicts remain **1213/40001**; uncertain
mutation effects remain **1180/40003** and must never be blindly replayed.

## Maintenance sequence

1. Record release/configuration/native hashes, catalog and bucket map,
   membership and lazy generations. Retain the old release and a verified
   format-1 backup. Confirm healthy RF3 quorums and following replicas.
2. Stop accepting new sessions and drain gateways. Resolve prepared
   transactions, staging decisions, DDL/topology jobs, bucket moves, lazy
   builds, reservations and outboxes. Retain decided completion records;
   they need not be erased. Check clock health and transaction-age settings.
3. Stop **all gateways and storage processes**. Preserve every replica's
   identity, work directory, replication configuration and seeded-voter
   gate. Install the new immutable release on every replica.
4. Start the new storage processes. Before setup/recovery, format-1
   admission checks reject unfinished locks, prepared/staging work,
   movement, DDL/topology metadata and lazy work. A missing marker is
   treated as format 1; unknown or coerced markers are refused. Correct a
   refusal by reopening with the old release and draining, only while the
   marker still says 1. Never force or edit the marker.
5. The writable leader synchronously persists **format 2 before any new
   movement representation**, then replaces the empty memtx receiver with
   Vinyl (preserving its ID) and creates `_tarn_move`. The first metadata
   write on a fresh shard still waits for joining replicas. An interrupted
   migration resumes idempotently with the new release. From this marker
   onward, downgrade is forbidden, including if later DDL was interrupted.
6. Wait for every replica to follow and report the complete format-2
   schema. Start new gateways with distinct node IDs. Verify exact SQL and
   native committed rows, single/distributed writes, rollback, DDL, lazy
   index results and a bounded move on disposable data. Exercise a leader
   handoff and check new connection admission. Monitor prepared age,
   decision cleanup, replication, lazy delivery and clock health.

Movement additionally requires every configured replica of both shards to
advertise movement protocol 2 and sufficient initial disk headroom. An
existing durable reservation may resume with a failed voter while quorum
remains available. A format-2 snapshot preserves Vinyl staging and progress;
cluster-consistent backup refuses unfinished movement and other outstanding
work. Restore format-2 files only with this or a separately verified
compatible release. See [cluster backup](cluster-backup.md) and
[bounded movement](bounded-moves.md).

## Rollback and validation boundary

Before the format marker advances, the drained predecessor can reopen the
unchanged files. After it advances, its startup check refuses the directory
before Tarn schema setup or recovery. Restore the preserved **format-1
backup into isolated directories** to return to old software, reconciling
any later acknowledged writes separately. There is no in-place downgrade
or reverse conversion. Do not use a pre-guard binary to bypass this boundary.

Run the pinned-toolchain, Linux/Tarantool/Python/PyMySQL regression:

```sh
git fetch origin 8b12c4525bd677499a91b973afbc86cbcbb0cafd
bash tests/rolling_upgrade.sh
ENGINE=vinyl bash tests/rolling_upgrade.sh
```

The historical script name is retained. It builds immutable old/new artifacts
in separate target directories and verifies receipts binding all compiled
inputs to binary hashes. The RF3/fsync fixture checks fresh bootstrap,
format-1 canaries, both gateway admission directions, maintenance migration,
post-migration DDL/moves/lazy results, leader handoff, snapshot/WAL reopening,
and actual predecessor downgrade refusal. A separate fixture retains
prepared work, verifies refusal before advancing format, then drains under
old software and completes migration. Native controls cover both ABI-14
modules, disabled native, loader failure and incompatible ABI fallback.
The startup test interrupts schema creation after observing the durable
format marker and checks successful resume, as well as unknown/coerced
format and unfinished-job refusal.

Each run retains source/binary/configuration identities, build logs,
`manifest.json`, `events.jsonl` and process logs, including failed runs. CI
runs memtx and vinyl and uploads evidence on failure too. These are bounded,
same-host functional drills; they do not establish rolling availability,
independent failure domains or production throughput. The movement and
commit TLA+ models cover movement visibility and transaction obligations;
they do not prove filesystem migration or Tarantool's implementation.
