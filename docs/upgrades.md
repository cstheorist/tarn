# Rolling upgrades

The supported predecessor is exactly
`2e8aaeb772542968e503212a143b2e6f8d27b8ab` (the merged statistics/freshness release, including cluster backup and current
persisted clock/retention, capture binding and engine-cut contracts). This release adds bounded query resource admission, connection compatibility
admission and a format marker. Oversized work may be refused with 1041/HY000
before commit under the configured resource policy; tune and validate that
policy before replacing gateways. The upgrade does not change stored user tuples, transaction records, lock semantics,
move/lazy metadata, or the native state layout. Catalog definitions add
`integrity_policy` declaration provenance; older gateways ignore it. Other historical
versions require a separate compatibility test before deployment.

The new gateway also rejects writes to predecessor-created tables by default:
old catalogs lack declaration provenance, including discarded CHECK/ENUM/SET
history. Inspect `SHOW TARN INTEGRITY` and follow [the migration contract](integrity.md)
before replacing gateways. A deliberate session-only `tarn_legacy_integrity='ALLOW'`
acknowledges the old unenforced behavior; it is never enabled automatically.
Mixed older gateways continue their old policy, and rollback restores that policy.
The upgrade matrix checks default refusal without an effect before explicitly
acknowledging only its owned, known-schema legacy canary sessions. This is a
SQL admission change, not a data-validation certificate or format conversion.

## Compatibility matrix

Keep Tarantool fixed at **3.9.0**, including its `3.9.0-…` build suffix.
Changing Tarantool itself is outside this tested upgrade. Both memtx and
vinyl user spaces retain synchronous memtx protocol metadata under RF3.

| Gateway | Lua/storage | Native module | Admission and behavior |
|---|---|---|---|
| Predecessor | Predecessor | Its ABI 14 module or disabled | Existing behavior; no compatibility handshake |
| Predecessor | This version | This version's ABI 14 module | Supported transition; existing RPCs; older gateway does not enforce new integrity admission |
| This version | This version | Matching ABI 14 module | Protocol 1 / format 1 handshake before use |
| This version | This version | Predecessor's ABI 14 module | Same state and RPC contract; tested explicitly |
| This version | This version | Disabled, load failure, or incompatible ABI | Explicit Lua fallback, with the reason in the compatibility report; the same SQL admission and fallback contracts apply |
| This version | Predecessor or missing compatibility RPC | Any | Refused before forwarding user RPCs; upgrade storage first |
| This version | Unknown protocol, disk format, runtime, shard identity, or expiry contract | Any | Refused; no unsafe compatibility override |

Deploy Lua files and their native module as one immutable release directory.
Restart a process to replace them. Hot-reloading Lua, changing C modules in
place, and changing the format marker on a running cluster are unsupported:
physical connections cache admission for their process lifetime. An old
gateway has no new admission guard; do not point it at an arbitrary future
storage release.

`tarn_compatibility()` returns the fixed eight-field array:

```text
["ok", protocol, storage_format, shard_id, tarantool_version,
 native_state_layout, native_status, max_txn_age]
```

Current values are protocol **1**, format **1**, native layout **14/392**, and
the configured transaction age (120 seconds by default). Native status is
`ready`, `disabled`, `load_failed`, or `abi_mismatch`. A node still starting
may report `pending`; a new gateway refuses it until initialization ends.
`tarn_info().compatibility` exposes the same report. A report selects a
compatible dispatch path; it is not a quorum/freshness barrier, a durability
receipt, or evidence about any mutation's outcome.

Every newly discovered physical pool connection is checked, before the
pool is published. That includes extra connections and replacement leaders
after a socket closes or ER_READONLY invalidates a pool. Failed negotiation
returns **1235/42000**, before that connection carries user work. Existing
in-flight writes still follow the established rules: uncertain effects
remain **1180/40003** and are never replayed; confirmed transaction conflicts
remain **1213/40001**. The handshake adds no retry or fallback after an
uncertain native commit.

Storage checks the runtime and `_tarn_meta['storage_format']` before schema
setup or recovery starts, and again before setup on promotion. A missing
marker means the explicitly supported predecessor's format 1; setup adds
the numeric marker 1 synchronously. Unknown values, including a string
`"1"`, are refused without schema migration. This version never raises the
format and provides no format-conversion operation.

## Upgrade order and gates

1. Record source revisions, Tarantool version, native module hashes, config,
   and the current catalog, bucket map, membership, and lazy generations.
   Retain the old release directory and a recoverable backup. Check that
   every shard has a healthy quorum, replicas follow, no synchronous queue
   is stuck, prepared transactions resolve, and lazy outboxes drain. Keep
   the existing clock/retention assumptions and identical `max_txn_age`.
2. Keep the predecessor gateways serving traffic. In each shard replace
   **one follower at a time** with the new immutable Lua/native directory,
   retaining its identity, work directory and replication configuration.
   Wait for it to follow with its WAL caught up before touching another
   replica. Never remove a quorum.
3. Promote an upgraded follower, wait for writable leadership and completed
   schema/native initialization, then replace the former leader. Wait for
   every replica to follow. Repeat for each shard. All eligible failover
   targets must be upgraded before starting new gateways; a missed legacy
   leader will be refused after failover.
4. From the predecessor gateway run an exact SQL canary: single-shard and
   distributed writes with unique keys, read-your-writes and rollback,
   ALTER on a disposable table, and a controlled move/lazy catch-up where
   those features are used. Verify full rows and counts, native committed
   tuples, prepared/decision cleanup and outbox progress. Check logs for
   native-copy differences, atomic-section yields, and new errors.
5. Start one new gateway with a distinct node ID. It must negotiate every
   live shard before binding its MySQL listener. Compare exact canary
   results through old and new gateways, then replace the remaining
   gateways one at a time. Drain a gateway's sessions before stopping it;
   a lost COMMIT reply is still outcome-unknown and needs reconciliation,
   not blind retry.
6. Exercise a controlled leader handoff. Confirm that old streams fail
   validation and fresh connections negotiate the replacement. Monitor
   SQL error classes, commit/recovery progress, prepared age, replication
   and lazy delivery before declaring the upgrade complete.

The automated regression starts a fresh RF3 cluster and verifies schema,
native initialization and SQL canaries before testing upgrades. It retains durable YES records during
follower replacement and gateway replacement, then applies twice to check
idempotence. It also runs DDL, moves and exact lazy-index comparisons in
mixed storage stages and through both gateway versions. This is bounded
functional evidence, not an availability or throughput guarantee under
arbitrary outages. RF1 has no rolling storage availability; stop traffic
while restarting its only replica.

## Rollback and future disk formats

For this exact pair, format stays **1**. Stop/drain all new gateways first,
wait for distributed transactions, DDL jobs, bucket moves and lazy delivery
to finish, then reverse the storage process above using the preserved
predecessor Lua/native directory. The predecessor ignores the added meta
key and understands all other persisted records. The regression tests this
rollback with data written by both gateways. Do not downgrade just Lua or
replace a module in a live process.

A future change to persisted semantics must raise `storage_format` **before
writing the new representation**, define reader/writer compatibility and
peer admission, and provide its own migration/restore tests. That is the
irreversible downgrade boundary: this version refuses such a disk.
The predecessor predates this protection and cannot refuse a future format;
operators must never use it past that boundary. No rolling compatibility
with future protocol/format numbers is implied by this document.

## Reproduce and retain evidence

With the pinned Rust toolchain, Tarantool 3.9.0, a C compiler, Python and
PyMySQL installed, run on a dedicated Linux test machine:

```sh
git fetch origin 2e8aaeb772542968e503212a143b2e6f8d27b8ab
bash tests/rolling_upgrade.sh
ENGINE=vinyl bash tests/rolling_upgrade.sh
```

The wrapper archives the exact predecessor from Git and builds old and new
artifacts separately with the same Cargo settings. The Python fixture owns
all processes and allocates local ports; it never attaches to an existing
cluster. Each run retains build logs, immutable artifacts and source trees,
`manifest.json` (per-file source hashes, binary hashes, versions, engine,
RF3/fsync and build overrides), `events.jsonl` (durable/restart/refusal/data
witnesses), and all process logs. A failed run records the exception and
cleans up its processes. CI runs both engines and uploads results even on
failure. Preserve the complete directory when reporting a result.

Preparing the restart witness may receive a confirmed Vinyl admission NO.
Setup records each vote and admits a fresh transaction identity only after
the prior decision and every participant are durably aborted, all fixture
values remain zero, and row/range locks are absent. It permits at most 32
attempts within 45 seconds; each PREPARE/cleanup RPC is bounded by the remaining deadline
and its normal timeout. A lost reply, unknown/expired vote, unrelated error,
or failed cleanup stops the fixture without replay. Every successful
witness still requires durable YES across restart and exact once-only APPLY.
The Vinyl lane forces first- and second-participant refusals under a leased
test pressure hook, then verifies the admitted identity across follower
restarts. Ordinary rolling stages use genuine admission settings and pressure.
Run `python3 tests/test_upgrade_prepare.py` for the uncertainty, cleanup,
deadline and attempt-limit negative controls. The original failed CI run and
focused control receipts remain in `tests/evidence/upgrade-admission/`.

Native controls use an absent module, an actual loader failure, and a
loadable C fixture whose state header declares ABI 10 and whose data
procedures refuse execution. Each must use Lua with exact SQL effects.
A warm new gateway is also tested against a backend restarted with actual
predecessor code: it must refuse an UPDATE without changing its row, then
recover after the compatible backend returns. The separate startup fixture
checks refusal of unknown/coerced persisted formats before schema creation.
The existing handback model and lost-reply fault suites remain the evidence
for uncertain mutation handling; compatibility negotiation does not extend
their proof boundary.

The supported predecessor already has ABI 14/392, request-specific first-read
barriers and final-prefix validation, persisted clock policy/high-water and
retention semantics, and the current lazy/engine capture binding contracts.
Those records and guarantees remain unchanged across this exact pair. The
older ABI-11 prototype is outside this rolling/rollback contract; it cannot
be inferred safe merely because an old gateway ignores a format marker.
Seeded voters retain their registered identity and `seed.json` election gate;
verify WAL follow before promotion through either release.

Every compatibility RPC used to discover an existing completion uses the
reserved completion lane, as do role and strong-read capability probes. A
cold pool must not spend ordinary capacity before it can finish decided work.
The overload unit test holds the entire normal lane while both negotiations
complete. Build roots have separate target directories regardless of an
inherited `CARGO_TARGET_DIR`; each build receipt binds compiled source and
artifact hashes, and the matrix refuses identical old/new gateway binaries.
