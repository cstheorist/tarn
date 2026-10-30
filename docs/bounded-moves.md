# Bounded bucket movement

Implementation contract for issue #176 (work in progress; acceptance evidence
must be added before this document describes a supported release).

The receiver stages rows in a synchronous Vinyl space. Small per-move metadata
holds its identity, phase, counters and last accepted page/cursor. Copy, delta,
install, reclamation and cancellation use row and encoded-byte bounded batches.
The sender's optional dirty-key accelerator has per-move and aggregate limits;
overflow or loss invalidates it and requires a complete copy under the durable
write fence. It never drops a user write or a decided completion obligation.

Ownership is withdrawn before source reclamation. Destination rows are installed
behind a durable visibility fence, and one small transaction publishes ownership
only after every staged row is installed. Supported routed reads reject the old
owner; unscoped scans and validations are conservatively refused while physical
rows are incomplete. Native dispatch hands back to the same guarded Lua path.
Raw administrative space access remains local physical inspection and must not
be used to infer ownership or a distributed snapshot during movement.

The previous receiver-side Lua copy of every held row is replaced by a coarse
predicate hold. An unscoped read may now conflict even if its predicate would
miss the moving bucket; this conservative admission cost is explicit. Reads
correctly scoped to other buckets retain their existing validation contract.

All progress and token checks are durable. An accepted page cannot be replaced
by a stale/reordered page, and an old move cannot act on a later incarnation.
After send starts, recovery only proceeds forward. Cancellation before send
reclaims staging in batches before dropping its resource reservation. Disk
pressure refuses new copying and leaves durable progress intact; inability to
write cleanup WAL requires capacity restoration rather than deleting obligations.

Configured bounds cover active moves, per-move and aggregate encoded staging,
batch rows/bytes, retained dirty keys/bytes, and available filesystem headroom.
Vinyl's shared L0/cache configuration bounds those engine pools. Page indexes,
Bloom filters, native user indexes, replication queues, WAL, allocator overhead
and the user's memtx data are separate; there is no whole-process RSS bound.

New movements require the bounded-movement capability on both replica sets.
Upgrade every replica and gateway before enabling them; drain legacy movements
before migrating an old memtx staging space. In-progress new states must not be
opened by older software. Cluster backup refuses unfinished movement; complete
backups preserve the new internal space definitions and progress metadata.

The abstract contract is `BoundedMove.tla`. Its bounded dirty tracker, frozen
recopy, crash-retained progress, guarded install/reclaim, cancellation and batch
limits are checked separately from transaction semantics. `TarnCommit.tla`
extends its existing movement actions with reclaim/install phases, keeping the
hold through publication. Neither model proves Tarantool, the filesystem,
resource accounting implementation, or unbounded liveness.

## Resource configuration

The storage launcher exposes these limits. Values are positive integers; invalid
combinations refuse initialization. All byte accounting below uses encoded data,
not total allocator RSS. The SQL coordinator retains one page at a time.

| Environment variable | Default | Bound |
|---|---:|---|
| `TARN_MOVE_BATCH_ROWS` | 512 | Staged or source rows per physical batch; maximum 4096 |
| `TARN_MOVE_BATCH_BYTES` | 1048576 | Encoded page/work allowance; maximum 4 MiB, also limited by scan-page settings |
| `TARN_MOVE_SLOTS` | 4 | Durable active move reservations and concurrent source copy buffers; maximum 64 |
| `TARN_MOVE_STAGING_BYTES` | 17179869184 | Encoded disk staging per move |
| `TARN_MOVE_TOTAL_STAGING_BYTES` | 34359738368 | Encoded disk staging across active moves on this replica set |
| `TARN_MOVE_DELTA_KEYS` | 16384 | Retained dirty-key hints per move; maximum 65536 |
| `TARN_MOVE_DELTA_BYTES` | 4194304 | Accounted dirty-key bytes per move |
| `TARN_MOVE_TOTAL_DELTA_BYTES` | 8388608 | Accounted dirty-key bytes across moves |
| `TARN_MOVE_DISK_RESERVE_BYTES` | 1073741824 | Minimum available space on the WAL, memtx and Vinyl filesystems |

Dirty-key accounting includes encoded keys and a conservative per-entry allowance;
it is not an allocator measurement. Copy admission allows one source page per
bucket and at most the configured number of active copy buffers. Network receive
buffers, replication queues, temporary encodings and Lua garbage awaiting collection
are outside the retained-hint accounting. Configure the shared Vinyl memory/cache
pools separately, and provision measured engine overhead. Disk staging and a
fixed page size do not make arbitrary user memtx tables fit in small memory.

All inventoried replicas must advertise protocol 2 and initial disk headroom
before a new move starts. Each receiving/installing leader checks headroom again
before work, with additional allowance for amplification. This is an admission
check, not a reservation against concurrent filesystem users. Followers still
require successful synchronous replication; follower I/O failure can pause the
move. A restart of an existing reservation does not require a failed voter to
return before quorum progress resumes.

## Progress and visibility

`_tarn_move` stores the move identity, side, encoded staging bytes, row count and
last accepted sequence, phase, generation and cursor. The page receipt includes
a hash of both its rows and checkpoint. Row effects and the receipt commit in one
transaction. A repeated page is accepted only with identical contents; gaps,
changed receipts, unknown target spaces and old move identities are refused.
A reset has its own durable sequence, so a late reset cannot delete later pages.
The clear, install, reclaim and cancel cursors are their remaining durable rows.
Each modern RPC commits at most one batch and returns `more` until complete.

An abandoned SQL job resumes copying from its receiver checkpoint. A lost or
full volatile dirty tracker causes a complete frozen recopy. That copy uses the
move identity as its generation: its write fence and drained cut survive source
leadership changes. Explicit cancellation is recorded as a job phase before
cleanup, while a recorded flip always proceeds forward. Recovery keeps the job
alive during long cleanup and installation.

`reclaiming` and `installing` are durable non-serving states. Native reads hand
back to the guarded Lua entry during those phases. A freshness token remembers
whether its snapshot began during physical movement and the movement generation;
it cannot serve a partial snapshot after publication. Existing streams can
conservatively conflict when any bucket starts or finishes physical movement.
Fresh, stateless reads scoped to other buckets retain their route checks. Raw
administrative reads of native spaces remain local physical inspection.

The native tuple insert and staging deletion share one transaction. The final
empty staging tail publishes ownership in a constant-size transaction. Source
reclamation follows withdrawal of ownership. Large WAL writer buffers may combine
transactions; the page bound is not a bound on a combined WAL syscall. The study
reports the largest decoded/re-encoded WAL transaction separately.

## Validation and evidence

`lua/test/bounded_move_test.lua` uses tiny budgets to exercise mixed-engine
copying, receipt replay/reordering, missing spaces, disk and aggregate admission,
dirty-tracker overflow, frozen recopy, bounded cleanup and snapshot refusal.
The existing bucket suite covers prepared transactions, READ COMMITTED locks,
predicates and queued synchronous commits. The lazy topology suite checks
unavailable delivery, transferred counters and acceptance floors.

`tests/bounded_moves.py` owns a two-shard, RF3/fsync cluster and two SQL servers.
It checks exact deterministic 8 KiB payloads, process high-water RSS, allocated
disk, staged bytes, batch statistics, procedure-boundary intervals and decoded
WAL transaction sizes. Its source/receiver/coordinator crashes, disk refusal and
executed-but-lost page reply use the actual SQL coordinator. A failed run writes
its error and source/build/configuration hashes; it does not become a pass because
a later attempt succeeds. This same-host fixture is not an independent-host
failure-domain or production capacity benchmark.

Example fixed-pool cohorts (run in new directories, with the same binaries,
configuration and repetition count):

```sh
export TARN_BIN="$PWD/target/release/tarn-server"
export TARN_NATIVE="$PWD/target/release/libtarn_storage.so"
python3 tests/bounded_moves.py --work /tmp/move-32 --engine vinyl --mib 32
python3 tests/bounded_moves.py --work /tmp/move-128 --engine vinyl --mib 128
python3 tests/bounded_moves.py --work /tmp/move-concurrent --engine vinyl --mib 32 --moves 2
```

Each uses a 16 MiB shared Vinyl memory pool and zero Vinyl cache. The 128 MiB
cohort has four times the baseline payload at the same pool, batch and staging
limits. Repeat for memtx/mixed engines and Lua/native dispatch. Result JSON and
process logs are retained by the CI movement step. The bounded model includes a
negative snapshot control as well as drain, delta, publication, visibility,
reclamation, batching and late-page controls. Shared Verus code is unchanged.
