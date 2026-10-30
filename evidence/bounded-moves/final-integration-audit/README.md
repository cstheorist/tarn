# Final integration audit

This evidence is published separately from the implementation branch to preserve
its current CI run. It adds no production changes.

## Hosted revision b3da8e6

The tested merge `5a79e046a2962e33f29fc3c1d68e3d903f672b33` has the same tree
(`ce1e5cdd50ffc367335ba0f6fc94311cb83a99ce`) as `b3da8e6`. Its parents are
`b3317b344d7f636f59f77031ae298cc1cfc708e1` and `b3da8e6`.

The complete main test log passes workspace/SQL/native storage, backup/restore,
the 18 RF3 movement cases, both engine SQL suites, join/growth, both RF3 failovers,
and strong KV histories. All 18 retained movement results independently reconcile
4,596 acknowledged effects exactly once and exclude 884 confirmed aborts. There
were no unknown user-transaction outcomes in these particular cases; older
source-bound runs retain such outcomes. All recorded source hashes match the
b3da8e6 checkout.

Both maintenance-migration artifacts pass. Their audit checks 294 source inputs
and four actual binary hashes per engine, ten SQL/DDL/move/lazy canaries and five
native admission modes. Every durable YES has the same identity in its later
apply-once event. The Vinyl run additionally proves cleanup of both explicit
admission-refusal cases before use of fresh transaction identities.

The full model job was cancelled when the clock-bootstrap integration superseded
this revision. Its partial output is retained and is not a complete model pass.
The earlier complete 293-outcome result in the parent acceptance report remains
bound to its earlier source. Current combined-source CI is a separate gate.

The local integrated workspace run passes 355 tests after the first attempt's
linker was killed by the test container's 2 GiB memory limit. That original
nonzero output is retained. Raising only the build/test container to 4 GiB allowed
the unchanged command to finish; this does not alter the original fixed-pool
movement scale measurements.

## Current revision c654eae

The complete current-head CI and release matrices passed. The tested merge
`11aeba6fefee84441175bc225f9686c6f9ed0249` has the same tree
(`b5dded087c6ec4ce071373d32413e35489478aa7`) as `c654eae`; its parents are
`6a120f1b7ce2f59672100039b8b93dbec1c6a40f` and `c654eae`.
`current-checks.json` retains the completed check snapshot, including the four
typed-read cells, four MySQL cells, four outcome cells, six topology cells and
six routing-cost cells. Conditional skipped jobs remain labeled skipped.

The complete hosted formal log (`124223627947`, run `41102681469`) contains
all 319 expected outcomes: 79 positive configurations and 240 intentional
violations. The audit checks the exact registry order and outcome for each.
Verus (`124223628557`) completed with 140 verified and zero errors. The duplicate
local model run was intentionally stopped once this hosted result was verified;
its partial log is retained and is not a complete local model pass.

All 18 current-source hosted movement cases pass. The independent exact-ledger
audit reconciles 3,182 acknowledged effects with 3,182 committed effects,
excludes 728 confirmed aborts, and records two unknown outcomes, neither committed.
All recorded source hashes match the tested tree. Native/Lua modes, memtx/Vinyl/
mixed engines, and source/receiver/coordinator/disk/reply/lazy cases are retained.

Both maintenance-migration artifacts pass. Each audit verifies 295 new-source
inputs, 239 guarded-predecessor inputs and four actual binary hashes, plus ten
SQL/DDL/move/lazy canaries and five native-admission modes. Durable YES identities
match the later apply-once identities: one memtx pair and three Vinyl pairs.
Both Vinyl admission refusals prove cleanup on both participants before fresh
transaction identities. The old-binary format-2 refusal, snapshot reopen and
unfinished-work refusal before marker publication remain required and passed.
The raw CI logs and migration manifests/events are retained here; the complete
binary artifacts are CI artifact IDs `13005035000` (memtx) and `13002914430`
(Vinyl), with archive hashes in `hosted-c654-audit.json`.

`audit-current-ci.py` reproduces these checks from a checkout containing the
tested Git objects, the three downloaded artifact ZIPs, and the retained logs.
Its `--help` lists the input paths. These are correctness checks, not additional
performance or whole-process memory measurements.

The current-source follow-up archive contains native and Lua source/receiver
restart controls during mixed-engine RF3/fsync movement. Exact acknowledgements,
confirmed aborts, duplicate exclusion, post-move progress and reclaimed resources
are checked. Rust/native build inputs are unchanged from the v7 build at b90abdf;
the recorded c654eae Lua source is loaded at runtime. These dev-build correctness
runs make no performance claim.

The first attempt failed before movement: fresh bootstrap had relay timeouts,
leader changes, old-term asynchronous schema replication and a duplicate internal
index. No movement acceptance check or user mutation began. Both a fresh preceding-
source control and a fresh current-source control then passed, as did the remaining
three current-source cases. This comparison does not explain the original failure
or establish its cause. It is not relabeled as a passing suite or claimed fixed.
Its result, node logs, snapshots and WAL are retained for investigation.

The current-head release gates are complete. The independent integration review
remains required before merge. Its broader changes are not implied by these
source-bound results, and the original local bootstrap failure remains unexplained.
