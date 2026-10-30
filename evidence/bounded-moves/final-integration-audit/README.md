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

The final release gates and integration review remain required before merge.
