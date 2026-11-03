# RC cost pilot evidence, 2026-11-02

Measured source: `d0c0ded421e6a0958722ade199d2a799e0f41418`.
Guard run: `41529987854`. Confirmation run: `41529992173`.
Scope: single-host memtx, native enabled, RF3/fsync, effective checks off;
five alternating control/candidate pairs per pilot, three workload shapes.
These are pilot artifacts, not full qualification evidence.

`manifest.json` binds both compressed archives and each retained archive
member by byte length and SHA256, and binds the original independent audit
code, complete recomputed results, and successful audit log. All 120 samples
and 679,255 attempts reconciled; no refused or unknown outcomes were observed.
The manifest lists every omitted original file and its reason individually.

Each archive preserves original run-relative paths for comparison/session
JSON, all raw sample JSONL, mechanism/startup logs, host/runtime receipts, and
source/build/binary-hash receipts. The original nested `fixtures.tar.gz` is
expanded to a `fixtures/` subtree containing both complete 111-file source
copies and cluster node logs, JSON configurations, params and startup scripts.
Bulk xlog/snap/sortdata/vylog files and executable binaries are excluded.
Formal model output artifacts are excluded; the available model-hash receipt
is retained. No qualification artifacts are represented or inferred.

`audit.py` is the exact successful original audit, preserved without claiming
portability. It expects the original `/private/tmp/tarn-186-pilots/run-<id>/`
artifact trees, including their original nested `fixtures.tar.gz` and downloaded
`tarn-server`/`libtarn_storage.so`, plus the Git checkout at
`/private/tmp/tarn-186-cost-pilot` containing the measured source. These compact
archives alone cannot rerun that script: omitted binaries cannot be rehashed
and the nested fixture layout differs. Its original binary hash checks are
recorded by the bound audit results/log; retained `binaries.sha256` receipts
provide the expected hashes, not executable contents.

`package_evidence.py` is a reusable packaging helper, not a benchmark runner.
Its paths and run list are explicit configuration for these two pilots.
Every member of both completed archives was reopened and independently
verified against the manifest immediately after packaging.

Stability findings distinguish sampled PID/role identity from archived-log
evidence: all 120 node logs record one startup/PID and only RAFT term 2.
The samples do not contain continuous election-term telemetry. Allocation and
CPU intervals include background work; five pairs provide descriptive ranges,
not confidence intervals. Guard helper probes exclude transaction throughput;
confirmation mechanism probes include memtx/vinyl cases but do not constitute
vinyl workload qualification.

## Full qualification and fixed repeat

The pilot archives above are followed by independently audited compact packages:

- [SQL qualification](sql-qualification/README.md): run 41532311462, four modes.
- [Direct qualification](direct-qualification/README.md): run 41532314628, four modes.
- [Fixed SQL Vinyl/native-enabled repeat](vinyl-repeat/README.md): run 41536728436.

Each subdirectory retains its manifest, source/run bindings, raw records, audit
results and stated omissions/dependencies. See [the study report](../../RC_PLAN_GUARD.md)
and [per-arm summary](../../rc_guard_results.csv). No noisy observation was dropped.
