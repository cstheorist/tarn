Deterministic Vinyl BUILD-before-PREPARE review

The old CI failure is retained exactly. Its half-duration wall-time heuristic is replaced by an actual phase witness on the dedicated test-hook fixture: each shard returns from its real BUILD with all intended indexes present, then holds that reply before coordinator PREPARE. Ten SQL mutations on every shard must be acknowledged while all BUILD replies remain held. The test verifies the final index result. Timings are observational and include the intentional hold; they do not establish capacity or latency bounds. Native writes stay enabled because no generic yield hook is held.

The sensitive negative issues actual PREPARE first on both shards. Its OK responses establish the injected ordering; writes then wait until the BUILD hold expires and the fixture rejects with a phase-specific assertion. No uncertain mutation is replayed. Positive native and Lua final cohorts pass. Safety deadlines, cleanup, exact source hashes and retained unsuccessful setup/control attempts are recorded in index.json. No production protocol or timeout is changed.

The archives `clusters.tar.gz` and `original-41110785622-job124252347419.out.gz`
are not in the tree, to keep the repository small. They are retained at
commit [`dfc2b6f`](https://github.com/cstheorist/tarn/tree/dfc2b6f41381a0ff2c126aa656d7a97c6b73e7cb/tests/evidence/index-build-review)
(#213); `index.json` gives their SHA-256 and their links.
