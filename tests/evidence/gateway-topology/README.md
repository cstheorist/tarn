# Gateway topology evidence (issue #188)

See [the measured envelope](../../../docs/gateway-resources.md#measured-envelope-and-retained-evidence)
for methods, results and limitations. `index.json` identifies every artifact
with byte count and SHA-256. `summary-final.json` contains min/median/max values
from all five pairs in each final integrated configuration; `summary.json`
preserves the earlier study.

`scale-41051749936.tar.gz` is the final integration against baseline `8b12c45`,
including rolling-upgrade admission. All 24,000 scheduled reads completed.
`scale-40972749238.tar.gz` is the earlier integrated branch cohort;
`scale-40972723697.tar.gz` is the additional automatic PR-merge cohort.
The two earlier cohorts use baseline main `a885dc4` and unchanged normal release profiles.
Extract an archive to inspect complete `report.json` files, source/binary
hashes, gateway and storage logs. All earlier scale cohorts are retained
separately, including the RF3 startup failure and cold-client missed slots.

`partition-3537a7e.tar.gz` contains the final integrated RF3 network partition,
heal and gateway restart result. It excludes WAL/snapshot binaries and Docker
inventory dumps; the source-bound report, configuration and owned process
logs remain. The earlier passing partition and local eight-group smoke run
are supplementary evidence, not measurements in the final table.

Compressed CI logs include the complete successful integrated run and the
failed earlier/manual attempts. The local metadata test's sandbox bind
failure and successful authorized rerun are both retained. There is no
mutation replay or relaxed safety guard in these validation changes.

The final CI is run 41051749810 on `0666a0a`. Its first attempt passed all
checks except Vinyl rolling upgrade: the predecessor process exhausted its
16 MiB Vinyl memory quota while replaying WAL during startup. Only that
unchanged job was repeated and passed. `final-ci-attempt1.log.gz`,
`final-ci-attempt2.log.gz`, both upgrade archives and the final CI JSON retain
that history. Local normal ARM64 link attempts killed under container memory
limits are retained separately from the successful normal-release CI builds.
