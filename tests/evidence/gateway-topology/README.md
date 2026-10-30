# Gateway topology evidence (issue #188)

See [the measured envelope](../../../docs/gateway-resources.md#measured-envelope-and-retained-evidence)
for methods, results and limitations. `index.json` identifies every artifact
with byte count and SHA-256. `summary.json` contains min/median/max values
from all five pairs in each primary integrated configuration.

`scale-40972749238.tar.gz` is the primary integrated branch cohort;
`scale-40972723697.tar.gz` is the additional automatic PR-merge cohort.
Both use baseline main `a885dc4` and unchanged normal release profiles.
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
