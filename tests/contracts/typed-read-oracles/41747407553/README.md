# Grouped aggregate descriptor reference

[Run 41747407553](https://github.com/cstheorist/tarn/actions/runs/41747407553)
used diagnostic branch commit `ff1acd7fc304f2808450ccd08ef1d44088866fe0`,
based on main `a14c789`. This is reference-only evidence, not a Tarn pass.

The original 8,076-byte artifact ZIP has SHA-256
`1af89e65dd921f73911aa41d7f947fca4fef2761a466001307a18f5efaf00ae2`,
matching the retained GitHub artifact receipt. It contains all 18 queries,
text and binary preparation/execution descriptors and typed results,
EXPLAIN output, clean source status, input hashes and container image identity.
The reference is MariaDB 10.11.19, image digest
`sha256:d769e37960821dfdcf096954f6e7ac89f747e116c1bba98e48edd265dfc7deae`.
The capture script and workflow are retained here under their input hashes;
its bounded binary decoder is `tests/sql/pushdown_contracts.py` at the recorded
commit (also retained in the earlier release study source bundle).

Global and index-ordered grouped queries send aggregate-item metadata.
Materialized grouping sends temporary-field metadata at text/binary
execution, while COM_STMT_PREPARE still reports aggregate-item metadata.
The observations cover grouping with matching/diverging order, primary and
secondary indexes, nonindexed grouping, empty input, HAVING and LIMIT.
Temporary numeric fields omit BINARY; direct MIN/MAX preserve the source's
NO_DEFAULT_VALUE flag where applicable. Materialized MIN(DOUBLE) has length
22, compared with 23 for its prepared/item descriptor; SUM(DOUBLE) stays 23.
All raw fields remain in the ZIP, including exploratory computed extrema.

These observations do not establish equivalence for every cost-dependent
MariaDB plan, join buffering or constant-key elimination. The implementation
and release probe qualify their stated subset; do not infer a universal
optimizer-to-descriptor mapping from this finite fixture.
