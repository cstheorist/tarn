# Exact key regression evidence

`decimal-negative-control.txt` retains the original failing run on base
`7b063f6` with the decimal regression added before the production fix.
`cargo test -p tarn-sql --locked exact_decimal_keys_above_double_precision`
reported `Some(Equal)` where `Some(Less)` was required for
`9007199254740992.01` versus `9007199254740992.02`. The current test also
covers canonical equality, typed probes, rounding and numeric boundaries.

PR #199's `Exact key contracts` artifacts retain every matrix cell's paired
MySQL traces, client versions, source and binary hashes, RF3 write outcomes,
native/Lua counters and cluster logs. A pass requires every registered case
and every acknowledged write to reconcile. Uncertain writes are not retried.
`CI` separately runs the full existing suite, the TLA+ registry and Verus.
Only the final PR head's complete checks qualify as release evidence.

Development failures are retained in runs `40936571765` and `40944071391`:
the former issued a new uniqueness control while a required clock peer was
still absent; the latter incorrectly required memtx native read counters
for Vinyl's guarded Lua path. The corrected harness observes promotion,
restores the clock peer, checks health, then runs new uniqueness/movement
controls and checks caught-up replica values. It checks native readiness
and the appropriate memtx service or Vinyl fallback counters.

Local macOS Docker runs also encountered clock-drift refusals and a zombie
PID restart guard. Those attempts do not establish passing fault evidence.
The recorded failures did not justify relaxing clock safety or replaying
uncertain mutations. The executable vectors and explicit supported/refused
matrix remain in the repository even after CI artifact retention expires.
