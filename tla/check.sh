#!/bin/bash
# Model-check the configurations that finish within minutes and check their
# outcomes: every Bug_* and Tradeoff_* configuration must violate an
# invariant, every other one must pass. CI runs this; see
# docs/verification.md. Arguments override the list of configurations.
set -u
cd "$(dirname "$0")"
DEFAULT="Bug_NoReadLocks Bug_LocalIgnoresRLocks Bug_NoWLockCheckOnRead Bug_NoTombstone
Bug_LocalReadsIgnoreWLocks Bug_ROValidateNoWLockCheck Bug_ROValidateNoReadCheck
Bug_GCIgnoresDecision Bug_GCNoExpiry Bug_FenceAfterExpiry
Tradeoff_DirectReadAtomic Tradeoff_AckOnPrepare_DirectRYW
Recov RecovReaders Recov_AckOnPrepare Local_ROValidate Frac_ROValidate
LocalFailover_ROValidate FracFailover_ROValidate Recov_GC Local_GC_ROValidate Frac_GC_ROValidate
Bug_RCNoLockCheck Bug_RCLockIgnoresWLocks Bug_RCReadBeforeLock Bug_IgnorePLocks Bug_RCLockReadsInFlight
RC_Counter RC_CounterFailover RC_Counter_AckOnPrepare RC_Mixed RC_MixedFailover RC_Local
Uniq UniqFailover Uniq_RC Bug_InsertNoExistCheck
Bug_MoveNoDrain Bug_MoveNoHold Bug_MoveServeStale Move_Scan Move_RC Move_ScanFailover
Bug_JoinNoCheck Bug_JoinNoCheck3 Bug_JoinNoCheck3Prepare Join_Scan Join_ScanFailover Join3_Scan Join3_ScanPrepare"
DEFAULT="$DEFAULT GroupedTables Bug_GroupedTablesNoWLockCheck GroupedRead Bug_GroupedReadSeparateSnapshots"
CONFIGS=${*:-$DEFAULT}
fail=0
for cfg in $CONFIGS; do
  ./run_all.sh "$cfg" || { echo "run_all.sh failed for $cfg"; fail=1; continue; }
  if grep -q "No error has been found" "out/$cfg.out"; then got=pass
  elif grep -qE "Invariant [A-Za-z]+ is violated" "out/$cfg.out"; then got=violation
  else got=error; fi
  case $cfg in Bug_*|Tradeoff_*) want=violation;; *) want=pass;; esac
  if [ "$got" != "$want" ]; then
    echo "UNEXPECTED: $cfg: $got (expected $want)"
    tail -n 30 "out/$cfg.out"
    fail=1
  fi
done
[ $fail = 0 ] && echo "all outcomes as expected" || echo "some outcomes were not as expected"
exit $fail
