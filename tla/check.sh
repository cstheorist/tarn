#!/bin/bash
# Model-check the configurations that finish within minutes and check their
# outcomes: every Bug_* and Tradeoff_* configuration must violate an
# invariant, every other one must pass. CI runs this; see
# docs/verification.md. Arguments override the list of configurations.
set -u
cd "$(dirname "$0")"
DEFAULT="Bug_NoReadLocks Bug_LocalIgnoresRLocks Bug_NoWLockCheckOnRead Bug_NoTombstone
Bug_LocalReadsIgnoreWLocks Bug_ROValidateNoWLockCheck Bug_ROValidateNoReadCheck Bug_ValidateLostStream
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
DEFAULT="$DEFAULT GroupedDDL Bug_GroupedDDLIgnoreGeneration Bug_GroupedDDLIgnoreStale"
DEFAULT="$DEFAULT Backfill Bug_BackfillNoPersistentFence Bug_BackfillSkipDrain Bug_BackfillLoseUndo
Bug_BackfillOverwriteUndo Bug_BackfillPublishEarly Bug_BackfillReleaseEarly Bug_BackfillStaleBatch
Bug_BackfillWrongOldPad Bug_BackfillAbortAfterApply"
DEFAULT="$DEFAULT BackfillDDL Bug_BackfillDDLNoQuiescence Bug_BackfillDDLNoFinisherSerialization Bug_BackfillDDLWrongSpace"
DEFAULT="$DEFAULT LazyIndex LazyCapacity LazyBarrier LazyHandoff
LazyHandoffLive Bug_LazyBarrierForgetUnresolved Bug_LazyBarrierLastOnly Bug_LazyBarrierMaxAck
Bug_LazyBarrierSkipApplyWait Bug_LazyBarrierSplitFrontierView Bug_LazyBarrierTrackOnlyWhenEnabled Bug_LazyBarrierWaitAfterSnapshot
Bug_LazyCapacityAbortLeaks Bug_LazyCapacityNoByteReserve Bug_LazyCapacityNoEntryReserve Bug_LazyCapacityNoSeqReserve
Bug_LazyHandoffDropFloors Bug_LazyHandoffDropLiveVersion Bug_LazyHandoffDropMarkers Bug_LazyHandoffNoTargetDrain
Bug_LazyHandoffNoTargetFence Bug_LazyHandoffNoTargetNote Bug_LazyHandoffRemovePending Bug_LazyHandoffResetCounter
Bug_LazyHandoffSourceNoFlush Bug_LazyIndexAckMissingTarget Bug_LazyIndexCaptureFailOpen Bug_LazyIndexFloorFromMaxAck
Bug_LazyIndexForgetBeforeAck Bug_LazyIndexForwardBeforeCapture Bug_LazyIndexGcBeforeFloor Bug_LazyIndexIgnoreGeneration
Bug_LazyIndexNoBaseRecheck Bug_LazyIndexNoKeyRecheck Bug_LazyIndexShipBeforeCommit Bug_LazyIndexSplitOutbox
Bug_LazyIndexStaleOverwrite"
DEFAULT="$DEFAULT LazyMeter Bug_LazyMeterNoPrecharge Bug_LazyMeterEarlyRelease
Bug_LazyMeterRefundUncertain Bug_LazyMeterMutateUnknown Bug_LazyMeterNoDrain
Bug_LazyMeterNoQueueCut Bug_LazyMeterStaleInstall Bug_LazyMeterStaleCallback Bug_LazyMeterApplyCap"
DEFAULT="$DEFAULT LazyTargetConfirm Bug_UnconfirmedAck"
DEFAULT="$DEFAULT LazyApplyConfirm Bug_UnconfirmedApply"
DEFAULT="$DEFAULT BootstrapCount Bug_CountsAuxMetadata"
DEFAULT="$DEFAULT VinylAdmission Bug_VinylAdmissionPerGatewayBudget Bug_VinylAdmissionGateAfterEffect
Bug_VinylAdmissionNegativeCredit Bug_VinylAdmissionThrottleExistingYes Bug_VinylAdmissionThrottleDecidedApply
Bug_VinylAdmissionReplayUnknown Bug_VinylAdmissionResetFullBurst"
DEFAULT="$DEFAULT PrepareConfirm Bug_UnconfirmedPrepare"

DEFAULT="$DEFAULT StreamMarks Bug_StreamMarksRoAfterTermChange Bug_StreamMarksWritableBeforeTrigger
Bug_StreamMarksNativeNoTermCheck Bug_StreamMarksNativeIgnoresLkStale Bug_StreamMarksLuaNoTermCheck
Bug_StreamMarksLuaTermCheckEarly Bug_StreamMarksValidateNoFenceFresh Bug_StreamMarksCommitInNewTxn
Bug_StreamMarksDoomBeforeClear Bug_StreamMarksDoomAfterCommit"
DEFAULT="$DEFAULT Handback Handback_Autocommit Bug_HandbackKeepsWrites Bug_HandbackAfterCommitBegan
Bug_HandbackAfterCommitBegan_Autocommit Bug_HandbackResendAfterLoss Bug_HandbackResendAfterLoss_Autocommit"
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
