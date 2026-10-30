#!/bin/bash
# Model-check every configuration; summarize outcome, states and time.
cd "$(dirname "$0")"; mkdir -p out
# tla2tools.jar: $TLA2TOOLS, or tla/tla2tools.jar (see docs/verification.md)
JAR=${TLA2TOOLS:-tla2tools.jar}
[ -f "$JAR" ] || { echo "tla2tools.jar not found: set TLA2TOOLS or download it into tla/" >&2; exit 2; }
for cfg in "$@"; do
  # Focused grouped-read and backfill modules check local refinement
  # premises; other configurations use the commit-protocol MC module.
  case "$cfg" in
    ClockBootstrap|Bug_ClockBootstrap*) module=ClockBootstrap.tla ;;
    GatewayResources|Bug_Gateway*) module=GatewayResources.tla ;;
    QueryBudget|Bug_Query*) module=QueryBudget.tla ;;
    ClusterBackup|Bug_ClusterBackup*) module=ClusterBackup.tla ;;
    FileBackup|Bug_FileBackup*) module=FileBackup.tla ;;
    PlainLazyBinding|Bug_PlainLazy*) module=PlainLazyBinding.tla ;;
    DepartureDependencies|Bug_Departure*) module=DepartureDependencies.tla ;;
    DecisionPlacement|Bug_Decision*) module=DecisionPlacement.tla ;;
    PipelineAdmission|Bug_Pipeline*) module=PipelineAdmission.tla ;;
    LazyIndex|Bug_LazyIndex*) module=LazyIndex.tla ;;
    LazyCapacity|Bug_LazyCapacity*) module=LazyCapacity.tla ;;
    LazyBarrier|Bug_LazyBarrier*) module=LazyBarrier.tla ;;
    LazyHandoff|LazyHandoffLive|Bug_LazyHandoff*) module=LazyHandoff.tla ;;
    LazyMeter|Bug_LazyMeter*) module=LazyMeter.tla ;;
    LazyTargetConfirm|Bug_UnconfirmedAck) module=LazyTargetConfirm.tla ;;
    LazyApplyConfirm|Bug_UnconfirmedApply) module=LazyApplyConfirm.tla ;;
    BootstrapCount|Bug_CountsAuxMetadata) module=BootstrapCount.tla ;;
    VinylAdmission|Bug_VinylAdmission*) module=VinylAdmission.tla ;;
    PrepareConfirm|Bug_UnconfirmedPrepare) module=PrepareConfirm.tla ;;
    GroupedRead|Bug_GroupedReadSeparateSnapshots) module=GroupedRead.tla ;;
    GroupedDDL|Bug_GroupedDDLIgnoreGeneration|Bug_GroupedDDLIgnoreStale) module=GroupedDDL.tla ;;
    StreamMarks*|Bug_StreamMarks*) module=StreamMarks.tla ;;
    Handback*|Bug_Handback*) module=NativeHandback.tla ;;
    EngineCopy|Bug_EngineCopy*) module=EngineCopy.tla ;;
    EngineCutover|Bug_EngineCutover*) module=EngineCutover.tla ;;
    EngineSourceDrain|Bug_EngineSourceDrain*) module=EngineSourceDrain.tla ;;
    EngineAlias|EngineAlias_Existing|Bug_EngineAlias*) module=EngineAlias.tla ;;
    StrongReadFreshness|Bug_StrongRead*) module=StrongReadFreshness.tla ;;
    RecordExpiryClock|Bug_RecordExpiry*) module=RecordExpiryClock.tla ;;
    RecordExpiryPolicy|Bug_RecordPolicy*) module=RecordExpiryPolicy.tla ;;
    CompletionRetention|Bug_CompletionRetention*) module=CompletionRetention.tla ;;
    LogicalClockExpiry|Bug_LogicalClock*) module=LogicalClockExpiry.tla ;;
    StrongStreamCertificate|Bug_StrongStream*) module=StrongStreamCertificate.tla ;;
    StrongStreamCompletion|Bug_StreamCompletion*) module=StrongStreamCompletion.tla ;;
    LockReplyOwnership|Bug_LockReply*) module=LockReplyOwnership.tla ;;
    WaiterAllocation*|Bug_WaiterLateAllocation) module=WaiterAllocation.tla ;;
    PlanGuard*|Bug_PlanGuard*) module=PlanGuardBatch.tla ;;
    NativeKeyBatch|Bug_KeyBatch*) module=NativeKeyBatch.tla ;;
    LockCopy|Bug_Lock*) module=NativeLockCopy.tla ;;
    Fairness*|Bug_Fairness*) module=RecoveryFairness.tla ;;
    BackfillDDL|Bug_BackfillDDL*) module=BackfillDDL.tla ;;
    Backfill|Bug_Backfill*) module=InstantBackfill.tla ;;
    EngineBuildReentry|Bug_EngineBuild*) module=EngineBuildReentry.tla ;;
    *) module=MC.tla ;;
  esac
  start=$(date +%s)
  java -XX:+UseParallelGC -Xmx${TLC_HEAP:-3g} -cp "$JAR" tlc2.TLC -workers ${WORKERS:-1} -cleanup \
       -metadir ${TLC_META:-/tmp/tlcmeta}/$cfg -config $cfg.cfg "$module" > out/$cfg.out 2>&1
  secs=$(( $(date +%s) - start ))
  res=$(grep -oE "Invariant [A-Za-z]+ is violated" out/$cfg.out | head -1)
  [ -z "$res" ] && res=$(grep -q "No error has been found" out/$cfg.out && echo "no errors" || echo "ERROR: $(grep -m1 -iE 'error|exception' out/$cfg.out)")
  # Use the final summary, not a comma-formatted progress count/rate.
  st=$(sed -nE 's/^.*states generated, ([0-9,]+) distinct states found, .*$/\1 distinct states found/p' out/$cfg.out | tail -1)
  depth=$(grep -oE "depth of the complete state graph search is [0-9]+" out/$cfg.out | grep -oE "[0-9]+$")
  printf "%-28s %-45s %-28s depth=%-4s %5ss\n" "$cfg" "$res" "$st" "${depth:--}" "$secs"
done
