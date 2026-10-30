---------------------------- MODULE BoundedMove ----------------------------
(* Bounded bucket movement. Physical rows are independent of serving ownership.
   Staging and progress survive crashes; the bounded dirty-key optimization may
   overflow or be lost, in which case a frozen full copy is mandatory.
   TarnCommit separately checks transaction/predicate/lock interleavings. *)
EXTENDS Integers, FiniteSets, TLC
CONSTANTS BugNoDrain, BugLostDelta, BugPublishEarly, BugExposeChunks,
          BugServeReclaim, BugBatch, BugLatePage, BugSnapshot
Keys == 1..3
Missing == -1
Batch == 1
Empty == [k \in Keys |-> Missing]
Present(rows) == {k \in Keys : rows[k] # Missing}
VARIABLES phase, src, staged, dst, truth, copied, dirty, lost, todo,
          pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot
vars == <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
          pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Init == /\ phase = "copy" /\ src = [k \in Keys |-> 0]
        /\ staged = Empty /\ dst = Empty /\ truth = src
        /\ copied = {} /\ dirty = {} /\ lost = FALSE /\ todo = {}
        /\ pending = {} /\ written = {} /\ live = TRUE /\ restarted = FALSE
        /\ lastBatch = 0 /\ partialRead = FALSE /\ diskOK = TRUE /\ partialSnapshot = FALSE
StartWrite(k) ==
    /\ live /\ phase = "copy" /\ pending = {} /\ k \notin written
    /\ pending' = {k}
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
                   written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
CommitWrite(k) ==
    /\ live /\ pending = {k}
    /\ src' = [src EXCEPT ![k] = 1] /\ truth' = [truth EXCEPT ![k] = 1]
    /\ written' = written \cup {k} /\ pending' = {}
    /\ lost' = (lost \/ Cardinality(dirty \cup {k}) > Batch)
    /\ dirty' = IF lost' THEN {} ELSE dirty \cup {k}
    /\ UNCHANGED <<phase, staged, dst, copied, todo, live, restarted,
                   lastBatch, partialRead, diskOK, partialSnapshot>>
Copy(k) ==
    /\ live /\ diskOK /\ phase = "copy" /\ k \notin copied
    /\ staged' = [staged EXCEPT ![k] = src[k]] /\ copied' = copied \cup {k}
    /\ lastBatch' = 1
    /\ UNCHANGED <<phase, src, dst, truth, dirty, lost, todo, pending,
                   written, live, restarted, partialRead, diskOK, partialSnapshot>>
Fence == /\ live /\ phase = "copy" /\ copied = Keys
         /\ phase' = "fenced"
         /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, todo,
                        pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Drain == /\ live /\ phase = "fenced" /\ (BugNoDrain \/ pending = {})
         /\ todo' = IF lost /\ ~BugLostDelta THEN Keys ELSE dirty
         /\ phase' = "delta"
         /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost,
                        pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Delta(k) ==
    /\ live /\ diskOK /\ phase = "delta" /\ k \in todo
    /\ staged' = [staged EXCEPT ![k] = src[k]] /\ todo' = todo \ {k}
    /\ lastBatch' = 1
    /\ UNCHANGED <<phase, src, dst, truth, copied, dirty, lost, pending,
                   written, live, restarted, partialRead, diskOK, partialSnapshot>>
Send == /\ live /\ phase = "delta" /\ todo = {}
        /\ phase' = "reclaim"
        /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, todo,
                       pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Reclaim(ks) ==
    /\ live /\ phase = "reclaim" /\ ks \subseteq Present(src) /\ ks # {}
    /\ Cardinality(ks) <= IF BugBatch THEN 2 ELSE Batch
    /\ src' = [k \in Keys |-> IF k \in ks THEN Missing ELSE src[k]]
    /\ lastBatch' = Cardinality(ks)
    /\ UNCHANGED <<phase, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, partialRead, diskOK, partialSnapshot>>
BeginInstall ==
    /\ live /\ phase = "reclaim" /\ Present(src) = {}
    /\ phase' = "install"
    /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Install(k) ==
    /\ live /\ diskOK /\ phase = "install" /\ k \in Present(staged)
    /\ dst' = [dst EXCEPT ![k] = staged[k]]
    /\ staged' = [staged EXCEPT ![k] = Missing] /\ lastBatch' = 1
    /\ UNCHANGED <<phase, src, truth, copied, dirty, lost, todo, pending,
                   written, live, restarted, partialRead, diskOK, partialSnapshot>>
Publish ==
    /\ live /\ phase = "install" /\ (BugPublishEarly \/ Present(staged) = {})
    /\ phase' = "done"
    /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
ReadPartial ==
    /\ live /\ ~partialRead
    /\ ((BugExposeChunks /\ phase = "install" /\ Present(dst) # {} /\ Present(dst) # Keys)
        \/ (BugServeReclaim /\ phase = "reclaim" /\ Present(src) # Keys)
        \/ (BugSnapshot /\ phase = "done" /\ partialSnapshot))
    /\ partialRead' = TRUE
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, lastBatch, diskOK, partialSnapshot>>
Snapshot ==
    /\ live /\ phase = "install" /\ Present(dst) # {} /\ Present(dst) # Keys
    /\ ~partialSnapshot /\ partialSnapshot' = TRUE
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, lastBatch, partialRead, diskOK>>
Cancel ==
    /\ live /\ phase \in {"copy", "fenced", "delta"} /\ pending = {}
    /\ phase' = "cancel" /\ todo' = {}
    /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, pending,
                   written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Clean(k) ==
    /\ live /\ phase = "cancel" /\ k \in Present(staged)
    /\ staged' = [staged EXCEPT ![k] = Missing] /\ lastBatch' = 1
    /\ UNCHANGED <<phase, src, dst, truth, copied, dirty, lost, todo, pending,
                   written, live, restarted, partialRead, diskOK, partialSnapshot>>
Cancelled ==
    /\ live /\ phase = "cancel" /\ Present(staged) = {}
    /\ phase' = "cancelled"
    /\ UNCHANGED <<src, staged, dst, truth, copied, dirty, lost, todo, pending,
                   written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
LatePage ==
    /\ BugLatePage /\ live /\ phase = "cancelled" /\ staged = Empty
    /\ staged' = [staged EXCEPT ![1] = truth[1]]
    /\ UNCHANGED <<phase, src, dst, truth, copied, dirty, lost, todo, pending,
                   written, live, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Crash ==
    /\ live /\ ~restarted /\ live' = FALSE
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, restarted, lastBatch, partialRead, diskOK, partialSnapshot>>
Restart ==
    /\ ~live /\ live' = TRUE /\ restarted' = TRUE
    /\ dirty' = {} /\ lost' = TRUE
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, todo,
                   pending, written, lastBatch, partialRead, diskOK, partialSnapshot>>
Pressure ==
    /\ diskOK' = ~diskOK
    /\ UNCHANGED <<phase, src, staged, dst, truth, copied, dirty, lost, todo,
                   pending, written, live, restarted, lastBatch, partialRead, partialSnapshot>>
Next == Snapshot \/ Fence \/ Drain \/ Send \/ BeginInstall \/ Publish \/ ReadPartial
        \/ Cancel \/ Cancelled \/ LatePage \/ Crash \/ Restart \/ Pressure
        \/ (\E k \in Keys: StartWrite(k) \/ CommitWrite(k) \/ Copy(k) \/ Delta(k) \/ Install(k) \/ Clean(k))
        \/ (\E ks \in SUBSET Keys: Reclaim(ks))
Spec == Init /\ [][Next]_vars
TypeOK == /\ src \in [Keys -> -1..1] /\ staged \in [Keys -> -1..1]
          /\ dst \in [Keys -> -1..1] /\ truth \in [Keys -> 0..1]
          /\ copied \subseteq Keys /\ dirty \subseteq Keys /\ todo \subseteq Keys
          /\ pending \subseteq Keys /\ written \subseteq Keys
          /\ phase \in {"copy", "fenced", "delta", "reclaim", "install", "done", "cancel", "cancelled"}
BoundedWork == lastBatch <= Batch /\ Cardinality(dirty) <= Batch
NoPartialReads == ~partialRead
AcknowledgedEffects == phase = "done" => dst = truth
CompletePublication == phase = "done" => Present(dst) = Keys /\ Present(src) = {}
DrainedHandoff == phase \in {"reclaim", "install", "done"} => pending = {}
CancelledResources == phase = "cancelled" => staged = Empty /\ dst = Empty /\ src = truth
=============================================================================
