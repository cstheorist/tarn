---------------------------- MODULE LazyHandoff ----------------------------
(* One base bucket changes S->R, even when empty. One index bucket changes
   X->Y. A native final check precedes a yielding commit: INFLIGHT covers
   that interval and notes cover commits during the copy. Scope is one
   fixed (index incarnation,target bucket,source bucket); floors must use
   the exact scope key when independently composing these state machines. *)
EXTENDS Naturals, FiniteSets
CONSTANTS LiveBase, BugSourceNoFlush, BugResetCounter,
          BugRemovePending, BugNoTargetDrain, BugNoTargetFence,
          BugNoTargetNote, BugDropMarkers, BugDropFloors
Targets == {"X","Y"}
VARIABLES source, sourcePhase, pending2, counter, newWritten,
          pending3, lost, oldSourceRemoved, owner, targetPhase, marker, floor,
          stagedMarker, stagedFloor, flight, captured, noted
vars == <<source,sourcePhase,pending2,counter,newWritten,pending3,
          lost,oldSourceRemoved,owner,targetPhase,marker,floor,stagedMarker,stagedFloor,
          flight,captured,noted>>
Marker(v,p) == [seq |-> v,present |-> p]
Zero == Marker(0,FALSE)
BasePresent == LiveBase \/ newWritten
Current == marker[owner]
Init ==
    /\ source = "S" /\ sourcePhase = "active" /\ pending2 = TRUE
    /\ counter = 2
    /\ newWritten = FALSE /\ pending3 = FALSE /\ lost = FALSE /\ oldSourceRemoved = FALSE
    /\ owner = "X" /\ targetPhase = "active"
    /\ marker = [x \in Targets |-> IF x = "X" THEN Marker(1,TRUE) ELSE Zero]
    /\ floor = [x \in Targets |-> 0]
    /\ stagedMarker = Zero /\ stagedFloor = 0
    /\ flight = "none" /\ captured = "X" /\ noted = FALSE
DeliverTwo ==
    /\ pending2 /\ targetPhase = "active"
    /\ marker' = [marker EXCEPT ![owner] = Marker(2,LiveBase)] /\ pending2' = FALSE
    /\ UNCHANGED <<source,sourcePhase,counter,newWritten,pending3,lost,
                   oldSourceRemoved,owner,targetPhase,floor,stagedMarker,stagedFloor,flight,captured,noted>>
FenceSource ==
    /\ source = "S" /\ sourcePhase = "active" /\ sourcePhase' = "fenced"
    /\ UNCHANGED <<source,pending2,counter,newWritten,pending3,lost,
                   oldSourceRemoved,owner,targetPhase,marker,floor,stagedMarker,stagedFloor,flight,captured,noted>>
MoveSource ==
    /\ source = "S" /\ sourcePhase = "fenced" /\ (~pending2 \/ BugSourceNoFlush)
    /\ source' = "R" /\ sourcePhase' = "active"
    /\ lost' = (lost \/ pending2) /\ pending2' = FALSE
    /\ counter' = IF BugResetCounter THEN 0 ELSE counter
    /\ UNCHANGED <<newWritten,pending3,oldSourceRemoved,owner,targetPhase,marker,floor,
                   stagedMarker,stagedFloor,flight,captured,noted>>
(* pending2 belongs to S; pending3 belongs to R after MoveSource. Removing
   old S cannot delete R's pending3. Removing current R is a new composition
   of the same source drain protocol, not another action in this instance. *)
RemoveOldSource ==
    /\ ~oldSourceRemoved /\ ((source = "R" /\ ~pending2) \/ BugRemovePending)
    /\ oldSourceRemoved' = TRUE /\ lost' = (lost \/ pending2) /\ pending2' = FALSE
    /\ UNCHANGED <<source,sourcePhase,counter,newWritten,pending3,
                   owner,targetPhase,marker,floor,stagedMarker,stagedFloor,flight,captured,noted>>
WriteNew ==
    /\ source = "R" /\ ~newWritten
    /\ newWritten' = TRUE /\ counter' = counter+1 /\ pending3' = TRUE
    /\ UNCHANGED <<source,sourcePhase,pending2,lost,oldSourceRemoved,owner,targetPhase,
                   marker,floor,stagedMarker,stagedFloor,flight,captured,noted>>
InstallAndGC ==
    /\ ~pending2 /\ ~newWritten /\ ~LiveBase /\ targetPhase = "active"
    /\ floor' = [floor EXCEPT ![owner] = 2] /\ marker' = [marker EXCEPT ![owner] = Zero]
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,targetPhase,stagedMarker,stagedFloor,flight,captured,noted>>
OldDuplicate ==
    /\ ~pending2
    /\ marker' = IF 1 > floor[owner] /\ 1 > Current.seq
                  THEN [marker EXCEPT ![owner] = Marker(1,TRUE)] ELSE marker
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,targetPhase,floor,stagedMarker,stagedFloor,flight,captured,noted>>
BeginTargetMove ==
    /\ owner = "X" /\ targetPhase = "active"
    /\ targetPhase' = "sending" /\ stagedMarker' = marker["X"] /\ stagedFloor' = floor["X"]
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,marker,floor,flight,captured,noted>>
BeginBatch ==
    /\ pending3 /\ flight = "none"
    /\ flight' = "read" /\ captured' = owner
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,targetPhase,marker,floor,stagedMarker,stagedFloor,noted>>
FinalCheck ==
    /\ flight = "read"
    /\ flight' = IF BugNoTargetFence \/ (captured = owner /\ targetPhase # "fenced")
                  THEN "committing" ELSE "none"
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,targetPhase,marker,floor,stagedMarker,stagedFloor,captured,noted>>
CommitBatch ==
    /\ flight = "committing"
    /\ marker' = IF counter > floor[captured] /\ counter > marker[captured].seq
                  THEN [marker EXCEPT ![captured] = Marker(counter,TRUE)] ELSE marker
    /\ pending3' = FALSE /\ flight' = "none"
    /\ noted' = (noted \/ (~BugNoTargetNote /\ targetPhase = "sending" /\ captured = "X"))
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   lost,oldSourceRemoved,owner,targetPhase,floor,stagedMarker,stagedFloor,captured>>
FenceTarget ==
    /\ targetPhase = "sending" /\ (flight = "none" \/ BugNoTargetDrain)
    /\ targetPhase' = "fenced"
    /\ stagedMarker' = IF noted THEN marker["X"] ELSE stagedMarker
    /\ stagedFloor' = floor["X"]
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,owner,marker,floor,flight,captured,noted>>
FlipTarget ==
    /\ owner = "X" /\ targetPhase = "fenced"
    /\ marker' = [marker EXCEPT !["Y"] = IF BugDropMarkers THEN Zero ELSE stagedMarker]
    /\ floor' = [floor EXCEPT !["Y"] = IF BugDropFloors THEN 0 ELSE stagedFloor]
    /\ owner' = "Y" /\ targetPhase' = "active"
    /\ UNCHANGED <<source,sourcePhase,pending2,counter,newWritten,
                   pending3,lost,oldSourceRemoved,stagedMarker,stagedFloor,flight,captured,noted>>
Next == DeliverTwo \/ FenceSource \/ MoveSource \/ RemoveOldSource \/ WriteNew
        \/ InstallAndGC \/ OldDuplicate \/ BeginTargetMove \/ BeginBatch \/ FinalCheck
        \/ CommitBatch \/ FenceTarget \/ FlipTarget
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ source \in {"S","R"} /\ sourcePhase \in {"active","fenced"}
    /\ pending2 \in BOOLEAN /\ counter \in 0..3
    /\ newWritten \in BOOLEAN /\ pending3 \in BOOLEAN /\ lost \in BOOLEAN /\ oldSourceRemoved \in BOOLEAN
    /\ owner \in Targets /\ targetPhase \in {"active","sending","fenced"}
    /\ marker \in [Targets -> [seq : 0..3,present : BOOLEAN]] /\ floor \in [Targets -> 0..2]
    /\ stagedMarker \in [seq : 0..3,present : BOOLEAN] /\ stagedFloor \in 0..2
    /\ flight \in {"none","read","committing"} /\ captured \in Targets /\ noted \in BOOLEAN
SourceObligationsSurvive == ~lost
CounterContinues == source = "R" => counter >= 2
DrainedProjection == ~pending2 /\ ~pending3 => Current.present = BasePresent
=============================================================================
