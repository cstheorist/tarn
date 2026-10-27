-------------------------- MODULE InstantBackfill --------------------------
(* Local refinement obligations of instant-column materialization (#70). *)
(* Two rows have a base field and a published instant field (fill 2). The *)
(* first ALTER appends a new field (fill 3); after ABORT, a second ALTER   *)
(* reuses that physical slot with fill 7. Only missing suffixes are       *)
(* written. An admitted old-layout writer can later apply field value 9. *)
(* A batch's final owner/phase test abstracts reading that metadata in    *)
(* the SAME native RW transaction, whose conflict prevents a stale       *)
(* commit. One crash and takeover bound recovery interleavings. This is  *)
(* not a refinement proof of native MVCC or the distributed DDL decision. *)
EXTENDS Naturals, Sequences

CONSTANTS BugNoPersistentFence, BugSkipDrain, BugLoseUndo,
          BugOverwriteUndo, BugPublishEarly, BugReleaseEarly,
          BugStaleBatch, BugWrongOldPad, BugAbortAfterApply
ASSUME /\ BugNoPersistentFence \in BOOLEAN /\ BugSkipDrain \in BOOLEAN
       /\ BugLoseUndo \in BOOLEAN /\ BugOverwriteUndo \in BOOLEAN
       /\ BugPublishEarly \in BOOLEAN /\ BugReleaseEarly \in BOOLEAN
       /\ BugStaleBatch \in BOOLEAN /\ BugWrongOldPad \in BOOLEAN
       /\ BugAbortAfterApply \in BOOLEAN

Rows == {"short", "long"}
Jobs == 1..2
Fill(j) == IF j = 1 THEN 3 ELSE 7
OldValue(t) == IF Len(t) < 2 THEN 2 ELSE t[2]
NewValue(t) == IF Len(t) < 3 THEN 0 ELSE t[3]
Pad(t, j) == IF Len(t) >= 3 THEN t
             ELSE IF Len(t) = 1
                  THEN t \o <<IF BugWrongOldPad THEN 0 ELSE 2, Fill(j)>>
                  ELSE t \o <<Fill(j)>>
Trim(t, n) == SubSeq(t, 1, IF n < Len(t) THEN n ELSE Len(t))

VARIABLES physical, wanted, undo, job, phase, owner, catalog, indexReady,
          aborted, fenced, memFence, awake, crashed, taken,
          writerUsed, writerPending, reqActive, reqJob, reqOwner, reqRow
dvars == <<physical, wanted, undo>>
jvars == <<job, phase, owner, catalog, indexReady, aborted>>
fvars == <<fenced, memFence, awake, crashed, taken>>
wvars == <<writerUsed, writerPending>>
qvars == <<reqActive, reqJob, reqOwner, reqRow>>
vars == <<dvars, jvars, fvars, wvars, qvars>>

EmptyUndo(j) == \A r \in Rows : undo[j][r] = 0
FullRows == \A r \in Rows : Len(physical[r]) = 3

Init ==
    /\ physical = [r \in Rows |-> IF r = "short" THEN <<0>> ELSE <<0, 5>>]
    /\ wanted = [r \in Rows |-> IF r = "short" THEN 2 ELSE 5]
    /\ undo = [j \in Jobs |-> [r \in Rows |-> 0]]
    /\ job = 0 /\ phase = "idle" /\ owner = 1 /\ catalog = 0
    /\ indexReady = FALSE /\ aborted = {}
    /\ fenced = FALSE /\ memFence = FALSE /\ awake = TRUE
    /\ crashed = FALSE /\ taken = FALSE
    /\ writerUsed = FALSE /\ writerPending = FALSE
    /\ reqActive = FALSE /\ reqJob = 1 /\ reqOwner = 1 /\ reqRow = "short"

Start(j) ==
    /\ awake
    /\ (job = 0 /\ j = 1) \/ (job = 1 /\ phase = "done" /\ catalog = 0 /\ j = 2)
    /\ job' = j /\ phase' = "preparing" /\ owner' = 1
    /\ indexReady' = FALSE
    /\ fenced' = ~BugNoPersistentFence /\ memFence' = TRUE
    /\ UNCHANGED <<dvars, catalog, aborted, awake, crashed, taken, wvars, qvars>>

OldWriterCheck ==
    /\ awake /\ catalog = 0 /\ ~memFence /\ ~writerUsed
    /\ writerUsed' = TRUE /\ writerPending' = TRUE
    /\ UNCHANGED <<dvars, jvars, fvars, qvars>>

OldWriterApply ==
    /\ awake /\ writerPending
    /\ physical' = [physical EXCEPT !["short"] = <<0, 9>>]
    /\ wanted' = [wanted EXCEPT !["short"] = 9]
    /\ writerPending' = FALSE
    /\ UNCHANGED <<undo, jvars, fvars, writerUsed, qvars>>

BeginBatch(r) ==
    /\ awake /\ phase = "preparing" /\ memFence /\ ~reqActive
    /\ BugSkipDrain \/ ~writerPending
    /\ reqActive' = TRUE /\ reqJob' = job /\ reqOwner' = owner /\ reqRow' = r
    /\ UNCHANGED <<dvars, jvars, fvars, wvars>>

FinishBatch ==
    LET valid == reqJob = job /\ reqOwner = owner /\ phase = "preparing" /\ memFence
        t == physical[reqRow]
        save == /\ ~BugLoseUndo
                /\ ((Len(t) < 3 /\ undo[reqJob][reqRow] = 0)
                    \/ (BugOverwriteUndo /\ undo[reqJob][reqRow] # 0))
    IN
    /\ awake /\ reqActive
    /\ IF valid \/ BugStaleBatch
       THEN /\ physical' = [physical EXCEPT ![reqRow] = Pad(t, reqJob)]
            /\ undo' = IF save THEN [undo EXCEPT ![reqJob][reqRow] = Len(t)] ELSE undo
       ELSE UNCHANGED <<physical, undo>>
    /\ reqActive' = FALSE
    /\ UNCHANGED <<wanted, jvars, fvars, wvars, reqJob, reqOwner, reqRow>>

Prepared ==
    /\ awake /\ phase = "preparing" /\ memFence /\ FullRows
    /\ BugSkipDrain \/ ~writerPending
    /\ phase' = "prepared" /\ indexReady' = TRUE
    /\ UNCHANGED <<dvars, job, owner, catalog, aborted, fvars, wvars, qvars>>

DecideApply ==
    /\ awake /\ (phase = "prepared" \/ (BugPublishEarly /\ phase = "preparing"))
    /\ phase' = "applying"
    /\ UNCHANGED <<dvars, job, owner, catalog, indexReady, aborted, fvars, wvars, qvars>>

Publish ==
    /\ awake /\ phase = "applying" /\ catalog = 0
    /\ catalog' = job
    /\ UNCHANGED <<dvars, job, phase, owner, indexReady, aborted, fvars, wvars, qvars>>

GCUndo(r) ==
    /\ awake /\ phase = "applying" /\ catalog = job /\ undo[job][r] # 0
    /\ undo' = [undo EXCEPT ![job][r] = 0]
    /\ UNCHANGED <<physical, wanted, jvars, fvars, wvars, qvars>>

FinishApply ==
    /\ awake /\ phase = "applying" /\ catalog = job /\ EmptyUndo(job)
    /\ phase' = "done"
    /\ UNCHANGED <<dvars, job, owner, catalog, indexReady, aborted, fvars, wvars, qvars>>

DecideAbort ==
    /\ awake
    /\ phase \in {"preparing", "prepared"} \/ (BugAbortAfterApply /\ phase = "applying")
    /\ phase' = "aborting"
    /\ UNCHANGED <<dvars, job, owner, catalog, indexReady, aborted, fvars, wvars, qvars>>

UndoRow(r) ==
    /\ awake /\ phase = "aborting" /\ undo[job][r] # 0
    /\ physical' = [physical EXCEPT ![r] = Trim(@, undo[job][r])]
    /\ undo' = [undo EXCEPT ![job][r] = 0]
    /\ UNCHANGED <<wanted, jvars, fvars, wvars, qvars>>

FinishAbort ==
    /\ awake /\ phase = "aborting"
    /\ EmptyUndo(job) \/ BugReleaseEarly
    /\ phase' = "done" /\ aborted' = aborted \cup {job}
    /\ fenced' = FALSE /\ memFence' = FALSE
    /\ UNCHANGED <<dvars, job, owner, catalog, indexReady, awake, crashed, taken, wvars, qvars>>

Crash ==
    /\ awake /\ ~crashed /\ job # 0 /\ phase # "done"
    /\ awake' = FALSE /\ memFence' = FALSE /\ crashed' = TRUE
    /\ UNCHANGED <<dvars, jvars, fenced, taken, wvars, qvars>>

Recover ==
    /\ ~awake
    /\ awake' = TRUE /\ memFence' = fenced
    /\ UNCHANGED <<dvars, jvars, fenced, crashed, taken, wvars, qvars>>

Takeover ==
    /\ awake /\ job # 0 /\ phase # "done" /\ ~taken
    /\ owner' = 2 /\ taken' = TRUE
    /\ UNCHANGED <<dvars, job, phase, catalog, indexReady, aborted,
                   fenced, memFence, awake, crashed, wvars, qvars>>

Next == (\E j \in Jobs : Start(j)) \/ OldWriterCheck \/ OldWriterApply
        \/ (\E r \in Rows : BeginBatch(r)) \/ FinishBatch \/ Prepared
        \/ DecideApply \/ Publish \/ (\E r \in Rows : GCUndo(r)) \/ FinishApply
        \/ DecideAbort \/ (\E r \in Rows : UndoRow(r)) \/ FinishAbort
        \/ Crash \/ Recover \/ Takeover
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ physical \in [Rows -> Seq(0..9)] /\ \A r \in Rows : Len(physical[r]) \in 1..3
    /\ wanted \in [Rows -> {2, 5, 9}]
    /\ undo \in [Jobs -> [Rows -> 0..3]]
    /\ job \in 0..2 /\ phase \in {"idle", "preparing", "prepared", "applying", "aborting", "done"}
    /\ owner \in 1..2 /\ catalog \in 0..2 /\ indexReady \in BOOLEAN
    /\ aborted \subseteq Jobs /\ fenced \in BOOLEAN /\ memFence \in BOOLEAN
    /\ awake \in BOOLEAN /\ crashed \in BOOLEAN /\ taken \in BOOLEAN
    /\ writerUsed \in BOOLEAN /\ writerPending \in BOOLEAN /\ reqActive \in BOOLEAN
    /\ reqJob \in Jobs /\ reqOwner \in 1..2 /\ reqRow \in Rows

OldProjectionPreserved == catalog = 0 => \A r \in Rows : OldValue(physical[r]) = wanted[r]
PublishedIndexCorrect == catalog # 0 => indexReady /\ \A r \in Rows : NewValue(physical[r]) = Fill(catalog)
AbortedUndoClean == \A j \in aborted : EmptyUndo(j)
FutureSlotReuseSafe == catalog = 2 => \A r \in Rows : NewValue(physical[r]) = 7
=============================================================================
