-------------------------- MODULE PlainLazyBinding --------------------------
(* Negative lazy-capture binding for an EXISTING marked READ_CONFIRMED
 * stream only. The original user read precedes metadata binding, initial
 * source images and tentative writes. No child transaction, new snapshot,
 * autocommit/RW/RC/PREPARE exemption or engine-absence proof is modeled.
 *
 * Confirmed metadata consists of the relevant catalog declaration, capture
 * point and pending-defs range. Each is independent evidence: malformed or
 * partially installed metadata must not be inferred to mean plain source.
 * A pending forward-generation case has already sampled its baseline;
 * wrongly exempting its write loses membership after publication/drain.
 * These are local metadata/certificate contracts, not a model of all DDL
 * lifecycle steps. Existing fence/engine/clock/lock/role checks remain
 * mandatory; their other rejection cases are outside this exemption.
 *
 * Point/range reads and earlier user history remain in one original native
 * transaction. Relevant changes create sticky MVCC conflict (including a
 * read view whose later write cannot succeed). The full pending range may
 * conservatively conflict with unrelated lazy DDL. The progress case is
 * ONLY unrelated nonlazy catalog/schema publication, during or after it.
 * Native global exclusion stays unknown throughout every dirty interval.
 * Native MVCC, confirmed observation and publication are trusted mappings,
 * exercised by live primitive controls, not proved by this bounded model.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS Profile, BugIgnoreDeclared, BugIgnoreCapture, BugIgnorePending,
          BugFirstWriteOnly, BugReplaceOriginal, BugRejectGlobalStale

Sources == {"A", "C"}
Cases == {"unrelated", "declared", "capture", "pending", "mixed", "history", "identity", "role"}
Selected == IF Profile = "all" THEN Cases ELSE {Profile}
EmptyMeta == [s \in Sources |-> {}]

VARIABLES kind, writes, phase, changed, globalStale, bPublished,
          metadata, data, membership, identity, hardEpoch,
          tx, userTracked, conflict, externalAck, proof,
          tracksDeclared, tracksCapture, tracksPending,
          initialTx, tentative, ourAck
vars == <<kind,writes,phase,changed,globalStale,bPublished,
          metadata,data,membership,identity,hardEpoch,
          tx,userTracked,conflict,externalAck,proof,
          tracksDeclared,tracksCapture,tracksPending,
          initialTx,tentative,ourAck>>

Init ==
    /\ kind \in Selected
    /\ writes = IF kind = "mixed" THEN Sources ELSE {"A"}
    /\ phase = "before" /\ changed = FALSE /\ globalStale = FALSE /\ bPublished = FALSE
    /\ metadata = EmptyMeta /\ data = [s \in Sources |-> 0]
    /\ membership = [s \in Sources |-> 0] /\ identity = [s \in Sources |-> 0]
    /\ hardEpoch = 0 /\ tx = 1 /\ userTracked = TRUE /\ conflict = FALSE
    /\ externalAck = FALSE /\ ourAck = FALSE
    /\ proof = [bound |-> FALSE, ids |-> {}, tx |-> 0,
                 identity |-> [s \in Sources |-> 0], hard |-> 0]
    /\ tracksDeclared = {} /\ tracksCapture = {} /\ tracksPending = FALSE
    /\ initialTx = 0 /\ tentative = [s \in Sources |-> 0]

CheckSources == IF BugFirstWriteOnly THEN {"A"} ELSE writes
EmptyEvidence(s) ==
    /\ (BugIgnoreDeclared \/ "declared" \notin metadata[s])
    /\ (BugIgnoreCapture \/ "capture" \notin metadata[s])
    /\ (BugIgnorePending \/ "pending" \notin metadata[s])

Bind ==
    /\ phase = "before"
    /\ LET nextTx == IF BugReplaceOriginal THEN 2 ELSE tx
       IN /\ tx' = nextTx
          /\ proof' = [bound |-> \A s \in CheckSources : EmptyEvidence(s),
                        ids |-> CheckSources, tx |-> nextTx,
                        identity |-> identity, hard |-> hardEpoch]
    /\ tracksDeclared' = IF BugIgnoreDeclared THEN {} ELSE CheckSources
    /\ tracksCapture' = IF BugIgnoreCapture THEN {} ELSE CheckSources
    (* The real implementation scans the whole defs range, filtering by
     * written base ID. A pending insert anywhere may conflict conservatively. *)
    /\ tracksPending' = ~BugIgnorePending
    /\ userTracked' = IF BugReplaceOriginal THEN FALSE ELSE userTracked
    /\ conflict' = IF BugReplaceOriginal THEN FALSE ELSE conflict
    /\ phase' = "bound"
    /\ UNCHANGED <<kind,writes,changed,globalStale,bPublished,metadata,data,membership,
                   identity,hardEpoch,externalAck,initialTx,tentative,ourAck>>

Change ==
    /\ ~changed /\ phase \in {"before", "bound", "initial", "written"}
    /\ changed' = TRUE /\ globalStale' = TRUE
    /\ bPublished' \in IF kind = "unrelated" THEN BOOLEAN ELSE {FALSE}
    /\ metadata' = CASE kind \in {"declared", "capture", "pending"}
                         -> [metadata EXCEPT !["A"] = {kind}]
                      [] kind = "mixed" -> [metadata EXCEPT !["C"] = {"declared", "capture"}]
                      [] OTHER -> metadata
    /\ data' = IF kind = "history" THEN [data EXCEPT !["A"] = 10] ELSE data
    /\ externalAck' = (kind = "history")
    /\ identity' = IF kind = "identity" THEN [identity EXCEPT !["A"] = 1] ELSE identity
    /\ hardEpoch' = IF kind = "role" THEN 1 ELSE hardEpoch
    /\ conflict' = (conflict
          \/ (kind = "history" /\ userTracked)
          \/ (kind = "declared" /\ "A" \in tracksDeclared)
          \/ (kind = "capture" /\ "A" \in tracksCapture)
          \/ (kind = "pending" /\ tracksPending)
          \/ (kind = "mixed" /\ "C" \in (tracksDeclared \cup tracksCapture)))
    /\ UNCHANGED <<kind,writes,phase,membership,tx,userTracked,proof,
                   tracksDeclared,tracksCapture,tracksPending,initialTx,tentative,ourAck>>

InitialImages ==
    /\ phase = "bound" /\ initialTx' = tx /\ phase' = "initial"
    /\ UNCHANGED <<kind,writes,changed,globalStale,bPublished,metadata,data,membership,
                   identity,hardEpoch,tx,userTracked,conflict,externalAck,proof,
                   tracksDeclared,tracksCapture,tracksPending,tentative,ourAck>>

TentativeWrite ==
    /\ phase = "initial" /\ phase' = "written"
    (* SQL's retained original read was zero. A replacement transaction
     * cannot make this stale-derived payload valid by rereading metadata. *)
    /\ tentative' = [s \in Sources |-> IF s \in writes THEN 1 ELSE data[s]]
    /\ UNCHANGED <<kind,writes,changed,globalStale,bPublished,metadata,data,membership,
                   identity,hardEpoch,tx,userTracked,conflict,externalAck,proof,
                   tracksDeclared,tracksCapture,tracksPending,initialTx,ourAck>>

BoundStillMatches ==
    /\ proof.bound /\ proof.tx = tx /\ initialTx = tx
    /\ (BugReplaceOriginal \/ tx = 1)
    /\ (BugFirstWriteOnly \/ proof.ids = writes)
    /\ \A s \in proof.ids : proof.identity[s] = identity[s]
    /\ proof.hard = hardEpoch
    (* Existing original-stream role/source checks are not exempted. *)
    /\ hardEpoch = 0 /\ \A s \in writes : identity[s] = 0

Finish ==
    /\ phase = "written" /\ changed
    (* Ineligible packets retain the old strict global-stale refusal. A
     * bound mismatch hard-refuses; it never falls back to a new registry. *)
    /\ LET accepted == BoundStillMatches /\ ~conflict
                        /\ (~BugRejectGlobalStale \/ ~globalStale)
       IN /\ ourAck' = accepted
          /\ data' = IF accepted THEN tentative ELSE data
    /\ phase' = "finished"
    /\ UNCHANGED <<kind,writes,changed,globalStale,bPublished,metadata,membership,
                   identity,hardEpoch,tx,userTracked,conflict,externalAck,proof,
                   tracksDeclared,tracksCapture,tracksPending,initialTx,tentative>>

PublishDrain ==
    /\ phase = "finished" /\ phase' = "drained" /\ bPublished' = (kind = "unrelated")
    (* Plain capture produced no outbox. Any incorrectly bypassed lazy
     * obligation therefore leaves its sampled baseline membership zero. *)
    /\ UNCHANGED <<kind,writes,changed,globalStale,metadata,data,membership,
                   identity,hardEpoch,tx,userTracked,conflict,externalAck,proof,
                   tracksDeclared,tracksCapture,tracksPending,initialTx,tentative,ourAck>>

Next == Bind \/ Change \/ InitialImages \/ TentativeWrite \/ Finish \/ PublishDrain
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ kind \in Cases /\ writes \subseteq Sources /\ "A" \in writes
    /\ phase \in {"before","bound","initial","written","finished","drained"}
    /\ changed \in BOOLEAN /\ globalStale \in BOOLEAN /\ bPublished \in BOOLEAN
    /\ metadata \in [Sources -> SUBSET {"declared","capture","pending"}]
    /\ data \in [Sources -> {0,1,10}] /\ membership \in [Sources -> {0,1}]
    /\ identity \in [Sources -> {0,1}] /\ hardEpoch \in {0,1}
    /\ tx \in {1,2} /\ initialTx \in {0,1,2}
    /\ userTracked \in BOOLEAN /\ conflict \in BOOLEAN /\ externalAck \in BOOLEAN
    /\ proof.bound \in BOOLEAN /\ proof.ids \subseteq Sources /\ proof.tx \in {0,1,2}
    /\ proof.identity \in [Sources -> {0,1}] /\ proof.hard \in {0,1}
    /\ tracksDeclared \subseteq Sources /\ tracksCapture \subseteq Sources /\ tracksPending \in BOOLEAN
    /\ tentative \in [Sources -> {0,1,10}] /\ ourAck \in BOOLEAN

NoMissingMembership ==
    (phase = "drained" /\ ourAck) =>
       \A s \in writes : metadata[s] # {} => membership[s] = data[s]
NoStaleReadCommit == ~(ourAck /\ externalAck)
UnrelatedOneCommit ==
    (phase = "drained" /\ kind = "unrelated") =>
       (ourAck /\ \A s \in writes : data[s] = 1)
NativeExclusionRetained == changed => globalStale
=============================================================================
