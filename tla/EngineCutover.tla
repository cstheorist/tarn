---------------------------- MODULE EngineCutover ----------------------------
(* Distributed publication refinement (#57). EngineCopy supplies each
   PREPARED image. Existing DDL jobs exclude bucket moves/JOIN throughout;
   global/unique and lazy bootstrap supply ReadyAux only when complete.
   Two shards can rename and publish separately, with one crash between
   native rename and catalog commit. The durable manifest binds ORIGINAL
   and NEW IDs, independently of whichever space now has the public name.
   Catalog publication, old-generation retirement and reader dooming are
   one local metadata step, acknowledged from confirmed state.
   The decision variable is confirmed central state, not a quorum-pending
   journal row. Exact binding/phase reads must use read-confirmed visibility;
   LazyTargetConfirm/LazyApplyConfirm/PrepareConfirm and runtime tests cover
   that native visibility premise, not this local publication model.
   Native DDL
   serialization and the DDL hold/read protocol are reused premises; this
   is not a proof of Tarantool replication or full DDL linearizability. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugPublishBeforeDecision, BugPublishBeforeAuxReady, BugDropBeforePublish,
          BugAbortAfterDecision, BugApplyAfterAbort, BugOldStreamValidates, BugForgetOldObligations,
          BugResetCounterIdentity, BugRenameLosesOriginalIdentity, BugCatalogByName

Shards == {"A", "B"}
Old == 1
New == 2
Ids == {Old, New}
VARIABLES prepared, auxReady, decision, catalog, spaces, renamed, oldRef, nameGone,
          awake, crashed, retired, pendingOld, ackedOld, doomed, streamCommitted,
          allocated, counter, allocatedNew, reused, observation
jvars == <<prepared, auxReady, decision>>
pvars == <<catalog, spaces, renamed, oldRef, nameGone>>
fvars == <<awake, crashed>>
avars == <<retired, pendingOld, ackedOld>>
rvars == <<doomed, streamCommitted, observation>>
cvars == <<allocated, counter, allocatedNew, reused>>
vars == <<jvars, pvars, fvars, avars, rvars, cvars>>

Init ==
    /\ prepared = {} /\ auxReady = {} /\ decision = "none"
    /\ catalog = [s \in Shards |-> Old] /\ spaces = [s \in Shards |-> Ids]
    /\ renamed = {} /\ nameGone = {} /\ oldRef = [s \in Shards |-> Old]
    /\ awake = Shards /\ crashed = FALSE
    /\ retired = {} /\ pendingOld = Shards /\ ackedOld = {}
    /\ doomed = {} /\ streamCommitted = {}
    /\ observation = [sid |-> Old, def_id |-> Old, json_version |-> 0]
    \* IDs 1 and 2 were allocated already; 2 may have been deleted. A
    \* max(current rows) floor cannot replace stable ai:/rid: identity.
    /\ allocated = {1, 2} /\ counter = 2 /\ allocatedNew = FALSE /\ reused = FALSE

Prepared(s) ==
    /\ s \in awake \ prepared /\ decision = "none"
    /\ prepared' = prepared \cup {s}
    /\ UNCHANGED <<auxReady, decision, pvars, fvars, avars, rvars, cvars>>
ReadyAux(s) ==
    /\ s \in (awake \cap prepared) \ auxReady /\ decision = "none"
    /\ auxReady' = auxReady \cup {s}
    /\ UNCHANGED <<prepared, decision, pvars, fvars, avars, rvars, cvars>>

DecideApply ==
    /\ decision = "none" \/ (BugApplyAfterAbort /\ decision = "abort")
    /\ prepared = Shards
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision' = "apply"
    /\ UNCHANGED <<prepared, auxReady, pvars, fvars, avars, rvars, cvars>>
DecideAbort ==
    /\ decision = "none" \/ (BugAbortAfterDecision /\ decision = "apply")
    /\ decision' = "abort"
    /\ UNCHANGED <<prepared, auxReady, pvars, fvars, avars, rvars, cvars>>

Unname(s) ==
    /\ s \in (awake \cap prepared) \ nameGone
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ nameGone' = nameGone \cup {s}
    /\ UNCHANGED <<jvars, catalog, spaces, renamed, oldRef, fvars, avars, rvars, cvars>>
Rename(s) ==
    /\ s \in (awake \cap prepared \cap nameGone) \ renamed
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ renamed' = renamed \cup {s}
    /\ UNCHANGED <<jvars, catalog, spaces, oldRef, nameGone, fvars, avars, rvars, cvars>>
Publish(s) ==
    /\ s \in awake \cap renamed /\ catalog[s] = Old
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ catalog' = [catalog EXCEPT ![s] = New]
    /\ retired' = retired \cup {s}
    /\ doomed' = IF BugOldStreamValidates THEN doomed ELSE doomed \cup {s}
    /\ UNCHANGED <<jvars, spaces, renamed, oldRef, nameGone, fvars, pendingOld, ackedOld,
                   streamCommitted, observation, cvars>>
DropOriginal(s) ==
    /\ s \in awake /\ oldRef[s] \in spaces[s]
    /\ (decision = "apply" /\ catalog[s] = New) \/ BugDropBeforePublish
    /\ spaces' = [spaces EXCEPT ![s] = @ \ {oldRef[s]}]
    /\ UNCHANGED <<jvars, catalog, renamed, oldRef, nameGone, fvars, avars, rvars, cvars>>
AbortShadow(s) ==
    /\ s \in awake /\ decision = "abort" /\ New \in spaces[s]
    /\ spaces' = [spaces EXCEPT ![s] = @ \ {New}]
    /\ UNCHANGED <<jvars, catalog, renamed, oldRef, nameGone, fvars, avars, rvars, cvars>>

Crash(s) ==
    /\ ~crashed /\ s \in awake
    /\ awake' = awake \ {s} /\ crashed' = TRUE
    /\ UNCHANGED <<jvars, pvars, avars, rvars, cvars>>
Resume(s) ==
    /\ s \in Shards \ awake
    /\ awake' = awake \cup {s}
    /\ oldRef' = IF BugRenameLosesOriginalIdentity /\ s \in renamed
                  THEN [oldRef EXCEPT ![s] = New] ELSE oldRef
    /\ UNCHANGED <<jvars, catalog, spaces, renamed, nameGone, crashed, avars, rvars, cvars>>

(* Explicit durable retirement certifies an obsolete generation; lack of
   a current source space or a not-ready replacement never certifies ACK. *)
AckOld(s) ==
    /\ s \in pendingOld /\ s \in retired
    /\ pendingOld' = pendingOld \ {s} /\ ackedOld' = ackedOld \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, retired, rvars, cvars>>
ForgetOld(s) ==
    /\ BugForgetOldObligations /\ s \in pendingOld
    /\ pendingOld' = pendingOld \ {s}
    /\ UNCHANGED <<jvars, pvars, fvars, retired, ackedOld, rvars, cvars>>

ValidateOldStream(s) ==
    /\ s \in awake \ streamCommitted /\ s \notin doomed
    /\ catalog[s] = New
    /\ streamCommitted' = streamCommitted \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, avars, doomed, observation, cvars>>
(* Definition bytes/version and SID belong to the SAME confirmed catalog
   observation. A native rename changes names before that metadata changes;
   resolving the public name can return 0 or New paired with Old's JSON.
   The observation is retained independently of later metadata changes. *)
ObserveCatalog(s) ==
    /\ s \in awake
    /\ LET committed == catalog[s]
           named == IF s \in renamed THEN New ELSE IF s \in nameGone THEN 0 ELSE Old
       IN observation' = [sid |-> IF BugCatalogByName THEN named ELSE committed,
                          def_id |-> committed, json_version |-> IF committed = Old THEN 0 ELSE 1]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, doomed, streamCommitted, cvars>>
AllocateNew ==
    /\ ~allocatedNew /\ \E s \in Shards : catalog[s] = New
    /\ LET n == IF BugResetCounterIdentity THEN 2 ELSE counter + 1
       IN /\ reused' = (n \in allocated)
          /\ allocated' = allocated \cup {n} /\ counter' = n
    /\ allocatedNew' = TRUE
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars>>

Next == (\E s \in Shards : Prepared(s) \/ ReadyAux(s) \/ Unname(s) \/ Rename(s) \/ Publish(s)
                 \/ DropOriginal(s) \/ AbortShadow(s) \/ Crash(s) \/ Resume(s)
                 \/ AckOld(s) \/ ForgetOld(s) \/ ValidateOldStream(s) \/ ObserveCatalog(s))
        \/ DecideApply \/ DecideAbort \/ AllocateNew
Spec == Init /\ [][Next]_vars

TypeOK == /\ prepared \subseteq Shards /\ auxReady \subseteq Shards
          /\ decision \in {"none", "apply", "abort"}
          /\ catalog \in [Shards -> Ids] /\ spaces \in [Shards -> SUBSET Ids]
          /\ renamed \subseteq Shards /\ nameGone \subseteq Shards /\ oldRef \in [Shards -> Ids]
          /\ awake \subseteq Shards /\ crashed \in BOOLEAN
          /\ retired \subseteq Shards /\ pendingOld \subseteq Shards /\ ackedOld \subseteq Shards
          /\ doomed \subseteq Shards /\ streamCommitted \subseteq Shards
          /\ allocated \subseteq 1..3 /\ counter \in 2..3
          /\ allocatedNew \in BOOLEAN /\ reused \in BOOLEAN
          /\ observation \in [sid : 0..2, def_id : Ids, json_version : 0..1]
CatalogHasSpace == \A s \in Shards : catalog[s] \in spaces[s]
PublishedReady == \A s \in Shards : catalog[s] = New => s \in auxReady
NoAbortPublished == decision = "abort" => \A s \in Shards : catalog[s] = Old
OldObligationsRetained == pendingOld \cup ackedOld = Shards
(* This stream is still open at publication. A validation before that is
   harmless, so only an actual post-publication validation is recorded. *)
OldStreamsRejected == \A s \in streamCommitted : catalog[s] # New
CounterNeverReuses == ~reused
CatalogObservationBound == /\ observation.sid = observation.def_id
                           /\ observation.json_version = IF observation.def_id = Old THEN 0 ELSE 1
=============================================================================
