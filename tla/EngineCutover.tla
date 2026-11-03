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
   is not a proof of Tarantool replication or full DDL linearizability.
   A rebuild in the table's own engine keeps the table's global index
   spaces instead (rebuild = 'aux', issue #184): both definitions write
   them, so they are neither doomed nor retired. They are named after the
   space that has the table's name, and found by that name or through the
   catalog entry of the table whose name a space has (UIX.spaces); APPLY
   renames them after the native renames and before the catalog commit.
   It keeps the table's lazy generations too: their targets, entries and
   deliveries name only the generation, but each generation's descriptor
   and the capture name the table's space. Publication binds them to the
   new space in its catalog transaction, and ABORT retires none of them.
   The shard's lazy registry, a cache of what is captured, is read from
   confirmed state: one read while the catalog's transaction waits for its
   quorum, and used after it, must not take the new space's writes
   uncaptured. A copy that adds a lazy index makes its generation, built as
   an ALTER in place builds it: online against the old space before the
   copy (or, on a shard that did not, scanned behind PREPARE's fence), its
   descriptor naming the old space until publication binds it with the kept
   ones, ready only after its scan: READY makes it so, or APPLY when the
   coordinator sends no READY. ABORT retires it, writing a retired
   descriptor for one that has none, so that a late start of the job's
   build, once a later job has replaced the job's record, finds it retired.
   The registry takes the new space as pending also when the copy keeps no
   generation and only makes one (keeps). For other jobs the kept and made
   sets are empty, and their invariants hold vacuously.
   A rebuild retires the global index spaces and lazy generations its ALTER
   drops as an engine change retires all of its table's (issue #184):
   retired is that retirement, in the catalog's transaction, where the
   generations retire and the old definition's last readers are doomed,
   and cleanup drops the global index spaces with the original. Until then
   the old definition names them, and its writes and readers reach them;
   ABORT retires none of them, and leaves the table as it was. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugPublishBeforeDecision, BugPublishBeforeAuxReady, BugDropBeforePublish,
          BugAbortAfterDecision, BugApplyAfterAbort, BugOldStreamValidates, BugForgetOldObligations,
          BugResetCounterIdentity, BugRenameLosesOriginalIdentity, BugCatalogByName,
          BugAuxStreamOmitted, BugKeptByNameOnly, BugPublishBeforeKeptRename, BugRetireKept,
          BugRebindAfterPublish, BugAbortRetiresKept, BugNoPendingNew,
          BugMadeUnbound, BugAbortKeepsMade, BugAbortSkipsMissing, BugPendingOnlyKept,
          BugReadyUnscanned, BugPublishUnready, BugAbortRetiresDropped, BugRetireBeforePublish

Shards == {"A", "B"}
Old == 1
New == 2
Ids == {Old, New}
VARIABLES prepared, auxReady, decision, catalog, spaces, renamed, oldRef, nameGone,
          awake, crashed, retired, pendingOld, ackedOld, doomed, streamCommitted,
          allocated, counter, allocatedNew, reused, observation, auxDoomed, auxStreamCommitted,
          keptName, keptAlive, lazyBase, lazyAlive, publishing, lazyView, uncaptured,
          madePhase, madeBase, madeScanned, abortedRec, recordGone, keeps, coordReady
jvars == <<prepared, auxReady, decision>>
pvars == <<catalog, spaces, renamed, oldRef, nameGone>>
fvars == <<awake, crashed>>
avars == <<retired, pendingOld, ackedOld>>
rvars == <<doomed, streamCommitted, observation, auxDoomed, auxStreamCommitted>>
cvars == <<allocated, counter, allocatedNew, reused>>
kvars == <<keptName, keptAlive>>
lvars == <<lazyBase, lazyAlive>>
wvars == <<publishing, lazyView, uncaptured>>
mvars == <<madePhase, madeBase, madeScanned, abortedRec, recordGone, keeps, coordReady>>
vars == <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars, mvars>>

Init ==
    /\ prepared = {} /\ auxReady = {} /\ decision = "none"
    /\ catalog = [s \in Shards |-> Old] /\ spaces = [s \in Shards |-> Ids]
    /\ renamed = {} /\ nameGone = {} /\ oldRef = [s \in Shards |-> Old]
    /\ awake = Shards /\ crashed = FALSE
    /\ retired = {} /\ pendingOld = Shards /\ ackedOld = {}
    /\ doomed = {} /\ streamCommitted = {}
    /\ auxDoomed = {} /\ auxStreamCommitted = {}
    /\ observation = [sid |-> Old, def_id |-> Old, json_version |-> 0]
    \* IDs 1 and 2 were allocated already; 2 may have been deleted. A
    \* max(current rows) floor cannot replace stable ai:/rid: identity.
    /\ allocated = {1, 2} /\ counter = 2 /\ allocatedNew = FALSE /\ reused = FALSE
    \* The kept space is named after the space with the table's name.
    /\ keptName = [s \in Shards |-> Old] /\ keptAlive = [s \in Shards |-> TRUE]
    \* The space a kept lazy generation's descriptor and capture name.
    /\ lazyBase = [s \in Shards |-> Old] /\ lazyAlive = [s \in Shards |-> TRUE]
    \* What each shard's lazy registry last read of the new space: nothing yet.
    /\ publishing = {} /\ lazyView = [s \in Shards |-> "none"] /\ uncaptured = FALSE
    \* The generation the copy makes: started online before the copy, its
    \* scan ended or not, or not on this shard (no descriptor). Whether the
    \* copy also keeps a generation, and whether its coordinators send READY
    \* (one from before such copies sends none).
    /\ madePhase \in [Shards -> {"absent", "building"}]
    /\ madeBase = [s \in Shards |-> IF madePhase[s] = "building" THEN Old ELSE 0]
    /\ madeScanned \in [Shards -> BOOLEAN]
    /\ \A s \in Shards : madePhase[s] = "absent" => ~madeScanned[s]
    /\ abortedRec = {} /\ recordGone = {} /\ keeps \in BOOLEAN /\ coordReady \in BOOLEAN

(* PREPARE also takes the generation the copy makes (L.prepare: a
   descriptor for the old space, if the online start wrote none). A scan
   that has not ended goes on behind the fence (ScanMade). *)
Prepared(s) ==
    /\ s \in awake \ prepared /\ decision = "none"
    /\ prepared' = prepared \cup {s}
    /\ madePhase' = [madePhase EXCEPT ![s] = IF @ = "absent" THEN "building" ELSE @]
    /\ madeBase' = [madeBase EXCEPT ![s] = IF madePhase[s] = "absent" THEN Old ELSE @]
    /\ UNCHANGED <<auxReady, decision, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   madeScanned, abortedRec, recordGone, keeps, coordReady>>
(* READY, which marks the made generation ready once its scan has ended
   and its deliveries are acknowledged (L.ready), from a coordinator that
   sends it. BugReadyUnscanned does not wait for the scan's end. *)
ReadyAux(s) ==
    /\ s \in (awake \cap prepared) \ auxReady /\ decision = "none"
    /\ ~coordReady \/ madeScanned[s] \/ BugReadyUnscanned
    /\ auxReady' = auxReady \cup {s}
    /\ madePhase' = IF coordReady THEN [madePhase EXCEPT ![s] = "ready"] ELSE madePhase
    /\ UNCHANGED <<prepared, decision, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   madeBase, madeScanned, abortedRec, recordGone, keeps, coordReady>>

DecideApply ==
    /\ decision = "none" \/ (BugApplyAfterAbort /\ decision = "abort")
    /\ prepared = Shards
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision' = "apply"
    /\ UNCHANGED <<prepared, auxReady, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars, mvars>>
DecideAbort ==
    /\ decision = "none" \/ (BugAbortAfterDecision /\ decision = "apply")
    /\ decision' = "abort"
    /\ UNCHANGED <<prepared, auxReady, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars, mvars>>

Unname(s) ==
    /\ s \in (awake \cap prepared) \ nameGone
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ nameGone' = nameGone \cup {s}
    /\ UNCHANGED <<jvars, catalog, spaces, renamed, oldRef, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   mvars>>
Rename(s) ==
    /\ s \in (awake \cap prepared \cap nameGone) \ renamed
    /\ auxReady = Shards \/ BugPublishBeforeAuxReady
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ renamed' = renamed \cup {s}
    /\ UNCHANGED <<jvars, catalog, spaces, oldRef, nameGone, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   mvars>>
(* The space with the table's name, if one has it: the one whose kept
   spaces a name lookup and the catalog both find. *)
Named(s) == IF s \in renamed THEN New ELSE IF s \in nameGone THEN 0 ELSE Old
(* Repeatable: by id, from whichever name it has. *)
RenameKept(s) ==
    /\ s \in awake \cap renamed /\ catalog[s] = Old /\ keptName[s] = Old
    /\ keptName' = [keptName EXCEPT ![s] = New]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, keptAlive, lvars, wvars, mvars>>
(* The catalog's transaction drops the shard's lazy registry before it
   commits, then waits for its quorum (Publish confirms it): a registry
   read meanwhile (ReadLazy) reads the confirmed state, from before it. *)
BeginPublish(s) ==
    /\ s \in (awake \cap renamed) \ publishing /\ catalog[s] = Old
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ keptName[s] = New \/ BugPublishBeforeKeptRename
    /\ publishing' = publishing \cup {s}
    /\ lazyView' = [lazyView EXCEPT ![s] = "stale"]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, uncaptured, mvars>>
Publish(s) ==
    /\ s \in awake \cap publishing /\ catalog[s] = Old
    /\ decision = "apply" \/ (BugPublishBeforeDecision /\ decision = "none")
    /\ keptName[s] = New \/ BugPublishBeforeKeptRename
    /\ catalog' = [catalog EXCEPT ![s] = New]
    /\ lazyBase' = IF BugRebindAfterPublish THEN lazyBase ELSE [lazyBase EXCEPT ![s] = New]
    \* (BugMadeUnbound: publication binds only the generations the copy keeps.)
    /\ madeBase' = IF BugMadeUnbound \/ madePhase[s] \notin {"building", "ready"} THEN madeBase
                   ELSE [madeBase EXCEPT ![s] = New]
    \* APPLY makes a made generation that no READY made ready so itself: it
    \* ends the scan, waits for the deliveries and marks it ready, before it
    \* publishes (BugPublishUnready publishes it as it is).
    /\ madePhase' = IF BugPublishUnready \/ madePhase[s] # "building" THEN madePhase
                    ELSE [madePhase EXCEPT ![s] = "ready"]
    /\ madeScanned' = IF BugPublishUnready \/ madePhase[s] # "building" THEN madeScanned
                      ELSE [madeScanned EXCEPT ![s] = TRUE]
    /\ retired' = retired \cup {s}
    /\ doomed' = IF BugOldStreamValidates THEN doomed ELSE doomed \cup {s}
    /\ auxDoomed' = IF BugAuxStreamOmitted THEN auxDoomed ELSE auxDoomed \cup {s}
    /\ publishing' = publishing \ {s}
    \* (A registry read before the confirmation may stay in use after it.)
    /\ UNCHANGED <<jvars, spaces, renamed, oldRef, nameGone, fvars, pendingOld, ackedOld,
                   streamCommitted, observation, auxStreamCommitted, cvars, kvars, lazyAlive,
                   lazyView, uncaptured, abortedRec, recordGone, keeps, coordReady>>
DropOriginal(s) ==
    /\ s \in awake /\ oldRef[s] \in spaces[s]
    /\ (decision = "apply" /\ catalog[s] = New) \/ BugDropBeforePublish
    /\ spaces' = [spaces EXCEPT ![s] = @ \ {oldRef[s]}]
    \* (Cleanup drops the retired global index spaces, never the kept ones.)
    /\ keptAlive' = IF BugRetireKept THEN [keptAlive EXCEPT ![s] = FALSE] ELSE keptAlive
    /\ UNCHANGED <<jvars, catalog, renamed, oldRef, nameGone, fvars, avars, rvars, cvars, keptName, lvars, wvars,
                   mvars>>
AbortShadow(s) ==
    /\ s \in awake /\ decision = "abort" /\ New \in spaces[s]
    /\ spaces' = [spaces EXCEPT ![s] = @ \ {New}]
    /\ UNCHANGED <<jvars, catalog, renamed, oldRef, nameGone, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   mvars>>

Crash(s) ==
    /\ ~crashed /\ s \in awake
    /\ awake' = awake \ {s} /\ crashed' = TRUE
    \* (The unconfirmed transaction is lost; a restart reads the registry anew.)
    /\ publishing' = publishing \ {s} /\ lazyView' = [lazyView EXCEPT ![s] = "stale"]
    /\ UNCHANGED <<jvars, pvars, avars, rvars, cvars, kvars, lvars, uncaptured, mvars>>
Resume(s) ==
    /\ s \in Shards \ awake
    /\ awake' = awake \cup {s}
    /\ oldRef' = IF BugRenameLosesOriginalIdentity /\ s \in renamed
                  THEN [oldRef EXCEPT ![s] = New] ELSE oldRef
    /\ UNCHANGED <<jvars, catalog, spaces, renamed, nameGone, crashed, avars, rvars, cvars, kvars, lvars, wvars,
                   mvars>>

(* Explicit durable retirement certifies an obsolete generation; lack of
   a current source space or a not-ready replacement never certifies ACK. *)
AckOld(s) ==
    /\ s \in pendingOld /\ s \in retired
    /\ pendingOld' = pendingOld \ {s} /\ ackedOld' = ackedOld \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, retired, rvars, cvars, kvars, lvars, wvars, mvars>>
ForgetOld(s) ==
    /\ BugForgetOldObligations /\ s \in pendingOld
    /\ pendingOld' = pendingOld \ {s}
    /\ UNCHANGED <<jvars, pvars, fvars, retired, ackedOld, rvars, cvars, kvars, lvars, wvars, mvars>>

ValidateOldStream(s) ==
    /\ s \in awake \ streamCommitted /\ s \notin doomed
    /\ catalog[s] = New
    /\ streamCommitted' = streamCommitted \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, avars, doomed, observation, auxDoomed, auxStreamCommitted, cvars, kvars, lvars, wvars,
                   mvars>>
(* A missing strong-global-index key reads only the old auxiliary space:
   there is no base fetch/read tracker to make the base watermark suffice. *)
ValidateOldAuxStream(s) ==
    /\ s \in awake \ auxStreamCommitted /\ s \notin auxDoomed
    /\ catalog[s] = New
    /\ auxStreamCommitted' = auxStreamCommitted \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, avars, doomed, streamCommitted, observation, auxDoomed, cvars, kvars, lvars, wvars,
                   mvars>>
(* Definition bytes/version and SID belong to the SAME confirmed catalog
   observation. A native rename changes names before that metadata changes;
   resolving the public name can return 0 or New paired with Old's JSON.
   The observation is retained independently of later metadata changes. *)
ObserveCatalog(s) ==
    /\ s \in awake
    /\ LET committed == catalog[s]
           named == Named(s)
       IN observation' = [sid |-> IF BugCatalogByName THEN named ELSE committed,
                          def_id |-> committed, json_version |-> IF committed = Old THEN 0 ELSE 1]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, doomed, streamCommitted, auxDoomed, auxStreamCommitted, cvars, kvars, lvars, wvars,
                   mvars>>
AllocateNew ==
    /\ ~allocatedNew /\ \E s \in Shards : catalog[s] = New
    /\ LET n == IF BugResetCounterIdentity THEN 2 ELSE counter + 1
       IN /\ reused' = (n \in allocated)
          /\ allocated' = allocated \cup {n} /\ counter' = n
    /\ allocatedNew' = TRUE
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, kvars, lvars, wvars, mvars>>

(* Controls: binding the kept generation after the catalog commit, in a step
   of its own; an ABORT that retires the generations the job names. *)
RebindLazy(s) ==
    /\ BugRebindAfterPublish /\ s \in awake /\ catalog[s] = New /\ lazyBase[s] = Old
    /\ lazyBase' = [lazyBase EXCEPT ![s] = New]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lazyAlive, wvars, mvars>>
AbortLazy(s) ==
    /\ BugAbortRetiresKept /\ s \in awake /\ decision = "abort" /\ lazyAlive[s]
    /\ lazyAlive' = [lazyAlive EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lazyBase, wvars, mvars>>

(* The shard's lazy registry, read from confirmed state at any time: the
   new space is a captured source once the catalog names it, and until then
   pending (refused), since the job's record keeps or makes generations for
   it; BugNoPendingNew leaves it unknown, and BugPendingOnlyKept does when
   the copy keeps none. A write to the new space with the new
   definition, which the engine registry admits once published, is refused
   by a stale or pending view, captured by a bound one, and taken uncaptured
   by one that does not know the space. *)
ReadLazy(s) ==
    /\ s \in awake
    /\ lazyView' = [lazyView EXCEPT ![s] = IF catalog[s] = New THEN "bound"
                                          ELSE IF BugNoPendingNew \/ (BugPendingOnlyKept /\ ~keeps)
                                               THEN "none" ELSE "pending"]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, publishing, uncaptured, mvars>>
WriteNew(s) ==
    /\ s \in awake /\ catalog[s] = New /\ lazyView[s] \in {"none", "bound"}
    /\ uncaptured' = (uncaptured \/ lazyView[s] = "none")
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, publishing, lazyView, mvars>>

(* The online start of the made generation's build (tarn_lazy_start), by
   the job's worker or a late one: it writes the generation's descriptor,
   for the old space, unless one exists (a retired one refuses it,
   L.prepare), the job's record says the job was aborted, or the catalog
   names the new space. *)
StartMade(s) ==
    /\ s \in awake /\ madePhase[s] = "absent" /\ catalog[s] = Old
    /\ s \notin abortedRec \/ s \in recordGone
    /\ madePhase' = [madePhase EXCEPT ![s] = "building"]
    /\ madeBase' = [madeBase EXCEPT ![s] = Old]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   madeScanned, abortedRec, recordGone, keeps, coordReady>>
(* Its baseline scan: online while the old space's writes are captured for
   it (tla/LazyBaseline.tla), or behind the fence in the coordinator's
   batches after PREPARE. *)
ScanMade(s) ==
    /\ s \in awake /\ madePhase[s] = "building" /\ ~madeScanned[s]
    /\ madeScanned' = [madeScanned EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   madePhase, madeBase, abortedRec, recordGone, keeps, coordReady>>
(* ABORT's transaction, once the shadow is dropped: the job's record is
   aborted, and the generation the copy made retires with it, a retired
   descriptor written for one that has none (L.retire_made). It retires
   none the copy keeps (KeptLazyAlive), nor any it would have dropped
   (RetiredOnlyUnnamed: BugAbortRetiresDropped retires those, as APPLY
   would). *)
AbortRecord(s) ==
    /\ s \in awake \ abortedRec /\ decision = "abort" /\ New \notin spaces[s]
    /\ abortedRec' = abortedRec \cup {s}
    /\ madePhase' = [madePhase EXCEPT ![s] =
                        IF BugAbortKeepsMade \/ (BugAbortSkipsMissing /\ @ = "absent") THEN @ ELSE "retired"]
    /\ retired' = IF BugAbortRetiresDropped THEN retired \cup {s} ELSE retired
    /\ UNCHANGED <<jvars, pvars, fvars, pendingOld, ackedOld, rvars, cvars, kvars, lvars, wvars,
                   madeBase, madeScanned, recordGone, keeps, coordReady>>
(* Control: what the copy drops is retired, and its global index spaces
   dropped, in a step of its own once the shard has prepared, before the
   catalog commit. APPLY retires the generations in the catalog's
   transaction (Publish), and cleanup drops the spaces after it. *)
RetireEarly(s) ==
    /\ BugRetireBeforePublish /\ s \in awake \cap prepared /\ s \notin retired /\ catalog[s] = Old
    /\ retired' = retired \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, pendingOld, ackedOld, rvars, cvars, kvars, lvars, wvars, mvars>>
(* A later job of the table replaces the aborted job's record: a start of
   the aborted job's build still in flight no longer finds it. *)
ForgetRecord(s) ==
    /\ s \in abortedRec \ recordGone
    /\ recordGone' = recordGone \cup {s}
    /\ UNCHANGED <<jvars, pvars, fvars, avars, rvars, cvars, kvars, lvars, wvars,
                   madePhase, madeBase, madeScanned, abortedRec, keeps, coordReady>>

Next == (\E s \in Shards : Prepared(s) \/ ReadyAux(s) \/ Unname(s) \/ Rename(s) \/ RenameKept(s)
                 \/ BeginPublish(s) \/ Publish(s)
                 \/ DropOriginal(s) \/ AbortShadow(s) \/ Crash(s) \/ Resume(s)
                 \/ AckOld(s) \/ ForgetOld(s) \/ ValidateOldStream(s) \/ ValidateOldAuxStream(s)
                 \/ ObserveCatalog(s) \/ RebindLazy(s) \/ AbortLazy(s) \/ ReadLazy(s) \/ WriteNew(s)
                 \/ StartMade(s) \/ ScanMade(s) \/ AbortRecord(s) \/ ForgetRecord(s) \/ RetireEarly(s))
        \/ DecideApply \/ DecideAbort \/ AllocateNew
Spec == Init /\ [][Next]_vars

TypeOK == /\ prepared \subseteq Shards /\ auxReady \subseteq Shards
          /\ decision \in {"none", "apply", "abort"}
          /\ catalog \in [Shards -> Ids] /\ spaces \in [Shards -> SUBSET Ids]
          /\ renamed \subseteq Shards /\ nameGone \subseteq Shards /\ oldRef \in [Shards -> Ids]
          /\ awake \subseteq Shards /\ crashed \in BOOLEAN
          /\ retired \subseteq Shards /\ pendingOld \subseteq Shards /\ ackedOld \subseteq Shards
          /\ doomed \subseteq Shards /\ streamCommitted \subseteq Shards
          /\ auxDoomed \subseteq Shards /\ auxStreamCommitted \subseteq Shards
          /\ allocated \subseteq 1..3 /\ counter \in 2..3
          /\ allocatedNew \in BOOLEAN /\ reused \in BOOLEAN
          /\ observation \in [sid : 0..2, def_id : Ids, json_version : 0..1]
          /\ keptName \in [Shards -> Ids] /\ keptAlive \in [Shards -> BOOLEAN]
          /\ lazyBase \in [Shards -> Ids] /\ lazyAlive \in [Shards -> BOOLEAN]
          /\ publishing \subseteq Shards /\ uncaptured \in BOOLEAN
          /\ lazyView \in [Shards -> {"stale", "none", "pending", "bound"}]
          /\ madePhase \in [Shards -> {"absent", "building", "ready", "retired"}]
          /\ madeBase \in [Shards -> {0} \cup Ids] /\ madeScanned \in [Shards -> BOOLEAN]
          /\ abortedRec \subseteq Shards /\ recordGone \subseteq Shards /\ keeps \in BOOLEAN
          /\ coordReady \in BOOLEAN
CatalogHasSpace == \A s \in Shards : catalog[s] \in spaces[s]
PublishedReady == \A s \in Shards : catalog[s] = New => s \in auxReady
NoAbortPublished == decision = "abort" => \A s \in Shards : catalog[s] = Old
OldObligationsRetained == pendingOld \cup ackedOld = Shards
(* This stream is still open at publication. A validation before that is
   harmless, so only an actual post-publication validation is recorded. *)
OldStreamsRejected == /\ \A s \in streamCommitted : catalog[s] # New
                      /\ \A s \in auxStreamCommitted : catalog[s] # New
CounterNeverReuses == ~reused
CatalogObservationBound == /\ observation.sid = observation.def_id
                           /\ observation.json_version = IF observation.def_id = Old THEN 0 ELSE 1
(* Whether a lookup of the kept spaces of a table by space sid finds the
   kept space: those named after sid, and those the catalog entry of the
   table whose name sid has lists (by its name: catalog[s] may still be the
   other space's). Whether the space still exists is KeptAlive's. *)
KeptFoundBy(s, sid) ==
    \/ keptName[s] = sid
    \/ ~BugKeptByNameOnly /\ sid # 0 /\ sid = Named(s)
(* DROP, TRUNCATE, drains, bucket moves and exports find the table's global
   index spaces by the space that has its name, throughout publication. *)
KeptFound == \A s \in Shards : Named(s) # 0 => KeptFoundBy(s, Named(s))
(* Once the catalog names the new space, the kept spaces are named after it,
   as the catalog's definition names them. *)
KeptNamedAfterCatalog == \A s \in Shards : catalog[s] = New => keptName[s] = New
KeptAlive == \A s \in Shards : keptAlive[s]
(* A kept lazy generation is bound to the space the catalog names: writes
   to that space are captured for it, and a later change of the table finds
   it bound to the table. ABORT leaves it to the table. *)
KeptLazyBound == \A s \in Shards : lazyBase[s] = catalog[s]
KeptLazyAlive == \A s \in Shards : lazyAlive[s]
(* Every write the new space takes is captured for the generations the
   copy keeps and makes. *)
KeptLazyCaptured == ~uncaptured
(* The generation the copy makes, while live, is bound to the space the
   catalog names: the old one until publication, whose writes it captures,
   then the new one. *)
MadeBound == \A s \in Shards : madePhase[s] \in {"building", "ready"} => madeBase[s] = catalog[s]
(* It is published ready, its scan ended: after the coordinator's READY,
   or, from a coordinator that sends none, after APPLY's own. *)
MadePublished == \A s \in Shards : catalog[s] = New => madePhase[s] = "ready" /\ madeScanned[s]
(* Nothing of an aborted copy's made generation lives on: it would capture
   the old space's writes and hold up its bucket moves (L.building). *)
AbortedMadeRetired == \A s \in abortedRec : madePhase[s] \in {"absent", "retired"}
(* The job retires a generation, or a global index space, that the old
   definition names only once the catalog names the new space: until then
   the old definition's writes must reach it, and its lookups find it. An
   aborted job leaves the table every index it had, those its ALTER would
   have dropped included. *)
RetiredOnlyUnnamed == \A s \in retired : catalog[s] = New
=============================================================================
