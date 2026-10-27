---------------------------- MODULE BackfillDDL ----------------------------
(* Local native-DDL refinement of issue #70, separate from row/undo safety. *)
(* A native index build has an invisible yielding interval. An ABORT may   *)
(* otherwise skip its drop because the index is not visible yet. Two       *)
(* ABORT RPCs may also overlap: one completes and allows a new job to reuse *)
(* the numeric index ID before the other's delayed cleanup. One mutex      *)
(* around each ENTIRE ALTER RPC prevents both schedules. Admission below   *)
(* is after acquisition. Nested same-fiber calls belong to the same RPC;   *)
(* they do not release its abstract lock. Native DDL completes or rolls    *)
(* back before that RPC returns. No native-DDL crash recovery is proved.   *)
EXTENDS Naturals, FiniteSets

CONSTANTS BugNoBuildQuiescence, BugNoFinisherSerialization, BugIgnoreSpaceIdentity
ASSUME /\ BugNoBuildQuiescence \in BOOLEAN
       /\ BugNoFinisherSerialization \in BOOLEAN
       /\ BugIgnoreSpaceIdentity \in BOOLEAN

Aborters == {"abortA", "abortB"}
Actors == Aborters \cup {"prepare", "next"}

VARIABLES active, job, phase, index, catalog, prepare, abort, next, incarnation
vars == <<active, job, phase, index, catalog, prepare, abort, next, incarnation>>

(* All the bounded requests carry the original wire space ID, 1. Names and
   catalog versions alone cannot bind a request after DROP + CREATE. *)
MatchesSpace == incarnation = 1 \/ BugIgnoreSpaceIdentity

(* The correct case admits one RPC. The controls omit two logical uses of
   the SAME runtime mutex, rather than proposing two independent locks. *)
MayEnter(a) ==
    \A b \in active :
        \/ /\ BugNoBuildQuiescence
              /\ ((a = "prepare" /\ b \in Aborters)
                  \/ (b = "prepare" /\ a \in Aborters))
        \/ /\ BugNoFinisherSerialization
              /\ a # "prepare" /\ b # "prepare"

Init ==
    /\ active = {}
    /\ job = 1 /\ phase = "preparing" /\ index = "none" /\ catalog = 0
    /\ prepare = "idle" /\ abort = [a \in Aborters |-> "idle"]
    /\ next = "idle"
    /\ incarnation = 1

BeginPrepare ==
    /\ prepare = "idle" /\ MayEnter("prepare")
    /\ MatchesSpace
    /\ job = 1 /\ phase = "preparing"
    /\ active' = active \cup {"prepare"} /\ prepare' = "building"
    /\ UNCHANGED <<job, phase, index, catalog, abort, next, incarnation>>

CompleteNativeBuild ==
    /\ prepare = "building"
    (* Its effect cannot be undone by a later row-transaction owner check. *)
    /\ index' = "old" /\ prepare' = "returning"
    /\ UNCHANGED <<active, job, phase, catalog, abort, next, incarnation>>

ReturnPrepare ==
    /\ prepare = "returning"
    /\ phase' = IF job = 1 /\ phase = "preparing" THEN "prepared" ELSE phase
    /\ active' = active \ {"prepare"} /\ prepare' = "done"
    /\ UNCHANGED <<job, index, catalog, abort, next, incarnation>>

BeginAbort(a) ==
    /\ abort[a] = "idle" /\ MayEnter(a)
    /\ MatchesSpace
    (* Durable version/phase is rechecked under the lock, before any DDL. *)
    /\ job = 1 /\ phase \in {"preparing", "prepared", "aborting"}
    /\ active' = active \cup {a}
    /\ phase' = "aborting" /\ abort' = [abort EXCEPT ![a] = "drop"]
    /\ UNCHANGED <<job, index, catalog, prepare, next, incarnation>>

DropCandidate(a) ==
    /\ abort[a] = "drop"
    (* Native lookup/drop uses the reusable numeric temporary index ID. *)
    /\ index' = "none" /\ abort' = [abort EXCEPT ![a] = "finish"]
    /\ UNCHANGED <<active, job, phase, catalog, prepare, next, incarnation>>

FinishAbort(a) ==
    /\ abort[a] = "finish"
    /\ phase' = IF job = 1 /\ phase = "aborting" THEN "aborted" ELSE phase
    /\ active' = active \ {a} /\ abort' = [abort EXCEPT ![a] = "done"]
    /\ UNCHANGED <<job, index, catalog, prepare, next, incarnation>>

BeginNext ==
    /\ next = "idle" /\ MayEnter("next")
    /\ MatchesSpace
    /\ job = 1 /\ phase = "aborted"
    /\ job' = 2 /\ phase' = "preparing"
    /\ active' = active \cup {"next"} /\ next' = "build"
    /\ UNCHANGED <<index, catalog, prepare, abort, incarnation>>

BuildNext ==
    /\ next = "build"
    (* Existing temporary IDs are skipped. An old candidate has a
       different definition, so it must have been removed before reuse. *)
    /\ index' = IF index = "none" THEN "new" ELSE index
    /\ next' = "publish"
    /\ UNCHANGED <<active, job, phase, catalog, prepare, abort, incarnation>>

PublishNext ==
    /\ next = "publish"
    /\ catalog' = 2 /\ phase' = "published" /\ next' = "returning"
    /\ UNCHANGED <<active, job, index, prepare, abort, incarnation>>

ReturnNext ==
    /\ next = "returning"
    /\ active' = active \ {"next"} /\ next' = "done"
    /\ UNCHANGED <<job, phase, index, catalog, prepare, abort, incarnation>>

RecreateSpace ==
    /\ incarnation = 1 /\ active = {}
    /\ incarnation' = 2
    (* The replacement's fresh catalog version is below the old request's
       version again. A delayed copy can therefore pass the version test. *)
    /\ job' = 1 /\ phase' = "preparing" /\ index' = "none" /\ catalog' = 0
    /\ prepare' = "idle" /\ abort' = [a \in Aborters |-> "idle"]
    /\ next' = "idle"
    /\ UNCHANGED active

Next == BeginPrepare \/ CompleteNativeBuild \/ ReturnPrepare
        \/ (\E a \in Aborters : BeginAbort(a) \/ DropCandidate(a) \/ FinishAbort(a))
        \/ BeginNext \/ BuildNext \/ PublishNext \/ ReturnNext \/ RecreateSpace
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ active \subseteq Actors /\ job \in 1..2
    /\ phase \in {"preparing", "prepared", "aborting", "aborted", "published"}
    /\ index \in {"none", "old", "new"} /\ catalog \in {0, 2}
    /\ prepare \in {"idle", "building", "returning", "done"}
    /\ abort \in [Aborters -> {"idle", "drop", "finish", "done"}]
    /\ next \in {"idle", "build", "publish", "returning", "done"}
    /\ incarnation \in 1..2

NoLateAbortedIndex == job = 1 /\ phase = "aborted" => index = "none"
PublishedIndexMatches == catalog = 2 => index = "new"
ReplacementIndexUnchanged == incarnation = 2 => index = "none"
=============================================================================
