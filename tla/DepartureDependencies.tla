------------------------- MODULE DepartureDependencies -------------------------
(* Dependency drain before a logical shard can be stopped (#130).
 * D=1, retiring participant=2, survivor participant=3. A delayed operation
 * creates either a survivor PREPARED receipt or a decision STAGING receipt;
 * participant2 is missing, so recovery must reach it to obtain a safe fence.
 * A missing/unreachable peer is never treated as a NO or as removed NONE.
 *
 * Publish draining on ALL members before querying any drain gate. Register
 * local/D/parts dependencies before the first yield, retain them until the
 * owned RPC finishes commit/rollback, and count all confirmed PREPARED and
 * STAGING references, not only records whose decision is the retiring shard.
 * New admissions read membership in their transaction and validate that read
 * at commit, in addition to checking the current membership cache. An old
 * already validated PENDING_WAL promise may confirm after draining ACK; it
 * remains registered and must prevent removal. The model conservatively
 * separates confirmed-row visibility from pending WAL/RPC lifetime; it does
 * not rely on a fresh-prefix wait independently settling that pending WAL.
 *
 * Recovery needs healthy relevant clocks for a NEW absence-based fence.
 * An existing decision completes without clock health. HealthyDrainCompletes
 * assumes continuously healthy needed peers plus weakly fair local/recovery
 * actions. Persistent uncertain clocks may deliberately prevent removal.
 * This is not a WAL/MVCC implementation proof or a clock-certificate proof.
 *)
EXTENDS Naturals, FiniteSets

CONSTANTS Kind, BugDecisionOnly, BugLateAdmission, BugIgnoreStaging,
          BugNoInflight, BugNoCommitCheck
Nodes == {1, 2, 3}
Departing == 2
Decision == 1

VARIABLES kind, members, gates, removed, offline, phase, txActive,
          admittedAfterAck, latePromise, prepared, staging, missing,
          decision, healthy
vars == <<kind, members, gates, removed, offline, phase, txActive,
          admittedAfterAck, latePromise, prepared, staging, missing,
          decision, healthy>>
Owner == IF kind = "prepare" THEN 3 ELSE 1
DrainAck == \A s \in Nodes : members[s] # "active"
Registered == phase \in {"started", "checked", "pending_wal"}
RecordCounted(s) ==
    \/ /\ s = 3 /\ prepared /\ ~BugDecisionOnly
    \/ /\ s = Decision /\ staging /\ ~BugDecisionOnly /\ ~BugIgnoreStaging
GateClear(s) == ~RecordCounted(s)
                /\ (BugNoInflight \/ s # Owner \/ ~Registered)

Init ==
    /\ kind \in IF Kind = "either" THEN {"prepare", "staging"} ELSE {Kind}
    /\ members = [s \in Nodes |-> "active"] /\ gates = {}
    /\ removed = FALSE /\ offline = FALSE /\ phase = "network"
    /\ txActive = FALSE /\ admittedAfterAck = FALSE /\ latePromise = FALSE
    /\ prepared = FALSE /\ staging = FALSE /\ missing = "none"
    /\ decision = "none" /\ healthy = TRUE

Start ==
    /\ phase = "network"
    (* Registration and capture precede the operation's first possible yield. *)
    /\ phase' = "started" /\ txActive' = (members[Owner] = "active")
    /\ UNCHANGED <<kind, members, gates, removed, offline, admittedAfterAck,
                   latePromise, prepared, staging, missing, decision, healthy>>

CheckMembership ==
    /\ phase = "started"
    /\ phase' = IF healthy /\ (BugLateAdmission \/ (txActive /\ members[Owner] = "active"))
                 THEN "checked" ELSE "done"
    /\ UNCHANGED <<kind, members, gates, removed, offline, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision, healthy>>

BeginCommit ==
    /\ phase = "checked"
    (* Same-TX membership readset/MVCC validation, before irreversible WAL. *)
    /\ LET accepted == healthy /\ (BugLateAdmission \/ BugNoCommitCheck \/ members[Owner] = "active")
       IN /\ phase' = IF accepted THEN "pending_wal" ELSE "done"
          /\ admittedAfterAck' = (accepted /\ DrainAck)
    /\ UNCHANGED <<kind, members, gates, removed, offline, txActive,
                   latePromise, prepared, staging, missing, decision, healthy>>

ConfirmPromise ==
    /\ phase = "pending_wal"
    /\ phase' = "done"
    /\ prepared' = (kind = "prepare") /\ staging' = (kind = "staging")
    /\ latePromise' = admittedAfterAck
    /\ UNCHANGED <<kind, members, gates, removed, offline, txActive,
                   admittedAfterAck, missing, decision, healthy>>

PublishDrain(s) ==
    /\ members[s] = "active" /\ ~removed
    /\ members' = [members EXCEPT ![s] = "draining"]
    /\ UNCHANGED <<kind, gates, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision, healthy>>

QueryGate(s) ==
    /\ DrainAck /\ s \notin gates /\ GateClear(s)
    /\ gates' = gates \cup {s}
    /\ UNCHANGED <<kind, members, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision, healthy>>

Remove ==
    /\ gates = Nodes /\ ~removed
    /\ removed' = TRUE /\ members' = [s \in Nodes |-> "removed"]
    /\ UNCHANGED <<kind, gates, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision, healthy>>

Stop ==
    /\ removed /\ ~offline /\ offline' = TRUE
    /\ UNCHANGED <<kind, members, gates, removed, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision, healthy>>

RecoveryFenceMissing ==
    /\ (prepared \/ staging) /\ missing = "none" /\ ~offline /\ healthy
    /\ missing' = "aborted"
    /\ UNCHANGED <<kind, members, gates, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, decision, healthy>>

Decide ==
    /\ (prepared \/ staging) /\ missing = "aborted" /\ decision = "none"
    (* Existing STAGING -> final is completion; NONE -> ABORT still needs health. *)
    /\ staging \/ healthy
    /\ decision' = "aborted"
    /\ UNCHANGED <<kind, members, gates, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, healthy>>

CompleteDecided ==
    /\ decision = "aborted" /\ (prepared \/ staging)
    /\ prepared' = FALSE /\ staging' = FALSE
    /\ UNCHANGED <<kind, members, gates, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, missing, decision, healthy>>

ClockFault ==
    /\ healthy /\ healthy' = FALSE
    /\ UNCHANGED <<kind, members, gates, removed, offline, phase, txActive,
                   admittedAfterAck, latePromise, prepared, staging, missing, decision>>

Next == Start \/ CheckMembership \/ BeginCommit \/ ConfirmPromise
        \/ (\E s \in Nodes : PublishDrain(s) \/ QueryGate(s))
        \/ Remove \/ Stop \/ RecoveryFenceMissing \/ Decide \/ CompleteDecided \/ ClockFault

Spec == Init /\ [][Next]_vars
        /\ WF_vars(Start) /\ WF_vars(CheckMembership) /\ WF_vars(BeginCommit)
        /\ WF_vars(ConfirmPromise) /\ WF_vars(Remove) /\ WF_vars(Stop)
        /\ WF_vars(RecoveryFenceMissing) /\ WF_vars(Decide) /\ WF_vars(CompleteDecided)
        /\ (\A s \in Nodes : WF_vars(PublishDrain(s)) /\ WF_vars(QueryGate(s)))

TypeOK ==
    /\ kind \in {"prepare", "staging"}
    /\ members \in [Nodes -> {"active", "draining", "removed"}] /\ gates \subseteq Nodes
    /\ phase \in {"network", "started", "checked", "pending_wal", "done"}
    /\ removed \in BOOLEAN /\ offline \in BOOLEAN /\ txActive \in BOOLEAN
    /\ admittedAfterAck \in BOOLEAN /\ latePromise \in BOOLEAN
    /\ prepared \in BOOLEAN /\ staging \in BOOLEAN /\ healthy \in BOOLEAN
    /\ missing \in {"none", "aborted"} /\ decision \in {"none", "aborted"}
NoOrphanPrepared == offline => ~prepared
NoOrphanStaging == offline => ~staging
(* First admission after all draining ACKs, not delayed confirmation of old WAL. *)
NoNewDependencyAfterDrainACK == ~latePromise
HealthyDrainCompletes == (<>[]healthy) => <>offline
DecidedCompletion == (decision = "aborted") ~> (~prepared /\ ~staging)
=============================================================================
