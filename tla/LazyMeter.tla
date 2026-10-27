----------------------------- MODULE LazyMeter -----------------------------
(* Local refinement of LazyCapacity: the durable outbox and reservations are
   truth; a volatile conservative meter must not be a shared MVCC write.
   One promised reservation can APPLY/ABORT, a disjoint local write can ACK,
   and an old sync/applier effect may remain after caller tracking ends.
   Completion and its callback are split conservatively: native callbacks
   may actually run atomically with completion. No delivery/liveness claim. *)
EXTENDS Integers, FiniteSets
CONSTANTS BugNoPrecharge, BugEarlyRelease, BugRefundUncertain,
          BugMutateUnknown, BugNoDrain, BugNoQueueCut, BugStaleInstall,
          BugStaleCallback, BugApplyCap
Ops == {"reserve", "apply", "abort", "local", "ack"}
Phases == {"idle", "active", "committed", "rolledback", "done"}
CapE == 2
CapB == 3
PosE(o) == IF o \in {"reserve", "apply", "local"} THEN 1 ELSE 0
PosB(o) == IF o = "reserve" THEN 2 ELSE PosE(o)
NegE(o) == IF o \in {"apply", "abort", "ack"} THEN 1 ELSE 0
NegB(o) == IF o \in {"apply", "abort"} THEN 2 ELSE NegE(o)
VARIABLE s
vars == s
DurableE == (IF s.p \in {"reserved", "applied"} THEN 1 ELSE 0)
          + (IF s.q = "present" THEN 1 ELSE 0)
          + (IF s.remote = "committed" THEN 1 ELSE 0)
DurableB == (IF s.p = "reserved" THEN 2 ELSE IF s.p = "applied" THEN 1 ELSE 0)
          + (IF s.q = "present" THEN 1 ELSE 0)
          + (IF s.remote = "committed" THEN 1 ELSE 0)
FutureE == Cardinality({o \in {"reserve", "apply", "local"} : s.phase[o] = "active"})
FutureB == FutureE + (IF s.phase["reserve"] = "active" THEN 1 ELSE 0)
Capacity(o) == s.me + PosE(o) <= CapE /\ s.mb + PosB(o) <= CapB
Logical(o) == CASE o = "reserve" -> s.p = "none"
                [] o \in {"apply", "abort"} -> s.p = "reserved"
                [] o = "local" -> s.q = "none"
                [] o = "ack" -> s.q = "present"
Admission(o) == /\ s.phase[o] = "idle" /\ Logical(o)
                /\ (s.ready \/ BugMutateUnknown)
                /\ (o \notin {"reserve", "local"} \/ Capacity(o))
                /\ (o # "apply" \/ ~BugApplyCap \/ Capacity(o))
Init == s = [phase |-> [o \in Ops |-> "idle"],
             ticketEpoch |-> [o \in Ops |-> 0], detached |-> {},
             p |-> "none", q |-> "none", remote |-> "pending",
             ready |-> FALSE, epoch |-> 0, meterEpoch |-> -1,
             me |-> 0, mb |-> 0, buildEpoch |-> -1, be |-> 0, bb |-> 0,
             refused |-> FALSE]
Admit(o) ==
    /\ Admission(o)
    /\ s' = [s EXCEPT !.phase[o] = "active", !.ticketEpoch[o] = s.epoch,
                    !.me = @ + (IF BugNoPrecharge THEN 0 ELSE PosE(o))
                              - (IF BugEarlyRelease THEN NegE(o) ELSE 0),
                    !.mb = @ + (IF BugNoPrecharge THEN 0 ELSE PosB(o))
                              - (IF BugEarlyRelease THEN NegB(o) ELSE 0)]
Confirm(o) ==
    /\ s.phase[o] = "active" /\ Logical(o)
    /\ s' = [s EXCEPT !.phase[o] = "committed",
                    !.p = CASE o = "reserve" -> "reserved"
                            [] o = "apply" -> "applied"
                            [] o = "abort" -> "aborted"
                            [] OTHER -> @,
                    !.q = CASE o = "local" -> "present"
                            [] o = "ack" -> "acked"
                            [] OTHER -> @]
Rollback(o) ==
    /\ s.phase[o] = "active"
    /\ s' = [s EXCEPT !.phase[o] = "rolledback"]
Callback(o) ==
    /\ s.phase[o] \in {"committed", "rolledback"}
    /\ LET current == s.ready /\ (s.ticketEpoch[o] = s.meterEpoch \/ BugStaleCallback)
           e == IF s.phase[o] = "rolledback" THEN PosE(o)
                ELSE IF BugEarlyRelease THEN 0 ELSE NegE(o)
           b == IF s.phase[o] = "rolledback" THEN PosB(o)
                ELSE IF BugEarlyRelease THEN 0 ELSE NegB(o)
       IN s' = [s EXCEPT !.phase[o] = "done",
                        !.me = @ - (IF current THEN e ELSE 0),
                        !.mb = @ - (IF current THEN b ELSE 0)]
(* A cancelled caller can detach from a still-undecided native sync commit.
   Only its eventual terminal callback may refund its positive charge. *)
Detach(o) ==
    /\ s.phase[o] = "active" /\ o \notin s.detached
    /\ s' = [s EXCEPT !.detached = @ \cup {o},
                    !.me = @ - (IF BugRefundUncertain THEN PosE(o) ELSE 0),
                    !.mb = @ - (IF BugRefundUncertain THEN PosB(o) ELSE 0)]
Invalidate ==
    /\ s.epoch < 2
    /\ s' = [s EXCEPT !.ready = FALSE, !.epoch = @ + 1]
Drained == \A o \in Ops : s.phase[o] # "active"
QueueCut == s.remote # "pending"
BeginRebuild ==
    /\ ~s.ready /\ (Drained \/ BugNoDrain) /\ (QueueCut \/ BugNoQueueCut)
    /\ s' = [s EXCEPT !.buildEpoch = s.epoch, !.be = DurableE, !.bb = DurableB]
Install ==
    /\ ~s.ready /\ s.buildEpoch >= 0
    /\ (s.buildEpoch = s.epoch \/ BugStaleInstall)
    /\ (Drained \/ BugNoDrain) /\ (QueueCut \/ BugNoQueueCut)
    /\ s' = [s EXCEPT !.ready = TRUE, !.me = s.be, !.mb = s.bb,
                    !.meterEpoch = s.buildEpoch, !.buildEpoch = -1]
RemoteComplete(outcome) ==
    /\ s.remote = "pending" /\ outcome \in {"committed", "rolledback"}
    /\ s' = [s EXCEPT !.remote = outcome]
RejectPromisedApply ==
    /\ BugApplyCap /\ s.ready /\ s.p = "reserved"
    /\ s.phase["apply"] = "idle" /\ ~Capacity("apply")
    /\ s' = [s EXCEPT !.refused = TRUE]
Next == Invalidate \/ BeginRebuild \/ Install \/ RejectPromisedApply
        \/ (\E outcome \in {"committed", "rolledback"} : RemoteComplete(outcome))
        \/ (\E o \in Ops : Admit(o) \/ Confirm(o) \/ Rollback(o) \/ Callback(o) \/ Detach(o))
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in [Ops -> Phases] /\ s.ticketEpoch \in [Ops -> 0..2]
          /\ s.detached \subseteq Ops
          /\ s.p \in {"none", "reserved", "applied", "aborted"}
          /\ s.q \in {"none", "present", "acked"}
          /\ s.remote \in {"pending", "committed", "rolledback"}
          /\ s.ready \in BOOLEAN /\ s.epoch \in 0..2 /\ s.meterEpoch \in -1..2
          /\ s.me \in -10..10 /\ s.mb \in -10..10
          /\ s.buildEpoch \in -1..2 /\ s.be \in 0..4 /\ s.bb \in 0..6
          /\ s.refused \in BOOLEAN
ReadyEpochCurrent == s.ready => s.meterEpoch = s.epoch
ReadyConservative == s.ready => /\ DurableE + FutureE <= s.me
                               /\ DurableB + FutureB <= s.mb
DurableCapacity == DurableE <= CapE /\ DurableB <= CapB
NoCapacityRefusal == ~s.refused
=============================================================================
