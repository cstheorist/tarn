-------------------------- MODULE WaiterAllocation --------------------------
(* Allocation-only refinement of the row-lock wait path. A condition may be
 * omitted for an immediate grant, but must exist before a waiter becomes
 * visible to grant, cancellation, disconnect or leadership-reset callbacks.
 * There is no yield between the compatible-holder test and queue publication.
 * The late-allocation control deliberately splits that atomic publication.
 *
 * Two one-shot exclusive requests and an initial holder bound the model.
 * Queue eligibility, shared upgrades, deadlock selection, generation fencing,
 * storage freshness, and durable commit behavior remain premises covered by
 * the existing commit/lock-reply models and live tests, not by this module.
 * No condition is reused across requests. This model makes no CPU or liveness
 * claim and does not by itself justify enabling a performance change.
 *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Deferred, BugLateAllocation
Clients == {"a", "b"}
None == "none"
InitialHolder == "initial"
VARIABLES holder, phase, queue, conditions, fast, queued, notified, missed
vars == <<holder, phase, queue, conditions, fast, queued, notified, missed>>

Init ==
    /\ holder = InitialHolder
    /\ phase = [t \in Clients |-> "new"]
    /\ queue = <<>> /\ conditions = {} /\ fast = {} /\ queued = {}
    /\ notified = {} /\ missed = {}

Request(t) ==
    /\ phase[t] = "new"
    /\ IF holder = None /\ Len(queue) = 0
       THEN /\ holder' = t
            /\ phase' = [phase EXCEPT ![t] = "granted"]
            /\ fast' = fast \cup {t}
            /\ conditions' = IF Deferred THEN conditions ELSE conditions \cup {t}
            /\ UNCHANGED <<queue, queued>>
       ELSE /\ queue' = Append(queue, t)
            /\ queued' = queued \cup {t}
            /\ phase' = [phase EXCEPT ![t] = "queued"]
            /\ conditions' = IF BugLateAllocation THEN conditions ELSE conditions \cup {t}
            /\ UNCHANGED <<holder, fast>>
    /\ UNCHANGED <<notified, missed>>

LateAllocate(t) ==
    /\ BugLateAllocation /\ phase[t] = "queued" /\ t \notin conditions
    /\ conditions' = conditions \cup {t}
    /\ UNCHANGED <<holder, phase, queue, fast, queued, notified, missed>>

Release ==
    /\ holder # None
    /\ LET oldPhase == IF holder \in Clients
                       THEN [phase EXCEPT ![holder] = "done"] ELSE phase
       IN IF Len(queue) = 0
          THEN /\ holder' = None /\ phase' = oldPhase
               /\ UNCHANGED <<queue, notified, missed>>
          ELSE LET next == Head(queue) IN
               /\ holder' = next /\ queue' = Tail(queue)
               /\ phase' = [oldPhase EXCEPT ![next] = "granted"]
               /\ notified' = notified \cup ({next} \cap conditions)
               /\ missed' = missed \cup ({next} \ conditions)
    /\ UNCHANGED <<conditions, fast, queued>>

Cancel(t) ==
    /\ phase[t] = "queued"
    /\ queue' = SelectSeq(queue, LAMBDA x : x # t)
    /\ phase' = [phase EXCEPT ![t] = "done"]
    /\ notified' = notified \cup ({t} \cap conditions)
    /\ missed' = missed \cup ({t} \ conditions)
    /\ UNCHANGED <<holder, conditions, fast, queued>>

Reset ==
    /\ holder # None \/ Len(queue) > 0
    /\ LET waiting == {t \in Clients : phase[t] = "queued"} IN
       /\ notified' = notified \cup (waiting \cap conditions)
       /\ missed' = missed \cup (waiting \ conditions)
    /\ holder' = None /\ queue' = <<>>
    /\ phase' = [t \in Clients |-> IF phase[t] = "new" THEN "new" ELSE "done"]
    /\ UNCHANGED <<conditions, fast, queued>>

Next == (\E t \in Clients : Request(t) \/ LateAllocate(t) \/ Cancel(t)) \/ Release \/ Reset
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ holder \in Clients \cup {None, InitialHolder}
    /\ phase \in [Clients -> {"new", "queued", "granted", "done"}]
    /\ queue \in Seq(Clients) /\ Len(queue) <= 2
    /\ conditions \subseteq Clients /\ fast \subseteq Clients /\ queued \subseteq Clients
    /\ notified \subseteq Clients /\ missed \subseteq Clients
PublishedConditionReady == \A t \in Clients : phase[t] = "queued" => t \in conditions
NoMissedSignal == missed = {}
QueuedGrantNotified == \A t \in queued : phase[t] = "granted" => t \in notified
NoFastCondition == Deferred => fast \cap conditions = {}
=============================================================================
