------------------------- MODULE PipelineAdmission -------------------------
(***************************************************************************)
(* Resource contract for the IPROTO pipeline (issue #127). A cancellation  *)
(* drops only the receiver: request identity and all scope charges remain. *)
(* Only an exact final response, or BOTH physical I/O owners dropping,    *)
(* releases them. Closing a queued/sent request reports uncertainty; only  *)
(* refusal before enqueue certifies that this RPC took no effect.          *)
(*                                                                         *)
(* Three connections, two shards, one gateway; count AND byte caps, with  *)
(* independent completion reserves. Tick is an operational timer service: *)
(* reaching the deadline begins retirement even for an abandoned caller. *)
(* Real-time guarantees assume the runtime schedules its timer/I/O tasks; *)
(* this finite safety model does not prove scheduler fairness or latency. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets
CONSTANTS BugFreeOnCancel, BugEarlyRetireRelease, BugBypassShard,
          BugBypassGlobal, BugNormalUsesReserve, BugForgetCancelledDeadline,
          BugRefuseAfterEnqueue

Requests == {"r1", "r2", "r3", "r4"}
Connections == {"a", "b", "c"}
Shards == {1, 2}
Owner == [r \in Requests |-> CASE r = "r1" -> "a" [] r = "r2" -> "b"
                              [] r = "r3" -> "c" [] OTHER -> "a"]
Shard(c) == IF c \in {"a", "b"} THEN 1 ELSE 2
Cost == [r \in Requests |-> IF r = "r3" THEN 2 ELSE 1]
Normal == {"r1", "r2", "r3"}
AtConnection(c) == {r \in Requests: Owner[r] = c}
AtShard(s) == {r \in Requests: Shard(Owner[r]) = s}
RECURSIVE Bytes(_)
Bytes(rs) == IF rs = {} THEN 0 ELSE LET r == CHOOSE r \in rs: TRUE
                                   IN Cost[r] + Bytes(rs \ {r})

VARIABLES phase, charged, abandoned, age, closed, ioOwners, ever, outcome
vars == <<phase, charged, abandoned, age, closed, ioOwners, ever, outcome>>
Pending == {r \in Requests: phase[r] \in {"queued", "sent"}}
Outstanding == Pending \cup {r \in Requests: phase[r] = "retiring"}

Fits(rs, r, count, bytes) == Cardinality(charged \cap rs) + 1 <= count
                            /\ Bytes(charged \cap rs) + Cost[r] <= bytes
NormalFits(rs, r, count, bytes) ==
    r \notin Normal \/ BugNormalUsesReserve \/
      (Cardinality(charged \cap rs \cap Normal) + 1 <= count
       /\ Bytes(charged \cap rs \cap Normal) + Cost[r] <= bytes)
CanEnqueue(r) ==
    /\ Fits(AtConnection(Owner[r]), r, 2, 3)
    /\ NormalFits(AtConnection(Owner[r]), r, 1, 2)
    /\ (BugBypassShard \/ (Fits(AtShard(Shard(Owner[r])), r, 2, 3)
         /\ NormalFits(AtShard(Shard(Owner[r])), r, 1, 2)))
    /\ (BugBypassGlobal \/ (Fits(Requests, r, 3, 3)
         /\ NormalFits(Requests, r, 2, 2)))

Init ==
    /\ phase = [r \in Requests |-> "idle"] /\ charged = {} /\ abandoned = {}
    /\ age = [r \in Requests |-> 0] /\ closed = {}
    /\ ioOwners = [c \in Connections |-> 2] /\ ever = {}
    /\ outcome = [r \in Requests |-> "none"]

Enqueue(r) ==
    /\ phase[r] = "idle" /\ Owner[r] \notin closed /\ CanEnqueue(r)
    /\ phase' = [phase EXCEPT ![r] = "queued"] /\ charged' = charged \cup {r}
    /\ ever' = ever \cup {r}
    /\ UNCHANGED <<abandoned, age, closed, ioOwners, outcome>>

Refuse(r) ==
    /\ (phase[r] = "idle" /\ Owner[r] \notin closed /\ ~CanEnqueue(r))
        \/ (BugRefuseAfterEnqueue /\ r \in Pending)
    /\ phase' = IF phase[r] = "idle" THEN [phase EXCEPT ![r] = "refused"] ELSE phase
    /\ outcome' = [outcome EXCEPT ![r] = "no_effect"]
    /\ UNCHANGED <<charged, abandoned, age, closed, ioOwners, ever>>

Send(r) ==
    /\ phase[r] = "queued" /\ phase' = [phase EXCEPT ![r] = "sent"]
    /\ UNCHANGED <<charged, abandoned, age, closed, ioOwners, ever, outcome>>

Cancel(r) ==
    /\ r \in Pending \ abandoned /\ abandoned' = abandoned \cup {r}
    /\ charged' = IF BugFreeOnCancel THEN charged \ {r} ELSE charged
    /\ UNCHANGED <<phase, age, closed, ioOwners, ever, outcome>>

Final(r) ==
    /\ phase[r] = "sent" /\ Owner[r] \notin closed
    /\ phase' = [phase EXCEPT ![r] = "done"] /\ charged' = charged \ {r}
    /\ outcome' = IF r \notin abandoned THEN [outcome EXCEPT ![r] = "ok"] ELSE outcome
    /\ UNCHANGED <<abandoned, age, closed, ioOwners, ever>>

Retire(cs) ==
    /\ closed' = closed \cup cs
    /\ phase' = [r \in Requests |-> IF r \in Pending /\ Owner[r] \in cs THEN "retiring" ELSE phase[r]]
    /\ charged' = IF BugEarlyRetireRelease THEN {r \in charged: Owner[r] \notin cs} ELSE charged
    /\ outcome' = [r \in Requests |-> IF r \in Pending \ abandoned /\ Owner[r] \in cs
                                        THEN "unknown" ELSE outcome[r]]

Close(c) ==
    /\ c \notin closed /\ Retire({c})
    /\ UNCHANGED <<abandoned, age, ioOwners, ever>>

DropIO(c) ==
    /\ c \in closed /\ ioOwners[c] > 0
    /\ ioOwners' = [ioOwners EXCEPT ![c] = @ - 1]
    /\ charged' = IF ioOwners[c] = 1 THEN charged \ AtConnection(c) ELSE charged
    /\ phase' = [r \in Requests |-> IF Owner[r] = c /\ phase[r] = "retiring" /\ ioOwners[c] = 1
                                      THEN "done" ELSE phase[r]]
    /\ UNCHANGED <<abandoned, age, closed, ever, outcome>>

Tick ==
    /\ \E r \in Pending: age[r] < 2
    /\ LET nextAge == [r \in Requests |-> IF r \in Pending /\ age[r] < 2 THEN age[r] + 1 ELSE age[r]]
           expired == {Owner[r]: r \in {r \in Pending: nextAge[r] = 2
                          /\ ~(BugForgetCancelledDeadline /\ r \in abandoned)}}
       IN /\ age' = nextAge /\ Retire(expired)
    /\ UNCHANGED <<abandoned, ioOwners, ever>>

Next == (\E r \in Requests: Enqueue(r) \/ Refuse(r) \/ Send(r) \/ Cancel(r) \/ Final(r))
        \/ (\E c \in Connections: Close(c) \/ DropIO(c)) \/ Tick
Spec == Init /\ [][Next]_vars

TypeOK == /\ phase \in [Requests -> {"idle", "queued", "sent", "retiring", "done", "refused"}]
          /\ charged \subseteq Requests /\ abandoned \subseteq Requests /\ ever \subseteq Requests
          /\ age \in [Requests -> 0..2] /\ closed \subseteq Connections
          /\ ioOwners \in [Connections -> 0..2]
          /\ outcome \in [Requests -> {"none", "ok", "unknown", "no_effect"}]
OutstandingCharged == \A r \in Outstanding: r \in charged
ScopeBound == /\ Cardinality(charged) <= 3 /\ Bytes(charged) <= 3
              /\ \A c \in Connections: Cardinality(charged \cap AtConnection(c)) <= 2
                                        /\ Bytes(charged \cap AtConnection(c)) <= 3
              /\ \A s \in Shards: Cardinality(charged \cap AtShard(s)) <= 2
                                   /\ Bytes(charged \cap AtShard(s)) <= 3
CompletionReserve ==
    /\ Cardinality(charged \cap Normal) <= 2 /\ Bytes(charged \cap Normal) <= 2
    /\ \A c \in Connections: Cardinality(charged \cap Normal \cap AtConnection(c)) <= 1
                              /\ Bytes(charged \cap Normal \cap AtConnection(c)) <= 2
    /\ \A s \in Shards: Cardinality(charged \cap Normal \cap AtShard(s)) <= 1
                         /\ Bytes(charged \cap Normal \cap AtShard(s)) <= 2
ExpiryVisible == \A r \in Pending: age[r] = 2 => Owner[r] \in closed
NoEffectBeforeEnqueue == \A r \in Requests: outcome[r] = "no_effect" => r \notin ever
=============================================================================
