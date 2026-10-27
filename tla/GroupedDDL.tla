---------------------------- MODULE GroupedDDL ----------------------------
(* A checked grouped read may yield after capturing a vinyl table. A DDL *)
(* hold is a protocol claim: no successful reply may use a snapshot that *)
(* existed only while that table was held. Releasing a hold invalidates  *)
(* the whole read, even when no hold remains at its final check. A cache *)
(* marked stale cannot authorize the reply; rebuilding it advances the  *)
(* generation, preserving evidence if another fiber rebuilds meanwhile.  *)
(* This is one attempt, one DDL and one cache invalidation, not a model of *)
(* global DDL serializability, native MVCC, or retry progress.             *)
EXTENDS Naturals

CONSTANTS BugIgnoreDDLGeneration, BugIgnoreFenceStale
ASSUME /\ BugIgnoreDDLGeneration \in BOOLEAN
       /\ BugIgnoreFenceStale \in BOOLEAN

Unread == 2
VARIABLES data, ddl, held, cachedHeld, stale, invalidated, generation,
          phase, snapshot, readGeneration, eligible
vars == <<data, ddl, held, cachedHeld, stale, invalidated, generation,
          phase, snapshot, readGeneration, eligible>>

Init ==
    /\ data = 1                         \* the root table has old rows
    /\ ddl = "none"
    /\ held = FALSE /\ cachedHeld = FALSE
    /\ stale = FALSE /\ invalidated = FALSE
    /\ generation = 0
    /\ phase = "idle" /\ snapshot = Unread /\ readGeneration = 0
    /\ eligible = {}

BeginRead ==
    /\ phase = "idle" /\ ~stale        \* entrance cache refresh succeeded
    /\ phase' = "scanning"
    /\ snapshot' = data
    /\ readGeneration' = generation
    /\ eligible' = IF held THEN {} ELSE {data}
    /\ UNCHANGED <<data, ddl, held, cachedHeld, stale, invalidated, generation>>

PrepareDDL ==
    /\ ddl = "none"
    /\ ddl' = "prepared" /\ held' = TRUE
    /\ cachedHeld' = IF stale THEN cachedHeld ELSE TRUE
    /\ generation' = generation + 1
    /\ UNCHANGED <<data, stale, invalidated, phase, snapshot, readGeneration, eligible>>

ApplyDDL ==
    /\ ddl = "prepared"
    /\ ddl' = "applied" /\ data' = 0   \* TRUNCATE empties the table
    /\ held' = FALSE /\ cachedHeld' = FALSE
    /\ generation' = generation + 1    \* ddl_release, before any reply
    /\ eligible' = IF phase = "scanning" THEN eligible \cup {0} ELSE eligible
    /\ UNCHANGED <<stale, invalidated, phase, snapshot, readGeneration>>

InvalidateCache ==
    /\ ~invalidated
    /\ invalidated' = TRUE /\ stale' = TRUE
    /\ cachedHeld' = FALSE             \* the cache can no longer prove a hold
    /\ UNCHANGED <<data, ddl, held, generation, phase, snapshot, readGeneration, eligible>>

RebuildCache ==
    /\ stale
    /\ stale' = FALSE /\ cachedHeld' = held
    /\ generation' = generation + 1    \* a hidden stale -> fresh cycle is not lost
    /\ UNCHANGED <<data, ddl, held, invalidated, phase, snapshot, readGeneration, eligible>>

FinishRead ==
    /\ phase = "scanning"
    /\ phase' = IF /\ (BugIgnoreFenceStale \/ ~stale)
                       /\ (BugIgnoreDDLGeneration \/ generation = readGeneration)
                       /\ ~cachedHeld
                  THEN "accepted" ELSE "rejected"
    /\ UNCHANGED <<data, ddl, held, cachedHeld, stale, invalidated, generation,
                   snapshot, readGeneration, eligible>>

Next == BeginRead \/ PrepareDDL \/ ApplyDDL \/ InvalidateCache \/ RebuildCache \/ FinishRead
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ data \in 0..1 /\ ddl \in {"none", "prepared", "applied"}
    /\ held \in BOOLEAN /\ cachedHeld \in BOOLEAN /\ stale \in BOOLEAN
    /\ invalidated \in BOOLEAN /\ generation \in 0..3
    /\ phase \in {"idle", "scanning", "accepted", "rejected"}
    /\ snapshot \in 0..2 /\ readGeneration \in 0..3
    /\ eligible \subseteq 0..1
    /\ ~stale => cachedHeld = held

NoHeldSnapshot == phase = "accepted" => snapshot \in eligible
=============================================================================
