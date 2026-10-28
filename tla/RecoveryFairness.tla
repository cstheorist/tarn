--------------------------- MODULE RecoveryFairness ---------------------------
(***************************************************************************)
(* How recovery and garbage collection go through records (issue #129):   *)
(* lua/tarn.lua "Recovery", "Scheduling".  Each pass looks at records in  *)
(* their order -- undecided transactions oldest first, expired records by *)
(* expiry -- at most Scan of them, and acts on at most Batch of those it  *)
(* may act on now: recovery tries to resolve them, garbage collection     *)
(* deletes them.  Some records are stuck: a transaction one of whose      *)
(* shards is down cannot be resolved, a prepared participant cannot be    *)
(* deleted.  A failed attempt makes the record wait Backoff passes.       *)
(*                                                                         *)
(* A pass continues after the last record the previous one looked at, and *)
(* one that reaches the last record starts the next lap from the first.   *)
(* BoundedWait: every record that is not stuck has been acted on within   *)
(* ceil(N / Batch) + 1 passes, whichever records are stuck (TLC tries      *)
(* every set of them).  BugPrefix makes every pass start from the first    *)
(* record again, as both did before: with more stuck records first than a *)
(* pass looks at, TLC finds the others never acted on.                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    N,          \* records 1..N, in the order the passes go through them
    Batch,      \* records a pass acts on (recovery_batch; gc_batch * 4)
    Scan,       \* records a pass looks at (recovery_batch * 5; gc_batch * 40)
    Backoff,    \* passes a record waits after a failed attempt
    BugPrefix   \* each pass starts from the first record

Recs == 1..N
Bound == (N + Batch - 1) \div Batch + 1
MaxPasses == Bound + 2

VARIABLES
    stuck,   \* the records no attempt resolves (or may delete): any set
    pos,     \* the last record the previous pass looked at (0: none, a new lap)
    done,    \* the records acted on
    wait,    \* wait[r]: the pass before which r is not tried again
    passes   \* the passes so far

vars == <<stuck, pos, done, wait, passes>>

TypeOK ==
    /\ stuck \subseteq Recs
    /\ pos \in 0..N
    /\ done \subseteq Recs
    /\ wait \in [Recs -> Nat]
    /\ passes \in 0..MaxPasses

Init ==
    /\ stuck \in SUBSET Recs
    /\ pos = 0
    /\ done = {}
    /\ wait = [r \in Recs |-> 0]
    /\ passes = 0

Eligible(r) == r \notin done /\ wait[r] <= passes

(* Look at the records from r on: <<the last one looked at, the eligible   *)
(* ones taken>>, stopping after Scan of them, once Batch are taken, or at  *)
(* the end.                                                                *)
RECURSIVE Visit(_, _, _)
Visit(r, seen, taken) ==
    IF r > N \/ seen = Scan \/ Cardinality(taken) = Batch
    THEN <<r - 1, taken>>
    ELSE Visit(r + 1, seen + 1, IF Eligible(r) THEN taken \cup {r} ELSE taken)

Pass ==
    /\ passes < MaxPasses
    /\ LET start == IF BugPrefix THEN 0 ELSE pos
           v == Visit(start + 1, 0, {})
           tried == v[2]
       IN /\ pos' = IF v[1] >= N THEN 0 ELSE v[1]
          /\ done' = done \cup (tried \ stuck)
          /\ wait' = [r \in Recs |-> IF r \in tried \cap stuck THEN passes + 1 + Backoff ELSE wait[r]]
          /\ passes' = passes + 1
          /\ UNCHANGED stuck

Spec == Init /\ [][Pass]_vars

BoundedWait == passes >= Bound => (Recs \ stuck) \subseteq done
=============================================================================
