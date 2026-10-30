------------------------------ MODULE TypedRead ------------------------------
(* Local refinement of projection/partial integer reductions (#135).
   The snapshot, full-range lock/DDL checks, routing fences and commit-time
   MVCC validation are premises supplied by the existing read contracts.
   Two one-row pages include an empty qualifying page. Own writes replace
   stored versions before filtering, including deletes. An incomplete fold
   cannot publish success. No SQL parser, arithmetic overflow, storage engine,
   liveness or whole-protocol refinement theorem is claimed. *)
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS BugOverlay, BugCoverage, BugEmptyPage, BugPartial, BugMixedSnapshots
Keys == {1, 2}
Images == [Keys -> {-1, 0, 1, 2}]  \* -1 absent/deleted, 0 models SQL NULL
Qualifies(v) == v > 0
Rows(image, ks) == {<<k, image[k]>> : k \in {i \in ks : Qualifies(image[i])}}
RECURSIVE Sum(_)
Sum(xs) == IF xs = {} THEN 0 ELSE LET x == CHOOSE e \in xs : TRUE IN x[2] + Sum(xs \ {x})
Minimum(xs) == IF xs = {} THEN 0 ELSE CHOOSE v \in {x[2] : x \in xs} : \A x \in xs : v <= x[2]
Maximum(xs) == IF xs = {} THEN 0 ELSE CHOOSE v \in {x[2] : x \in xs} : \A x \in xs : v >= x[2]
Fold(xs) == <<Cardinality(xs), Sum(xs), Minimum(xs), Maximum(xs)>>
VARIABLES live, snap, own, written, phase, pos, out, covered, complete
vars == <<live, snap, own, written, phase, pos, out, covered, complete>>
Expected == Rows(snap, Keys \ written) \cup Rows(own, written)
Init == /\ live \in Images /\ snap = live /\ own \in Images /\ written \subseteq Keys
        /\ phase = "idle" /\ pos = 1 /\ out = {} /\ covered = {} /\ complete = FALSE
Start == /\ phase = "idle" /\ phase' = "reading" /\ snap' = live
         /\ UNCHANGED <<live, own, written, pos, out, covered, complete>>
Write == /\ phase = "reading" /\ live' \in Images
         /\ UNCHANGED <<snap, own, written, phase, pos, out, covered, complete>>
Page == /\ phase = "reading" /\ pos <= 2
        /\ LET image == IF BugMixedSnapshots THEN live ELSE snap
               row == IF BugOverlay \/ pos \notin written THEN Rows(image, {pos}) ELSE {}
           IN /\ out' = out \cup row
              /\ covered' = covered \cup (IF BugCoverage /\ row = {} THEN {} ELSE {pos})
              /\ phase' = IF BugEmptyPage /\ row = {} THEN "finish" ELSE "reading"
        /\ pos' = pos + 1 /\ UNCHANGED <<live, snap, own, written, complete>>
Finish == /\ (phase = "reading" /\ pos = 3) \/ phase = "finish"
          /\ phase' = "done" /\ complete' = TRUE
          /\ out' = out \cup Rows(own, written)
          /\ UNCHANGED <<live, snap, own, written, pos, covered>>
Refuse == /\ phase = "reading" /\ pos <= 2
          /\ phase' = IF BugPartial THEN "done" ELSE "fallback"
          /\ complete' = BugPartial
          /\ UNCHANGED <<live, snap, own, written, pos, out, covered>>
Next == Start \/ Write \/ Page \/ Finish \/ Refuse
TypeOK == /\ live \in Images /\ snap \in Images /\ own \in Images /\ written \subseteq Keys
          /\ phase \in {"idle", "reading", "finish", "done", "fallback"}
          /\ pos \in 1..3 /\ out \subseteq (Keys \X {1, 2}) /\ covered \subseteq Keys
          /\ complete \in BOOLEAN
ExactProjection == phase = "done" => out = Expected
ExactFold == phase = "done" => Fold(out) = Fold(Expected)
FullCoverage == phase = "done" => covered = Keys
CompleteOnly == phase = "done" => complete /\ pos = 3
Spec == Init /\ [][Next]_vars
=============================================================================
