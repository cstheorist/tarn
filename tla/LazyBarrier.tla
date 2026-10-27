---------------------------- MODULE LazyBarrier ----------------------------
(* Cumulative session obligations: own ACKed tx1 on bucket A can still await
   row APPLY when own tx2 on B has applied. Each bucket's stable counter and
   minimum pending sequence are read in ONE confirmed view after all own
   APPLY obligations resolve. Frontiers precede the index read snapshot.
   This is causal index catch-up, not global completeness/serializability. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugLastOnly, BugSkipApplyWait, BugMaxAck, BugWaitAfterSnapshot,
          BugSplitFrontierView, BugForgetUnresolved, BugTrackOnlyWhenEnabled
Buckets == {"A","B"}
Ids == {1,2,3}
Bucket(i) == IF i = 2 THEN "B" ELSE "A"
Seq(i) == IF i = 3 THEN 2 ELSE 1
VARIABLES acknowledged, applied, pending, delivered, obligations, requested,
          begun, ready, snapshot, returned, sampleEmpty, sampled, ryw
vars == <<acknowledged,applied,pending,delivered,obligations,requested,
          begun,ready,snapshot,returned,sampleEmpty,sampled,ryw>>
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Counter(b) == Max({0} \cup {Seq(i) : i \in {j \in applied : Bucket(j) = b}})
Frontier(b) == IF BugMaxAck THEN Max({0} \cup {Seq(i) : i \in {j \in delivered : Bucket(j) = b}})
              ELSE IF {i \in pending : Bucket(i) = b} = {} THEN Counter(b)
              ELSE Min({Seq(i) : i \in {j \in pending : Bucket(j) = b}}) - 1
Init ==
    /\ acknowledged = {} /\ applied = {} /\ pending = {} /\ delivered = {} /\ obligations = {}
    /\ requested = [b \in Buckets |-> 0] /\ begun = FALSE /\ ready = FALSE
    /\ snapshot = {} /\ returned = FALSE /\ sampleEmpty = [b \in Buckets |-> TRUE] /\ sampled = FALSE /\ ryw = FALSE
AckOwn(i) ==
    /\ i \in {1,2} /\ i \notin acknowledged /\ ~begun
    /\ (i = 1 \/ 1 \in acknowledged)
    /\ acknowledged' = acknowledged \cup {i}
    /\ obligations' = IF BugTrackOnlyWhenEnabled /\ ~ryw THEN obligations
                      ELSE IF BugLastOnly THEN {i} ELSE obligations \cup {i}
    /\ UNCHANGED <<applied,pending,delivered,requested,begun,ready,snapshot,
                   returned,sampleEmpty,sampled,ryw>>
Apply(i) ==
    /\ i \notin applied /\ (i \in acknowledged \/ (i = 3 /\ 1 \in applied))
    /\ applied' = applied \cup {i} /\ pending' = pending \cup {i}
    /\ UNCHANGED <<acknowledged,delivered,obligations,requested,begun,ready,
                   snapshot,returned,sampleEmpty,sampled,ryw>>
Deliver(i) ==
    /\ i \in pending /\ delivered' = delivered \cup {i} /\ pending' = pending \ {i}
    /\ UNCHANGED <<acknowledged,applied,obligations,requested,begun,ready,
                   snapshot,returned,sampleEmpty,sampled,ryw>>
ForgetHistory ==
    /\ BugForgetUnresolved /\ obligations # {} /\ ~begun
    /\ obligations' = {}
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,requested,begun,ready,
                   snapshot,returned,sampleEmpty,sampled,ryw>>
BeginRead ==
    /\ acknowledged = {1,2} /\ ~begun /\ ryw
    /\ obligations \subseteq applied \/ BugSkipApplyWait
    /\ begun' = TRUE /\ requested' = [b \in Buckets |-> Counter(b)]
    /\ snapshot' = IF BugWaitAfterSnapshot THEN delivered ELSE snapshot
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,obligations,ready,
                   returned,sampleEmpty,sampled,ryw>>
SampleFirstHalf ==
    /\ BugSplitFrontierView /\ ~begun /\ ~sampled
    /\ acknowledged = {1,2} /\ obligations \subseteq applied
    /\ sampleEmpty' = [b \in Buckets |-> {i \in pending : Bucket(i) = b} = {}] /\ sampled' = TRUE
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,obligations,requested,
                   begun,ready,snapshot,returned,ryw>>
PassBarrier ==
    /\ begun /\ ~ready
    /\ \A b \in Buckets :
        (IF BugSplitFrontierView /\ sampled /\ sampleEmpty[b]
         THEN Counter(b) ELSE Frontier(b)) >= requested[b]
    /\ ready' = TRUE
    /\ snapshot' = IF BugWaitAfterSnapshot THEN snapshot ELSE delivered
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,obligations,requested,
                   begun,returned,sampleEmpty,sampled,ryw>>
Return ==
    /\ ready /\ ~returned /\ returned' = TRUE
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,obligations,requested,
                   begun,ready,snapshot,sampleEmpty,sampled,ryw>>
Enable ==
    /\ ~ryw /\ acknowledged = {1,2} /\ ryw' = TRUE
    /\ UNCHANGED <<acknowledged,applied,pending,delivered,obligations,requested,
                   begun,ready,snapshot,returned,sampleEmpty,sampled>>
Next == (\E i \in {1,2} : AckOwn(i)) \/ (\E i \in Ids : Apply(i) \/ Deliver(i))
        \/ Enable \/ ForgetHistory \/ BeginRead \/ SampleFirstHalf \/ PassBarrier \/ Return
Spec == Init /\ [][Next]_vars
TypeOK == /\ acknowledged \subseteq {1,2} /\ applied \subseteq Ids /\ pending \subseteq Ids
          /\ delivered \subseteq Ids /\ obligations \subseteq {1,2}
          /\ requested \in [Buckets -> 0..2] /\ sampleEmpty \in [Buckets -> BOOLEAN]
          /\ begun \in BOOLEAN /\ ready \in BOOLEAN /\ returned \in BOOLEAN /\ sampled \in BOOLEAN
          /\ snapshot \subseteq Ids /\ ryw \in BOOLEAN
OwnWritesCovered == returned => acknowledged \subseteq snapshot
PrefixCovered == returned => \A i \in applied : Seq(i) <= requested[Bucket(i)] => i \in snapshot
=============================================================================
