---------------------------- MODULE LazyRouting ----------------------------
(* Refinement boundary: a frozen, caught-up lazy incarnation with two index
   owners, one stale candidate and typed integer order. LazyIndex covers
   assignment version/frontier/GC; LazyHandoff covers the transfer itself.
   This model covers capability admission, immutable routing, all-owner
   prefix coverage, merge ordering, post-recheck LIMIT, and map fencing. *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS BugCapability, BugRewrite, BugMissingOwner, BugLexical,
          BugEarlyLimit, BugEpoch
Owners == {1,2}
Pages == [o \in Owners |-> IF o = 1 THEN <<2,10>> ELSE <<3,12>>]
VARIABLES admitted, routing, epoch, captured, started, at, result, examined, done
vars == <<admitted,routing,epoch,captured,started,at,result,examined,done>>
Init == /\ admitted = FALSE /\ routing = 2 /\ epoch = 0 /\ captured = 0
        /\ started = FALSE /\ at = [o \in Owners |-> 1]
        /\ result = <<>> /\ examined = 0 /\ done = FALSE
AdmitOld == /\ ~admitted /\ BugCapability /\ admitted' = TRUE
            /\ UNCHANGED <<routing,epoch,captured,started,at,result,examined,done>>
Rewrite == /\ routing = 2 /\ BugRewrite /\ routing' = 1
           /\ UNCHANGED <<admitted,epoch,captured,started,at,result,examined,done>>
Start == /\ ~started /\ started' = TRUE /\ captured' = epoch
         /\ at' = [o \in Owners |-> IF BugMissingOwner /\ o = 2 THEN 3 ELSE 1]
         /\ UNCHANGED <<admitted,routing,epoch,result,examined,done>>
Move == /\ ~done /\ epoch = 0 /\ epoch' = 1
        /\ UNCHANGED <<admitted,routing,captured,started,at,result,examined,done>>
Available == {o \in Owners : at[o] <= 2}
(* Lexical order of the decimal strings: 10,12,2,3. *)
Rank(v) == IF ~BugLexical THEN v ELSE CASE v = 10 -> 1 [] v = 12 -> 2 [] v = 2 -> 3 [] OTHER -> 4
Front(o) == Pages[o][at[o]]
Take(o) == /\ started /\ ~done /\ o \in Available
           /\ \A p \in Available : Rank(Front(o)) <= Rank(Front(p))
           /\ at' = [at EXCEPT ![o] = @ + 1]
           /\ result' = IF Front(o) = 2 THEN result ELSE Append(result,Front(o))
           /\ examined' = examined + 1
           /\ UNCHANGED <<admitted,routing,epoch,captured,started,done>>
Finish == /\ started /\ ~done
          /\ (Len(result) = 2 \/ Available = {} \/ (BugEarlyLimit /\ examined = 2))
          /\ (epoch = captured \/ BugEpoch)
          /\ done' = TRUE
          /\ UNCHANGED <<admitted,routing,epoch,captured,started,at,result,examined>>
Next == AdmitOld \/ Rewrite \/ Start \/ Move \/ Finish
        \/ (Len(result) < 2 /\ \E o \in Owners : Take(o))
Spec == Init /\ [][Next]_vars
TypeOK == /\ admitted \in BOOLEAN /\ routing \in 1..2
          /\ epoch \in 0..1 /\ captured \in 0..1 /\ started \in BOOLEAN
          /\ at \in [Owners -> 1..3] /\ result \in Seq({3,10,12})
          /\ examined \in 0..4 /\ done \in BOOLEAN
Capability == ~admitted
ImmutableRouting == routing = 2
CompleteOrderedLimit == done => result = <<3,10>>
StableMap == done => epoch = captured
=============================================================================
