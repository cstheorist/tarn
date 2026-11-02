------------------------ MODULE ReplicatedBuildFence ------------------------
(* The vinyl index builds of an ALTER TABLE on a replicated shard, across  *)
(* leader changes (issue #271). Three nodes; an election needs a candidate *)
(* and one voter. Only the builds' DDL rows and the PROMOTE rows of the    *)
(* elected leaders are modelled: a node's log holds the rows it has, in    *)
(* the order it wrote them.                                                *)
(*                                                                         *)
(* The Tarantool behaviour it rests on, abstracted:                        *)
(*  - A node checks that it is writable when a statement starts. A build   *)
(*    ends with an asynchronous DDL row, which commits on the node that    *)
(*    built it, writable or not.                                           *)
(*  - A follower applies a DDL row by running the build in its applier.    *)
(*    It reads each node's log in order and applies the rows it lacks; a   *)
(*    row it is applying holds up every log that has it next. When it      *)
(*    starts to apply an asynchronous row whose author did not write the   *)
(*    newest PROMOTE it holds, it stops: "Split-Brain discovered: got an   *)
(*    async transaction from an old term".                                 *)
(*  - A voter votes for a candidate that holds the rows it holds, once it  *)
(*    holds the candidate's (the vote request follows them in the          *)
(*    candidate's log); a row it is still applying is not among them. Both *)
(*    write the new leader's PROMOTE. A leader may resign at any time      *)
(*    (fencing). A follower applying a build acknowledges nothing, so the  *)
(*    leader resigns while the followers apply a long one, and the         *)
(*    follower may stand and vote meanwhile (issue #267).                  *)
(*  - A node that is not applying a row and takes part in an election      *)
(*    holds every row there is: rows reach the followers before the next   *)
(*    election, unless PromptDelivery is FALSE (a follower cut off from    *)
(*    the leader as a build commits, or restarting).                       *)
(*                                                                         *)
(* Tarn's protections: before a build the leader waits until a follower    *)
(* holds every row it wrote (BugNoQuorumWait does not wait), and a build   *)
(* commits only if its node still leads once it has built                  *)
(* (BugNoCommitCheck commits it anyway). Rows of other writes are not      *)
(* modelled.                                                               *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Builds, MaxTerm, BugNoQuorumWait, BugNoCommitCheck, PromptDelivery
ASSUME /\ Builds \in Nat \ {0} /\ MaxTerm \in Nat \ {0}
       /\ BugNoQuorumWait \in BOOLEAN /\ BugNoCommitCheck \in BOOLEAN
       /\ PromptDelivery \in BOOLEAN

Nodes == {"n1", "n2", "n3"}
\* A build's DDL row (k = "B", v = the build) or a PROMOTE (k = "P", v = its term), by its author.
Rows == [k : {"B"}, by : Nodes, v : 1..Builds] \cup [k : {"P"}, by : Nodes, v : 1..MaxTerm]
None == [k |-> "none", by |-> "none", v |-> 0]

VARIABLES
    leader,     \* the node that leads, or "none"
    term,       \* the newest term
    log,        \* node -> the rows it holds, in order
    applying,   \* node -> the DDL row its applier runs, or None
    building,   \* node -> the build it runs (0: none)
    split       \* some node stopped at a row it refuses
vars == <<leader, term, log, applying, building, split>>

Held(n) == {log[n][i] : i \in 1..Len(log[n])}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
\* The term of the newest PROMOTE that node n holds by node a, or by anyone (0: none).
PTerm(n, a) == Max({0} \cup {r.v : r \in {x \in Held(n) : x.k = "P" /\ x.by = a}})
Newest(n) == Max({0} \cup {r.v : r \in {x \in Held(n) : x.k = "P"}})
\* Rows node n refuses: an asynchronous row whose author did not write its
\* newest PROMOTE, or a PROMOTE no newer than the newest it holds.
Refuses(n, r) == IF r.k = "B" THEN PTerm(n, r.by) # Newest(n) ELSE r.v <= Newest(n)
\* The first row of node s's log that node n lacks, or None.
Lacks(n, s) ==
    IF Held(s) \subseteq Held(n) THEN None
    ELSE log[s][CHOOSE i \in 1..Len(log[s]) : log[s][i] \notin Held(n)
                                            /\ \A j \in 1..(i - 1) : log[s][j] \in Held(n)]
HasBuild(n, b) == \E r \in Held(n) : r.k = "B" /\ r.v = b
\* The next build of the ALTER TABLE on node n: the first index it lacks (0: none).
NextBuild(n) ==
    IF \A b \in 1..Builds : HasBuild(n, b) THEN 0
    ELSE CHOOSE b \in 1..Builds : ~HasBuild(n, b) /\ \A c \in 1..(b - 1) : HasBuild(n, c)
\* A node that takes part in an election: applying a row, or holding every row there is.
Ready(n) == applying[n] # None \/ ~PromptDelivery \/ \A s \in Nodes : Held(s) \subseteq Held(n)

Init ==
    /\ leader = "n1"
    /\ term = 1
    /\ log = [n \in Nodes |-> <<[k |-> "P", by |-> "n1", v |-> 1]>>]
    /\ applying = [n \in Nodes |-> None]
    /\ building = [n \in Nodes |-> 0]
    /\ split = FALSE

\* The leader starts its next build: it is writable, its applier runs no build of
\* the table, and (unless BugNoQuorumWait) a follower holds every row it wrote.
StartBuild(n) ==
    /\ leader = n /\ building[n] = 0 /\ applying[n] = None /\ NextBuild(n) # 0
    /\ BugNoQuorumWait \/ \E f \in Nodes \ {n} : {r \in Held(n) : r.by = n} \subseteq Held(f)
    /\ building' = [building EXCEPT ![n] = NextBuild(n)]
    /\ UNCHANGED <<leader, term, log, applying, split>>

\* It ends and commits its DDL row if its node still leads (unless
\* BugNoCommitCheck), and if no other DDL row of that index came in meanwhile
\* (_index refuses the second one).
EndBuild(n) ==
    /\ building[n] # 0
    /\ log' = IF (BugNoCommitCheck \/ leader = n) /\ ~HasBuild(n, building[n])
              THEN [log EXCEPT ![n] = Append(@, [k |-> "B", by |-> n, v |-> building[n]])]
              ELSE log
    /\ building' = [building EXCEPT ![n] = 0]
    /\ UNCHANGED <<leader, term, applying, split>>

\* Node n's applier starts on the next DDL row of node s's log, unless n
\* refuses it; not while n builds itself.
StartApply(n, s) ==
    LET r == Lacks(n, s) IN
    /\ n # s /\ applying[n] = None /\ building[n] = 0 /\ ~split
    /\ r # None /\ r.k = "B"
    /\ IF Refuses(n, r) THEN split' = TRUE /\ UNCHANGED applying
       ELSE applying' = [applying EXCEPT ![n] = r] /\ UNCHANGED split
    /\ UNCHANGED <<leader, term, log, building>>

\* ... and ends: an asynchronous row lands whatever happened meanwhile.
EndApply(n) ==
    /\ applying[n] # None
    /\ log' = [log EXCEPT ![n] = Append(@, applying[n])]
    /\ applying' = [applying EXCEPT ![n] = None]
    /\ UNCHANGED <<leader, term, building, split>>

\* Node n applies the next PROMOTE of node s's log (alongside a build).
ApplyPromote(n, s) ==
    LET r == Lacks(n, s) IN
    /\ n # s /\ ~split /\ r # None /\ r.k = "P"
    /\ IF Refuses(n, r) THEN split' = TRUE /\ UNCHANGED log
       ELSE log' = [log EXCEPT ![n] = Append(@, r)] /\ UNCHANGED split
    /\ UNCHANGED <<leader, term, applying, building>>

Resign(n) ==
    /\ leader = n
    /\ leader' = "none"
    /\ UNCHANGED <<term, log, applying, building, split>>

\* Node c wins an election with the vote of node v: they hold the same rows.
Elect(c) ==
    /\ leader = "none" /\ term < MaxTerm
    /\ \E v \in Nodes \ {c} :
        /\ Held(v) = Held(c) /\ Ready(c) /\ Ready(v)
        /\ log' = [log EXCEPT ![c] = Append(@, [k |-> "P", by |-> c, v |-> term + 1]),
                              ![v] = Append(@, [k |-> "P", by |-> c, v |-> term + 1])]
    /\ leader' = c
    /\ term' = term + 1
    /\ UNCHANGED <<applying, building, split>>

NextAction ==
    \/ \E n \in Nodes : StartBuild(n) \/ EndBuild(n) \/ EndApply(n) \/ Resign(n) \/ Elect(n)
    \/ \E n, s \in Nodes : StartApply(n, s) \/ ApplyPromote(n, s)
Spec == Init /\ [][NextAction]_vars

TypeOK ==
    /\ leader \in Nodes \cup {"none"}
    /\ term \in 1..MaxTerm
    /\ log \in [Nodes -> Seq(Rows)]
    /\ applying \in [Nodes -> Rows \cup {None}]
    /\ building \in [Nodes -> 0..Builds]
    /\ split \in BOOLEAN

\* No node stops at a row it refuses: every node can apply every row.
NoSplit == ~split
\* No node holds two DDL rows of one index.
OneRowPerIndex ==
    \A n \in Nodes : \A i, j \in 1..Len(log[n]) :
        (i # j /\ log[n][i].k = "B" /\ log[n][j].k = "B") => log[n][i].v # log[n][j].v
=============================================================================
