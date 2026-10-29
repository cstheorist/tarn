------------------------ MODULE StrongReadFreshness ------------------------
(* Local refinement for a fresh quorum/vclock read barrier (issue #130).
 * Three replicas, three leadership terms (A -> B -> A), two acknowledged
 * writes and two reads. Directed links can partition/heal; A can pause/resume.
 * A former leader stays writable until it OBSERVES a replacement term.
 *
 * Native replication premises, NOT proved here: an acknowledged synchronous
 * write has immutable WAL evidence on WriteQuorum replicas; all represented
 * WAL effects are committed; local queue settlement applies that evidence.
 * Speculative WAL/rollback and divergent histories are abstracted away, not
 * assumed safe merely because box.info.ro is false. Election's up-to-date
 * voter check preserves represented committed entries, not arbitrary Raft.
 *
 * The barrier collects a request-specific reply from ReadQuorum replicas,
 * joins their observed WAL, waits for local catch-up, then queue settlement.
 * No action tests the desired freshness predicate or reads the ACK floor.
 * Quorum intersection plus monotone WAL derives StrongReadsSeePriorAcks.
 * A fresh first snapshot must follow the barrier. Existing stream MVCC,
 * read-view validation, term watermarks, locks and DDL are separate premises.
 * Raw IPROTO reads have no such barrier; opted-in lazy indexes may still miss
 * matches after base-row freshness. Neither is certified by this invariant.
 *)
EXTENDS Naturals, FiniteSets

CONSTANTS WriteQuorum, ReadQuorum, BugRoleOnly, BugReuseBarrier,
          BugSnapshotBeforeBarrier, BugSkipCatchup, BugSkipSettlement

Nodes == {"A", "B", "C"}
Writes == {1, 2}
Edges == {<<a, b>> \in Nodes \X Nodes : a # b}
(* Fully connected, isolated A, A can send only, and A can receive only.
 * B/C retain a quorum in the fault profiles. This is a bounded selection
 * of directed networks, not exhaustive checking of all 64 edge subsets.
 *)
FaultLinks == {
    {<<"B", "C">>, <<"C", "B">>},
    {<<"A", "B">>, <<"A", "C">>, <<"B", "C">>, <<"C", "B">>},
    {<<"B", "A">>, <<"C", "A">>, <<"B", "C">>, <<"C", "B">>}}
Leader(t) == IF t = 2 THEN "B" ELSE "A"
Max(a, b) == IF a >= b THEN a ELSE b
Quorums(q) == {Q \in SUBSET Nodes : Cardinality(Q) = q}

ASSUME /\ WriteQuorum \in 1..3 /\ ReadQuorum \in 1..3
       /\ BugRoleOnly \in BOOLEAN /\ BugReuseBarrier \in BOOLEAN
       /\ BugSnapshotBeforeBarrier \in BOOLEAN
       /\ BugSkipCatchup \in BOOLEAN /\ BugSkipSettlement \in BOOLEAN

VARIABLES term, observed, writable, wal, settled, acked, links, netStep,
          paused, pauseStep, readId, phase, target, readTerm, need,
          sent, replied, sample, snapshot, cached, completed
vars == <<term, observed, writable, wal, settled, acked, links, netStep,
          paused, pauseStep, readId, phase, target, readTerm, need,
          sent, replied, sample, snapshot, cached, completed>>

Running(n) == n \notin paused
Link(a, b) == a = b \/ <<a, b>> \in links
RoundTrip(a, b) == Link(a, b) /\ Link(b, a)

Init ==
    /\ term = 1
    /\ observed = [n \in Nodes |-> 1]
    /\ writable = {"A"}
    /\ wal = [n \in Nodes |-> {}]
    /\ settled = [n \in Nodes |-> {}]
    /\ acked = {}
    /\ links = Edges /\ netStep = 0
    /\ paused = {} /\ pauseStep = 0
    /\ readId = 0 /\ phase = "idle" /\ target = "A" /\ readTerm = 1
    /\ need = {} /\ sent = {} /\ replied = {} /\ sample = {}
    /\ snapshot = {} /\ cached = {} /\ completed = {}

(* ACK folds transmission and durable storage at the write quorum into one
 * action. Its clients' receipt follows that storage, never precedes it.
 * Each write can be the replacement leader's first acknowledged write.
 *)
AckWrite(w, Q) ==
    LET n == Leader(term) IN
    /\ w \in Writes \ acked /\ Q \in Quorums(WriteQuorum) /\ n \in Q
    /\ (w = 1 /\ term = 2) \/ (w = 2 /\ term = 3 /\ 1 \in acked)
    /\ Running(n) /\ n \in writable /\ observed[n] = term
    /\ \A v \in Q : Running(v) /\ RoundTrip(n, v) /\ observed[v] <= term
    /\ wal' = [v \in Nodes |-> IF v \in Q THEN wal[v] \cup {w} ELSE wal[v]]
    /\ settled' = [settled EXCEPT ![n] = @ \cup {w}]
    /\ observed' = [v \in Nodes |-> IF v \in Q THEN term ELSE observed[v]]
    /\ writable' = writable \ (Q \ {n})
    /\ acked' = acked \cup {w}
    /\ UNCHANGED <<term, links, netStep, paused, pauseStep, readId, phase,
                   target, readTerm, need, sent, replied, sample, snapshot,
                   cached, completed>>

Elect(Q) ==
    LET t == term + 1 n == Leader(t) IN
    /\ term < 3 /\ Q \in Quorums(WriteQuorum) /\ n \in Q
    /\ \A v \in Q : Running(v) /\ RoundTrip(n, v) /\ wal[v] \subseteq wal[n]
    /\ term' = t
    /\ observed' = [v \in Nodes |-> IF v \in Q THEN t ELSE observed[v]]
    /\ writable' = (writable \ Q) \cup {n}
    /\ UNCHANGED <<wal, settled, acked, links, netStep, paused, pauseStep,
                   readId, phase, target, readTerm, need, sent, replied,
                   sample, snapshot, cached, completed>>

ObserveTerm(a, b) ==
    /\ Running(a) /\ Running(b) /\ Link(a, b) /\ observed[a] > observed[b]
    /\ observed' = [observed EXCEPT ![b] = observed[a]]
    /\ writable' = writable \ {b}
    /\ UNCHANGED <<term, wal, settled, acked, links, netStep, paused,
                   pauseStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

Replicate(a, b) ==
    /\ Running(a) /\ Running(b) /\ Link(a, b) /\ ~wal[a] \subseteq wal[b]
    /\ wal' = [wal EXCEPT ![b] = @ \cup wal[a]]
    /\ UNCHANGED <<term, observed, writable, settled, acked, links, netStep,
                   paused, pauseStep, readId, phase, target, readTerm, need,
                   sent, replied, sample, snapshot, cached, completed>>

Settle(n) ==
    /\ Running(n) /\ settled[n] # wal[n]
    /\ settled' = [settled EXCEPT ![n] = wal[n]]
    /\ UNCHANGED <<term, observed, writable, wal, acked, links, netStep,
                   paused, pauseStep, readId, phase, target, readTerm, need,
                   sent, replied, sample, snapshot, cached, completed>>

(* One of the bounded directed partitions, then one healing.
 * Removing a single direction is enough to retain a socket while denying
 * the fresh reply that it needs; Sending and Reply are separate actions.
 *)
Partition(E) ==
    /\ netStep = 0 /\ E \in FaultLinks
    /\ links' = E /\ netStep' = 1
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, paused,
                   pauseStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

Heal ==
    /\ netStep = 1 /\ links' = Edges /\ netStep' = 2
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, paused,
                   pauseStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

Pause ==
    /\ pauseStep = 0 /\ paused' = {"A"} /\ pauseStep' = 1
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

Resume ==
    /\ pauseStep = 1 /\ paused' = {} /\ pauseStep' = 2
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

BeginRead(n) ==
    /\ phase = "idle" /\ readId < 2 /\ n \in writable /\ Running(n)
    /\ readId' = readId + 1 /\ target' = n /\ readTerm' = observed[n]
    /\ need' = acked
    /\ phase' = IF BugRoleOnly THEN "read"
                ELSE IF BugReuseBarrier /\ readId = 1 THEN "catchup"
                ELSE "collect"
    /\ sent' = {n} /\ replied' = {n}
    /\ sample' = IF BugReuseBarrier /\ readId = 1 THEN cached ELSE wal[n]
    /\ snapshot' = IF BugSnapshotBeforeBarrier THEN settled[n] ELSE {}
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, cached, completed>>

Send(n) ==
    /\ phase = "collect" /\ n \notin sent
    /\ Running(target) /\ Running(n) /\ Link(target, n)
    /\ sent' = sent \cup {n}
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, phase, target,
                   readTerm, need, replied, sample, snapshot, cached, completed>>

Reply(n) ==
    /\ phase = "collect" /\ n \in sent \ replied
    /\ Running(target) /\ Running(n) /\ Link(n, target)
    /\ replied' = replied \cup {n}
    /\ sample' = sample \cup wal[n]
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, phase, target,
                   readTerm, need, sent, snapshot, cached, completed>>

Collected ==
    /\ phase = "collect" /\ Cardinality(replied) >= ReadQuorum
    /\ phase' = "catchup"
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, target, readTerm,
                   need, sent, replied, sample, snapshot, cached, completed>>

CaughtUp ==
    /\ phase = "catchup" /\ Running(target)
    /\ sample \subseteq wal[target] \/ BugSkipCatchup
    /\ phase' = "settle"
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, target, readTerm,
                   need, sent, replied, sample, snapshot, cached, completed>>

Barrier ==
    /\ phase = "settle" /\ Running(target)
    /\ wal[target] \subseteq settled[target] \/ BugSkipSettlement
    /\ phase' = "read" /\ cached' = sample
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, target, readTerm,
                   need, sent, replied, sample, snapshot, completed>>

FirstRead ==
    /\ phase = "read" /\ Running(target)
    /\ target \in writable /\ observed[target] = readTerm
    /\ completed' = completed \cup {
        [required |-> need,
         seen |-> IF BugSnapshotBeforeBarrier THEN snapshot ELSE settled[target]]}
    /\ phase' = "idle"
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, target, readTerm,
                   need, sent, replied, sample, snapshot, cached>>

Next == (\E w \in Writes, Q \in Quorums(WriteQuorum) : AckWrite(w, Q))
        \/ (\E Q \in Quorums(WriteQuorum) : Elect(Q))
        \/ (\E a, b \in Nodes : ObserveTerm(a, b) \/ Replicate(a, b))
        \/ (\E n \in Nodes : Settle(n) \/ BeginRead(n) \/ Send(n) \/ Reply(n))
        \/ (\E E \in FaultLinks : Partition(E))
        \/ Heal \/ Pause \/ Resume \/ Collected \/ CaughtUp \/ Barrier \/ FirstRead

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ term \in 1..3 /\ observed \in [Nodes -> 1..3] /\ writable \subseteq Nodes
    /\ wal \in [Nodes -> SUBSET Writes] /\ settled \in [Nodes -> SUBSET Writes]
    /\ acked \subseteq Writes /\ links \subseteq Edges /\ netStep \in 0..2
    /\ paused \subseteq {"A"} /\ pauseStep \in 0..2
    /\ readId \in 0..2 /\ phase \in {"idle", "collect", "catchup", "settle", "read"}
    /\ target \in Nodes /\ readTerm \in 1..3 /\ need \subseteq Writes
    /\ sent \subseteq Nodes /\ replied \subseteq sent /\ sample \subseteq Writes
    /\ snapshot \subseteq Writes /\ cached \subseteq Writes
    /\ completed \subseteq [required : SUBSET Writes, seen : SUBSET Writes]

VisibleHasWAL == \A n \in Nodes : settled[n] \subseteq wal[n]
AckedHasQuorum == \A w \in acked : Cardinality({n \in Nodes : w \in wal[n]}) >= WriteQuorum
CollectedCoversPriorAcks ==
    (~BugRoleOnly /\ ~BugReuseBarrier /\ phase \in {"catchup", "settle", "read"})
    => need \subseteq sample
StrongReadsSeePriorAcks == \A r \in completed : r.required \subseteq r.seen
=============================================================================
