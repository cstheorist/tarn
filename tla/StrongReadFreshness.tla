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
 *
 * Two behaviours of Tarantool 3.9's synchronous queue (txn_limbo.c, raft.c,
 * applier.cc; issue #314), which the base configurations do not assume:
 *   PromoteSettles: a new leader's PROMOTE confirms the queue it inherits and
 *     leaves it empty before box_raft_is_ro() lets the leader write.
 *   TermBeforeRows: a term's rows follow its PROMOTE in every WAL, and
 *     preparing that PROMOTE makes its receiver read-only.
 * With both, a writable owner's queue holds only its own unacknowledged
 * transactions, and OwnerBarrier (replies and catch-up, without the queue
 * wait) suffices before a read that checks its role again afterwards.
 * LockCall is tarn_lock's order: the term is taken after the barrier, when
 * the call checks that it is writable, and checked again at its end
 * (writable() and pl_intact). BugNoRecheck drops those checks.
 *)
EXTENDS Naturals, FiniteSets

CONSTANTS WriteQuorum, ReadQuorum, BugRoleOnly, BugReuseBarrier,
          BugSnapshotBeforeBarrier, BugSkipCatchup, BugSkipSettlement,
          OwnerBarrier, LockCall, PromoteSettles, TermBeforeRows, BugNoRecheck

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
(* Write w is acknowledged in term w + 1 (AckWrite). *)
WriteTerm(w) == w + 1
RowsTerm(S) == IF S = {} THEN 0
               ELSE CHOOSE t \in {WriteTerm(w) : w \in S} :
                        \A w \in S : WriteTerm(w) <= t
Quorums(q) == {Q \in SUBSET Nodes : Cardinality(Q) = q}

ASSUME /\ WriteQuorum \in 1..3 /\ ReadQuorum \in 1..3
       /\ BugRoleOnly \in BOOLEAN /\ BugReuseBarrier \in BOOLEAN
       /\ BugSnapshotBeforeBarrier \in BOOLEAN
       /\ BugSkipCatchup \in BOOLEAN /\ BugSkipSettlement \in BOOLEAN
       /\ OwnerBarrier \in BOOLEAN /\ LockCall \in BOOLEAN
       /\ PromoteSettles \in BOOLEAN /\ TermBeforeRows \in BOOLEAN
       /\ BugNoRecheck \in BOOLEAN

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

(* PromoteSettles: the new leader's PROMOTE confirms what its queue holds
 * before the leader can write (txn_limbo_promote).
 *)
Elect(Q) ==
    LET t == term + 1 n == Leader(t) IN
    /\ term < 3 /\ Q \in Quorums(WriteQuorum) /\ n \in Q
    /\ \A v \in Q : Running(v) /\ RoundTrip(n, v) /\ wal[v] \subseteq wal[n]
    /\ term' = t
    /\ observed' = [v \in Nodes |-> IF v \in Q THEN t ELSE observed[v]]
    /\ writable' = (writable \ Q) \cup {n}
    /\ settled' = IF PromoteSettles THEN [settled EXCEPT ![n] = wal[n]]
                   ELSE settled
    /\ UNCHANGED <<wal, acked, links, netStep, paused, pauseStep,
                   readId, phase, target, readTerm, need, sent, replied,
                   sample, snapshot, cached, completed>>

ObserveTerm(a, b) ==
    /\ Running(a) /\ Running(b) /\ Link(a, b) /\ observed[a] > observed[b]
    /\ observed' = [observed EXCEPT ![b] = observed[a]]
    /\ writable' = writable \ {b}
    /\ UNCHANGED <<term, wal, settled, acked, links, netStep, paused,
                   pauseStep, readId, phase, target, readTerm, need, sent,
                   replied, sample, snapshot, cached, completed>>

(* TermBeforeRows: a row of a term newer than b knows follows that term's
 * PROMOTE, and preparing the PROMOTE makes b read-only (txn_limbo_req_prepare
 * before the applier's WAL write).
 *)
Replicate(a, b) ==
    LET t == RowsTerm(wal[a] \ wal[b]) IN
    /\ Running(a) /\ Running(b) /\ Link(a, b) /\ ~wal[a] \subseteq wal[b]
    /\ wal' = [wal EXCEPT ![b] = @ \cup wal[a]]
    /\ IF TermBeforeRows /\ t > observed[b]
       THEN /\ observed' = [observed EXCEPT ![b] = t]
            /\ writable' = writable \ {b}
       ELSE UNCHANGED <<observed, writable>>
    /\ UNCHANGED <<term, settled, acked, links, netStep,
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

(* OwnerBarrier skips the queue wait (txn_limbo_wait_last_txn) as
 * BugSkipSettlement does: only the two premises make that safe. With
 * LockCall, the call checks that it is writable once the barrier ends and
 * takes its term then (pl_begin).
 *)
Barrier ==
    /\ phase = "settle" /\ Running(target)
    /\ wal[target] \subseteq settled[target] \/ BugSkipSettlement \/ OwnerBarrier
    /\ LockCall => (target \in writable \/ BugNoRecheck)
    /\ readTerm' = IF LockCall THEN observed[target] ELSE readTerm
    /\ phase' = "read" /\ cached' = sample
    /\ UNCHANGED <<term, observed, writable, wal, settled, acked, links,
                   netStep, paused, pauseStep, readId, target,
                   need, sent, replied, sample, snapshot, completed>>

(* The read, and the role check that follows it (LockCall: writable() and
 * pl_intact at the end of the call).
 *)
FirstRead ==
    /\ phase = "read" /\ Running(target)
    /\ (target \in writable /\ observed[target] = readTerm) \/ BugNoRecheck
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
