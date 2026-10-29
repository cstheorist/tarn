----------------------- MODULE StrongStreamCertificate ----------------------
(* Transaction provenance for strong Lua/native stream reads (#130).
 * The first fresh barrier is established by StrongReadFreshness. This local
 * model checks WHERE its certificate may be installed and subsequently used.
 * A transaction created empty by Tarn is marked after that barrier and before
 * any read. Arbitrary ambient transactions are refused; refreshing one late
 * cannot repair a snapshot it already took. Registry membership is keyed by
 * the actual current native transaction ID, not a stream/session/global bit.
 * Commit/rollback callbacks clean only that ID; a savepoint rollback may also
 * conservatively remove it, after which a later read must refuse, not recertify.
 *
 * Two stream slots, three never-reused transaction IDs, registry capacity one.
 * The small cap represents production's bounded registry, not its exact 4096.
 * Callbacks, registry operations, mark/check and native read admission are
 * atomic/non-yielding premises. Native ID uniqueness, callback execution,
 * first-barrier implementation, FFI ABI and MVCC/read-view correctness remain
 * runtime obligations. This is not a new distributed freshness proof.
 *)
EXTENDS Naturals, FiniteSets

CONSTANTS BugCertifyLateSnapshot, BugMarkBeforeBarrier, BugWrongTxnID,
          BugNoCleanup, BugIgnoreCap

Handles == {"A", "B"}
Ids == 1..3
Cap == 1
Unread == 2

VARIABLES nextId, current, live, bornOwned, phase, snapshot, barrierDone,
          certified, markedEmpty, replies, writeDone
vars == <<nextId, current, live, bornOwned, phase, snapshot, barrierDone,
          certified, markedEmpty, replies, writeDone>>

Init ==
    /\ nextId = 1 /\ current = [h \in Handles |-> 0] /\ live = {}
    /\ bornOwned = [i \in Ids |-> FALSE]
    /\ phase = [i \in Ids |-> "unused"]
    /\ snapshot = [i \in Ids |-> Unread]
    /\ barrierDone = {} /\ certified = {} /\ markedEmpty = {}
    /\ replies = {} /\ writeDone = FALSE

Begin(h, owned) ==
    /\ current[h] = 0 /\ nextId \in Ids
    /\ current' = [current EXCEPT ![h] = nextId]
    /\ live' = live \cup {nextId}
    /\ bornOwned' = [bornOwned EXCEPT ![nextId] = owned]
    /\ phase' = [phase EXCEPT ![nextId] = IF owned THEN "waiting" ELSE "ambient"]
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<snapshot, barrierDone, certified, markedEmpty, replies, writeDone>>

AmbientRead(h) ==
    LET i == current[h] IN
    /\ i \in live /\ phase[i] = "ambient" /\ snapshot[i] = Unread
    /\ snapshot' = [snapshot EXCEPT ![i] = IF writeDone THEN 1 ELSE 0]
    /\ UNCHANGED <<nextId, current, live, bornOwned, phase, barrierDone,
                   certified, markedEmpty, replies, writeDone>>

(* Only the broken path attempts a fresh barrier in a transaction which
 * has already read. The correct path leaves every unmarked ambient txn
 * unmarked, even one which happens to be empty. *)
LateBarrier(h) ==
    LET i == current[h] IN
    /\ BugCertifyLateSnapshot /\ i \in live
    /\ phase[i] = "ambient" /\ snapshot[i] # Unread
    /\ phase' = [phase EXCEPT ![i] = "waiting"]
    /\ UNCHANGED <<nextId, current, live, bornOwned, snapshot, barrierDone,
                   certified, markedEmpty, replies, writeDone>>

PassBarrier(h) ==
    LET i == current[h] IN
    /\ i \in live /\ phase[i] = "waiting"
    /\ barrierDone' = barrierDone \cup {i}
    /\ phase' = [phase EXCEPT ![i] = "barrier"]
    /\ UNCHANGED <<nextId, current, live, bornOwned, snapshot,
                   certified, markedEmpty, replies, writeDone>>

Mark(h) ==
    LET i == current[h] IN
    /\ i \in live
    /\ phase[i] = "barrier" \/ (BugMarkBeforeBarrier /\ phase[i] = "waiting")
    /\ i \in barrierDone \/ BugMarkBeforeBarrier
    /\ (bornOwned[i] /\ snapshot[i] = Unread) \/ BugCertifyLateSnapshot
    /\ Cardinality(certified) < Cap \/ BugIgnoreCap
    /\ certified' = certified \cup {i}
    /\ markedEmpty' = IF snapshot[i] = Unread THEN markedEmpty \cup {i} ELSE markedEmpty
    /\ phase' = [phase EXCEPT ![i] = "ready"]
    /\ UNCHANGED <<nextId, current, live, bornOwned, snapshot, barrierDone, replies, writeDone>>

RejectFull(h) ==
    LET i == current[h] IN
    /\ i \in live /\ phase[i] = "barrier" /\ Cardinality(certified) >= Cap
    /\ ~BugIgnoreCap
    /\ phase' = [phase EXCEPT ![i] = "refused"]
    /\ UNCHANGED <<nextId, current, live, bornOwned, snapshot, barrierDone,
                   certified, markedEmpty, replies, writeDone>>

StrongRead(h) ==
    LET i == current[h] IN
    /\ i \in live /\ phase[i] \in {"ambient", "ready", "read"}
    /\ IF i \in certified \/ (BugWrongTxnID /\ certified # {})
       THEN /\ replies' = replies \cup {[id |-> i, exact |-> i \in certified]}
            /\ phase' = [phase EXCEPT ![i] = "read"]
            /\ snapshot' = [snapshot EXCEPT ![i] =
                               IF @ = Unread THEN IF writeDone THEN 1 ELSE 0 ELSE @]
       ELSE /\ phase' = [phase EXCEPT ![i] = "refused"]
            /\ UNCHANGED <<replies, snapshot>>
    /\ UNCHANGED <<nextId, current, live, bornOwned, barrierDone,
                   certified, markedEmpty, writeDone>>

EndTxn(h) ==
    LET i == current[h] IN
    /\ i \in live
    /\ current' = [current EXCEPT ![h] = 0] /\ live' = live \ {i}
    /\ certified' = IF BugNoCleanup THEN certified ELSE certified \ {i}
    /\ phase' = [phase EXCEPT ![i] = "ended"]
    /\ UNCHANGED <<nextId, bornOwned, snapshot, barrierDone, markedEmpty, replies, writeDone>>

SavepointRollback(h) ==
    LET i == current[h] IN
    /\ i \in live /\ i \in certified /\ certified' = certified \ {i}
    /\ UNCHANGED <<nextId, current, live, bornOwned, phase, snapshot,
                   barrierDone, markedEmpty, replies, writeDone>>

AckWrite ==
    /\ ~writeDone /\ writeDone' = TRUE
    /\ UNCHANGED <<nextId, current, live, bornOwned, phase, snapshot,
                   barrierDone, certified, markedEmpty, replies>>

Next == (\E h \in Handles : Begin(h, TRUE) \/ Begin(h, FALSE) \/ AmbientRead(h)
            \/ LateBarrier(h) \/ PassBarrier(h) \/ Mark(h) \/ RejectFull(h)
            \/ StrongRead(h) \/ EndTxn(h) \/ SavepointRollback(h)) \/ AckWrite
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ nextId \in 1..4 /\ current \in [Handles -> 0..3] /\ live \subseteq Ids
    /\ bornOwned \in [Ids -> BOOLEAN]
    /\ phase \in [Ids -> {"unused", "waiting", "ambient", "barrier", "ready", "read", "refused", "ended"}]
    /\ snapshot \in [Ids -> 0..2]
    /\ barrierDone \subseteq Ids /\ certified \subseteq Ids /\ markedEmpty \subseteq Ids
    /\ replies \subseteq [id : Ids, exact : BOOLEAN] /\ writeDone \in BOOLEAN

RegistryBounded == Cardinality(certified) <= Cap
RegistryOnlyLive == certified \subseteq live
CertificateBeforeRead == certified \subseteq markedEmpty
CertificateHasProvenance == \A i \in certified : bornOwned[i] /\ i \in barrierDone
ReadHasOwnCertificate == \A r \in replies : r.exact
=============================================================================
