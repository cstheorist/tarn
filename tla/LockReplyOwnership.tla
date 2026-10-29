-------------------------- MODULE LockReplyOwnership ------------------------
(* Ownership of the FIRST locking-RPC reply, including RPCs which yield.
 * Capture PL_GEN and the role epoch AFTER the fresh barrier and pl_begin.
 * A point/range RPC can grant x, yield, lose leadership/locks, regain the
 * writable role and read/grant y. Labeling its result with the NEW generation
 * cannot make its old rows owned. No earlier gateway generation exists yet
 * for this first RPC, so comparing only at the next commit is insufficient.
 *
 * Each returned row was actually granted a lock. Final admission compares
 * the immutable entry generation/role epoch and live session, with no yield
 * before reply. This is the observed local role epoch; the entry barrier
 * does not promise leadership persists, so later commit validation remains
 * necessary. ReturnedRowsLocked is DERIVED from those checks and lock
 * invalidation transitions, not assumed as an extra final guard.
 * Two rows, one demote/promote cycle and one disconnect. Fresh quorum data,
 * native locking/FIFO, callback ordering, session identity, buckets/DDL,
 * public generation parsing and SQL result consumption remain premises.
 *)
EXTENDS Naturals

CONSTANTS BugRelabelGeneration, BugIgnoreDisconnect
Keys == {"x", "y"}
NoLock == 3

VARIABLES phase, generation, epoch, writable, roleStep, liveSession,
          disconnected, entryGen, entryEpoch, locked, granted, rows, replies
vars == <<phase, generation, epoch, writable, roleStep, liveSession,
          disconnected, entryGen, entryEpoch, locked, granted, rows, replies>>

Init ==
    /\ phase = "idle" /\ generation = 0 /\ epoch = 0 /\ writable = TRUE
    /\ roleStep = 0 /\ liveSession = TRUE /\ disconnected = FALSE
    /\ entryGen = 0 /\ entryEpoch = 0
    /\ locked = [k \in Keys |-> NoLock] /\ granted = {} /\ rows = {} /\ replies = {}

StartBarrier ==
    /\ phase = "idle" /\ phase' = "barrier"
    /\ UNCHANGED <<generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, locked, granted, rows, replies>>

PassBarrier ==
    /\ phase = "barrier" /\ phase' = "begin"
    /\ UNCHANGED <<generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, locked, granted, rows, replies>>

PLBegin ==
    /\ phase = "begin" /\ writable /\ liveSession
    /\ entryGen' = generation /\ entryEpoch' = epoch /\ phase' = "scan"
    /\ UNCHANGED <<generation, epoch, writable, roleStep, liveSession,
                   disconnected, locked, granted, rows, replies>>

Grant(k) ==
    /\ phase = "scan" /\ writable /\ liveSession /\ k \notin granted
    /\ locked' = [locked EXCEPT ![k] = generation]
    /\ granted' = granted \cup {k}
    /\ UNCHANGED <<phase, generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, rows, replies>>

Read(k) ==
    /\ phase = "scan" /\ k \in granted \ rows /\ rows' = rows \cup {k}
    /\ UNCHANGED <<phase, generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, locked, granted, replies>>

Demote ==
    /\ roleStep = 0 /\ roleStep' = 1 /\ writable' = FALSE
    /\ generation' = generation + 1 /\ epoch' = epoch + 1
    /\ locked' = [k \in Keys |-> NoLock]
    /\ UNCHANGED <<phase, liveSession, disconnected, entryGen, entryEpoch, granted, rows, replies>>

Promote ==
    /\ roleStep = 1 /\ roleStep' = 2 /\ writable' = TRUE
    /\ generation' = generation + 1 /\ epoch' = epoch + 1
    /\ locked' = [k \in Keys |-> NoLock]
    /\ UNCHANGED <<phase, liveSession, disconnected, entryGen, entryEpoch, granted, rows, replies>>

Disconnect ==
    /\ ~disconnected /\ disconnected' = TRUE /\ liveSession' = FALSE
    /\ locked' = [k \in Keys |-> NoLock]
    /\ UNCHANGED <<phase, generation, epoch, writable, roleStep, entryGen,
                   entryEpoch, granted, rows, replies>>

Reply ==
    /\ phase = "scan" /\ rows = Keys /\ writable
    /\ liveSession \/ BugIgnoreDisconnect
    /\ (entryGen = generation /\ entryEpoch = epoch) \/ BugRelabelGeneration
    /\ LET label == IF BugRelabelGeneration THEN generation ELSE entryGen IN
       replies' = replies \cup {
           [held |-> {k \in Keys : locked[k] = label /\ liveSession},
            entry |-> entryGen, label |-> label,
            entryRole |-> entryEpoch, replyRole |-> epoch]}
    /\ phase' = "done"
    /\ UNCHANGED <<generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, locked, granted, rows>>

Refuse ==
    /\ phase = "scan"
    /\ ~writable \/ ~liveSession \/ generation # entryGen \/ epoch # entryEpoch
    /\ phase' = "refused"
    /\ UNCHANGED <<generation, epoch, writable, roleStep, liveSession,
                   disconnected, entryGen, entryEpoch, locked, granted, rows, replies>>

Next == StartBarrier \/ PassBarrier \/ PLBegin \/ Demote \/ Promote \/ Disconnect
        \/ (\E k \in Keys : Grant(k) \/ Read(k)) \/ Reply \/ Refuse
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "barrier", "begin", "scan", "done", "refused"}
    /\ generation \in 0..2 /\ epoch \in 0..2 /\ roleStep \in 0..2
    /\ writable \in BOOLEAN /\ liveSession \in BOOLEAN /\ disconnected \in BOOLEAN
    /\ entryGen \in 0..2 /\ entryEpoch \in 0..2 /\ locked \in [Keys -> 0..3]
    /\ granted \subseteq Keys /\ rows \subseteq granted
    /\ replies \subseteq [held : SUBSET Keys, entry : 0..2, label : 0..2, entryRole : 0..2, replyRole : 0..2]

ReturnedRowsLocked == \A r \in replies : r.held = Keys
ReplyKeepsEntryGeneration == \A r \in replies : r.entry = r.label /\ r.entryRole = r.replyRole
=============================================================================
