----------------------------- MODULE LazyIndex -----------------------------
(* Local eventual-index refinement, NOT a distributed SQL isolation model.
   One PK moves A->B, is deleted, and is reinserted at B. Bootstrap is
   fenced. Two key buckets retain independent versioned membership markers.
   A speculative uncommitted write may be rolled back. Durable source
   assignments, lossy messages, ACKs, duplicate old frames and tombstone GC
   are distinct steps. A second index incarnation projects the opposite key.
   Native RW atomicity/confirmation and authoritative retirement are premises. *)
EXTENDS Naturals, Integers, FiniteSets
CONSTANTS BugSplitOutbox, BugShipBeforeCommit, BugForgetBeforeAck,
          BugStaleOverwrite, BugGcBeforeFloor, BugFloorFromMaxAck,
          BugNoBaseRecheck, BugNoKeyRecheck, BugForwardBeforeCapture,
          BugCaptureFailOpen, BugIgnoreGeneration, BugAckMissingTarget

Keys == {"A", "B"}
Other(k) == IF k = "A" THEN "B" ELSE "A"
Events == 0..7
ESeq(e) == CASE e = 0 -> 0 [] e \in {1,2} -> 1 [] e = 3 -> 2
               [] e \in {4,7} -> 3 [] OTHER -> 4
EKey(e) == IF e \in {0,1,5,7} THEN "A" ELSE "B"
EPut(e) == e \in {0,2,4,6,7}
EGen(e) == IF e = 7 THEN 2 ELSE 1
Changes(s) == CASE s = 1 -> {1,2} [] s = 2 -> {3} [] OTHER -> {4}
Value(s) == IF s = 2 THEN "none" ELSE "B"
EmptyMarkers == [k \in Keys |-> [seq |-> -1, present |-> FALSE]]

VARIABLES base, counter, step, outbox, sent, wire, acks, markers, floors,
          prepared, published, capturing, gen, retired, speculative,
          uncommittedApplied, badBaseRead, badKeyRead
vars == <<base,counter,step,outbox,sent,wire,acks,markers,floors,
          prepared,published,capturing,gen,retired,speculative,
          uncommittedApplied,badBaseRead,badKeyRead>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Prefix == IF outbox = {} THEN counter ELSE Min({ESeq(e) : e \in outbox}) - 1
Projection == IF base = "none" THEN "none" ELSE IF gen = 1 THEN base ELSE Other(base)
BootEvent == IF gen = 1 THEN 0 ELSE 7
Visible(k) == markers[k].present
Matches == \A k \in Keys : Visible(k) = (Projection = k)

Init ==
    /\ base = "A" /\ counter = 0 /\ step = 0 /\ outbox = {0}
    /\ sent = {} /\ wire = {} /\ acks = {} /\ markers = EmptyMarkers
    /\ floors = [k \in Keys |-> -1]
    /\ prepared = FALSE /\ published = FALSE /\ capturing = FALSE
    /\ gen = 1 /\ retired = FALSE /\ speculative = FALSE
    /\ uncommittedApplied = FALSE /\ badBaseRead = FALSE /\ badKeyRead = FALSE

PrepareReady ==
    /\ ~prepared /\ ~retired /\ BootEvent \in acks /\ outbox = {}
    /\ prepared' = TRUE /\ capturing' = FALSE
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   published,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>
Publish ==
    /\ prepared /\ ~published /\ ~retired
    /\ published' = TRUE /\ capturing' = TRUE
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   prepared,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

CommitRow ==
    /\ gen = 1 /\ ~retired /\ step < 3
    /\ (published /\ (capturing \/ BugCaptureFailOpen)) \/ (BugForwardBeforeCapture /\ prepared)
    /\ base' = Value(step+1) /\ counter' = step+1 /\ step' = step+1
    /\ outbox' = IF capturing /\ ~BugSplitOutbox THEN outbox \cup Changes(step+1) ELSE outbox
    /\ UNCHANGED <<sent,wire,acks,markers,floors,prepared,published,capturing,
                   gen,retired,speculative,uncommittedApplied,badBaseRead,badKeyRead>>
BeginSpeculative ==
    /\ gen = 1 /\ step = 3 /\ ~retired /\ ~speculative
    /\ speculative' = TRUE
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   prepared,published,capturing,gen,retired,
                   uncommittedApplied,badBaseRead,badKeyRead>>

Ship(e) ==
    /\ e \in outbox \/ (BugShipBeforeCommit /\ speculative /\ e \in {5,6})
    /\ sent' = sent \cup {e} /\ wire' = wire \cup {e}
    /\ UNCHANGED <<base,counter,step,outbox,acks,markers,floors,
                   prepared,published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>
Duplicate(e) ==
    /\ e \in sent /\ wire' = wire \cup {e}
    /\ UNCHANGED <<base,counter,step,outbox,sent,acks,markers,floors,
                   prepared,published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>
Lose(e) ==
    /\ e \in wire /\ wire' = wire \ {e}
    /\ UNCHANGED <<base,counter,step,outbox,sent,acks,markers,floors,
                   prepared,published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

Deliver(e) ==
    /\ e \in wire
    /\ LET valid == ~retired /\ (EGen(e) = gen \/ BugIgnoreGeneration)
           accept == valid /\ ESeq(e) > floors[EKey(e)]
                     /\ (ESeq(e) > markers[EKey(e)].seq \/ BugStaleOverwrite)
       IN /\ markers' = IF accept
                         THEN [markers EXCEPT ![EKey(e)] = [seq |-> ESeq(e),present |-> EPut(e)]]
                         ELSE markers
          /\ uncommittedApplied' = (uncommittedApplied \/ (accept /\ ESeq(e) > counter))
    /\ acks' = acks \cup {e} /\ wire' = wire \ {e}
    /\ UNCHANGED <<base,counter,step,outbox,sent,floors,prepared,published,
                   capturing,gen,retired,speculative,badBaseRead,badKeyRead>>
MissingTarget(e) ==
    /\ BugAckMissingTarget /\ e \in wire /\ ~retired
    /\ acks' = acks \cup {e} /\ wire' = wire \ {e}
    /\ UNCHANGED <<base,counter,step,outbox,sent,markers,floors,prepared,published,
                   capturing,gen,retired,speculative,uncommittedApplied,badBaseRead,badKeyRead>>
Forget(e) ==
    /\ e \in outbox /\ (e \in acks \/ (BugForgetBeforeAck /\ e \in sent))
    /\ outbox' = outbox \ {e}
    /\ UNCHANGED <<base,counter,step,sent,wire,acks,markers,floors,prepared,
                   published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

InstallFloor(k) ==
    /\ prepared /\ ~retired
    /\ LET f == IF BugFloorFromMaxAck THEN Max({0} \cup {ESeq(e) : e \in acks}) ELSE Prefix
       IN /\ f > floors[k] /\ floors' = [floors EXCEPT ![k] = f]
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,prepared,
                   published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>
GC(k) ==
    /\ ~markers[k].present /\ markers[k].seq >= 0
    /\ floors[k] >= markers[k].seq \/ BugGcBeforeFloor
    /\ markers' = [markers EXCEPT ![k] = [seq |-> -1,present |-> FALSE]]
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,floors,prepared,
                   published,capturing,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

Read(k) ==
    /\ published /\ ~retired /\ Visible(k)
    /\ LET exists == base # "none"
           returned == (exists \/ BugNoBaseRecheck)
                       /\ (~exists \/ Projection = k \/ BugNoKeyRecheck)
       IN /\ badBaseRead' = (badBaseRead \/ (returned /\ ~exists))
          /\ badKeyRead' = (badKeyRead \/ (returned /\ exists /\ Projection # k))
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   prepared,published,capturing,gen,retired,speculative,uncommittedApplied>>
Retire ==
    /\ gen = 1 /\ step = 3 /\ ~retired
    /\ retired' = TRUE
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   prepared,published,capturing,gen,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>
Recreate ==
    /\ gen = 1 /\ retired /\ outbox = {}
    /\ gen' = 2 /\ retired' = FALSE /\ outbox' = {7}
    /\ prepared' = FALSE /\ published' = FALSE /\ capturing' = FALSE
    /\ markers' = EmptyMarkers /\ floors' = [k \in Keys |-> -1]
    /\ UNCHANGED <<base,counter,step,sent,wire,acks,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

Registry(fresh) ==
    /\ published /\ ~retired /\ capturing # fresh
    /\ capturing' = fresh
    /\ UNCHANGED <<base,counter,step,outbox,sent,wire,acks,markers,floors,
                   prepared,published,gen,retired,speculative,
                   uncommittedApplied,badBaseRead,badKeyRead>>

Next == PrepareReady \/ Publish \/ CommitRow \/ BeginSpeculative \/ Retire \/ Recreate
        \/ Registry(TRUE) \/ Registry(FALSE)
        \/ (\E e \in Events : Ship(e) \/ Duplicate(e) \/ Lose(e) \/ Deliver(e)
                             \/ MissingTarget(e) \/ Forget(e))
        \/ (\E k \in Keys : InstallFloor(k) \/ GC(k) \/ Read(k))
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ base \in Keys \cup {"none"} /\ counter \in 0..3 /\ step \in 0..3
    /\ outbox \subseteq Events /\ sent \subseteq Events /\ wire \subseteq Events /\ acks \subseteq Events
    /\ markers \in [Keys -> [seq : -1..4,present : BOOLEAN]] /\ floors \in [Keys -> -1..4]
    /\ prepared \in BOOLEAN /\ published \in BOOLEAN /\ capturing \in BOOLEAN
    /\ gen \in 1..2 /\ retired \in BOOLEAN /\ speculative \in BOOLEAN
    /\ uncommittedApplied \in BOOLEAN /\ badBaseRead \in BOOLEAN /\ badKeyRead \in BOOLEAN
CommittedOnly == ~uncommittedApplied
DrainedConverges == prepared /\ ~retired /\ step = 3 /\ outbox = {} => Matches
BaseReadSafe == ~badBaseRead
CandidateKeySafe == ~badKeyRead
=============================================================================
