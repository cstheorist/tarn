---------------------------- MODULE LazyBaseline ----------------------------
(***************************************************************************)
(* The sequence a lazy-index bootstrap gives a row's baseline assignment   *)
(* (issue #177; lua/tarn_lazy.lua, L.bootstrap).  One row of one source    *)
(* bucket, whose index key is A, B or none (deleted), changes before a new *)
(* incarnation is bootstrapped, which those writes do not capture, and     *)
(* after it is ready, which they do: an old-key delete and a new-key put,  *)
(* both with the write's sequence, the bucket counter plus one.  The two   *)
(* keys are target buckets with their own markers and floors.  Delivery   *)
(* ships, loses, duplicates and reorders frames; a target applies a frame  *)
(* above both its floor and its marker, and acknowledges the others as     *)
(* obsolete; floors install the bucket's completed prefix once the         *)
(* incarnation is ready, and tombstones at or below them are collected.    *)
(*                                                                         *)
(* Bootstrap reads the row and the counter in one view while writes are    *)
(* fenced, and may give the baseline any sequence up to that counter: the  *)
(* bucket counter itself, the row's last sequence as the per-row ledger    *)
(* recorded it, or 0 for a row with no ledger entry.  DrainedConverges:    *)
(* once the outbox is drained, the markers are the row's projection,       *)
(* however frames were reordered or duplicated, also after collection.     *)
(* BugBaselineAboveCounter gives the baseline the counter plus one: a      *)
(* later write's old-key delete then no longer supersedes it.              *)
(*                                                                         *)
(* Retirement (DROP or a rotation) is permanent: the target is dropped and *)
(* every later frame is acknowledged without reading a floor, so the       *)
(* floors go too (L.clear_retired).  BugClearLiveFloors also clears them   *)
(* while the incarnation is ready: a duplicate of a frame whose tombstone  *)
(* was collected then brings a stale membership back.                      *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets

CONSTANTS BugBaselineAboveCounter, BugClearLiveFloors

Keys == {"A", "B"}
Values == Keys \cup {"none"}
MaxBefore == 2   \* writes before the bootstrap (not captured)
MaxAfter == 2    \* writes after READY (captured)
MaxSeq == MaxBefore + MaxAfter + 1
Frames == [seq : 0..MaxSeq, key : Keys, put : BOOLEAN, boot : BOOLEAN]

VARIABLES base, counter, phase, before, after, outbox, sent, wire, markers, floors
vars == <<base, counter, phase, before, after, outbox, sent, wire, markers, floors>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
\* The bucket's completed prefix: its counter, or one less than the smallest
\* pending sequence.
Prefix == IF outbox = {} THEN counter ELSE Min({f.seq : f \in outbox}) - 1
NoMarker == [seq |-> -1, present |-> FALSE]

TypeOK ==
    /\ base \in Values /\ counter \in 0..MaxSeq
    /\ phase \in {"before", "building", "ready", "retired"}
    /\ before \in 0..MaxBefore /\ after \in 0..MaxAfter
    /\ outbox \subseteq Frames /\ sent \subseteq Frames /\ wire \subseteq Frames
    /\ markers \in [Keys -> [seq : -1..MaxSeq, present : BOOLEAN]]
    /\ floors \in [Keys -> -1..MaxSeq]

Init ==
    /\ base \in Values /\ counter = 0 /\ phase = "before"
    /\ before = 0 /\ after = 0
    /\ outbox = {} /\ sent = {} /\ wire = {}
    /\ markers = [k \in Keys |-> NoMarker] /\ floors = [k \in Keys |-> -1]

\* A write before the incarnation exists: it takes a sequence, captured for
\* other incarnations only.
WriteBefore ==
    /\ phase = "before" /\ before < MaxBefore
    /\ \E v \in Values :
         /\ v # base \/ v # "none"
         /\ base' = v
         /\ counter' = counter + 1
         /\ before' = before + 1
    /\ UNCHANGED <<phase, after, outbox, sent, wire, markers, floors>>

\* Writes are fenced: the row and the counter come from one view.
Bootstrap ==
    /\ phase = "before"
    /\ \E b \in (IF BugBaselineAboveCounter THEN {counter + 1} ELSE 0..counter) :
         outbox' = IF base = "none" THEN {}
                   ELSE {[seq |-> b, key |-> base, put |-> TRUE, boot |-> TRUE]}
    /\ phase' = "building"
    /\ UNCHANGED <<base, counter, before, after, sent, wire, markers, floors>>

\* Every baseline assignment is acknowledged before READY.
Ready ==
    /\ phase = "building" /\ outbox = {}
    /\ phase' = "ready"
    /\ UNCHANGED <<base, counter, before, after, outbox, sent, wire, markers, floors>>

\* A captured write: the old key's delete (when the key changes) and the new
\* key's put, with the write's sequence (L.rowplans, publish).
WriteAfter ==
    /\ phase = "ready" /\ after < MaxAfter
    /\ \E v \in Values :
         /\ v # base \/ v # "none"
         /\ LET s == counter + 1
                del == IF base # "none" /\ base # v
                       THEN {[seq |-> s, key |-> base, put |-> FALSE, boot |-> FALSE]} ELSE {}
                put == IF v # "none" THEN {[seq |-> s, key |-> v, put |-> TRUE, boot |-> FALSE]} ELSE {}
            IN /\ base' = v
               /\ counter' = s
               /\ after' = after + 1
               /\ outbox' = outbox \cup del \cup put
    /\ UNCHANGED <<phase, before, sent, wire, markers, floors>>

Ship(f) ==
    /\ f \in outbox
    /\ sent' = sent \cup {f} /\ wire' = wire \cup {f}
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, markers, floors>>

\* A frame shipped before is delivered again, at any later time.
Duplicate(f) ==
    /\ f \in sent /\ f \notin wire
    /\ wire' = wire \cup {f}
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, sent, markers, floors>>

Lose(f) ==
    /\ f \in wire
    /\ wire' = wire \ {f}
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, sent, markers, floors>>

\* L.apply: a frame above the floor and the marker is applied; a baseline
\* the target would apply once the incarnation is ready is refused; any other
\* frame is acknowledged as obsolete.  Once the incarnation is retired,
\* every frame is acknowledged, from the retirement alone.
Deliver(f) ==
    /\ f \in wire
    /\ LET accept == phase # "retired" /\ f.seq > floors[f.key] /\ f.seq > markers[f.key].seq
           refused == accept /\ f.boot /\ phase = "ready"
       IN /\ markers' = IF accept /\ ~refused
                         THEN [markers EXCEPT ![f.key] = [seq |-> f.seq, present |-> f.put]]
                         ELSE markers
          /\ outbox' = IF refused THEN outbox ELSE outbox \ {f}
    /\ wire' = wire \ {f}
    /\ UNCHANGED <<base, counter, phase, before, after, sent, floors>>

\* Floors begin once the incarnation is ready, at the completed prefix.
InstallFloor(k) ==
    /\ phase = "ready" /\ Prefix > floors[k]
    /\ floors' = [floors EXCEPT ![k] = Prefix]
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, sent, wire, markers>>

\* Tombstone collection at or below the floor.
GC(k) ==
    /\ ~markers[k].present /\ markers[k].seq >= 0 /\ floors[k] >= markers[k].seq
    /\ markers' = [markers EXCEPT ![k] = NoMarker]
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, sent, wire, floors>>

\* DROP or a rotation retires the incarnation for good, and its target is
\* dropped (L.retire, L.collect_once).
Retire ==
    /\ phase = "ready"
    /\ phase' = "retired"
    /\ markers' = [k \in Keys |-> NoMarker]
    /\ UNCHANGED <<base, counter, before, after, outbox, sent, wire, floors>>

\* L.clear_retired: a retired incarnation's floors are deleted.
ClearFloors(k) ==
    /\ \/ phase = "retired"
       \/ BugClearLiveFloors /\ phase = "ready"
    /\ floors[k] >= 0
    /\ floors' = [floors EXCEPT ![k] = -1]
    /\ UNCHANGED <<base, counter, phase, before, after, outbox, sent, wire, markers>>

Next ==
    \/ WriteBefore \/ Bootstrap \/ Ready \/ WriteAfter \/ Retire
    \/ \E f \in Frames : Ship(f) \/ Duplicate(f) \/ Lose(f) \/ Deliver(f)
    \/ \E k \in Keys : InstallFloor(k) \/ GC(k) \/ ClearFloors(k)

Spec == Init /\ [][Next]_vars

\* Once every assignment is acknowledged, the index holds exactly the row's
\* current key; late duplicates delivered afterwards must not change that.
DrainedConverges ==
    phase = "ready" /\ outbox = {} => \A k \in Keys : markers[k].present = (base = k)
=============================================================================
