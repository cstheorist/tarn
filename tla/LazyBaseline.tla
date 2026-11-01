---------------------------- MODULE LazyBaseline ----------------------------
(***************************************************************************)
(* The sequence a lazy-index bootstrap gives a row's baseline assignment   *)
(* (issue #177; lua/tarn_lazy.lua, L.bootstrap), and a bootstrap that runs *)
(* while writes go on (issue #184; L.capture_start).  One row of one       *)
(* source bucket, whose index key is A, B or none (deleted), changes       *)
(* before a new incarnation exists, which those writes do not capture,    *)
(* while it is built, and after it is ready.  A captured write is an       *)
(* old-key delete and a new-key put, both with the write's sequence, the   *)
(* bucket counter plus one.  The two keys are target buckets with their    *)
(* own markers and floors.  Delivery ships, loses, duplicates and reorders *)
(* frames; a target applies a frame above both its floor and its marker,   *)
(* and acknowledges the others as obsolete; floors install the bucket's    *)
(* completed prefix once the incarnation is ready, and tombstones at or    *)
(* below them are collected.                                               *)
(*                                                                         *)
(* The build starts under a short fence that drains the table's commits    *)
(* and prepared transactions: from then on every write, including the      *)
(* APPLY of a transaction prepared later, captures the new incarnation.    *)
(* The baseline scan then reads the row and the counter in one view, while *)
(* writes go on, and enqueues the baseline assignment later, in another    *)
(* transaction, with any sequence up to the counter it read: the bucket    *)
(* counter itself, the row's last sequence as the per-row ledger of older  *)
(* releases recorded it, or 0 for a row with no ledger entry.  The         *)
(* incarnation becomes ready once the scan is done and the outbox is       *)
(* drained.  DrainedConverges: once the outbox is drained, the markers are *)
(* the row's projection, however frames were reordered or duplicated, also *)
(* after collection.                                                       *)
(*                                                                         *)
(* BugBaselineAboveCounter gives the baseline the counter plus one: a      *)
(* later write's old-key delete then no longer supersedes it.              *)
(* BugScanBeforeCapture lets the scan read the row before writes capture   *)
(* the incarnation: a write in between reaches neither the baseline nor    *)
(* the outbox.  BugNoPrepareDrain starts the build while a transaction     *)
(* prepared before it is pending: its APPLY uses the capture plan of its   *)
(* PREPARE, without the new incarnation.  BugReadyBeforeScan declares the  *)
(* incarnation ready once the outbox is drained, whether or not the scan   *)
(* is done.                                                                *)
(*                                                                         *)
(* Retirement (DROP or a rotation) is permanent: the target is dropped and *)
(* every later frame is acknowledged without reading a floor, so the       *)
(* floors go too (L.clear_retired).  BugClearLiveFloors also clears them   *)
(* while the incarnation is ready: a duplicate of a frame whose tombstone  *)
(* was collected then brings a stale membership back.                      *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets

CONSTANTS BugBaselineAboveCounter, BugClearLiveFloors, BugScanBeforeCapture,
          BugNoPrepareDrain, BugReadyBeforeScan

Keys == {"A", "B"}
Values == Keys \cup {"none"}
MaxBefore == 1   \* writes before the build (not captured for it)
MaxDuring == 2   \* writes while it is built (captured once capture is on)
MaxAfter == 1    \* writes after READY (captured)
MaxSeq == MaxBefore + MaxDuring + MaxAfter + 2
Frames == [seq : 0..MaxSeq, key : Keys, put : BOOLEAN, boot : BOOLEAN]
NoView == [seq |-> -1, key |-> "none"]

VARIABLES base, counter, phase, capturing, prepared, view, scanned,
          writes, outbox, sent, wire, markers, floors
vars == <<base, counter, phase, capturing, prepared, view, scanned,
          writes, outbox, sent, wire, markers, floors>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
\* The bucket's completed prefix: its counter, or one less than the smallest
\* pending sequence.
Prefix == IF outbox = {} THEN counter ELSE Min({f.seq : f \in outbox}) - 1
NoMarker == [seq |-> -1, present |-> FALSE]

TypeOK ==
    /\ base \in Values /\ counter \in 0..MaxSeq
    /\ phase \in {"before", "building", "ready", "retired"}
    /\ capturing \in BOOLEAN /\ scanned \in BOOLEAN
    /\ prepared \in Values \cup {"idle", "done"}
    /\ view \in [seq : -1..MaxSeq, key : Values]
    /\ writes \in [before : 0..MaxBefore, during : 0..MaxDuring, after : 0..MaxAfter]
    /\ outbox \subseteq Frames /\ sent \subseteq Frames /\ wire \subseteq Frames
    /\ markers \in [Keys -> [seq : -1..MaxSeq, present : BOOLEAN]]
    /\ floors \in [Keys -> -1..MaxSeq]

Init ==
    /\ base \in Values /\ counter = 0 /\ phase = "before"
    /\ capturing = FALSE /\ prepared = "idle" /\ view = NoView /\ scanned = FALSE
    /\ writes = [before |-> 0, during |-> 0, after |-> 0]
    /\ outbox = {} /\ sent = {} /\ wire = {}
    /\ markers = [k \in Keys |-> NoMarker] /\ floors = [k \in Keys |-> -1]

\* The frames of a captured write of value v at sequence s.
Captured(v, s) ==
    (IF base # "none" /\ base # v
     THEN {[seq |-> s, key |-> base, put |-> FALSE, boot |-> FALSE]} ELSE {})
    \cup (IF v # "none" THEN {[seq |-> s, key |-> v, put |-> TRUE, boot |-> FALSE]} ELSE {})

\* A write the incarnation does not capture: it takes a sequence, captured
\* for other incarnations only.
Uncaptured(v) ==
    /\ base' = v
    /\ counter' = counter + 1
    /\ UNCHANGED <<outbox>>

Captures(v) ==
    /\ base' = v
    /\ counter' = counter + 1
    /\ outbox' = outbox \cup Captured(v, counter + 1)

Change(v) == v # base \/ v # "none"

WriteBefore ==
    /\ phase = "before" /\ writes.before < MaxBefore
    /\ \E v \in Values : Change(v) /\ Uncaptured(v)
    /\ writes' = [writes EXCEPT !.before = @ + 1]
    /\ UNCHANGED <<phase, capturing, prepared, view, scanned, sent, wire, markers, floors>>

\* A cross-shard transaction prepares before the build: its PREPARE fixes
\* the capture plan its APPLY will use (L.reserve, L.release).
Prepare ==
    /\ phase = "before" /\ prepared = "idle"
    /\ \E v \in Values : Change(v) /\ prepared' = v
    /\ UNCHANGED <<base, counter, phase, capturing, view, scanned, writes, outbox, sent, wire, markers, floors>>

\* Its APPLY, whenever it comes: the plan of its PREPARE has no frame for an
\* incarnation that did not exist then.
ApplyPrepared ==
    /\ prepared \in Values
    /\ Uncaptured(prepared)
    /\ prepared' = "done"
    /\ UNCHANGED <<phase, capturing, view, scanned, writes, sent, wire, markers, floors>>

\* The build begins (L.prepare, L.capture_start): under a short fence that
\* waits for the table's commits in flight and prepared transactions, the
\* descriptor is stored and writes capture the incarnation from then on.
StartBuild ==
    /\ phase = "before"
    /\ prepared \notin Values \/ BugNoPrepareDrain
    /\ phase' = "building"
    /\ capturing' = ~BugScanBeforeCapture
    /\ UNCHANGED <<base, counter, prepared, view, scanned, writes, outbox, sent, wire, markers, floors>>

\* (BugScanBeforeCapture only) capture is switched on later.
StartCapture ==
    /\ phase = "building" /\ ~capturing
    /\ capturing' = TRUE
    /\ UNCHANGED <<base, counter, phase, prepared, view, scanned, writes, outbox, sent, wire, markers, floors>>

WriteDuring ==
    /\ phase = "building" /\ writes.during < MaxDuring
    /\ \E v \in Values : Change(v) /\ IF capturing THEN Captures(v) ELSE Uncaptured(v)
    /\ writes' = [writes EXCEPT !.during = @ + 1]
    /\ UNCHANGED <<phase, capturing, prepared, view, scanned, sent, wire, markers, floors>>

\* A batch of the scan reads the row and its bucket's counter in one view;
\* writes go on meanwhile.
ScanRead ==
    /\ phase = "building" /\ ~scanned /\ view = NoView
    /\ capturing \/ BugScanBeforeCapture
    /\ \E b \in (IF BugBaselineAboveCounter THEN {counter + 1} ELSE 0..counter) :
         view' = [seq |-> b, key |-> base]
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, scanned, writes, outbox, sent, wire, markers, floors>>

\* The batch's assignments and cursor are written in a later transaction.
ScanEnqueue ==
    /\ phase = "building" /\ view # NoView
    /\ outbox' = IF view.key = "none" THEN outbox
                 ELSE outbox \cup {[seq |-> view.seq, key |-> view.key, put |-> TRUE, boot |-> TRUE]}
    /\ scanned' = TRUE /\ view' = NoView
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, writes, sent, wire, markers, floors>>

\* Every baseline assignment is acknowledged before READY.
Ready ==
    /\ phase = "building" /\ outbox = {} /\ capturing
    /\ scanned \/ BugReadyBeforeScan
    /\ phase' = "ready"
    /\ UNCHANGED <<base, counter, capturing, prepared, view, scanned, writes, outbox, sent, wire, markers, floors>>

\* A captured write after READY.
WriteAfter ==
    /\ phase = "ready" /\ writes.after < MaxAfter
    /\ \E v \in Values : Change(v) /\ Captures(v)
    /\ writes' = [writes EXCEPT !.after = @ + 1]
    /\ UNCHANGED <<phase, capturing, prepared, view, scanned, sent, wire, markers, floors>>

Ship(f) ==
    /\ f \in outbox
    /\ sent' = sent \cup {f} /\ wire' = wire \cup {f}
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, markers, floors>>

\* A frame shipped before is delivered again, at any later time.
Duplicate(f) ==
    /\ f \in sent /\ f \notin wire
    /\ wire' = wire \cup {f}
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, sent, markers, floors>>

Lose(f) ==
    /\ f \in wire
    /\ wire' = wire \ {f}
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, sent, markers, floors>>

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
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, sent, floors>>

\* Floors begin once the incarnation is ready, at the completed prefix.
InstallFloor(k) ==
    /\ phase = "ready" /\ Prefix > floors[k]
    /\ floors' = [floors EXCEPT ![k] = Prefix]
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, sent, wire, markers>>

\* Tombstone collection at or below the floor.
GC(k) ==
    /\ ~markers[k].present /\ markers[k].seq >= 0 /\ floors[k] >= markers[k].seq
    /\ markers' = [markers EXCEPT ![k] = NoMarker]
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, sent, wire, floors>>

\* DROP or a rotation retires the incarnation for good, and its target is
\* dropped (L.retire, L.collect_once).
Retire ==
    /\ phase = "ready"
    /\ phase' = "retired"
    /\ markers' = [k \in Keys |-> NoMarker]
    /\ UNCHANGED <<base, counter, capturing, prepared, view, scanned, writes, outbox, sent, wire, floors>>

\* L.clear_retired: a retired incarnation's floors are deleted.
ClearFloors(k) ==
    /\ \/ phase = "retired"
       \/ BugClearLiveFloors /\ phase = "ready"
    /\ floors[k] >= 0
    /\ floors' = [floors EXCEPT ![k] = -1]
    /\ UNCHANGED <<base, counter, phase, capturing, prepared, view, scanned, writes, outbox, sent, wire, markers>>

Next ==
    \/ WriteBefore \/ Prepare \/ ApplyPrepared \/ StartBuild \/ StartCapture
    \/ WriteDuring \/ ScanRead \/ ScanEnqueue \/ Ready \/ WriteAfter \/ Retire
    \/ \E f \in Frames : Ship(f) \/ Duplicate(f) \/ Lose(f) \/ Deliver(f)
    \/ \E k \in Keys : InstallFloor(k) \/ GC(k) \/ ClearFloors(k)

Spec == Init /\ [][Next]_vars

\* Once every assignment is acknowledged, the index holds exactly the row's
\* current key; late duplicates delivered afterwards must not change that.
DrainedConverges ==
    phase = "ready" /\ outbox = {} => \A k \in Keys : markers[k].present = (base = k)
=============================================================================
