---------------------------- MODULE ShadowUnique ----------------------------
(* A shadow copy whose unique secondary index exists before its first row
   (issue #267, lua/tarn_engine.lua). Rows of the source table are copied
   while writes go on; a write's capture marks its key dirty before it can
   commit, and a reread of the dirty keys behind the final fence settles
   them. EngineCopy checks that refinement with a shadow that has only its
   primary index; this model adds the unique index the shadow now has during
   the copy, over a source that need not be unique (an ALTER that adds a
   unique key may meet duplicates, which must fail it with 1062).

   A copied row is the source's row as long as its key is clean. A copied
   row may be stale once its key is dirty, so it can hold a value that
   another row has taken since. So:
     - copying a row whose value a dirty row's copy holds evicts that copy
       (the final pass puts the dirty row back) and copies the row, and
       copying one whose value a clean row's copy holds fails the change:
       both rows held the value in the source the page was read from;
     - behind the fence, every dirty key first leaves the shadow, and only
       then do the dirty keys' rows go in, where a conflict is a duplicate in
       the fenced source.
   Only writes make keys dirty, so the final pass covers no more keys than
   the writes touched. Pages are read from the confirmed source and written
   later, as separate actions; writes have an unconfirmed interval and may
   roll back.

   A catch-up pass (issue #184) takes the dirty keys so far, and a fresh set
   takes the writes from then on. Once the write noted in its set has ended,
   every key of its set leaves the shadow, and then their confirmed rows go
   back in, each read and written in separate steps as pages are. A copy
   that holds such a row's value leaves if its key is in the fresh set (a
   write to it began since the swap: a later pass, or the final one, puts
   it back), and is otherwise a duplicate in the source. A copied row is
   then the source's row as long as its key is in neither set. The copy's
   fiber runs a pass between pages, and the final fence waits for it; a
   pass that fails puts its keys back into the dirty set.

   PreparedImage: a prepared shadow is the source, and the source holds no
   duplicate. NoFalseDuplicate: a change fails with 1062 only if the source
   held a duplicate at some moment while it ran. DirtyOnlyWritten: the
   final pass, and a pass, have no more keys than writes began. *)
EXTENDS Naturals, FiniteSets
CONSTANTS MaxWrites,
          BugFailOnDirtyHolder,   \* a stale copy's value fails the change
          BugEvictClean,          \* a clean row's copy is evicted too
          BugTailInPlace,         \* the final pass replaces dirty rows one by one
          BugPassHolderOldSet,    \* a pass takes its own set for the stale copies
          BugPassInPlace          \* a pass replaces its rows one by one

Keys == {1, 2, 3}
Vals == 0..2                       \* 0: no row; 1, 2: the unique column's value
Rows == [Keys -> Vals]
Dup(r) == \E j, k \in Keys : j # k /\ r[j] # 0 /\ r[j] = r[k]
Holders(r, k, v) == {j \in Keys \ {k} : r[j] = v}
NoPage == [active |-> FALSE, key |-> 1, value |-> 0]
NoWrite == [active |-> FALSE, key |-> 1, value |-> 0]
Phases == {"copy", "delete", "insert", "prepared", "failed"}

VARIABLES source, shadow, dirty, copied, page, pending, writes,
          phase, deleted, inserted, dupSeen,
          pass, pdeleted, pdone, reread, unsettled
pvars == <<pass, pdeleted, pdone, reread, unsettled>>
vars == <<source, shadow, dirty, copied, page, pending, writes,
          phase, deleted, inserted, dupSeen, pvars>>

Init ==
    /\ source \in Rows /\ shadow = [k \in Keys |-> 0]
    /\ dirty = {} /\ copied = {} /\ page = NoPage /\ pending = NoWrite
    /\ writes = 0 /\ phase = "copy" /\ deleted = {} /\ inserted = {}
    /\ dupSeen = Dup(source)
    /\ pass = {} /\ pdeleted = {} /\ pdone = {} /\ reread = NoPage /\ unsettled = FALSE

(* A source write: its capture marks the key before it can commit (the
   on_replace trigger). It is confirmed or rolled back later; the final
   fence drains it. *)
StartWrite(k, v) ==
    /\ phase = "copy" /\ ~pending.active /\ writes < MaxWrites
    /\ pending' = [active |-> TRUE, key |-> k, value |-> v]
    /\ dirty' = dirty \cup {k}
    /\ writes' = writes + 1
    /\ UNCHANGED <<source, shadow, copied, page, phase, deleted, inserted, dupSeen, pvars>>

FinishWrite(commit) ==
    /\ pending.active
    /\ LET next == IF commit THEN [source EXCEPT ![pending.key] = pending.value] ELSE source
       IN /\ source' = next
          /\ dupSeen' = (dupSeen \/ Dup(next))
    /\ pending' = NoWrite
    \* (A pass waits for this write if its set noted it. Only one write is
    \* pending at a time, so its end settles the pass.)
    /\ unsettled' = FALSE
    /\ UNCHANGED <<shadow, dirty, copied, page, writes, phase, deleted, inserted,
                   pass, pdeleted, pdone, reread>>

(* A page is read from the confirmed source, and written in a later
   transaction; the copy's cursor visits each key once. *)
ReadPage(k) ==
    /\ phase = "copy" /\ ~page.active /\ k \notin copied /\ pass = {}
    /\ page' = [active |-> TRUE, key |-> k, value |-> source[k]]
    /\ UNCHANGED <<source, shadow, dirty, copied, pending, writes, phase, deleted, inserted, dupSeen, pvars>>

WritePage ==
    /\ phase = "copy" /\ page.active
    /\ LET k == page.key
           v == page.value
           h == IF v = 0 THEN {} ELSE Holders(shadow, k, v)
           stale == IF BugEvictClean THEN h ELSE h \cap dirty
       IN IF h \ stale # {} \/ (BugFailOnDirtyHolder /\ h # {})
          THEN /\ phase' = "failed"
               /\ UNCHANGED <<shadow, copied>>
          ELSE /\ shadow' = [j \in Keys |-> IF j = k THEN v
                                            ELSE IF j \in stale THEN 0 ELSE shadow[j]]
               /\ copied' = copied \cup {k}
               /\ UNCHANGED phase
    /\ page' = NoPage
    /\ UNCHANGED <<source, dirty, pending, writes, deleted, inserted, dupSeen, pvars>>

(* The final fence: no write starts any more, and the ones begun have
   finished (drain). *)
Fence ==
    /\ phase = "copy" /\ copied = Keys /\ ~page.active /\ ~pending.active /\ pass = {}
    /\ phase' = IF BugTailInPlace \/ dirty = {} THEN "insert" ELSE "delete"
    /\ UNCHANGED <<source, shadow, dirty, copied, page, pending, writes, deleted, inserted, dupSeen, pvars>>

TailDelete(k) ==
    /\ phase = "delete" /\ k \in dirty \ deleted
    /\ shadow' = [shadow EXCEPT ![k] = 0]
    /\ deleted' = deleted \cup {k}
    /\ phase' = IF deleted' = dirty THEN "insert" ELSE "delete"
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, inserted, dupSeen, pvars>>

TailInsert(k) ==
    /\ phase = "insert" /\ k \in dirty \ inserted
    /\ LET v == source[k]
       IN IF v # 0 /\ Holders(shadow, k, v) # {}
          THEN /\ phase' = "failed"
               /\ UNCHANGED <<shadow, inserted>>
          ELSE /\ shadow' = [shadow EXCEPT ![k] = v]
               /\ inserted' = inserted \cup {k}
               /\ UNCHANGED phase
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, deleted, dupSeen, pvars>>

Prepare ==
    /\ phase = "insert" /\ inserted = dirty
    /\ phase' = "prepared"
    /\ UNCHANGED <<source, shadow, dirty, copied, page, pending, writes, deleted, inserted, dupSeen, pvars>>

(* A catch-up pass, between pages: its set is the dirty keys so far, and a
   fresh set takes the writes from now on. The write in flight, if any, is
   noted in its set, and the pass waits for it to end. *)
BeginPass ==
    /\ phase = "copy" /\ ~page.active /\ pass = {} /\ dirty # {}
    /\ pass' = dirty /\ dirty' = {} /\ unsettled' = pending.active
    /\ UNCHANGED <<source, shadow, copied, page, pending, writes, phase, deleted, inserted, dupSeen,
                   pdeleted, pdone, reread>>

\* Every key of the pass leaves the shadow first (BugPassInPlace: none does).
PassDelete(k) ==
    /\ phase = "copy" /\ k \in pass \ pdeleted /\ ~unsettled /\ ~BugPassInPlace
    /\ shadow' = [shadow EXCEPT ![k] = 0]
    /\ pdeleted' = pdeleted \cup {k}
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, phase, deleted, inserted, dupSeen,
                   pass, pdone, reread, unsettled>>

PassRead(k) ==
    /\ phase = "copy" /\ ~reread.active /\ k \in pass \ pdone /\ ~unsettled
    /\ pdeleted = pass \/ BugPassInPlace
    /\ reread' = [active |-> TRUE, key |-> k, value |-> source[k]]
    /\ UNCHANGED <<source, shadow, dirty, copied, page, pending, writes, phase, deleted, inserted, dupSeen,
                   pass, pdeleted, pdone, unsettled>>

\* A copy that holds the row's value: stale, and evicted, if a write to its
\* key began since the swap; otherwise a duplicate in the source.
PassWrite ==
    /\ phase = "copy" /\ reread.active
    /\ LET k == reread.key
           v == reread.value
           h == IF v = 0 THEN {} ELSE Holders(shadow, k, v)
           stale == h \cap (IF BugPassHolderOldSet THEN pass ELSE dirty)
       IN IF h \ stale # {}
          THEN /\ phase' = "failed"
               /\ UNCHANGED <<shadow, pass, pdeleted, pdone>>
          ELSE /\ shadow' = [j \in Keys |-> IF j = k THEN v
                                            ELSE IF j \in stale THEN 0 ELSE shadow[j]]
               /\ IF pdone \cup {k} = pass
                  THEN pass' = {} /\ pdeleted' = {} /\ pdone' = {}
                  ELSE pdone' = pdone \cup {k} /\ UNCHANGED <<pass, pdeleted>>
               /\ UNCHANGED phase
    /\ reread' = NoPage
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, deleted, inserted, dupSeen, unsettled>>

\* A pass that fails (a lost cut, a deadline) puts its keys back.
AbandonPass ==
    /\ phase = "copy" /\ pass # {}
    /\ dirty' = dirty \cup pass
    /\ pass' = {} /\ pdeleted' = {} /\ pdone' = {} /\ reread' = NoPage /\ unsettled' = FALSE
    /\ UNCHANGED <<source, shadow, copied, page, pending, writes, phase, deleted, inserted, dupSeen>>

Next ==
    \/ \E k \in Keys, v \in Vals : StartWrite(k, v)
    \/ FinishWrite(TRUE) \/ FinishWrite(FALSE)
    \/ \E k \in Keys : ReadPage(k)
    \/ WritePage \/ Fence
    \/ \E k \in Keys : TailDelete(k) \/ TailInsert(k)
    \/ Prepare
    \/ BeginPass \/ PassWrite \/ AbandonPass
    \/ \E k \in Keys : PassDelete(k) \/ PassRead(k)
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ source \in Rows /\ shadow \in Rows
    /\ dirty \subseteq Keys /\ copied \subseteq Keys
    /\ deleted \subseteq Keys /\ inserted \subseteq Keys
    /\ page \in [active : BOOLEAN, key : Keys, value : Vals]
    /\ pending \in [active : BOOLEAN, key : Keys, value : Vals]
    /\ writes \in 0..MaxWrites /\ phase \in Phases /\ dupSeen \in BOOLEAN
    /\ pass \subseteq Keys /\ pdeleted \subseteq pass /\ pdone \subseteq pass
    /\ reread \in [active : BOOLEAN, key : Keys, value : Vals] /\ unsettled \in BOOLEAN
\* The shadow's native unique index never holds a value twice.
ShadowUnique == ~Dup(shadow)
PreparedImage == phase = "prepared" => shadow = source /\ ~Dup(source)
NoFalseDuplicate == phase = "failed" => dupSeen
\* The copy adds no key to the final pass, or to a pass: only writes do
\* (lua/tarn_engine.lua bounds those keys, and a copy that keeps meeting stale
\* copies of duplicates must not outgrow that bound instead of failing with
\* 1062).
DirtyOnlyWritten == Cardinality(dirty \cup pass) <= writes
=============================================================================
