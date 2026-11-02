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

   PreparedImage: a prepared shadow is the source, and the source holds no
   duplicate. NoFalseDuplicate: a change fails with 1062 only if the source
   held a duplicate at some moment while it ran. DirtyOnlyWritten: the
   final pass has no more keys than writes began. *)
EXTENDS Naturals, FiniteSets
CONSTANTS MaxWrites,
          BugFailOnDirtyHolder,   \* a stale copy's value fails the change
          BugEvictClean,          \* a clean row's copy is evicted too
          BugTailInPlace          \* the final pass replaces dirty rows one by one

Keys == {1, 2, 3}
Vals == 0..2                       \* 0: no row; 1, 2: the unique column's value
Rows == [Keys -> Vals]
Dup(r) == \E j, k \in Keys : j # k /\ r[j] # 0 /\ r[j] = r[k]
Holders(r, k, v) == {j \in Keys \ {k} : r[j] = v}
NoPage == [active |-> FALSE, key |-> 1, value |-> 0]
NoWrite == [active |-> FALSE, key |-> 1, value |-> 0]
Phases == {"copy", "delete", "insert", "prepared", "failed"}

VARIABLES source, shadow, dirty, copied, page, pending, writes,
          phase, deleted, inserted, dupSeen
vars == <<source, shadow, dirty, copied, page, pending, writes,
          phase, deleted, inserted, dupSeen>>

Init ==
    /\ source \in Rows /\ shadow = [k \in Keys |-> 0]
    /\ dirty = {} /\ copied = {} /\ page = NoPage /\ pending = NoWrite
    /\ writes = 0 /\ phase = "copy" /\ deleted = {} /\ inserted = {}
    /\ dupSeen = Dup(source)

(* A source write: its capture marks the key before it can commit (the
   on_replace trigger). It is confirmed or rolled back later; the final
   fence drains it. *)
StartWrite(k, v) ==
    /\ phase = "copy" /\ ~pending.active /\ writes < MaxWrites
    /\ pending' = [active |-> TRUE, key |-> k, value |-> v]
    /\ dirty' = dirty \cup {k}
    /\ writes' = writes + 1
    /\ UNCHANGED <<source, shadow, copied, page, phase, deleted, inserted, dupSeen>>

FinishWrite(commit) ==
    /\ pending.active
    /\ LET next == IF commit THEN [source EXCEPT ![pending.key] = pending.value] ELSE source
       IN /\ source' = next
          /\ dupSeen' = (dupSeen \/ Dup(next))
    /\ pending' = NoWrite
    /\ UNCHANGED <<shadow, dirty, copied, page, writes, phase, deleted, inserted>>

(* A page is read from the confirmed source, and written in a later
   transaction; the copy's cursor visits each key once. *)
ReadPage(k) ==
    /\ phase = "copy" /\ ~page.active /\ k \notin copied
    /\ page' = [active |-> TRUE, key |-> k, value |-> source[k]]
    /\ UNCHANGED <<source, shadow, dirty, copied, pending, writes, phase, deleted, inserted, dupSeen>>

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
    /\ UNCHANGED <<source, dirty, pending, writes, deleted, inserted, dupSeen>>

(* The final fence: no write starts any more, and the ones begun have
   finished (drain). *)
Fence ==
    /\ phase = "copy" /\ copied = Keys /\ ~page.active /\ ~pending.active
    /\ phase' = IF BugTailInPlace \/ dirty = {} THEN "insert" ELSE "delete"
    /\ UNCHANGED <<source, shadow, dirty, copied, page, pending, writes, deleted, inserted, dupSeen>>

TailDelete(k) ==
    /\ phase = "delete" /\ k \in dirty \ deleted
    /\ shadow' = [shadow EXCEPT ![k] = 0]
    /\ deleted' = deleted \cup {k}
    /\ phase' = IF deleted' = dirty THEN "insert" ELSE "delete"
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, inserted, dupSeen>>

TailInsert(k) ==
    /\ phase = "insert" /\ k \in dirty \ inserted
    /\ LET v == source[k]
       IN IF v # 0 /\ Holders(shadow, k, v) # {}
          THEN /\ phase' = "failed"
               /\ UNCHANGED <<shadow, inserted>>
          ELSE /\ shadow' = [shadow EXCEPT ![k] = v]
               /\ inserted' = inserted \cup {k}
               /\ UNCHANGED phase
    /\ UNCHANGED <<source, dirty, copied, page, pending, writes, deleted, dupSeen>>

Prepare ==
    /\ phase = "insert" /\ inserted = dirty
    /\ phase' = "prepared"
    /\ UNCHANGED <<source, shadow, dirty, copied, page, pending, writes, deleted, inserted, dupSeen>>

Next ==
    \/ \E k \in Keys, v \in Vals : StartWrite(k, v)
    \/ FinishWrite(TRUE) \/ FinishWrite(FALSE)
    \/ \E k \in Keys : ReadPage(k)
    \/ WritePage \/ Fence
    \/ \E k \in Keys : TailDelete(k) \/ TailInsert(k)
    \/ Prepare
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ source \in Rows /\ shadow \in Rows
    /\ dirty \subseteq Keys /\ copied \subseteq Keys
    /\ deleted \subseteq Keys /\ inserted \subseteq Keys
    /\ page \in [active : BOOLEAN, key : Keys, value : Vals]
    /\ pending \in [active : BOOLEAN, key : Keys, value : Vals]
    /\ writes \in 0..MaxWrites /\ phase \in Phases /\ dupSeen \in BOOLEAN
\* The shadow's native unique index never holds a value twice.
ShadowUnique == ~Dup(shadow)
PreparedImage == phase = "prepared" => shadow = source /\ ~Dup(source)
NoFalseDuplicate == phase = "failed" => dupSeen
\* The copy adds no key to the final pass: only writes do (lua/tarn_engine.lua
\* bounds those keys, and a copy that keeps meeting stale copies of
\* duplicates must not outgrow that bound instead of failing with 1062).
DirtyOnlyWritten == Cardinality(dirty) <= writes
=============================================================================
