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
     - copying a row whose value a dirty row's copy holds defers the row
       (its key becomes dirty, the row stays out of the shadow), and copying
       one whose value a clean row's copy holds fails the change: both rows
       held the value in the source the page was read from;
     - behind the fence, every dirty key first leaves the shadow, and only
       then do the dirty keys' rows go in, where a conflict is a duplicate in
       the fenced source.
   Pages are read from the confirmed source and written later, as separate
   actions; writes have an unconfirmed interval and may roll back.

   PreparedImage: a prepared shadow is the source, and the source holds no
   duplicate. NoFalseDuplicate: a change fails with 1062 only if the source
   held a duplicate at some moment while it ran. *)
EXTENDS Naturals, FiniteSets
CONSTANTS MaxWrites,
          BugFailOnDirtyHolder,   \* a stale copy's value fails the change
          BugSkipWithoutDefer,    \* a deferred row is left out, not made dirty
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
       IN IF h = {}
          THEN /\ shadow' = [shadow EXCEPT ![k] = v]
               /\ copied' = copied \cup {k}
               /\ UNCHANGED <<dirty, phase>>
          ELSE IF (\E j \in h : j \in dirty) /\ ~BugFailOnDirtyHolder
               THEN /\ dirty' = IF BugSkipWithoutDefer THEN dirty ELSE dirty \cup {k}
                    /\ copied' = copied \cup {k}
                    /\ UNCHANGED <<shadow, phase>>
               ELSE /\ phase' = "failed"
                    /\ UNCHANGED <<shadow, dirty, copied>>
    /\ page' = NoPage
    /\ UNCHANGED <<source, pending, writes, deleted, inserted, dupSeen>>

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
=============================================================================
