-------------------------- MODULE GlobalUniqueRules --------------------------
(* The rules a write follows on a building global unique index (issue #292),
   which GlobalUniqueBuild and GlobalUniqueWrites instantiate, and
   uix::leave_key and uix::join_key prove in crates/tarn-verified. The
   index's entries and conflicts are a record st:
     entry     key -> the pk whose entry holds it (0: none);
     conflict  <<key, pk>> pairs: rows with a key another row's entry holds.
   Row p leaves key o, then joins key n (NoKey: no row, or a key with a NULL
   part, which has no entry). *)
CONSTANTS Pks, NoKey,
          BugSkipConflict, \* a row joining a key another row's entry holds is not recorded
          BugNoPromote,    \* the holder leaves its key, and no conflict takes the entry over
          BugBlindLeave,   \* a row leaving a key another row's entry holds deletes that entry
          BugJoinReplace   \* a row joining a key another row's entry holds takes the entry over

Left(st, p, o) ==
    IF o = NoKey THEN st
    ELSE IF st.entry[o] = p THEN
        LET others == {q \in Pks : <<o, q>> \in st.conflict} IN
        IF others # {} /\ ~BugNoPromote
        THEN LET q == CHOOSE q \in others : TRUE IN
             [entry |-> [st.entry EXCEPT ![o] = q], conflict |-> st.conflict \ {<<o, q>>}]
        ELSE [entry |-> [st.entry EXCEPT ![o] = 0], conflict |-> st.conflict]
    ELSE [entry |-> IF BugBlindLeave THEN [st.entry EXCEPT ![o] = 0] ELSE st.entry,
          conflict |-> st.conflict \ {<<o, p>>}]
Joined(st, p, n) ==
    IF n = NoKey THEN st
    ELSE IF st.entry[n] = 0 \/ (BugJoinReplace /\ st.entry[n] # p)
    THEN [entry |-> [st.entry EXCEPT ![n] = p], conflict |-> st.conflict]
    ELSE IF st.entry[n] = p \/ BugSkipConflict THEN st
    ELSE [entry |-> st.entry, conflict |-> st.conflict \cup {<<n, p>>}]
(* A write that keeps its row's key reads and writes no entry. *)
Moved(st, p, o, n) == IF o = n THEN st ELSE Joined(Left(st, p, o), p, n)
=============================================================================
