------------------------------- MODULE MC -------------------------------
(* Model-checking instances for TarnCommit.  Each .cfg file picks one     *)
(* workload (programs for three transactions) and a set of Bug* flags.    *)
(* Keys x and z live on shard A, y on shard B.                            *)
EXTENDS TarnCommit

MCShards == {"A", "B"}
MCKeys   == {"x", "y"}
MCOwner  == [k \in MCKeys |-> IF k = "y" THEN "B" ELSE "A"]
MCTxns   == {"T1", "T2", "T3"}
MCReaders == {"R1"}
MCRKeys  == [r \in MCReaders |-> {"x", "y"}]
\* Direct-read safety follows from PlainOnlyCommitted, so the large fault
\* models run without readers; the failover and Tradeoff models keep one.
NoReaders == {}
NoRKeys  == [r \in NoReaders |-> {}]

\* WriteSkew: two cross-shard read-modify-writes that read each other's
\* write target, plus a cross-shard read-only transaction.
RSetSkew == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"y"} [] t = "T3" -> {"x", "y"}]
WSetSkew == [t \in MCTxns |-> CASE t = "T1" -> {"y"} [] t = "T2" -> {"x"} [] t = "T3" -> {}]

\* LocalVsMulti: a single-shard blind write races a cross-shard
\* transaction that read the same key, observed by a cross-shard reader.
RSetLocal == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {} [] t = "T3" -> {"x", "y"}]
WSetLocal == [t \in MCTxns |-> CASE t = "T1" -> {"y"} [] t = "T2" -> {"x"} [] t = "T3" -> {}]

\* Fracture: a cross-shard blind writer, a cross-shard reader, and a
\* single-shard read-modify-write on x.
RSetFrac == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {"x"}]
WSetFrac == [t \in MCTxns |-> CASE t = "T1" -> {"x", "y"} [] t = "T2" -> {} [] t = "T3" -> {"x"}]

\* Recovery: a cross-shard read-modify-write under coordinator crashes and
\* message loss, a single-shard writer on B, and a single-shard reader on A.
RSetRecov == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"y"} [] t = "T3" -> {"x"}]
WSetRecov == [t \in MCTxns |-> CASE t = "T1" -> {"x", "y"} [] t = "T2" -> {"y"} [] t = "T3" -> {}]

\* No READ COMMITTED transactions (every workload above).
NoRC   == {}
NoLSet == [t \in MCTxns |-> {}]

\* Counter: two READ COMMITTED read-modify-writes of x (UPDATE ... SET x =
\* x - 1) race a serializable one: T1 on A only (after a plain read of x),
\* T2 across shards (x and y, through PREPARE/APPLY), and T3 a serializable
\* cross-shard read-modify-write of x and y (W locks while prepared).
RCCounter    == {"T1", "T2"}
RSetCounter  == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {} [] t = "T3" -> {"x", "y"}]
WSetCounter  == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {"x", "y"}]
LSetCounter  == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {}]

\* Mixed: an RC writer of x that reads y, a serializable cross-shard
\* read-modify-write of y, and an RC transaction that locks y FOR UPDATE
\* (no write) and reads x.
RCMixed    == {"T1", "T3"}
RSetMixed  == [t \in MCTxns |-> CASE t = "T1" -> {"y"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {"x"}]
WSetMixed  == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"y"} [] t = "T3" -> {}]
LSetMixed  == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {} [] t = "T3" -> {"y"}]

\* Local: RC read-modify-writes of x (T1 on A, T2 across shards) race a
\* serializable single-shard read-modify-write of x (T3: a local commit,
\* like an autocommit UPDATE), whose commit may still be in flight when an
\* RC lock on x is granted.
RCLocal     == {"T1", "T2"}
RSetRCLocal == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {} [] t = "T3" -> {"x"}]
WSetRCLocal == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {"x"}]
LSetRCLocal == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {}]
=============================================================================
