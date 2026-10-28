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

\* Exercise the other decision writer for the common two-shard workload.
\* The selector depends only on t and the frozen candidates, as the gateway
\* hash does. Recovery still asks D(t), regardless of later bucket movement.
RotatedDecision(t, candidates) ==
    LET preferred == IF t = "T1" THEN "B" ELSE "A" IN
    CHOOSE s \in candidates : s = preferred \/ preferred \notin candidates

\* A grouped read of two tables on A (issue #83). x and z stand for keys
\* or bounded prefixes of different tables; y is on B. T1 applies y before
\* z, T3 sees that y and commits x, then T2 reads x and z in one snapshot.
\* Checking only the first table (x) would admit new x with old z while
\* T1 still holds z's W lock: no serial order explains that result.
MCKeysGrouped  == {"x", "y", "z"}
MCOwnerGrouped == [k \in MCKeysGrouped |-> IF k = "y" THEN "B" ELSE "A"]
RSetGrouped == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {"x", "z"} [] t = "T3" -> {"y"}]
WSetGrouped == [t \in MCTxns |-> CASE t = "T1" -> {"y", "z"} [] t = "T2" -> {} [] t = "T3" -> {"x"}]

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

\* No inserts (every workload above).
NoIKeys == [t \in MCTxns |-> {}]

\* Uniq: a unique key without the shard key (issue #14). x is the global
\* index entry of one value (shard A); y is T1's row (shard B). T1 inserts
\* its row and the entry across shards; T2 inserts another row with the
\* same value, its row and the entry both on A (a local commit); T3 reads
\* the entry and T1's row across shards.
RSetUniq  == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {} [] t = "T3" -> {"x", "y"}]
WSetUniq  == [t \in MCTxns |-> CASE t = "T1" -> {"x", "y"} [] t = "T2" -> {"x"} [] t = "T3" -> {}]
IKeysUniq == [t \in MCTxns |-> CASE t = "T1" -> {"x", "y"} [] t = "T2" -> {"x"} [] t = "T3" -> {}]
\* ... and with T2 at READ COMMITTED, locking the entry key first.
RCUniq    == {"T2"}
LSetUniq  == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {"x"} [] t = "T3" -> {}]
\* Bucket moves (issue #24): none in the workloads above, and no scans.
NoSSet  == [t \in MCTxns |-> {}]
NoMKeys == {}
MCFrom  == "A"
MCTo    == "B"
\* x moves from A to B.
MKeysX  == {"x"}

\* MoveScan: T1 scans both shards for x and y (a range read on every shard:
\* a lookup by a non-unique index, a full scan); T2 writes x blindly (an
\* autocommit UPDATE: a local commit where x is); T3 a read-modify-write of
\* x and y (PREPARE/APPLY while x is on A, a local commit on B after).
SSetScan == [t \in MCTxns |-> IF t = "T1" THEN {"x", "y"} ELSE {}]
RSetMove == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {} [] t = "T3" -> {"x", "y"}]
WSetMove == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {"x"} [] t = "T3" -> {"x", "y"}]

\* A shard that joins (issue #25): B joins the cluster and then x moves to
\* it.  T1's scan (OldScan) was routed with the membership from before B
\* joined: it does not read B.  Every row starts on A (the scan reads one
\* shard, in one call), or, with a third shard C, y starts on C (the scan
\* reads A and C in streams, and validates or prepares there).
MCOwnerJoin   == [k \in MCKeys |-> "A"]
MCShardsJoin3 == {"A", "B", "C"}
MCOwnerJoin3  == [k \in MCKeys |-> IF k = "y" THEN "C" ELSE "A"]
NoOld   == {}
OldScan == {"T1"}

\* MoveRC: T1 a READ COMMITTED read-modify-write of x (it locks x where x
\* is), T2 one of x and y, T3 a serializable read-modify-write of x (a
\* local commit, which may still be in flight when the move fences x).
RCMove     == {"T1", "T2"}
RSetMoveRC == [t \in MCTxns |-> CASE t = "T1" -> {} [] t = "T2" -> {} [] t = "T3" -> {"x"}]
WSetMoveRC == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {"x"}]
LSetMoveRC == [t \in MCTxns |-> CASE t = "T1" -> {"x"} [] t = "T2" -> {"x", "y"} [] t = "T3" -> {}]
=============================================================================
