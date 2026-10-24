---- MODULE MC_TTrace_1792881172 ----
EXTENDS Sequences, TLCExt, MC, Toolbox, Naturals, TLC

_expression ==
    LET MC_TEExpression == INSTANCE MC_TEExpression
    IN MC_TEExpression!expression
----

_trace ==
    LET MC_TETrace == INSTANCE MC_TETrace
    IN MC_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        dirty = ([R1 |-> FALSE])
        /\
        msgs = ({[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "A", type |-> "apply", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "apply", yes |-> FALSE]})
        /\
        obs = ([T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]])
        /\
        recovering = ({})
        /\
        nFail = (0)
        /\
        nLoss = (0)
        /\
        everC = ([T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE])
        /\
        nCrash = (0)
        /\
        data = ([x |-> "T1", y |-> "init"])
        /\
        part = ([A |-> [T1 |-> "applied", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]])
        /\
        started = ({"T1"})
        /\
        locks = ([A |-> {}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}])
        /\
        acked = ({})
        /\
        local = ([T1 |-> "none", T2 |-> "none", T3 |-> "none"])
        /\
        rec = ([T1 |-> "staging", T2 |-> "none", T3 |-> "none"])
        /\
        wh = ([x |-> <<"T1">>, y |-> <<>>])
        /\
        rtPred = ([T1 |-> {}, T2 |-> {}, T3 |-> {}])
        /\
        coord = ([T1 |-> "applying", T2 |-> "exec", T3 |-> "exec"])
        /\
        dobs = ([R1 |-> [x |-> "T1", y |-> "init"]])
        /\
        lost = ([T1 |-> {}, T2 |-> {}, T3 |-> {}])
        /\
        nTimeout = (0)
        /\
        dpos = ([R1 |-> [x |-> 1, y |-> 0]])
    )
----

_init ==
    /\ nFail = _TETrace[1].nFail
    /\ dobs = _TETrace[1].dobs
    /\ dirty = _TETrace[1].dirty
    /\ data = _TETrace[1].data
    /\ rtPred = _TETrace[1].rtPred
    /\ rec = _TETrace[1].rec
    /\ recovering = _TETrace[1].recovering
    /\ msgs = _TETrace[1].msgs
    /\ dpos = _TETrace[1].dpos
    /\ part = _TETrace[1].part
    /\ nLoss = _TETrace[1].nLoss
    /\ lost = _TETrace[1].lost
    /\ everC = _TETrace[1].everC
    /\ obs = _TETrace[1].obs
    /\ locks = _TETrace[1].locks
    /\ nCrash = _TETrace[1].nCrash
    /\ acked = _TETrace[1].acked
    /\ coord = _TETrace[1].coord
    /\ started = _TETrace[1].started
    /\ nTimeout = _TETrace[1].nTimeout
    /\ wh = _TETrace[1].wh
    /\ local = _TETrace[1].local
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ nFail  = _TETrace[i].nFail
        /\ nFail' = _TETrace[j].nFail
        /\ dobs  = _TETrace[i].dobs
        /\ dobs' = _TETrace[j].dobs
        /\ dirty  = _TETrace[i].dirty
        /\ dirty' = _TETrace[j].dirty
        /\ data  = _TETrace[i].data
        /\ data' = _TETrace[j].data
        /\ rtPred  = _TETrace[i].rtPred
        /\ rtPred' = _TETrace[j].rtPred
        /\ rec  = _TETrace[i].rec
        /\ rec' = _TETrace[j].rec
        /\ recovering  = _TETrace[i].recovering
        /\ recovering' = _TETrace[j].recovering
        /\ msgs  = _TETrace[i].msgs
        /\ msgs' = _TETrace[j].msgs
        /\ dpos  = _TETrace[i].dpos
        /\ dpos' = _TETrace[j].dpos
        /\ part  = _TETrace[i].part
        /\ part' = _TETrace[j].part
        /\ nLoss  = _TETrace[i].nLoss
        /\ nLoss' = _TETrace[j].nLoss
        /\ lost  = _TETrace[i].lost
        /\ lost' = _TETrace[j].lost
        /\ everC  = _TETrace[i].everC
        /\ everC' = _TETrace[j].everC
        /\ obs  = _TETrace[i].obs
        /\ obs' = _TETrace[j].obs
        /\ locks  = _TETrace[i].locks
        /\ locks' = _TETrace[j].locks
        /\ nCrash  = _TETrace[i].nCrash
        /\ nCrash' = _TETrace[j].nCrash
        /\ acked  = _TETrace[i].acked
        /\ acked' = _TETrace[j].acked
        /\ coord  = _TETrace[i].coord
        /\ coord' = _TETrace[j].coord
        /\ started  = _TETrace[i].started
        /\ started' = _TETrace[j].started
        /\ nTimeout  = _TETrace[i].nTimeout
        /\ nTimeout' = _TETrace[j].nTimeout
        /\ wh  = _TETrace[i].wh
        /\ wh' = _TETrace[j].wh
        /\ local  = _TETrace[i].local
        /\ local' = _TETrace[j].local

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MC_TTrace_1792881172.json", _TETrace)

=============================================================================

 Note that you can extract this module `MC_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MC_TEExpression.tla` file takes precedence 
  over the module `MC_TEExpression` below).

---- MODULE MC_TEExpression ----
EXTENDS Sequences, TLCExt, MC, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `MC` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        nFail |-> nFail
        ,dobs |-> dobs
        ,dirty |-> dirty
        ,data |-> data
        ,rtPred |-> rtPred
        ,rec |-> rec
        ,recovering |-> recovering
        ,msgs |-> msgs
        ,dpos |-> dpos
        ,part |-> part
        ,nLoss |-> nLoss
        ,lost |-> lost
        ,everC |-> everC
        ,obs |-> obs
        ,locks |-> locks
        ,nCrash |-> nCrash
        ,acked |-> acked
        ,coord |-> coord
        ,started |-> started
        ,nTimeout |-> nTimeout
        ,wh |-> wh
        ,local |-> local
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_nFailUnchanged |-> nFail = nFail'
        
        \* Format the `nFail` variable as Json value.
        \* ,_nFailJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(nFail)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_nFailModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].nFail # _TETrace[s-1].nFail
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MC_TETrace ----
\*EXTENDS IOUtils, MC, TLC
\*
\*trace == IODeserialize("MC_TTrace_1792881172.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MC_TETrace ----
EXTENDS MC, TLC

trace == 
    <<
    ([dirty |-> [R1 |-> FALSE],msgs |-> {},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> FALSE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "init", y |-> "init"],part |-> [A |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"]],started |-> {},locks |-> [A |-> {}, B |-> {}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<>>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "exec", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> FALSE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "init", y |-> "init"],part |-> [A |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {}, B |-> {}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<>>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "preparing", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> FALSE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "init", y |-> "init"],part |-> [A |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {[k |-> "x", t |-> "T1", m |-> "W"]}, B |-> {}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<>>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "preparing", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "init", y |-> "init"],part |-> [A |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {[k |-> "x", t |-> "T1", m |-> "W"]}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<>>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "preparing", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "A", type |-> "apply", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "apply", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "init", y |-> "init"],part |-> [A |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {[k |-> "x", t |-> "T1", m |-> "W"]}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<>>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "applying", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "A", type |-> "apply", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "apply", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "T1", y |-> "init"],part |-> [A |-> [T1 |-> "applied", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<"T1">>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "applying", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "unread", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 0, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "A", type |-> "apply", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "apply", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "T1", y |-> "init"],part |-> [A |-> [T1 |-> "applied", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<"T1">>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "applying", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "T1", y |-> "unread"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 1, y |-> 0]]]),
    ([dirty |-> [R1 |-> FALSE],msgs |-> {[t |-> "T1", s |-> "A", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "A", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "A", type |-> "apply", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "prepare", yes |-> FALSE], [t |-> "T1", s |-> "B", type |-> "vote", yes |-> TRUE], [t |-> "T1", s |-> "B", type |-> "apply", yes |-> FALSE]},obs |-> [T1 |-> [x |-> "unread", y |-> "unread"], T2 |-> [x |-> "unread", y |-> "unread"], T3 |-> [x |-> "unread", y |-> "unread"]],recovering |-> {},nFail |-> 0,nLoss |-> 0,everC |-> [T1 |-> TRUE, T2 |-> FALSE, T3 |-> FALSE],nCrash |-> 0,data |-> [x |-> "T1", y |-> "init"],part |-> [A |-> [T1 |-> "applied", T2 |-> "none", T3 |-> "none"], B |-> [T1 |-> "prepared", T2 |-> "none", T3 |-> "none"]],started |-> {"T1"},locks |-> [A |-> {}, B |-> {[k |-> "y", t |-> "T1", m |-> "W"]}],acked |-> {},local |-> [T1 |-> "none", T2 |-> "none", T3 |-> "none"],rec |-> [T1 |-> "staging", T2 |-> "none", T3 |-> "none"],wh |-> [x |-> <<"T1">>, y |-> <<>>],rtPred |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],coord |-> [T1 |-> "applying", T2 |-> "exec", T3 |-> "exec"],dobs |-> [R1 |-> [x |-> "T1", y |-> "init"]],lost |-> [T1 |-> {}, T2 |-> {}, T3 |-> {}],nTimeout |-> 0,dpos |-> [R1 |-> [x |-> 1, y |-> 0]]])
    >>
----


=============================================================================

---- CONFIG MC_TTrace_1792881172 ----
CONSTANTS
    Shards <- MCShards
    Keys <- MCKeys
    Owner <- MCOwner
    Txns <- MCTxns
    Readers <- MCReaders
    RKeys <- MCRKeys
    RSet <- RSetFrac
    WSet <- WSetFrac
    MaxFailovers = 0
    MaxCrashes = 0
    MaxLoss = 0
    MaxTimeouts = 1
    BugNoReadLocks = FALSE
    BugNoWLockCheckOnRead = FALSE
    BugNoTombstone = FALSE
    BugLocalIgnoresRLocks = FALSE
    BugAckBeforeApply = FALSE

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Sat Oct 24 18:32:53 EDT 2026