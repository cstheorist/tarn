//! Coordinator decision logic of the cross-shard commit protocol, verified
//! with Verus against the corresponding actions of tla/TarnCommit.tla.
//!
//! `decide` is TarnCommit!CoordDecide: COMMIT exactly when every participant
//! voted YES, ABORT exactly when some participant voted NO (a NO leaves a
//! tombstone, so that participant can never prepare), and otherwise defer to
//! recovery.
//!
//! `resolve_step` is one step of the recovery procedure
//! (TarnCommit!RecoverFenceRecord, RecoverFenceParticipant, RecoverDecide)
//! given the decision shard's record and every participant's state. Its
//! postconditions are the protocol's safety argument in local form:
//!   * it reports/decides COMMIT only when the transaction is committed in
//!     the sense of TarnCommit!CommittedNow (record COMMITTED, or STAGING
//!     with every participant prepared/applied);
//!   * it reports/decides ABORT only when the transaction can never become
//!     committed (record ABORTED/NONE-fenced, or a participant tombstoned);
//!   * it only ever fences participants that never prepared (fencing a
//!     prepared participant is the bug TLC finds in Bug_NoTombstone's
//!     neighbourhood).

use vstd::prelude::*;

verus! {

#[derive(Clone, Copy, Debug)]
pub enum Vote {
    Yes,
    No,
    /// The reply was lost or the participant was unreachable.
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub enum Decision {
    Commit,
    Abort,
    Unknown,
}

pub open spec fn all_yes(v: Seq<Vote>) -> bool {
    forall|i: int| 0 <= i < v.len() ==> (#[trigger] v[i]) is Yes
}

pub open spec fn some_no(v: Seq<Vote>) -> bool {
    exists|i: int| 0 <= i < v.len() && (#[trigger] v[i]) is No
}

pub fn decide(votes: &[Vote]) -> (d: Decision)
    requires
        votes.len() > 0,
    ensures
        (d is Commit) <==> all_yes(votes@),
        (d is Abort) <==> some_no(votes@),
        (d is Unknown) <==> (!all_yes(votes@) && !some_no(votes@)),
{
    let mut all = true;
    let mut any_no = false;
    let mut i: usize = 0;
    while i < votes.len()
        invariant
            i <= votes.len(),
            all <==> (forall|j: int| 0 <= j < i ==> (#[trigger] votes@[j]) is Yes),
            any_no <==> (exists|j: int| 0 <= j < i && (#[trigger] votes@[j]) is No),
        decreases votes.len() - i,
    {
        match votes[i] {
            Vote::Yes => {},
            Vote::No => {
                all = false;
                any_no = true;
            },
            Vote::Unknown => {
                all = false;
            },
        }
        proof {
            if any_no {
                assert(exists|j: int| 0 <= j < i + 1 && (#[trigger] votes@[j]) is No);
            }
        }
        i = i + 1;
    }
    if any_no {
        proof {
            // a NO vote rules out unanimity
            let j = choose|j: int| 0 <= j < votes@.len() && (#[trigger] votes@[j]) is No;
            assert(!(votes@[j] is Yes));
        }
        Decision::Abort
    } else if all {
        Decision::Commit
    } else {
        Decision::Unknown
    }
}

/// Status of the transaction record on the decision shard (TarnCommit: rec).
#[derive(Clone, Copy, Debug)]
pub enum RecStatus {
    None,
    Staging,
    Committed,
    Aborted,
}

/// A participant's record (TarnCommit: part[s][t]).
#[derive(Clone, Copy, Debug)]
pub enum PartState {
    None,
    Prepared,
    Applied,
    /// Voted NO, was aborted, or was fenced (tombstone).
    Aborted,
}

#[derive(Debug)]
pub enum Step {
    /// The record says COMMITTED: (re)send APPLY.
    Committed,
    /// The record says ABORTED: (re)send ABORT.
    Aborted,
    /// STAGING and every participant prepared: record COMMITTED.
    DecideCommit,
    /// STAGING and some participant is tombstoned: record ABORTED.
    DecideAbort,
    /// No record on the decision shard: write ABORTED there (fences its PREPARE).
    FenceRecord,
    /// STAGING, nobody tombstoned, these participants never prepared:
    /// tombstone them so a late PREPARE is refused.
    FenceParticipants(Vec<usize>),
}

pub open spec fn prepared_or_applied(p: PartState) -> bool {
    (p is Prepared) || (p is Applied)
}

/// TarnCommit!CommittedNow, restricted to the record and participant states.
pub open spec fn committed_now(rec: RecStatus, parts: Seq<PartState>) -> bool {
    (rec is Committed) || ((rec is Staging) && forall|i: int| 0 <= i < parts.len() ==> prepared_or_applied(#[trigger] parts[i]))
}

/// Some participant is tombstoned: since tombstones are permanent, the
/// STAGING record can never be completed by unanimous PREPAREs.
pub open spec fn doomed(parts: Seq<PartState>) -> bool {
    exists|i: int| 0 <= i < parts.len() && (#[trigger] parts[i]) is Aborted
}

pub fn resolve_step(rec: RecStatus, parts: &[PartState]) -> (s: Step)
    requires
        parts.len() > 0,
    ensures
        (s is Committed) ==> (rec is Committed),
        (s is Aborted) ==> (rec is Aborted),
        (s is DecideCommit) ==> (rec is Staging) && committed_now(rec, parts@),
        (s is DecideAbort) ==> (rec is Staging) && doomed(parts@) && !committed_now(rec, parts@),
        (s is FenceRecord) ==> (rec is None) && !committed_now(rec, parts@),
        (s matches Step::FenceParticipants(m) ==> {
            &&& (rec is Staging)
            &&& !doomed(parts@)
            &&& m@.len() > 0
            &&& forall|k: int| 0 <= k < m@.len() ==> {
                    &&& 0 <= #[trigger] m@[k] < parts@.len()
                    &&& parts@[m@[k] as int] is None
                }
        }),
{
    match rec {
        RecStatus::Committed => Step::Committed,
        RecStatus::Aborted => Step::Aborted,
        RecStatus::None => Step::FenceRecord,
        RecStatus::Staging => {
            let mut missing: Vec<usize> = Vec::new();
            let mut any_aborted = false;
            let mut i: usize = 0;
            while i < parts.len()
                invariant
                    i <= parts.len(),
                    any_aborted <==> (exists|j: int| 0 <= j < i && (#[trigger] parts@[j]) is Aborted),
                    forall|k: int| 0 <= k < missing@.len() ==> {
                        &&& 0 <= #[trigger] missing@[k] < i
                        &&& parts@[missing@[k] as int] is None
                    },
                    // every participant seen so far is prepared/applied, tombstoned, or listed as missing
                    forall|j: int| 0 <= j < i ==> prepared_or_applied(#[trigger] parts@[j])
                        || parts@[j] is Aborted
                        || missing@.contains(j as usize),
                decreases parts.len() - i,
            {
                let ghost old_missing = missing@;
                match parts[i] {
                    PartState::Aborted => {
                        any_aborted = true;
                    },
                    PartState::None => {
                        missing.push(i);
                        proof {
                            assert(missing@.last() == i);
                            assert(missing@.contains(i));
                        }
                    },
                    _ => {},
                }
                proof {
                    if any_aborted {
                        if parts@[i as int] is Aborted {
                            assert(exists|j: int| 0 <= j < i + 1 && (#[trigger] parts@[j]) is Aborted);
                        }
                    }
                    // missing only grew by appending i
                    assert(missing@ == old_missing || missing@ == old_missing.push(i));
                    assert forall|j: int| 0 <= j < i + 1 implies prepared_or_applied(#[trigger] parts@[j])
                        || parts@[j] is Aborted
                        || missing@.contains(j as usize) by {
                        if j < i && old_missing.contains(j as usize) {
                            let k = choose|k: int| 0 <= k < old_missing.len() && old_missing[k] == j as usize;
                            assert(missing@[k] == j as usize);
                        }
                    }
                }
                i = i + 1;
            }
            if any_aborted {
                proof {
                    let j = choose|j: int| 0 <= j < parts@.len() && (#[trigger] parts@[j]) is Aborted;
                    assert(!prepared_or_applied(parts@[j]));
                }
                Step::DecideAbort
            } else if missing.len() == 0 {
                proof {
                    assert forall|j: int| 0 <= j < parts@.len() implies prepared_or_applied(#[trigger] parts@[j]) by {
                        assert(!(parts@[j] is Aborted));
                        assert(!missing@.contains(j as usize));
                    }
                }
                Step::DecideCommit
            } else {
                Step::FenceParticipants(missing)
            }
        },
    }
}

} // verus!

impl RecStatus {
    pub fn parse(s: &str) -> RecStatus {
        match s {
            "staging" => RecStatus::Staging,
            "committed" => RecStatus::Committed,
            "aborted" => RecStatus::Aborted,
            _ => RecStatus::None,
        }
    }
}

impl PartState {
    pub fn parse(s: &str) -> PartState {
        match s {
            "prepared" => PartState::Prepared,
            "applied" => PartState::Applied,
            "aborted" => PartState::Aborted,
            _ => PartState::None,
        }
    }
}
