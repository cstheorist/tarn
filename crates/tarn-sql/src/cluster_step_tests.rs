//! Schema-change steps that may be repeated (APPLY) are sent again after a
//! transient refusal, for a bounded time, and only then; not to a shard
//! that is down.
use super::*;

use std::sync::atomic::AtomicU32;

const STEP: Duration = Duration::from_millis(1);
const LIMIT: Duration = Duration::from_millis(200);

/// Fails with `errors` in turn, then succeeds; counts its attempts.
async fn run(errors: &[SqlError], limit: Duration) -> (SqlResult<u32>, u32) {
    let attempts = AtomicU32::new(0);
    let r = retry_transient(limit, STEP, || {
        let n = attempts.fetch_add(1, Ordering::Relaxed);
        let e = errors.get(n as usize).cloned();
        async move {
            match e {
                Some(e) => Err(e),
                None => Ok(n),
            }
        }
    })
    .await;
    (r, attempts.load(Ordering::Relaxed))
}

#[tokio::test]
async fn a_conflict_is_retried_until_the_step_succeeds() {
    let conflicts = [
        SqlError::conflict("Transaction has been aborted by conflict"),
        SqlError::conflict("Transaction has been aborted by conflict"),
    ];
    let (r, attempts) = run(&conflicts, LIMIT).await;
    assert_eq!((r.unwrap(), attempts), (2, 3));
}

/// A shard whose leader resigned has none until an election ends (#267);
/// a step lost with its connection, or rolled back by a new leader, may be
/// sent again too.
#[tokio::test]
async fn a_leader_change_is_ridden_out() {
    let refusals = [
        SqlError::new(2013, "HY000", "Lost connection to storage"),
        SqlError::new(
            2003,
            "HY000",
            "shard 1 has no reachable leader (127.0.0.1:3313 is read-only)",
        ),
        SqlError::new(
            2003,
            "HY000",
            "shard 1 has no reachable leader (127.0.0.1:3313 is read-only)",
        ),
        SqlError::new(
            1105,
            "HY000",
            "storage error 217: A rollback for a synchronous transaction is received",
        ),
        // (a new leader still applying the index build of the same space)
        SqlError::new(
            1105,
            "HY000",
            "storage error 12: Can't modify space 'db.t': the space is already being modified",
        ),
    ];
    let (r, attempts) = run(&refusals, LIMIT).await;
    assert_eq!((r.unwrap(), attempts), (5, 6));
}

#[tokio::test]
async fn refusals_past_the_limit_are_reported() {
    let refusals = vec![
        SqlError::new(
            2003,
            "HY000",
            "shard 1 has no reachable leader (127.0.0.1:3313 is read-only)"
        );
        1000
    ];
    let started = Instant::now();
    let (r, attempts) = run(&refusals, Duration::from_millis(50)).await;
    assert_eq!(r.unwrap_err().code, 2003);
    assert!(attempts > 1 && attempts < 1000, "{attempts} attempts");
    assert!(started.elapsed() >= Duration::from_millis(50));
}

/// A shard none of whose replicas answers is down, not between leaders: the
/// step fails at once (after a lost connection, at the next attempt), and a
/// server finishes the job once the shard is back.
#[tokio::test]
async fn a_shard_that_is_down_fails_at_once() {
    let down = SqlError::new(
        2003,
        "HY000",
        "shard 2 has no reachable leader (127.0.0.1:3821: tarantool i/o error: Connection refused (os error 111))",
    );
    let (r, attempts) = run(std::slice::from_ref(&down), LIMIT).await;
    assert_eq!((r.unwrap_err().code, attempts), (2003, 1));
    let lost = SqlError::new(2013, "HY000", "Lost connection to storage");
    let (r, attempts) = run(&[lost, down], LIMIT).await;
    assert_eq!((r.unwrap_err().code, attempts), (2003, 2));
}

#[tokio::test]
async fn other_errors_are_not_retried() {
    for e in [
        SqlError::new(1205, "HY000", "Lock wait timeout exceeded"),
        SqlError::new(1412, "HY000", "Table definition has changed"),
        SqlError::new(1105, "HY000", "storage error 1: something else"),
        SqlError::new(
            1105,
            "HY000",
            "storage error 12: Can't modify space 'db.t': the field is in use",
        ),
    ] {
        let code = e.code;
        let (r, attempts) = run(&[e], LIMIT).await;
        assert_eq!((r.unwrap_err().code, attempts), (code, 1));
    }
}
