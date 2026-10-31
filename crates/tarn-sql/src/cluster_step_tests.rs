//! Schema-change steps that may be repeated are sent again after a
//! transaction conflict, a bounded number of times, and only then.
use super::*;

use std::sync::atomic::AtomicU32;

const STEP: Duration = Duration::from_millis(1);

/// Fails with `errors` in turn, then succeeds; counts its attempts.
async fn run(errors: &[SqlError], retries: u32) -> (SqlResult<u32>, u32) {
    let attempts = AtomicU32::new(0);
    let r = retry_conflicts(retries, STEP, || {
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
    let (r, attempts) = run(&conflicts, 3).await;
    assert_eq!((r.unwrap(), attempts), (2, 3));
}

#[tokio::test]
async fn conflicts_beyond_the_bound_are_reported() {
    let conflicts = vec![SqlError::conflict("Transaction has been aborted by conflict"); 5];
    let (r, attempts) = run(&conflicts, 3).await;
    assert_eq!((r.unwrap_err().code, attempts), (1213, 4));
}

#[tokio::test]
async fn other_errors_are_not_retried() {
    for e in [
        SqlError::new(1205, "HY000", "Lock wait timeout exceeded"),
        SqlError::new(2013, "HY000", "Lost connection to storage"),
        SqlError::new(1412, "HY000", "Table definition has changed"),
    ] {
        let code = e.code;
        let (r, attempts) = run(&[e], 3).await;
        assert_eq!((r.unwrap_err().code, attempts), (code, 1));
    }
}
