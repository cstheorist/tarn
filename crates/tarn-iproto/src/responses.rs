//! Encoded response ownership across callers and zero-copy field slices.
//! The declared frame is charged before its read buffer grows. Completed
//! bodies are detached from the reader buffer and keep the charge until the
//! last Bytes slice drops. Decoded Rust row/expression allocations are separate.

use crate::Error;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default)]
pub struct ResponseStats {
    pub bytes: usize,
    pub peak: usize,
    pub refusals: u64,
}

#[derive(Clone, Debug)]
pub struct ResponseBudget(Arc<(usize, Mutex<ResponseStats>)>);

impl ResponseBudget {
    pub fn new(bytes: usize) -> Self {
        Self(Arc::new((bytes, Mutex::new(ResponseStats::default()))))
    }

    pub fn stats(&self) -> ResponseStats {
        *self.0 .1.lock()
    }

    pub(crate) fn acquire(&self, bytes: usize) -> Result<ResponsePermit, Error> {
        let mut s = self.0 .1.lock();
        if bytes > self.0 .0.saturating_sub(s.bytes) {
            s.refusals = s.refusals.saturating_add(1);
            // Already sent requests may have effects: never label this a
            // before-enqueue Admission refusal or authorize mutation replay.
            return Err(Error::Protocol(
                "gateway response byte budget exceeded after enqueue".into(),
            ));
        }
        s.bytes += bytes;
        s.peak = s.peak.max(s.bytes);
        Ok(ResponsePermit {
            budget: self.clone(),
            bytes,
        })
    }
}

pub(crate) struct ResponsePermit {
    budget: ResponseBudget,
    bytes: usize,
}

impl ResponsePermit {
    pub(crate) fn own(self, body: &Bytes) -> Bytes {
        struct Owned {
            body: Box<[u8]>,
            _permit: ResponsePermit,
        }
        impl AsRef<[u8]> for Owned {
            fn as_ref(&self) -> &[u8] {
                &self.body
            }
        }
        Bytes::from_owner(Owned {
            body: body.to_vec().into_boxed_slice(),
            _permit: self,
        })
    }
}

impl Drop for ResponsePermit {
    fn drop(&mut self) {
        self.budget.0 .1.lock().bytes -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slices_keep_the_entire_frame_charge() {
        let b = ResponseBudget::new(32);
        let body = b.acquire(32).unwrap().own(&Bytes::from_static(b"row"));
        let field = body.slice(1..2);
        drop(body);
        assert!(b.acquire(1).is_err());
        assert_eq!(b.stats().bytes, 32);
        drop(field);
        assert_eq!(b.stats().bytes, 0);
    }
}
