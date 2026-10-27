//! Components of Tarn whose functional correctness is proven with Verus.
//! The same source builds with plain cargo (ghost code is erased by the
//! `verus!` macro) and is verified with `verus --crate-type=lib src/lib.rs`.
#![allow(unused_imports)]
// Verified code follows Verus idioms (`i = i + 1`, explicit arithmetic,
// range and length comparisons that the specs mirror) that clippy's style
// lints would rewrite; keep it as the verifier sees it.
#![allow(clippy::assign_op_pattern, clippy::identity_op, clippy::len_zero)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::doc_lazy_continuation, clippy::doc_overindented_list_items)]

pub mod batch;
pub mod coord;
pub mod locks;
pub mod mp;
pub mod route;
pub mod uix;
pub mod wire;
