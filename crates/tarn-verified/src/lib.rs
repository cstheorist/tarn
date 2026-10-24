//! Components of Tarn whose functional correctness is proven with Verus.
//! The same source builds with plain cargo (ghost code is erased by the
//! `verus!` macro) and is verified with `verus --crate-type=lib src/lib.rs`.
#![allow(unused_imports)]

pub mod coord;
pub mod route;
pub mod wire;
