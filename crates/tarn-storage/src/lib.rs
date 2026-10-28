//! Native storage procedures for Tarn's shards (issue #28): a Tarantool C
//! module that serves the most frequent storage requests without running
//! Lua, which costs a shard's transaction thread more than the storage
//! work itself (bench/profile.py). Each procedure serves the requests it
//! can decide as the Lua procedure would, from the state the Lua module
//! keeps current for it (`state`), and hands the others to the Lua
//! procedure.
//!
//! lua/tarn.lua loads it ("Native procedures"); SQL servers call
//! `tarn_storage.ts_select` instead of `tarn_select` where it is.

pub mod fallback;
pub mod locks;
pub mod mp;
pub mod select;
pub mod state;
pub mod write;

// (Tarantool's module API is only there inside tarantool: the unit tests
// leave the procedures out.)
#[cfg(not(test))]
mod procs;
