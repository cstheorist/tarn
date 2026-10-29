//! Tarn's SQL layer: parsing, planning and execution of MySQL statements over
//! Tarantool shards, plus the session/transaction logic of the commit
//! protocol specified in tla/TarnCommit.tla.

// Plans and read sets are nested tuples/maps by design; naming every one of
// them would not make the code clearer.
#![allow(clippy::type_complexity)]

pub mod admin;
pub mod alter;
pub mod buckets;
pub mod catalog;
pub mod cluster;
pub mod collation;
pub mod members;
#[rustfmt::skip]
mod collation_data;
pub mod complex;
pub mod exec;
pub mod expr;
pub mod kv;
pub mod lazy;
pub mod lint;
pub mod mpw;
pub mod normalize;
mod observe;
mod placement;
pub mod plan;
pub mod planner;
pub mod prep;
pub mod rebalance;
pub mod route;
mod rpcs;
pub use rpcs::RpcConfig;
pub mod session;
pub mod statistics;
pub mod txn;
pub mod types;
pub mod vinyl;

pub use cluster::{Cluster, ClusterConfig, ShardConfig};
pub use session::{Output, Session};

/// Version string reported to clients (MySQL 8 compatible).
pub const SERVER_VERSION: &str = "8.0.40-tarn-0.1";
