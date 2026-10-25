# Getting started

This guide builds Tarn from source, starts a local cluster, runs SQL against
it, reads the same rows directly from Tarantool, and walks through a leader
failover. It takes about ten minutes, most of it the first build.

## Prerequisites

| Tool | Version | Used for |
|---|---|---|
| Linux x86_64 | | Tarn is developed and tested on Linux |
| [Rust](https://rustup.rs) via rustup | `rust-toolchain.toml` pins 1.98.1; rustup installs it on the first build | building |
| [Tarantool](https://www.tarantool.io/en/download/) | 3.x (developed with 3.9.0) | storage nodes |
| `mysql` or `mariadb` client | any | talking to Tarn |
| Python 3 with [PyMySQL](https://pypi.org/project/PyMySQL/) | 3.10+ | the SQL test suites (optional) |

The scripts look for `tarantool` on your `PATH`; set `TARANTOOL=/path/to/tarantool`
to use another binary.

## Build

```sh
git clone https://github.com/cstheorist/tarn.git
cd tarn
cargo build --release -p tarn-server
```

The binary is `target/release/tarn-server`.

## Start a local cluster

`scripts/cluster.sh` starts Tarantool storage nodes and one or more
`tarn-server` front ends on localhost:

```sh
scripts/cluster.sh start -n 2 -r 1 -p 4000
# cluster up: 2 shard(s) x 1 replica(s); SQL on 127.0.0.1:4000..4000; dir /tmp/tarn-cluster
```

That is two shards with one Tarantool instance each. Storage instance *j* of
shard *i* listens on port `3300 + 10·i + j`, so the shards are on 3311 and
3321, and SQL is on port 4000. Data and logs live under `/tmp/tarn-cluster`.

## Run SQL

Without `--user` options, `tarn-server` accepts any user name and password:

```sh
mysql -h 127.0.0.1 -P 4000 -u root
```

```sql
CREATE DATABASE demo;
USE demo;

CREATE TABLE orders (
  id       BIGINT PRIMARY KEY AUTO_INCREMENT,
  customer INT NOT NULL,
  total    DECIMAL(10,2),
  KEY by_customer (customer)
);

INSERT INTO orders (customer, total) VALUES (7, 19.99), (8, 5.00), (7, 42.00);

SELECT * FROM orders WHERE id = 1;                              -- one shard, one round trip
SELECT customer, SUM(total) FROM orders GROUP BY customer;      -- all shards

-- A transaction whose rows live on different shards commits atomically
-- (two-phase commit) and is strictly serializable.
BEGIN;
UPDATE orders SET total = total - 5 WHERE id = 1;
UPDATE orders SET total = total + 5 WHERE id = 2;
COMMIT;

SHOW TARN STATUS;   -- counters: queries, local / read-only / 2PC commits, conflicts
```

Rows are placed by a hash of the primary key unless the table declares
another shard key with `PARTITION BY KEY(col, ...)`. Unique keys must include
the shard key (as in MySQL's partitioned tables). See
[SQL compatibility](sql-compatibility.md) for what is supported.

## Read the same rows as key/value

Every table is an ordinary Tarantool space named `"<database>.<table>"`, with
the table's columns followed by a `bucket_id` field. Committed rows can be
read with any Tarantool client, without going through SQL:

```sh
tarantool
```

```lua
net_box = require('net.box')
for _, port in ipairs({3311, 3321}) do
  local c = net_box.connect('tarn:tarn@127.0.0.1:' .. port)
  print(port, c.space['demo.orders'].index.primary:get{1})
end
-- prints the row on the shard that owns it, e.g.  3321  [1, 7, 14.99, 1622]
```

A real client computes the owning shard from the key instead of asking every
shard: `crates/tarn-sql/examples/kvbench.rs` does that with the same
(Verus-verified) bucket function the SQL layer uses. Direct readers only ever
see committed data, and a direct read issued after a `COMMIT` returned sees
that commit.

## High availability and failover

Restart with three synchronous replicas per shard (quorum two, Raft leader
election):

```sh
scripts/cluster.sh stop
scripts/cluster.sh start -n 2 -r 3 -p 4000
scripts/cluster.sh status
```

Kill the leader of shard 1 (find it with `scripts/cluster.sh status`, or try
ports 3311–3313) and keep using the SQL connection: a new leader is elected
in about 2–3 seconds, and `tarn-server` finds it. Meanwhile, statements fail
with a retryable error (1213), with 2003 while the shard has no leader, or,
if a commit was cut off, with "outcome unknown" (1180); afterwards
everything continues. `tests/failover.sh`
automates exactly this under a concurrent workload and checks that no
committed transaction was lost or applied twice.

## Run the tests

```sh
cargo test --workspace                         # unit tests + IPROTO integration tests
lua/test/run.sh                                # storage protocol tests (starts two shards)

scripts/cluster.sh start -n 2 -r 1 -p 4000     # the SQL suites need a cluster
python3 tests/sql/txntest.py --tarn 127.0.0.1:4000 --kv 127.0.0.1:3311,127.0.0.1:3321
```

[Verification](verification.md) describes every suite, the TLA+ model and the
Verus proofs, and how to run them.

## Stop

```sh
scripts/cluster.sh stop
```

## Next steps

* [Operations](operations.md): configuration reference, deployment layout,
  monitoring, error codes.
* [Design](design.md): how transactions, recovery and the record lifecycle work, and why they are correct.
* [Benchmarks](../bench/RESULTS.md): sysbench against MariaDB and MySQL.
