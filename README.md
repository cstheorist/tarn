# Tarn

A distributed, MySQL-compatible SQL server on top of Tarantool shards.

* **MySQL protocol** — the `mysql`/`mariadb` CLI, sysbench, PyMySQL and other
  MySQL clients connect unchanged (text and binary protocol, prepared
  statements).
* **Tarantool storage** — every SQL table is a plain Tarantool space
  (`"db.table"`), sharded by a hash of the shard key with vshard's bucket
  function. Committed rows can be read directly as Key/Value
  (`box.space['db.t'].index.primary:get{42}`) without going through SQL.
* **Strictly serializable transactions** across shards: lock-aware reads,
  MVCC-validated local commits, and a parallel-commit two-phase commit with
  recovery, specified in TLA+ and model-checked with TLC.
* **Verus-verified core pieces**: shard routing, coordinator decisions and
  recovery steps, and the MySQL wire codec (length-encoded values, packet
  framing) are proven and compiled into the server.

See [DESIGN.md](DESIGN.md) for the architecture, the protocol and its
verification, and [bench/RESULTS.md](bench/RESULTS.md) for benchmarks.

## Layout

```
crates/tarn-server    the tarn-server binary
crates/tarn-mysql     MySQL wire protocol (server side)
crates/tarn-sql       SQL parsing/planning/execution, transactions, 2PC coordinator
crates/tarn-iproto    async pipelined Tarantool IPROTO client
crates/tarn-verified  Verus-verified routing, coordinator logic, wire codec
crates/tarn-common    shared value/column/error types
lua/                  storage-node module (tarn.lua) and instance bootstrap (storage.lua)
tla/                  TarnCommit.tla, model configs, TLC results (tla/out)
tests/                SQL differential tests, transaction tests, failover test
bench/                sysbench harness, baselines, results
scripts/cluster.sh    start/stop a local cluster
```

## Build

Requirements: Rust 1.98.1 (pinned in `rust-toolchain.toml` to the compiler of
the Verus release used for the proofs; rustup installs it on the first build),
Tarantool 3.x (developed with 3.9.0), Python 3 with PyMySQL for the SQL tests,
sysbench for benchmarks, Java 11+ and `tla2tools.jar` for TLC, Verus
0.2026.10.04 for the proofs.

```sh
export TARANTOOL=$(which tarantool)   # scripts and tests default to /opt/src/tarantool/build/src/tarantool
cargo build --release -p tarn-server
cargo test -p tarn-verified -p tarn-mysql -p tarn-sql -p tarn-iproto   # iproto tests start their own Tarantool
```

## Run a local cluster

```sh
# 2 shards x 1 replica, SQL on 127.0.0.1:4000 (data under /tmp/tarn-cluster)
scripts/cluster.sh start -n 2 -r 1 -p 4000
# HA: 2 shards x 3 synchronous replicas with automatic leader election
scripts/cluster.sh start -n 2 -r 3 -p 4000

mysql -h 127.0.0.1 -P 4000 -u root -e "
  CREATE DATABASE demo;
  CREATE TABLE demo.orders (id BIGINT PRIMARY KEY AUTO_INCREMENT,
                            customer INT NOT NULL, total DECIMAL(10,2), KEY c (customer));
  INSERT INTO demo.orders (customer, total) VALUES (7, 19.99), (8, 5.00);
  SELECT * FROM demo.orders WHERE id = 1;"

scripts/cluster.sh stop
```

Options of `scripts/cluster.sh start`: `-n` shards, `-r` replicas per shard,
`-p` SQL port, `-s` number of SQL servers, `-m` memtx memory (MB), `-w` WAL
mode, `-a` ack-on-prepare, `-u` connect SQL servers to storage over Unix
sockets, `-d` data directory, `-b` base port (storage instance *(i, j)*
listens on `base + 10·i + j`).

### Direct Key/Value reads

```lua
-- any Tarantool client; the row lives on the shard that owns its bucket
local c = require('net.box').connect('tarn:tarn@127.0.0.1:3311')
c.space['demo.orders'].index.primary:get{1}
-- -> [1, 7, 19.99, <bucket_id>]   (columns in table order, then bucket_id)
```

`crates/tarn-sql/examples/kvbench.rs` routes keys to shards with the same
(Verus-verified) bucket function the SQL layer uses.

### tarn-server

```
tarn-server (--config cluster.json | --shard ADDR[,ADDR..] ...)
            [--listen HOST:PORT] [--threads N] [--user NAME:PASSWORD]...
            [--allow-all] [--ack-on-prepare] [--pool N] [--node-id N]
```

`cluster.json`:

```json
{ "shards": [ {"id": 1, "replicas": ["127.0.0.1:3311", "127.0.0.1:3312", "127.0.0.1:3313"]},
              {"id": 2, "replicas": ["127.0.0.1:3321", "127.0.0.1:3322", "127.0.0.1:3323"]} ],
  "bucket_count": 3000, "user": "tarn", "password": "tarn", "pool_size": 4,
  "ack_on_prepare": false, "max_txn_age": 120 }
```

Replica addresses may be `unix/:/path/to/socket`. The writable replica of
each shard is discovered automatically and re-discovered after failover
(including a demoted leader whose connection stays open). `max_txn_age`
(seconds) must match the storage nodes' setting (`lua/tarn.lua`, default
120). Without `--user`, any MySQL user/password is accepted.

## Verification and tests

```sh
# TLA+: model-check one configuration (or several)
tla/run_all.sh Recov Frac_GC_ROValidate Bug_GCIgnoresDecision
python3 tla/summarize.py                       # table of all results in tla/out

# Verus (from crates/tarn-verified)
verus --crate-type=lib src/lib.rs              # 59 verified, 0 errors

# Storage protocol tests (starts two shards)
lua/test/run.sh

# SQL tests against a running cluster (and MariaDB for the differential test)
python3 tests/sql/difftest.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407
python3 tests/sql/fuzzdiff.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407 -n 2000
python3 tests/sql/txntest.py  --tarn 127.0.0.1:4000 --kv 127.0.0.1:3311,127.0.0.1:3321
tests/failover.sh                               # moves and kills shard leaders under load
tests/faults.sh                                 # lost reply to an autocommit write
```

## Benchmarks

`bench/run_matrix.sh` runs sysbench's OLTP workloads against MariaDB, MySQL
and Tarn configurations on one machine; results land in `bench/results.csv`
and are summarized in [bench/RESULTS.md](bench/RESULTS.md).
