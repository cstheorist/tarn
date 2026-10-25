# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-25T06:14:12Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              INTEL(R) XEON(R) PLATINUM 8573C
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
CPU(s) scaling MHz:                      156%
               total        used        free      shared  buff/cache   available
Mem:           15988        1127        9688          48        5568       14861
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=100000 THREADS='1 16 64' TIME=15 MANY=1000
mariadb: 10.11.19-MariaDB-ubu2204
tarn: 61ceb63 on Tarantool 3.9.0-0-g8b9a0f5
tidb: 8.0.11-TiDB-v8.5.8
mariadb: 10.11.19-MariaDB-ubu2204
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 21,898 | 126,017 | 126,765 | 0.06 | 0.23 | 0.81 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 8,365 | 55,418 | 75,636 | 0.15 | 0.50 | 1.55 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 8,459 | 58,323 | 86,608 | 0.15 | 0.48 | 1.34 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 2,472 | 14,008 | 19,505 | 0.49 | 2.22 | 5.88 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 2,383 | 12,681 | 16,485 | 0.50 | 2.52 | 7.17 | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 981 | 4,477 | 4,440 | 1.25 | 5.09 | 24.38 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 354 | 1,304 | 1,585 | 3.62 | 19.65 | 53.85 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 359 | 1,317 | 1,625 | 3.49 | 20.00 | 51.94 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 127 | 612 | 713 | 8.43 | 33.12 | 114.72 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 123 | 540 | 617 | 8.74 | 37.56 | 130.13 | 0 |

## oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 798 | 3,579 | 3,365 | 1.61 | 6.43 | 30.81 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 253 | 840 | 875 | 4.65 | 23.10 | 132.49 | 1,386 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 231 | 541 | 606 | 5.09 | 38.94 | 189.93 | 1,276 |
| TiDB, 1 TiKV (snapshot isolation) | 94 | 381 | 459 | 11.24 | 51.94 | 173.58 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 78 | 281 | 341 | 13.95 | 73.13 | 235.74 | 0 |

## oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 19,330 | 81,720 | 75,730 | 0.07 | 0.47 | 2.43 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 5,623 | 32,961 | 44,102 | 0.22 | 0.87 | 2.91 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,458 | 12,605 | 22,525 | 0.48 | 2.00 | 4.91 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 757 | 3,071 | 4,800 | 1.50 | 7.84 | 19.65 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 525 | 1,854 | 2,900 | 2.14 | 13.22 | 32.53 | 0 |

## oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 17,451 | 94,482 | 90,502 | 0.09 | 0.39 | 1.89 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 5,466 | 32,315 | 45,171 | 0.22 | 0.92 | 2.86 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,508 | 12,700 | 23,031 | 0.47 | 2.00 | 5.00 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 813 | 4,575 | 7,785 | 1.37 | 5.28 | 12.52 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 689 | 3,209 | 5,392 | 1.58 | 7.43 | 17.95 | 0 |

## oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 19,016 | 74,722 | 53,255 | 0.07 | 0.62 | 5.28 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 5,490 | 30,453 | 39,938 | 0.22 | 0.94 | 3.19 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,439 | 12,332 | 21,932 | 0.48 | 2.03 | 4.91 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 910 | 3,399 | 5,141 | 1.23 | 7.04 | 18.61 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 630 | 2,200 | 3,444 | 1.86 | 11.24 | 28.16 | 0 |

## oltp_write_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 3,795 | 18,422 | 17,167 | 0.35 | 1.76 | 8.58 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 889 | 2,896 | 3,719 | 1.50 | 8.58 | 25.28 | 50 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 607 | 1,722 | 2,307 | 2.14 | 13.22 | 39.65 | 36 |
| TiDB, 1 TiKV (snapshot isolation) | 315 | 1,201 | 1,591 | 3.49 | 17.95 | 53.85 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 241 | 798 | 1,094 | 4.74 | 27.66 | 78.60 | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 400,000 | 1.1 | 365,501 |
| Tarn, 2 shards x 1 (strict serializable) | 400,000 | 8.5 | 46,996 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 400,000 | 15.1 | 26,453 |
| TiDB, 1 TiKV (snapshot isolation) | 400,000 | 16.5 | 24,173 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 400,000 | 23.8 | 16,785 |

## Many tables

| System | tables created | DDL seconds | first 100 | last 100 | MB per 1000 tables | load s | 1 thr QPS | 16 thr QPS | 16 thr p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 1000 | 6.3 | 0.2 | 1.1 | 82 | 1.0 | 8,507 | 59,958 | 0.46 |
| TiDB, 1 TiKV (snapshot isolation) | 1000 | 23.0 | 2.2 | 2.3 | 472 | 1.6 | 2,228 | 11,941 | 2.57 |
| MariaDB, 1 node (REPEATABLE READ) | 1000 | 0.4 | 0.0 | 0.0 | 8 | 0.2 | 20,402 | 100,023 | 0.30 |

## Configurations

| Configuration | status | seconds |
|---|---|---:|
| mariadb | ok | 327 |
| tarn_2x1 | ok | 325 |
| tarn_2x3 | ok | 332 |
| tidb_rf1 | ok | 371 |
| tidb_rf3 | ok | 364 |
| yb_rf1 | FAILED | 635 |
| yb_rf1_ser | FAILED | 614 |
| yb_rf3 | FAILED | 614 |
| many_tarn | ok | 43 |
| many_tidb | ok | 73 |
| many_yb | FAILED | 615 |
| many_yb_colocated | FAILED | 615 |
| many_mariadb | ok | 41 |
