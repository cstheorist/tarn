# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-25T11:19:20Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              AMD EPYC 9V45 96-Core Processor
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
               total        used        free      shared  buff/cache   available
Mem:           15989        1108       10813          48        4461       14880
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=100000 THREADS='1 16 64' TIME=15 MANY=1000
mariadb: 10.11.19-MariaDB-ubu2204
tarn: 25619b8 on Tarantool 3.9.0-0-g8b9a0f5
tidb: 8.0.11-TiDB-v8.5.8
yugabytedb: PostgreSQL 15.12-YB-2026.1.2.0-b0 on x86_64-pc-linux-gnu, compiled by clang version 21.1.1 (https://github.com/yugabyte/llvm-project.git efca861cc42178cc4c555d605b36c79d7d121cc1), 64-bit
mariadb: 10.11.19-MariaDB-ubu2204
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 29,061 | 121,513 | 125,676 | 0.04 | 0.23 | 0.83 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 11,025 | 54,492 | 79,322 | 0.11 | 0.54 | 1.55 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 11,663 | 57,576 | 83,699 | 0.10 | 0.50 | 1.52 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 11,183 | 54,408 | 76,621 | 0.10 | 0.54 | 1.67 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 4,231 | 21,526 | 27,044 | 0.29 | 1.50 | 4.57 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 4,659 | 23,856 | 29,411 | 0.26 | 1.27 | 4.03 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 4,066 | 19,894 | 24,349 | 0.30 | 1.76 | 5.18 | 0 |
| YugabyteDB, 1 node (default isolation) | 9,024 | 31,157 | 16,517 | 0.13 | 0.81 | 6.91 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 2,121 | 5,492 | 4,537 | 0.52 | 4.65 | 23.52 | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 1,340 | 5,020 | 4,619 | 0.83 | 5.18 | 27.17 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 482 | 1,454 | 1,665 | 2.81 | 18.95 | 54.83 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 532 | 1,642 | 1,990 | 2.71 | 19.65 | 49.21 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 486 | 1,434 | 1,621 | 2.71 | 19.29 | 54.83 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 203 | 831 | 963 | 5.37 | 25.28 | 90.78 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 153 | 764 | 941 | 7.17 | 25.28 | 87.56 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 197 | 782 | 863 | 5.67 | 28.16 | 99.33 | 0 |
| YugabyteDB, 1 node (default isolation) | 8 | 21 | 18 | 139.85 | 877.61 | 4,943.53 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 7 | 18 | 16 | 161.51 | 1,069.86 | 5,709.50 | 0 |

## oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 1,062 | 4,006 | 3,646 | 1.03 | 5.99 | 28.16 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 332 | 856 | 847 | 3.96 | 27.66 | 134.90 | 1,419 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 327 | 1,049 | 1,194 | 3.75 | 19.65 | 66.84 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 254 | 535 | 577 | 4.82 | 40.37 | 200.47 | 1,118 |
| TiDB, 1 TiKV (snapshot isolation) | 142 | 461 | 578 | 7.70 | 48.34 | 147.61 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 108 | 397 | 520 | 10.27 | 55.82 | 158.63 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 108 | 376 | 434 | 10.65 | 56.84 | 193.38 | 0 |
| YugabyteDB, 1 node (default isolation) | 7 | 16 | 14 | 144.97 | 1,304.21 | 6,135.91 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 7 | 0 | 0 | 164.45 | – | – | 0 |

## oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 23,786 | 87,071 | 85,041 | 0.05 | 0.46 | 2.11 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 7,413 | 36,638 | 47,586 | 0.16 | 0.84 | 2.97 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 7,655 | 37,084 | 46,911 | 0.16 | 0.86 | 3.13 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,592 | 11,402 | 20,601 | 0.46 | 2.22 | 5.88 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,036 | 4,548 | 6,244 | 1.10 | 5.57 | 15.83 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 964 | 4,066 | 5,521 | 1.27 | 6.21 | 17.63 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 707 | 2,461 | 3,531 | 1.76 | 10.46 | 28.16 | 0 |
| YugabyteDB, 1 node (default isolation) | 1,659 | 3,526 | 2,791 | 0.67 | 6.79 | 49.21 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 21,945 | 92,165 | 99,790 | 0.06 | 0.42 | 1.64 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 7,788 | 37,595 | 49,152 | 0.16 | 0.83 | 2.86 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 7,665 | 37,894 | 49,775 | 0.15 | 0.83 | 2.81 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,656 | 11,740 | 21,374 | 0.45 | 2.18 | 5.67 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,207 | 6,480 | 9,598 | 0.89 | 4.03 | 11.04 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 1,203 | 6,490 | 10,173 | 0.90 | 3.89 | 9.73 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 961 | 4,414 | 6,823 | 1.16 | 5.77 | 15.00 | 0 |
| YugabyteDB, 1 node (default isolation) | 4,563 | 15,807 | 11,054 | 0.27 | 1.58 | 11.24 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 23,380 | 78,847 | 63,271 | 0.05 | 0.53 | 4.10 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 7,610 | 36,017 | 45,487 | 0.16 | 0.84 | 2.97 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 7,709 | 35,733 | 45,236 | 0.15 | 0.84 | 2.97 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 2,612 | 11,622 | 20,564 | 0.45 | 2.18 | 5.67 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,203 | 4,856 | 6,836 | 0.99 | 5.37 | 14.73 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 1,214 | 4,832 | 6,749 | 0.94 | 5.28 | 14.46 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 844 | 2,875 | 4,367 | 1.44 | 9.22 | 23.52 | 0 |
| YugabyteDB, 1 node (default isolation) | 1,651 | 3,709 | 2,823 | 0.94 | 6.67 | 41.10 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_write_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 4,705 | 18,156 | 18,354 | 0.24 | 1.67 | 7.04 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 1,269 | 3,481 | 3,975 | 1.04 | 7.30 | 25.74 | 59 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 947 | 2,880 | 3,359 | 1.39 | 8.90 | 30.81 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 638 | 1,704 | 2,210 | 2.07 | 13.70 | 42.61 | 33 |
| TiDB, 1 TiKV (snapshot isolation) | 443 | 1,676 | 2,043 | 2.52 | 13.95 | 43.39 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 371 | 1,522 | 1,922 | 2.97 | 14.21 | 45.79 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 325 | 1,055 | 1,327 | 3.62 | 21.89 | 68.05 | 0 |
| YugabyteDB, 1 node (default isolation) | 463 | 1,314 | 986 | 2.97 | 17.95 | 114.72 | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 400,000 | 0.9 | 440,882 |
| Tarn, 2 shards x 1 (strict serializable) | 400,000 | 9.6 | 41,874 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 400,000 | 10.3 | 38,963 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 400,000 | 14.5 | 27,609 |
| TiDB, 1 TiKV (snapshot isolation) | 400,000 | 12.0 | 33,361 |
| TiDB, 1 TiKV (READ COMMITTED) | 400,000 | 10.5 | 37,962 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 400,000 | 18.9 | 21,166 |
| YugabyteDB, 1 node (default isolation) | 400,000 | 37.2 | 10,746 |
| YugabyteDB, 1 node (SERIALIZABLE) | 400,000 | 32.5 | 12,312 |

## Many tables

| System | tables created | DDL seconds | first 100 | last 100 | MB per 1000 tables | load s | 1 thr QPS | 16 thr QPS | 16 thr p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 1000 | 9.0 | 0.2 | 1.6 | 84 | 0.9 | 10,354 | 50,658 | 0.58 |
| TiDB, 1 TiKV (snapshot isolation) | 1000 | 24.1 | 2.3 | 2.6 | 472 | 1.2 | 3,243 | 15,118 | 2.14 |
| MariaDB, 1 node (REPEATABLE READ) | 1000 | 0.5 | 0.1 | 0.0 | 9 | 0.2 | 25,504 | 99,584 | 0.28 |

## Configurations

| Configuration | status | seconds |
|---|---|---:|
| mariadb | ok | 326 |
| tarn_2x1 | ok | 326 |
| tarn_2x1_rc | ok | 327 |
| tarn_2x3 | ok | 332 |
| tidb_rf1 | ok | 364 |
| tidb_rf1_rc | ok | 346 |
| tidb_rf3 | ok | 355 |
| yb_rf1 | ok | 391 |
| yb_rf1_ser | ok | 5700 |
| yb_rf3 | FAILED | 15 |
| many_tarn | ok | 45 |
| many_tidb | ok | 76 |
| many_yb | FAILED | 415 |
| many_yb_colocated | FAILED | 416 |
| many_mariadb | ok | 43 |
