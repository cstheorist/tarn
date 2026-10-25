# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-25T09:58:56Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              AMD EPYC 7763 64-Core Processor
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
               total        used        free      shared  buff/cache   available
Mem:           15989        1047       10923          47        4412       14941
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=100000 THREADS='1 16 64' TIME=15 MANY=1000
mariadb: 10.11.19-MariaDB-ubu2204
tarn: 5d1c366 on Tarantool 3.9.0-0-g8b9a0f5
tidb: 8.0.11-TiDB-v8.5.8
yugabytedb: PostgreSQL 15.12-YB-2026.1.2.0-b0 on x86_64-pc-linux-gnu, compiled by clang version 21.1.1 (https://github.com/yugabyte/llvm-project.git efca861cc42178cc4c555d605b36c79d7d121cc1), 64-bit
mariadb: 10.11.19-MariaDB-ubu2204
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 10,411 | 47,771 | 46,876 | 0.11 | 0.56 | 2.14 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 5,184 | 28,343 | 44,090 | 0.22 | 0.95 | 2.52 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 5,215 | 30,712 | 46,456 | 0.22 | 0.87 | 2.43 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 5,135 | 29,062 | 43,024 | 0.22 | 0.92 | 2.66 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 2,042 | 9,337 | 11,704 | 0.55 | 3.19 | 9.56 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 2,090 | 9,029 | 10,627 | 0.54 | 3.30 | 10.46 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 2,063 | 8,492 | 9,761 | 0.55 | 3.68 | 11.87 | 0 |
| YugabyteDB, 1 node (default isolation) | – | – | – | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | – | – | – | – | – | – | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 531 | 2,078 | 2,023 | 2.11 | 9.91 | 44.98 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 253 | 836 | 1,018 | 4.49 | 28.16 | 80.03 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 268 | 920 | 1,129 | 4.25 | 30.26 | 81.48 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 253 | 837 | 1,010 | 4.41 | 27.66 | 80.03 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 118 | 447 | 513 | 8.90 | 44.17 | 155.80 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 81 | 368 | 437 | 12.98 | 51.94 | 176.73 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 105 | 397 | 435 | 10.09 | 51.02 | 186.54 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 438 | 1,664 | 1,600 | 2.57 | 12.98 | 54.83 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 186 | 519 | 530 | 6.09 | 41.10 | 215.44 | 918 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 174 | 585 | 701 | 6.55 | 33.72 | 110.66 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 147 | 346 | 385 | 7.43 | 57.87 | 297.92 | 812 |
| TiDB, 1 TiKV (snapshot isolation) | 79 | 280 | 334 | 12.52 | 75.82 | 235.74 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 55 | 243 | 294 | 18.61 | 78.60 | 267.41 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 60 | 207 | 262 | 18.95 | 95.81 | 308.84 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 8,604 | 31,134 | 32,902 | 0.13 | 1.27 | 5.28 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 3,499 | 17,155 | 23,734 | 0.32 | 1.64 | 5.57 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 3,627 | 18,745 | 26,719 | 0.31 | 1.44 | 4.91 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 1,463 | 6,549 | 12,678 | 0.78 | 3.75 | 8.74 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 640 | 2,249 | 3,187 | 1.76 | 10.46 | 28.67 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 634 | 2,300 | 3,219 | 1.79 | 10.46 | 29.19 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 455 | 1,513 | 2,306 | 2.57 | 15.83 | 41.10 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 9,095 | 37,964 | 37,378 | 0.13 | 0.90 | 4.41 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 3,598 | 17,828 | 25,667 | 0.31 | 1.55 | 5.09 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 3,693 | 19,109 | 27,750 | 0.31 | 1.44 | 4.65 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 1,483 | 6,686 | 13,069 | 0.78 | 3.68 | 8.43 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 777 | 3,654 | 5,280 | 1.37 | 6.55 | 17.95 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 740 | 3,512 | 4,946 | 1.44 | 6.91 | 19.29 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 622 | 2,639 | 3,924 | 1.73 | 9.06 | 24.38 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 9,189 | 34,875 | 31,959 | 0.13 | 1.06 | 6.79 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 3,461 | 16,871 | 23,365 | 0.32 | 1.61 | 5.28 | 0 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 3,528 | 17,920 | 25,096 | 0.32 | 1.50 | 4.82 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 1,456 | 6,500 | 12,505 | 0.78 | 3.82 | 8.74 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 805 | 2,799 | 3,844 | 1.44 | 8.58 | 25.28 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 822 | 2,928 | 3,944 | 1.42 | 8.43 | 24.38 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 554 | 1,847 | 2,599 | 2.22 | 12.98 | 37.56 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## oltp_write_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 1,917 | 7,579 | 7,687 | 0.61 | 3.43 | 16.12 | 0 |
| Tarn, 2 shards x 1 (strict serializable) | 651 | 1,835 | 2,126 | 1.89 | 14.21 | 44.98 | 34 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 491 | 1,620 | 1,970 | 2.48 | 14.21 | 49.21 | 0 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 384 | 1,053 | 1,383 | 3.25 | 21.11 | 65.65 | 17 |
| TiDB, 1 TiKV (snapshot isolation) | 267 | 872 | 1,108 | 4.10 | 24.83 | 75.82 | 0 |
| TiDB, 1 TiKV (READ COMMITTED) | 222 | 799 | 1,037 | 4.91 | 26.20 | 78.60 | 0 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 205 | 634 | 804 | 5.67 | 34.33 | 108.68 | 0 |
| YugabyteDB, 1 node (default isolation) | 0 | 0 | 0 | – | – | – | 0 |
| YugabyteDB, 1 node (SERIALIZABLE) | 0 | 0 | 0 | – | – | – | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 400,000 | 1.4 | 281,198 |
| Tarn, 2 shards x 1 (strict serializable) | 400,000 | 13.2 | 30,240 |
| Tarn, 2 shards x 1 (READ COMMITTED) | 400,000 | 13.9 | 28,763 |
| Tarn, 2 shards x 3 sync replicas (strict serializable) | 400,000 | 20.3 | 19,743 |
| TiDB, 1 TiKV (snapshot isolation) | 400,000 | 17.8 | 22,526 |
| TiDB, 1 TiKV (READ COMMITTED) | 400,000 | 13.3 | 30,087 |
| TiDB, 3 TiKV, 3 replicas (snapshot isolation) | 400,000 | 23.9 | 16,754 |
| YugabyteDB, 1 node (default isolation) | 400,000 | 27.8 | 14,367 |
| YugabyteDB, 1 node (SERIALIZABLE) | 400,000 | 27.7 | 14,417 |

## Many tables

| System | tables created | DDL seconds | first 100 | last 100 | MB per 1000 tables | load s | 1 thr QPS | 16 thr QPS | 16 thr p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 1000 | 9.5 | 0.2 | 1.7 | 88 | 1.3 | 5,218 | 29,776 | 0.92 |
| TiDB, 1 TiKV (snapshot isolation) | 1000 | 29.5 | 2.7 | 2.9 | 476 | 1.6 | 1,950 | 7,873 | 3.82 |
| YugabyteDB, 1 node (default isolation) | 400 | 249.2 | 76.4 | 52.7 | 1,120 | 3.1 | 2,343 | 1,540 | 38.25 |
| YugabyteDB, 1 node, colocated database | 1000 | 493.0 | 66.1 | 51.4 | -153 | 7.0 | 1,779 | 698 | 38.94 |
| MariaDB, 1 node (REPEATABLE READ) | 1000 | 0.7 | 0.0 | 0.1 | 8 | 0.3 | 9,414 | 37,793 | 0.77 |

## Configurations

| Configuration | status | seconds |
|---|---|---:|
| mariadb | ok | 327 |
| tarn_2x1 | ok | 330 |
| tarn_2x1_rc | ok | 331 |
| tarn_2x3 | ok | 337 |
| tidb_rf1 | ok | 377 |
| tidb_rf1_rc | ok | 355 |
| tidb_rf3 | ok | 363 |
| yb_rf1 | ok | 80 |
| yb_rf1_ser | ok | 51 |
| yb_rf3 | FAILED | 4 |
| many_tarn | ok | 47 |
| many_tidb | ok | 81 |
| many_yb | ok | 294 |
| many_yb_colocated | ok | 545 |
| many_mariadb | ok | 41 |
