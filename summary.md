# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-25T06:08:08Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              AMD EPYC 9V74 80-Core Processor
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
               total        used        free      shared  buff/cache   available
Mem:           15989        1115        9704          48        5565       14874
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=100000 THREADS='1 16 64' TIME=15 MANY=1000
mariadb: 10.11.19-MariaDB-ubu2204
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 19,508 | 64,658 | 62,525 | 0.06 | 0.38 | 1.61 | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 873 | 3,203 | 3,050 | 1.27 | 7.04 | 29.72 | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| MariaDB, 1 node (REPEATABLE READ) | 400,000 | 1.1 | 377,369 |

