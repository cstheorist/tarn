# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-25T17:48:17Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              AMD EPYC 9V74 80-Core Processor
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
               total        used        free      shared  buff/cache   available
Mem:           15989        1063       10854          48        4467       14926
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=1000000 THREADS='1 16 64' TIME=30 PERCENTILE=99 WARM=1 MANY=1000
data: about 957 MB uncompressed; caches of the 10x configurations: 95 MB in all
tarn: b203650 on Tarantool 3.9.0-0-g8b9a0f5
tidb: 8.0.11-TiDB-v8.5.8
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 8,615 | 42,183 | 58,010 | 0.23 | 0.86 | 2.86 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 8,234 | 38,776 | 51,657 | 0.19 | 0.95 | 3.43 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 5,601 | 26,400 | 37,180 | 0.26 | 1.37 | 4.57 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 2,836 | 11,968 | 16,402 | 0.44 | 4.49 | 8.90 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 2,537 | 9,690 | 12,704 | 0.48 | 4.82 | 11.24 | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 363 | 1,072 | 1,223 | 6.43 | 31.37 | 77.19 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 345 | 914 | 1,013 | 6.21 | 34.95 | 90.78 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 225 | 597 | 631 | 6.55 | 44.17 | 134.90 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 149 | 604 | 665 | 7.98 | 39.65 | 139.85 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 134 | 521 | 551 | 8.28 | 44.17 | 164.45 | 0 |

## oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 5,205 | 21,883 | 27,216 | 0.28 | 2.00 | 7.70 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 5,188 | 19,203 | 21,768 | 0.28 | 2.61 | 10.46 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 4,099 | 15,116 | 20,120 | 0.34 | 4.74 | 9.73 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 821 | 2,927 | 3,954 | 1.67 | 10.27 | 29.19 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 714 | 2,399 | 3,249 | 2.00 | 12.08 | 34.33 | 0 |

## oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 5,450 | 23,512 | 30,416 | 0.27 | 1.93 | 7.04 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 5,083 | 21,385 | 25,214 | 0.29 | 1.96 | 8.58 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 4,253 | 19,336 | 23,917 | 0.32 | 1.93 | 7.70 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 995 | 4,540 | 6,195 | 1.25 | 7.04 | 20.00 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 728 | 4,054 | 5,487 | 1.93 | 7.43 | 21.11 | 0 |

## oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 5,236 | 22,945 | 31,220 | 0.27 | 1.70 | 5.99 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 4,940 | 20,963 | 22,992 | 0.31 | 1.89 | 9.56 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 4,949 | 17,740 | 24,692 | 0.31 | 3.55 | 7.84 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 911 | 3,206 | 4,616 | 1.67 | 9.39 | 25.74 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 886 | 2,595 | 4,242 | 1.64 | 12.08 | 27.66 | 0 |

## oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 267 | 673 | 791 | 6.21 | 38.94 | 147.61 | 292 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 192 | 500 | 578 | 8.74 | 50.11 | 248.83 | 212 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 168 | 394 | 415 | 10.09 | 56.84 | 277.21 | 140 |
| TiDB, 1 TiKV (snapshot isolation) | 93 | 338 | 404 | 12.52 | 71.83 | 240.02 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 82 | 315 | 352 | 13.95 | 73.13 | 253.35 | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 4,000,000 | 125.4 | 31,896 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 4,000,000 | 133.1 | 30,063 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 4,000,000 | 127.7 | 31,334 |
| TiDB, 1 TiKV (snapshot isolation) | 4,000,000 | 69.9 | 57,228 |
| TiDB, 1 TiKV, data 10x the block cache | 4,000,000 | 67.1 | 59,568 |

## Vinyl sizes (MB, per storage node)

after `.load`: loaded and checkpointed; after `.end`: after the workloads

| When | node | data | indexes | cache limit | cache used | in-memory level | page index | bloom |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| tarn_2x1_vinyl.load | 127.0.0.1:3311 | 402.0 | 99.6 | 1213.0 | 556.2 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl.load | 127.0.0.1:3321 | 402.5 | 100.1 | 1213.0 | 556.7 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl.end | 127.0.0.1:3311 | 491.1 | 123.0 | 1213.0 | 547.3 | 106.0 | 3.8 | 2.8 |
| tarn_2x1_vinyl.end | 127.0.0.1:3321 | 561.2 | 137.5 | 1213.0 | 547.8 | 10.0 | 4.4 | 3.3 |
| tarn_2x1_vinyl_10x.load | 127.0.0.1:3311 | 402.0 | 99.6 | 47.0 | 47.0 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl_10x.load | 127.0.0.1:3321 | 402.5 | 100.2 | 47.0 | 47.0 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl_10x.end | 127.0.0.1:3311 | 514.1 | 127.8 | 47.0 | 47.0 | 63.8 | 4.0 | 3.0 |
| tarn_2x1_vinyl_10x.end | 127.0.0.1:3321 | 522.9 | 130.5 | 47.0 | 47.0 | 47.3 | 4.1 | 3.1 |

## Configurations

| Configuration | status | seconds |
|---|---|---:|
| tarn_2x1 | ok | 674 |
| tarn_2x1_vinyl | ok | 684 |
| tarn_2x1_vinyl_10x | ok | 685 |
| tidb_rf1 | ok | 644 |
| tidb_rf1_10x | ok | 633 |
