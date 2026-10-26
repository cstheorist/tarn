# Tarn vs TiDB vs YugabyteDB: same-host results

```
date: 2026-10-26T14:25:35Z
kernel: 6.17.0-1022-azure
CPU(s):                                  4
Model name:                              AMD EPYC 9V45 96-Core Processor
Thread(s) per core:                      2
Core(s) per socket:                      2
Socket(s):                               1
               total        used        free      shared  buff/cache   available
Mem:           15989        1077       10828          47        4476       14911
sysbench: sysbench 1.0.20
settings: TABLES=4 SIZE=1000000 THREADS='1 16 64' TIME=30 PERCENTILE=99 WARM=1 MANY=1000
data: about 957 MB uncompressed; caches of the 10x configurations: 95 MB in all
tarn: 27022f0 on Tarantool 3.9.0-0-g8b9a0f5
tidb: 8.0.11-TiDB-v8.5.8
```

## oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 11,833 | 61,637 | 82,426 | 0.18 | 0.61 | 2.11 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 11,294 | 55,798 | 75,701 | 0.14 | 0.74 | 2.43 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 7,979 | 38,281 | 46,940 | 0.21 | 1.08 | 3.82 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 4,449 | 21,859 | 27,723 | 0.31 | 2.35 | 5.77 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 3,923 | 19,490 | 20,421 | 0.35 | 2.26 | 9.06 | 0 |

## oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 498 | 1,411 | 1,672 | 4.10 | 24.83 | 58.92 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 487 | 1,353 | 1,522 | 4.18 | 22.69 | 63.32 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 300 | 849 | 912 | 6.21 | 34.33 | 101.13 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 202 | 843 | 929 | 5.99 | 31.37 | 106.75 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 189 | 751 | 823 | 5.99 | 33.12 | 116.80 | 0 |

## oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 7,391 | 31,153 | 39,650 | 0.21 | 1.52 | 5.67 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 7,531 | 30,424 | 31,592 | 0.20 | 1.52 | 7.98 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 5,927 | 25,206 | 28,916 | 0.25 | 1.73 | 8.90 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,028 | 4,142 | 5,701 | 1.47 | 8.28 | 21.11 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 965 | 3,830 | 5,047 | 1.39 | 8.28 | 23.95 | 0 |

## oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 7,708 | 35,031 | 45,414 | 0.20 | 1.32 | 4.91 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 7,115 | 31,466 | 38,791 | 0.21 | 1.44 | 5.57 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 6,122 | 29,258 | 36,038 | 0.23 | 1.37 | 6.32 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,275 | 6,137 | 10,204 | 0.97 | 6.21 | 12.98 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 1,237 | 6,298 | 8,738 | 0.97 | 5.28 | 14.73 | 0 |

## oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 7,860 | 35,224 | 45,025 | 0.18 | 1.12 | 4.03 | 0 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 7,403 | 30,255 | 37,063 | 0.22 | 1.64 | 6.09 | 0 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 7,532 | 31,725 | 33,809 | 0.21 | 1.44 | 8.28 | 0 |
| TiDB, 1 TiKV (snapshot isolation) | 1,306 | 5,045 | 7,259 | 1.12 | 6.79 | 18.61 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 1,263 | 4,659 | 6,943 | 1.14 | 7.56 | 18.28 | 0 |

## oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p99 ms | 16 thr p99 ms | 64 thr p99 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 345 | 914 | 948 | 5.37 | 25.74 | 121.08 | 313 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 252 | 720 | 776 | 8.90 | 33.72 | 142.39 | 248 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 230 | 590 | 612 | 10.09 | 41.10 | 176.73 | 187 |
| TiDB, 1 TiKV (snapshot isolation) | 147 | 540 | 599 | 8.74 | 47.47 | 158.63 | 0 |
| TiDB, 1 TiKV, data 10x the block cache | 131 | 489 | 564 | 12.98 | 49.21 | 167.44 | 0 |

## Data load (sysbench prepare)

| System | rows | seconds | rows/s |
|---|---:|---:|---:|
| Tarn, 2 shards x 1 (strict serializable) | 4,000,000 | 93.1 | 42,953 |
| Tarn, 2 shards x 1, vinyl, cache larger than the data | 4,000,000 | 109.5 | 36,527 |
| Tarn, 2 shards x 1, vinyl, data 10x the cache | 4,000,000 | 112.4 | 35,598 |
| TiDB, 1 TiKV (snapshot isolation) | 4,000,000 | 61.0 | 65,612 |
| TiDB, 1 TiKV, data 10x the block cache | 4,000,000 | 48.7 | 82,084 |

## Vinyl sizes (MB, per storage node)

after `.load`: loaded and checkpointed; after `.end`: after the workloads

| When | node | data | indexes | cache limit | cache used | in-memory level | page index | bloom |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| tarn_2x1_vinyl.load | 127.0.0.1:3311 | 402.0 | 99.6 | 1213.0 | 556.2 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl.load | 127.0.0.1:3321 | 402.5 | 100.1 | 1213.0 | 556.7 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl.end | 127.0.0.1:3311 | 594.2 | 147.9 | 1213.0 | 550.9 | 75.2 | 4.6 | 3.6 |
| tarn_2x1_vinyl.end | 127.0.0.1:3321 | 583.7 | 145.9 | 1213.0 | 551.5 | 91.3 | 4.5 | 3.6 |
| tarn_2x1_vinyl_10x.load | 127.0.0.1:3311 | 402.0 | 99.6 | 47.0 | 47.0 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl_10x.load | 127.0.0.1:3321 | 402.5 | 100.1 | 47.0 | 47.0 | 0.0 | 3.1 | 2.1 |
| tarn_2x1_vinyl_10x.end | 127.0.0.1:3311 | 616.9 | 153.7 | 47.0 | 47.0 | 28.2 | 4.8 | 3.8 |
| tarn_2x1_vinyl_10x.end | 127.0.0.1:3321 | 617.6 | 154.4 | 47.0 | 47.0 | 29.4 | 4.8 | 3.8 |

## Configurations

| Configuration | status | seconds |
|---|---|---:|
| tarn_2x1 | ok | 640 |
| tarn_2x1_vinyl | ok | 658 |
| tarn_2x1_vinyl_10x | ok | 666 |
| tidb_rf1 | ok | 645 |
| tidb_rf1_10x | ok | 613 |
