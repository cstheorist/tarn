#!/bin/bash
# Full benchmark matrix on this machine: MariaDB 10.11, MySQL 9.7 and Tarn
# configurations under the same sysbench workloads. Each system runs alone.
# TLC model checking (if running) is paused for the duration.
#
#   bench/run_matrix.sh [systems...]
#   systems: mariadb mysql tarn tarn_aop tarn_r3 mariadb_ser   (default: all)
#
# Environment: TABLES (4) SIZE (100000) THREADS ("1 16 64") TIME (15)
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
export TABLES=${TABLES:-4} SIZE=${SIZE:-100000} THREADS=${THREADS:-"1 16 64"} TIME=${TIME:-15}
export USER_NAME=sb OUT=${OUT:-$ROOT/bench/results.csv} LOGDIR=$ROOT/bench/logs
SYSTEMS=${*:-"mariadb mysql tarn tarn_aop tarn_r3 mariadb_ser"}
ALL="oltp_point_select oltp_read_only oltp_read_write oltp_update_index oltp_update_non_index oltp_insert oltp_write_only"
TLC=$(pgrep -f "[t]lc2.TLC" | head -1)
[ -n "$TLC" ] && kill -STOP $TLC
trap '[ -n "$TLC" ] && kill -CONT $TLC' EXIT
D=/tmp/tarn-bench

stop_all() {
  bench/baselines.sh stop mariadb >/dev/null 2>&1
  bench/baselines.sh stop mysql >/dev/null 2>&1
  scripts/cluster.sh stop -d $D >/dev/null 2>&1
  scripts/cluster.sh stop -d /tmp/tarn-e2e >/dev/null 2>&1
  sleep 1
}

kv_bench() {  # label shards...
  local label=$1 shards=$2
  for th in $THREADS; do
    local line
    line=$(./target/release/examples/kvbench --shards "$shards" --space sbtest.sbtest1 --keys "$SIZE" \
           --tasks "$th" --secs "$TIME" --check 2>&1 | tail -1)
    echo "$label kv_point_read t=$th: $line"
    local ops p95 p50
    ops=$(echo "$line" | grep -oE "= [0-9]+ ops/s" | grep -oE "[0-9]+")
    p50=$(echo "$line" | grep -oE "p50 [0-9]+" | grep -oE "[0-9]+$")
    p95=$(echo "$line" | grep -oE "p95 [0-9]+" | grep -oE "[0-9]+$")
    echo "$label,kv_point_read,$th,$ops,$ops,$(echo "scale=3; $p50/1000" | bc),$(echo "scale=3; $p95/1000" | bc),0,0,$TABLES,$SIZE,$TIME" >> "$OUT"
  done
}

for sys in $SYSTEMS; do
  stop_all
  case $sys in
    mariadb)
      bench/baselines.sh start mariadb && TESTS="$ALL" bench/sysbench.sh mariadb 127.0.0.1 3407 ;;
    mariadb_ser)
      ISOLATION=SERIALIZABLE bench/baselines.sh start mariadb && \
        TESTS="oltp_read_only oltp_read_write oltp_write_only" bench/sysbench.sh mariadb_serializable 127.0.0.1 3407 ;;
    mysql)
      bench/baselines.sh start mysql && TESTS="$ALL" bench/sysbench.sh mysql 127.0.0.1 3408 ;;
    tarn)
      scripts/cluster.sh start -n 2 -r 1 -d $D -m 1500 -p 4000 && \
        TESTS="$ALL" bench/sysbench.sh tarn_2x1 127.0.0.1 4000 && \
        kv_bench tarn_2x1 127.0.0.1:3311,127.0.0.1:3321 ;;
    tarn_aop)
      scripts/cluster.sh start -n 2 -r 1 -d $D -m 1500 -p 4000 -a && \
        TESTS="oltp_read_write oltp_write_only" bench/sysbench.sh tarn_2x1_ackprepare 127.0.0.1 4000 ;;
    tarn_r3)
      # one shard, 3 synchronous replicas (quorum 2): the HA configuration
      scripts/cluster.sh start -n 1 -r 3 -d $D -m 1200 -p 4000 && \
        TESTS="oltp_point_select oltp_read_write oltp_update_index oltp_insert" bench/sysbench.sh tarn_1x3sync 127.0.0.1 4000 ;;
  esac
done
stop_all
echo "results in $OUT"
