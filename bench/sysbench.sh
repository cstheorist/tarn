#!/bin/bash
# Run the standard sysbench OLTP workloads against any MySQL-protocol endpoint
# and append one CSV line per (workload, threads) to $OUT.
#
#   bench/sysbench.sh LABEL HOST PORT
#
# Environment: TABLES (4) SIZE (100000) THREADS ("1 8 32 64") TIME (20)
#              TESTS ("oltp_point_select oltp_read_only oltp_read_write
#                      oltp_update_index oltp_update_non_index oltp_insert oltp_write_only")
#              USER (root) PASS ("") DB (sbtest) PREPARE (1) OUT (bench/results.csv)
#              PS_MODE (auto|disable)
set -u
LABEL=$1 HOST=$2 PORT=$3
TABLES=${TABLES:-4} SIZE=${SIZE:-100000} TIME=${TIME:-20}
THREADS=${THREADS:-"1 8 32 64"}
TESTS=${TESTS:-"oltp_point_select oltp_read_only oltp_read_write oltp_update_index oltp_update_non_index oltp_insert oltp_write_only"}
USER=${USER_NAME:-root} PASS=${PASS:-} DB=${DB:-sbtest}
OUT=${OUT:-$(dirname "$0")/results.csv}
PS_MODE=${PS_MODE:-auto}
LOGDIR=${LOGDIR:-$(dirname "$0")/logs}
mkdir -p "$LOGDIR"
[ -f "$OUT" ] || echo "label,test,threads,tps,qps,lat_avg_ms,lat_p95_ms,errors,reconnects,tables,size,time" > "$OUT"

COMMON=(--db-driver=mysql --mysql-host="$HOST" --mysql-port="$PORT" --mysql-user="$USER"
        --mysql-password="$PASS" --mysql-db="$DB" --tables="$TABLES" --table-size="$SIZE"
        --db-ps-mode="$PS_MODE" --mysql-ignore-errors=1213,1205,1020,1062)

if [ "${PREPARE:-1}" = 1 ]; then
  mysql -h "$HOST" -P "$PORT" -u "$USER" ${PASS:+-p"$PASS"} -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB" || exit 1
  t0=$(date +%s.%N)
  sysbench oltp_common "${COMMON[@]}" --threads="$TABLES" prepare > "$LOGDIR/$LABEL.prepare.log" 2>&1 || {
    tail -20 "$LOGDIR/$LABEL.prepare.log"; exit 1; }
  t1=$(date +%s.%N)
  echo "$LABEL prepare: $TABLES x $SIZE rows in $(echo "$t1 - $t0" | bc) s"
fi

for test in $TESTS; do
  for th in $THREADS; do
    log="$LOGDIR/$LABEL.$test.$th.log"
    sysbench "$test" "${COMMON[@]}" --threads="$th" --time="$TIME" --report-interval=0 \
      --percentile=95 --rand-type=uniform run > "$log" 2>&1
    tps=$(awk '/transactions:/ {gsub("\\(","",$3); print $3}' "$log")
    qps=$(awk '/queries:/ && /per sec/ {gsub("\\(","",$3); print $3}' "$log")
    avg=$(awk '/avg:/ {print $2}' "$log" | head -1)
    p95=$(awk '/95th percentile:/ {print $3}' "$log")
    err=$(awk '/ignored errors:/ {print $3}' "$log")
    rec=$(awk '/reconnects:/ {print $2}' "$log")
    if [ -z "$tps" ]; then echo "$LABEL $test t=$th FAILED:"; tail -5 "$log"; tps=0; fi
    echo "$LABEL,$test,$th,$tps,$qps,$avg,$p95,$err,$rec,$TABLES,$SIZE,$TIME" >> "$OUT"
    printf "%-14s %-22s t=%-3s tps=%-10s qps=%-10s avg=%-7s p95=%-7s err=%s\n" "$LABEL" "$test" "$th" "$tps" "$qps" "$avg" "$p95" "$err"
  done
done
