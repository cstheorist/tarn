#!/bin/bash
# Run the standard sysbench OLTP workloads against a MySQL-protocol (or, with
# DRIVER=pgsql, PostgreSQL-protocol) endpoint and append one CSV line per
# (workload, threads) to $OUT.
#
#   bench/sysbench.sh LABEL HOST PORT
#
# Environment: TABLES (4) SIZE (100000) THREADS ("1 8 32 64") TIME (20)
#              TESTS ("oltp_point_select oltp_read_only oltp_read_write
#                      oltp_update_index oltp_update_non_index oltp_insert oltp_write_only")
#              USER_NAME (root) PASS ("") DB (sbtest) PREPARE (1) OUT (bench/results.csv)
#              PS_MODE (auto|disable) DRIVER (mysql|pgsql) IGNORE_ERRORS (MySQL error codes)
#              PREPARE_OUT (CSV of data-load times, default: none)
#              PREPARE_THREADS (default TABLES: one loader per table)
#              PERCENTILE (95): the latency percentile reported
#              AFTER_PREPARE: a command run after the load, before the workloads
#              WARMUP: SQL statements run after that (MySQL protocol only)
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
PCT=${PERCENTILE:-95}
[ -f "$OUT" ] || echo "label,test,threads,tps,qps,lat_avg_ms,lat_p${PCT}_ms,errors,reconnects,tables,size,time" > "$OUT"

DRIVER=${DRIVER:-mysql}
if [ "$DRIVER" = pgsql ]; then
  # sysbench's PostgreSQL driver retries serialization failures, deadlocks
  # and unique violations (40001, 40P01, 23505) by itself.
  COMMON=(--db-driver=pgsql --pgsql-host="$HOST" --pgsql-port="$PORT" --pgsql-user="$USER"
          --pgsql-password="$PASS" --pgsql-db="$DB" --tables="$TABLES" --table-size="$SIZE"
          --db-ps-mode="$PS_MODE")
  recreate_db() {
    PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "${PGADMIN_DB:-postgres}" -v ON_ERROR_STOP=1 -q \
      -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB ${DB_OPTIONS:-}" &&
    if [ -n "${DB_SETUP:-}" ]; then
      PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -q -c "$DB_SETUP"
    fi
  }
else
  COMMON=(--db-driver=mysql --mysql-host="$HOST" --mysql-port="$PORT" --mysql-user="$USER"
          --mysql-password="$PASS" --mysql-db="$DB" --tables="$TABLES" --table-size="$SIZE"
          --db-ps-mode="$PS_MODE" --mysql-ignore-errors="${IGNORE_ERRORS:-1213,1205,1020,1062}")
  recreate_db() {
    mysql -h "$HOST" -P "$PORT" -u "$USER" ${PASS:+-p"$PASS"} -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB"
  }
fi

# Every table must hold SIZE rows after prepare (sysbench can exit 0 when
# some of its threads failed).
count_rows() {  # table
  if [ "$DRIVER" = pgsql ]; then
    PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -qAt -c "SELECT COUNT(*) FROM $1" 2> /dev/null
  else
    mysql -h "$HOST" -P "$PORT" -u "$USER" ${PASS:+-p"$PASS"} -N -e "SELECT COUNT(*) FROM $DB.$1" 2> /dev/null
  fi
}

if [ "${PREPARE:-1}" = 1 ]; then
  recreate_db || exit 1
  t0=$(date +%s.%N)
  # PREPARE_THREADS=1 for systems whose concurrent DDL fails (YugabyteDB).
  sysbench oltp_common "${COMMON[@]}" --threads="${PREPARE_THREADS:-$TABLES}" prepare > "$LOGDIR/$LABEL.prepare.log" 2>&1 || {
    tail -20 "$LOGDIR/$LABEL.prepare.log"; exit 1; }
  t1=$(date +%s.%N)
  for i in $(seq 1 "$TABLES"); do
    n=$(count_rows "sbtest$i")
    if [ "${n:-0}" != "$SIZE" ]; then
      echo "$LABEL prepare: sbtest$i has '${n:-?}' rows, expected $SIZE"; tail -20 "$LOGDIR/$LABEL.prepare.log"; exit 1
    fi
  done
  secs=$(echo "$t1 - $t0" | bc)
  echo "$LABEL prepare: $TABLES x $SIZE rows in $secs s"
  if [ -n "${PREPARE_OUT:-}" ]; then
    [ -f "$PREPARE_OUT" ] || echo "label,tables,size,seconds" > "$PREPARE_OUT"
    echo "$LABEL,$TABLES,$SIZE,$secs" >> "$PREPARE_OUT"
  fi
fi
if [ -n "${AFTER_PREPARE:-}" ]; then
  bash -c "$AFTER_PREPARE" > "$LOGDIR/$LABEL.after_prepare.log" 2>&1 || {
    echo "$LABEL: AFTER_PREPARE failed"; tail -20 "$LOGDIR/$LABEL.after_prepare.log"; exit 1; }
fi
if [ -n "${WARMUP:-}" ]; then
  t0=$(date +%s.%N)
  mysql -h "$HOST" -P "$PORT" -u "$USER" ${PASS:+-p"$PASS"} -D "$DB" -e "$WARMUP" > "$LOGDIR/$LABEL.warmup.log" 2>&1 || {
    echo "$LABEL: warm-up failed"; tail -20 "$LOGDIR/$LABEL.warmup.log"; exit 1; }
  echo "$LABEL warm-up: $(echo "$(date +%s.%N) - $t0" | bc) s"
fi

for test in $TESTS; do
  for th in $THREADS; do
    log="$LOGDIR/$LABEL.$test.$th.log"
    sysbench "$test" "${COMMON[@]}" --threads="$th" --time="$TIME" --report-interval=0 \
      --percentile="$PCT" --rand-type=uniform run > "$log" 2>&1
    tps=$(awk '/transactions:/ {gsub("\\(","",$3); print $3}' "$log")
    qps=$(awk '/queries:/ && /per sec/ {gsub("\\(","",$3); print $3}' "$log")
    avg=$(awk '/avg:/ {print $2}' "$log" | head -1)
    p95=$(awk "/${PCT}th percentile:/ {print \$3}" "$log")
    err=$(awk '/ignored errors:/ {print $3}' "$log")
    rec=$(awk '/reconnects:/ {print $2}' "$log")
    if [ -z "$tps" ]; then echo "$LABEL $test t=$th FAILED:"; tail -5 "$log"; tps=0; fi
    echo "$LABEL,$test,$th,$tps,$qps,$avg,$p95,$err,$rec,$TABLES,$SIZE,$TIME" >> "$OUT"
    printf "%-14s %-22s t=%-3s tps=%-10s qps=%-10s avg=%-7s p$PCT=%-7s err=%s\n" "$LABEL" "$test" "$th" "$tps" "$qps" "$avg" "$p95" "$err"
  done
done
