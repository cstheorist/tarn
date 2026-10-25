#!/bin/bash
# Many-tables test: create $MANY tables (100 per batch, each with a primary
# key and one secondary index), load 10 rows into each, then run point selects
# against random tables. Appends CSV lines to $OUT:
#   label,metric,value
#
#   OUT=... DRIVER=mysql|pgsql PORT=... PROCS=a,b,c [DB_OPTIONS=...] manytables.sh LABEL
#
# Environment: MANY (1000) MANY_CAP (seconds allowed for the DDL, 1200)
#              TIME (seconds of point selects, 15) USER_NAME LOGDIR
set -u
LABEL=$1
HERE=$(cd "$(dirname "$0")" && pwd)
MANY=${MANY:-1000} CAP=${MANY_CAP:-1200} TIME=${TIME:-15}
LOGDIR=${LOGDIR:-/tmp} DB=sbmany
WORK=$(mktemp -d)
[ -f "$OUT" ] || echo "label,metric,value" > "$OUT"
emit() { echo "$LABEL,$1,$2" >> "$OUT"; echo "$LABEL $1 = $2"; }
rss() { ps -C "$PROCS" -o rss= 2> /dev/null | awk '{s += $1} END {print s + 0}'; }
now() { date +%s.%N; }

if [ "$DRIVER" = pgsql ]; then
  USER=${USER_NAME:-yugabyte}
  [ "$USER" = sb ] && USER=yugabyte
  admin() { psql -h 127.0.0.1 -p "$PORT" -U "$USER" -d yugabyte -v ON_ERROR_STOP=1 -qAt -c "$1"; }
  run_file() { psql -h 127.0.0.1 -p "$PORT" -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -q -f "$1"; }
  admin "DROP DATABASE IF EXISTS $DB" && admin "CREATE DATABASE $DB ${DB_OPTIONS:-}" || exit 1
  table_ddl() { printf 'CREATE TABLE t%04d (id BIGINT PRIMARY KEY, k INT NOT NULL, c VARCHAR(64));\nCREATE INDEX t%04d_k ON t%04d (k);\n' "$1" "$1" "$1"; }
  SB=(--db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port="$PORT" --pgsql-user="$USER" --pgsql-db="$DB")
else
  USER=${USER_NAME:-root}
  admin() { mysql -h 127.0.0.1 -P "$PORT" -u "$USER" -N -e "$1"; }
  run_file() { mysql -h 127.0.0.1 -P "$PORT" -u "$USER" "$DB" < "$1"; }
  admin "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB" || exit 1
  table_ddl() { printf 'CREATE TABLE t%04d (id BIGINT PRIMARY KEY, k INT NOT NULL, c VARCHAR(64), KEY k_idx (k));\n' "$1"; }
  SB=(--db-driver=mysql --mysql-host=127.0.0.1 --mysql-port="$PORT" --mysql-user="$USER" --mysql-db="$DB")
fi

emit rss_before_kb "$(rss)"
start=$(now) created=0 first=""
for b in $(seq 0 $(( (MANY + 99) / 100 - 1 ))); do
  f=$WORK/ddl$b.sql; : > "$f"
  for i in $(seq $((b * 100)) $((b * 100 + 99))); do [ "$i" -lt "$MANY" ] && table_ddl "$i" >> "$f"; done
  t0=$(now)
  if ! run_file "$f" > "$LOGDIR/$LABEL.many.ddl$b.log" 2>&1; then
    tail -5 "$LOGDIR/$LABEL.many.ddl$b.log"; emit ddl_error_at_batch "$b"; break
  fi
  t1=$(now)
  created=$(( b * 100 + $(grep -c '^CREATE TABLE' "$f") ))
  secs=$(echo "$t1 - $t0" | bc)
  [ -z "$first" ] && first=$secs
  emit "create_batch_${b}_seconds" "$secs"
  if [ "$(echo "$t1 - $start > $CAP" | bc)" = 1 ]; then emit ddl_capped_after_seconds "$CAP"; break; fi
done
total=$(echo "$(now) - $start" | bc)
emit tables_created "$created"
emit ddl_total_seconds "$total"
emit ddl_first_100_seconds "${first:-0}"
emit ddl_last_100_seconds "${secs:-0}"
sleep 5
emit rss_after_ddl_kb "$(rss)"

# 10 rows per table.
f=$WORK/load.sql; : > "$f"
for i in $(seq 0 $((created - 1))); do
  printf "INSERT INTO t%04d VALUES (1,1,'a'),(2,2,'b'),(3,3,'c'),(4,4,'d'),(5,5,'e'),(6,6,'f'),(7,7,'g'),(8,8,'h'),(9,9,'i'),(10,10,'j');\n" "$i" >> "$f"
done
t0=$(now)
if run_file "$f" > "$LOGDIR/$LABEL.many.load.log" 2>&1; then
  emit load_seconds "$(echo "$(now) - $t0" | bc)"
else
  tail -5 "$LOGDIR/$LABEL.many.load.log"; emit load_error 1
fi

# Point selects over random tables (no prepared statements: 1000 tables x
# threads statements would measure the statement cache instead).
for th in 1 16; do
  log=$LOGDIR/$LABEL.many.select.$th.log
  sysbench "$HERE/manytables.lua" "${SB[@]}" --tables="$created" --threads="$th" --time="$TIME" \
    --db-ps-mode=disable --percentile=95 run > "$log" 2>&1
  emit "select_${th}thr_qps" "$(awk '/transactions:/ {gsub("\\(","",$3); print $3}' "$log")"
  emit "select_${th}thr_p95_ms" "$(awk '/95th percentile:/ {print $3}' "$log")"
done
emit rss_after_queries_kb "$(rss)"
rm -rf "$WORK"
