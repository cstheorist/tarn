#!/bin/bash
# Same-host comparison of Tarn with TiDB, YugabyteDB and (as a single-node
# reference) MariaDB: sysbench OLTP at replication factor 1 and 3, and a
# many-tables DDL test. Written for a GitHub-hosted runner (ubuntu-24.04,
# 4 vCPUs, 16 GB RAM, docker); .github/workflows/compare.yml runs it.
#
#   bench/compare/run.sh RESULTS_DIR [CONFIG...]
#
# CONFIG (default: all, in this order):
#   mariadb            MariaDB 10.11, one node (reference)
#   tarn_2x1           Tarn, 2 shards x 1 replica
#   tarn_2x1_rc        Tarn, 2 shards x 1 replica, READ COMMITTED
#   tarn_2x3           Tarn, 2 shards x 3 synchronous replicas
#   tarn_2x1_vinyl     Tarn, 2 shards x 1 replica, vinyl tables, a cache larger than the data
#   tarn_2x1_vinyl_10x Tarn, 2 shards x 1 replica, vinyl tables, data ten times the cache
#   tidb_rf1           TiDB: 1 TiDB + 1 PD + 1 TiKV
#   tidb_rf1_rc        TiDB: 1 TiDB + 1 PD + 1 TiKV, READ COMMITTED
#   tidb_rf3           TiDB: 1 TiDB + 1 PD + 3 TiKV (3 replicas)
#   tidb_rf1_10x       TiDB: 1 TiDB + 1 PD + 1 TiKV, data ten times the block cache
#   yb_rf1             YugabyteDB: 1 node
#   yb_rf1_ser         YugabyteDB: 1 node, SERIALIZABLE
#   yb_rf3             YugabyteDB: 3 nodes (3 replicas)
#   many_<system>      many-tables test for tarn, tidb, yb, yb_colocated, mariadb
#
# Every system keeps its data on tmpfs (/dev/shm): fsync is free, so the
# numbers measure CPU and round trips, not disks. All processes, including
# the load generator, share the runner's vCPUs. In the "10x" configurations
# the engine's cache (vinyl's, TiKV's block cache) holds a tenth of the data,
# so most reads go to the engine's files -- which, on tmpfs, are in RAM: they
# measure what a cache miss costs the engine, not a disk.
#
# Environment: TABLES (4) SIZE (100000) THREADS ("1 16 64") TIME (15)
#   TESTS (all seven workloads) PERCENTILE (95) WARM (0; 1: read every table
#   once after the load) MANY (1000) MANY_CAP (1200 s) TIDB_VERSION (latest)
#   YB_IMAGE MARIADB_IMAGE
set -u
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
RES=$(mkdir -p "$1" && cd "$1" && pwd); shift
CONFIGS=${*:-"mariadb tarn_2x1 tarn_2x1_rc tarn_2x3 tidb_rf1 tidb_rf1_rc tidb_rf3 yb_rf1 yb_rf1_ser yb_rf3 many_tarn many_tidb many_yb many_yb_colocated many_mariadb"}
export TABLES=${TABLES:-4} SIZE=${SIZE:-100000} THREADS=${THREADS:-"1 16 64"} TIME=${TIME:-15}
export PERCENTILE=${PERCENTILE:-95} WARM=${WARM:-0}
export LOGDIR=$RES/logs PREPARE_OUT=$RES/prepare.csv USER_NAME=sb
export MANY=${MANY:-1000} MANY_CAP=${MANY_CAP:-1200}
YB_IMAGE=${YB_IMAGE:-yugabytedb/yugabyte:latest}
MARIADB_IMAGE=${MARIADB_IMAGE:-mariadb:10.11}
SHM=/dev/shm/tarn-compare
TIUP=$HOME/.tiup/bin/tiup
ALL="oltp_point_select oltp_read_only oltp_read_write oltp_update_index oltp_update_non_index oltp_insert oltp_write_only"
TESTS=${TESTS:-$ALL}
# What sysbench loads, uncompressed: about 251 bytes per row with its
# secondary index (measured on vinyl, docs/operations.md#sizing). The 10x
# configurations give their engine's cache a tenth of it.
DATA_MB=$(( TABLES * SIZE * 251 / 1048576 ))
TENTH_MB=$(( DATA_MB / 10 > 8 ? DATA_MB / 10 : 8 ))
TIDB_ERRORS=1213,1205,1020,1062,8002,8022,8028,9007
mkdir -p "$LOGDIR" "$SHM"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }
version() { echo "$1: $2" >> "$RES/versions.txt"; }

wait_for() {  # seconds command...: retry until the command succeeds
  local secs=$1; shift
  for _ in $(seq "$secs"); do "$@" > /dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

stop_all() {
  "$ROOT/scripts/cluster.sh" stop -d "$SHM/tarn" > /dev/null 2>&1
  if [ -f "$SHM/playground.pid" ]; then
    kill -INT "$(cat "$SHM/playground.pid")" 2> /dev/null
    for _ in $(seq 60); do kill -0 "$(cat "$SHM/playground.pid")" 2> /dev/null || break; sleep 1; done
  fi
  pkill -9 -f '[t]iup-playground' 2> /dev/null; pkill -9 -x tidb-server 2> /dev/null
  pkill -9 -x tikv-server 2> /dev/null; pkill -9 -x pd-server 2> /dev/null
  docker rm -f yb mariadb > /dev/null 2>&1
  sleep 2
  rm -rf "${SHM:?}"/*
}

rss_kb() {  # total resident memory of the named processes, in KB
  ps -C "$1" -o rss= 2> /dev/null | awk '{s += $1} END {print s + 0}'
}

# --- MariaDB -----------------------------------------------------------------

start_mariadb() {
  mkdir -p "$SHM/mariadb"
  docker run -d --name mariadb --network host -v "$SHM/mariadb:/var/lib/mysql" \
    -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 "$MARIADB_IMAGE" --port=3407 --bind-address=127.0.0.1 \
    --innodb-buffer-pool-size=1G --innodb-flush-log-at-trx-commit=2 --skip-log-bin --innodb-doublewrite=0 \
    --innodb-log-file-size=512M --max-connections=2000 --max-prepared-stmt-count=100000 \
    --performance-schema=OFF --query-cache-type=0 > /dev/null || return 1
  wait_for 180 mysql -h 127.0.0.1 -P 3407 -u root -e "CREATE USER IF NOT EXISTS 'sb'@'%'; GRANT ALL ON *.* TO 'sb'@'%'" ||
    { docker logs --tail 30 mariadb; return 1; }
  version mariadb "$(mysql -h 127.0.0.1 -P 3407 -u root -N -e 'SELECT VERSION()')"
}

# --- Tarn --------------------------------------------------------------------

start_tarn() {  # shards replicas memory_mb [isolation] [engine [vinyl cache MB per shard]]
  TARN_VINYL_CACHE_MB=${6:-128} \
    "$ROOT/scripts/cluster.sh" start -n "$1" -r "$2" -d "$SHM/tarn" -m "$3" -p 4000 ${4:+-I "$4"} ${5:+-e "$5"} \
    > "$LOGDIR/tarn_$1x$2.start.log" 2>&1 ||
    { tail -20 "$LOGDIR/tarn_$1x$2.start.log"; return 1; }
  grep -q '^tarn:' "$RES/versions.txt" 2> /dev/null ||
    version tarn "$(git -C "$ROOT" rev-parse --short HEAD) on $(tarantool --version | head -1)"
}

# --- TiDB --------------------------------------------------------------------

start_tidb() {  # number of TiKV instances (= replicas) [isolation] [block cache MB per TiKV]
  local kv=$1
  if [ ! -x "$TIUP" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh > "$LOGDIR/tiup-install.log" 2>&1 ||
      { tail -20 "$LOGDIR/tiup-install.log"; return 1; }
  fi
  mkdir -p "$SHM/tiup-data" "$HOME/.tiup"
  rm -rf "$HOME/.tiup/data"; ln -sfn "$SHM/tiup-data" "$HOME/.tiup/data"
  # Keep the TiKV block caches (default: 45% of RAM each) inside the runner.
  printf '[storage.block-cache]\ncapacity = "%dMB"\n' "${3:-$((3072 / kv))}" > "$SHM/tikv.toml"
  printf '[replication]\nmax-replicas = %d\n' "$kv" > "$SHM/pd.toml"
  # shellcheck disable=SC2086
  "$TIUP" playground ${TIDB_VERSION:-} --tag cmp --db 1 --pd 1 --kv "$kv" --tiflash 0 --without-monitor \
    --host 127.0.0.1 --db.port 4100 --kv.config "$SHM/tikv.toml" --pd.config "$SHM/pd.toml" \
    > "$LOGDIR/tidb_rf$kv.playground.log" 2>&1 < /dev/null &
  echo $! > "$SHM/playground.pid"
  local up=0
  for _ in $(seq 900); do
    up=$(mysql -h 127.0.0.1 -P 4100 -u root -N -e \
      "SELECT COUNT(*) FROM information_schema.tikv_store_status WHERE store_state_name = 'Up'" 2> /dev/null)
    [ "${up:-0}" -ge "$kv" ] && break
    sleep 1
  done
  [ "${up:-0}" -ge "$kv" ] || { tail -30 "$LOGDIR/tidb_rf$kv.playground.log"; return 1; }
  mysql -h 127.0.0.1 -P 4100 -u root -e "CREATE USER IF NOT EXISTS 'sb'@'%'; GRANT ALL ON *.* TO 'sb'@'%'" || return 1
  [ -n "${2:-}" ] && { mysql -h 127.0.0.1 -P 4100 -u root -e "SET GLOBAL transaction_isolation = '$2'" || return 1; }
  grep -q '^tidb:' "$RES/versions.txt" 2> /dev/null ||
    version tidb "$(mysql -h 127.0.0.1 -P 4100 -u root -N -e 'SELECT VERSION()')"
  mysql -h 127.0.0.1 -P 4100 -u root -e "SELECT @@transaction_isolation, @@tidb_txn_mode;
    SHOW CONFIG WHERE type = 'pd' AND name = 'replication.max-replicas'" > "$LOGDIR/tidb_rf$kv${2:+_rc}.settings.txt" 2>&1
}

# --- YugabyteDB --------------------------------------------------------------

ysql() { psql -h 127.0.0.1 -p 5433 -U yugabyte -d "${2:-yugabyte}" -v ON_ERROR_STOP=1 -qAt -c "$1"; }

start_yb() {  # number of nodes (= replicas)
  local n=$1 script="" i join
  docker pull -q "$YB_IMAGE" > /dev/null || return 1
  mkdir -p "$SHM/yb"
  chmod 777 "$SHM/yb"   # the container runs as a non-root user
  for i in $(seq 1 "$n"); do
    join=""; [ "$i" -gt 1 ] && join="--join=127.0.0.1"
    script+="bin/yugabyted start --base_dir=/var/yb/n$i --advertise_address=127.0.0.$i $join"
    script+=" --tserver_flags=memory_limit_hard_bytes=$((6144 / n * 1048576))"
    script+=" --master_flags=memory_limit_hard_bytes=536870912 > /var/yb/start$i.log 2>&1 || echo 'start failed' >> /var/yb/start$i.log; "
    # A node can only join once the previous one serves YSQL.
    if [ "$i" -lt "$n" ]; then
      script+="for t in \$(seq 150); do bin/yugabyted status --base_dir=/var/yb/n$i 2> /dev/null | grep -q 'YSQL Status: Ready' && break; sleep 2; done; "
    fi
  done
  [ "$n" -ge 3 ] && script+="bin/yugabyted configure data_placement --rf=3 --base_dir=/var/yb/n1 > /var/yb/configure.log 2>&1 || true; "
  script+="exec sleep infinity"
  docker run -d --name yb --network host -v "$SHM/yb:/var/yb" "$YB_IMAGE" bash -c "$script" > /dev/null || return 1
  local ok=0 got=""
  for _ in $(seq 400); do
    got=$(ysql 'SELECT COUNT(*) FROM yb_servers()' 2>&1)
    [ "$got" = "$n" ] && { ok=1; break; }
    [ "$(docker inspect -f '{{.State.Running}}' yb 2> /dev/null)" = true ] || break
    sleep 1
  done
  [ $ok = 1 ] || { yb_failure "$n" "$got"; return 1; }
  grep -q '^yugabytedb:' "$RES/versions.txt" 2> /dev/null || version yugabytedb "$(ysql 'SELECT version()')"
  {
    ysql "SHOW transaction_isolation"
    ysql "SHOW yb_effective_transaction_isolation_level" 2>&1
    docker exec yb bash -c "bin/yb-admin -master_addresses 127.0.0.1:7100 get_universe_config" 2>&1 | head -c 2000
  } > "$LOGDIR/yb_rf$n.settings.txt" 2>&1
}

# Why YugabyteDB did not come up: kept with the results (job logs are not
# always at hand) and put in an annotation.
yb_failure() {  # nodes last-query-output
  local f="$LOGDIR/yb_rf$1.start_failure.txt"
  {
    echo "last yb_servers() query: $2"
    echo "--- docker ps -a"; docker ps -a
    echo "--- docker logs (tail)"; docker logs --tail 80 yb 2>&1
    for l in "$SHM"/yb/start*.log "$SHM"/yb/configure.log; do
      [ -f "$l" ] && { echo "--- $l"; tail -60 "$l"; }
    done
    echo "--- yugabyted status"
    docker exec yb bash -c "bin/yugabyted status --base_dir=/var/yb/n1" 2>&1 | head -40
    echo "--- listening"; ss -ltnp 2> /dev/null | grep -E ':(5433|7000|7100|9000|9100|15433) ' | head
    for l in "$SHM"/yb/n1/logs/yugabyted.log "$SHM"/yb/n1/logs/tserver/yb-tserver.INFO "$SHM"/yb/n1/logs/master/yb-master.INFO; do
      [ -f "$l" ] && { echo "--- $l"; tail -40 "$l"; }
    done
    echo "--- memory"; free -m; df -h /dev/shm
  } > "$f" 2>&1
  cat "$f"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    echo "::error title=YugabyteDB did not start (rf$1)::$(head -c 4000 "$f" | sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')"
  fi
}

# --- runs --------------------------------------------------------------------

sb() {  # label host port [extra env...] -- run every workload
  local label=$1 host=$2 port=$3 warm="" i; shift 3
  if [ "$WARM" = 1 ]; then
    for i in $(seq 1 "$TABLES"); do warm+="SELECT SUM(LENGTH(c)) FROM sbtest$i; "; done
  fi
  env OUT="$RES/results.csv" TESTS="$TESTS" WARMUP="$warm" "$@" "$ROOT/bench/sysbench.sh" "$label" "$host" "$port"
}

# Vinyl's sizes on both storage nodes, recorded in vinyl.csv; after the
# load (snapshot = 1) every node checkpoints first, so that vinyl's
# in-memory level goes to disk and the workloads read the data from files.
vinyl_stats() {  # label snapshot
  echo "tarantool '$ROOT/bench/compare/vinyl_stats.lua' '$RES/vinyl.csv' $1 $2 127.0.0.1:3311 127.0.0.1:3321"
}

many() {  # label driver port db_options process-names
  env OUT="$RES/many.csv" DRIVER="$2" PORT="$3" DB_OPTIONS="${4:-}" PROCS="$5" \
    "$ROOT/bench/compare/manytables.sh" "$1"
}

run_config() {
  case $1 in
    mariadb)     start_mariadb && sb mariadb 127.0.0.1 3407 ;;
    tarn_2x1)    start_tarn 2 1 1500 && sb tarn_2x1 127.0.0.1 4000 ;;
    tarn_2x1_rc) start_tarn 2 1 1500 READ-COMMITTED && sb tarn_2x1_rc 127.0.0.1 4000 ;;
    tarn_2x3)    start_tarn 2 3 1000 && sb tarn_2x3 127.0.0.1 4000 ;;
    tarn_2x1_vinyl)       # each shard's cache: twice its half of the data, and some
                 start_tarn 2 1 1500 "" vinyl $(( DATA_MB + 256 )) &&
                   sb tarn_2x1_vinyl 127.0.0.1 4000 AFTER_PREPARE="$(vinyl_stats tarn_2x1_vinyl.load 1)" &&
                   eval "$(vinyl_stats tarn_2x1_vinyl.end 0)" ;;
    tarn_2x1_vinyl_10x)   # each shard's cache: a tenth of its half of the data
                 start_tarn 2 1 1500 "" vinyl $(( TENTH_MB / 2 > 4 ? TENTH_MB / 2 : 4 )) &&
                   sb tarn_2x1_vinyl_10x 127.0.0.1 4000 AFTER_PREPARE="$(vinyl_stats tarn_2x1_vinyl_10x.load 1)" &&
                   eval "$(vinyl_stats tarn_2x1_vinyl_10x.end 0)" ;;
    tidb_rf1)    start_tidb 1 && sb tidb_rf1 127.0.0.1 4100 IGNORE_ERRORS=$TIDB_ERRORS ;;
    tidb_rf1_rc) start_tidb 1 READ-COMMITTED && sb tidb_rf1_rc 127.0.0.1 4100 IGNORE_ERRORS=$TIDB_ERRORS ;;
    tidb_rf3)    start_tidb 3 && sb tidb_rf3 127.0.0.1 4100 IGNORE_ERRORS=$TIDB_ERRORS ;;
    tidb_rf1_10x) start_tidb 1 "" "$TENTH_MB" && sb tidb_rf1_10x 127.0.0.1 4100 IGNORE_ERRORS=$TIDB_ERRORS ;;
    yb_rf1)      start_yb 1 && sb yb_rf1 127.0.0.1 5433 DRIVER=pgsql PREPARE_THREADS=1 USER_NAME=yugabyte PGADMIN_DB=yugabyte ;;
    yb_rf1_ser)  start_yb 1 && sb yb_rf1_serializable 127.0.0.1 5433 DRIVER=pgsql PREPARE_THREADS=1 USER_NAME=yugabyte PGADMIN_DB=yugabyte \
                   "DB_SETUP=ALTER DATABASE sbtest SET default_transaction_isolation TO 'serializable'" ;;
    yb_rf3)      start_yb 3 && sb yb_rf3 127.0.0.1 5433 DRIVER=pgsql PREPARE_THREADS=1 USER_NAME=yugabyte PGADMIN_DB=yugabyte ;;
    many_tarn)   start_tarn 2 1 1500 && many tarn_2x1 mysql 4000 "" tarantool,tarn-server ;;
    many_tidb)   start_tidb 1 && many tidb_rf1 mysql 4100 "" tidb-server,tikv-server,pd-server ;;
    many_yb)     start_yb 1 && many yb_rf1 pgsql 5433 "" yb-master,yb-tserver,postgres ;;
    many_yb_colocated)
                 start_yb 1 && many yb_rf1_colocated pgsql 5433 "WITH COLOCATION = true" yb-master,yb-tserver,postgres ;;
    many_mariadb) start_mariadb && many mariadb mysql 3407 "" mariadbd ;;
    *) echo "unknown configuration $1"; return 2 ;;
  esac
}

{
  echo "date: $(date -u +%FT%TZ)"
  echo "kernel: $(uname -r)"
  lscpu | grep -E '^(Model name|CPU\(s\)|Thread|Core|Socket)'
  free -m | head -2
  echo "sysbench: $(sysbench --version)"
  echo "settings: TABLES=$TABLES SIZE=$SIZE THREADS='$THREADS' TIME=$TIME PERCENTILE=$PERCENTILE WARM=$WARM MANY=$MANY"
  echo "data: about $DATA_MB MB uncompressed; caches of the 10x configurations: $TENTH_MB MB in all"
} > "$RES/environment.txt"
cat "$RES/environment.txt"
echo "config,status,seconds" > "$RES/configs.csv"

for c in $CONFIGS; do
  stop_all
  log "=== $c"
  t0=$(date +%s)
  if run_config "$c"; then st=ok; else st=FAILED; fi
  log "=== $c: $st in $(( $(date +%s) - t0 )) s"
  echo "$c,$st,$(( $(date +%s) - t0 ))" >> "$RES/configs.csv"
done
stop_all
python3 "$ROOT/bench/compare/render.py" "$RES" > "$RES/summary.md" && cat "$RES/summary.md"
