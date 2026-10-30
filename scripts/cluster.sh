#!/bin/bash
# Start / stop a local Tarn cluster: N shards x R Tarantool replicas each, plus
# one or more tarn-server SQL front ends.
#
#   scripts/cluster.sh start [-n SHARDS] [-r REPLICAS] [-d DIR] [-p TARN_PORT]
#                            [-s SERVERS] [-w WAL_MODE] [-m MEMORY_MB] [-t THREADS]
#                            [-a]  (ack-on-prepare)  [-u]  (SQL servers reach storage via Unix sockets)
#                            [-I ISOLATION]  (default level of new sessions, e.g. READ-COMMITTED)
#                            [-e ENGINE]  (memtx | vinyl: engine of tables created without ENGINE=)
#   Environment: TARN_VINYL_MEMORY_MB, TARN_VINYL_CACHE_MB (vinyl memory per storage instance),
#                TARN_VINYL_READ_THREADS, _WRITE_THREADS, _PAGE_SIZE, _RANGE_SIZE, _BLOOM_FPR,
#                TARN_CHECK_YIELDS=1 (an atomic protocol step that yields fails: tests),
#                TARN_CHECK_LOCKS=1 (the native module's copy of the lock index is checked: tests),
#                TARN_TEST_HOOKS=1 (tarn_test_hook may hold protocol steps: tests),
#                TARN_DUMP_EVERY=N (checkpoint every N s, putting vinyl rows on disk: tests),
#                TARN_LOAD_WINDOW=SECS (the seconds of requests a bucket's load counts),
#                TARN_NATIVE=PATH|0 (the native procedures' libtarn_storage.so; default: target/release's),
#                TARN_QUERY_PUSHDOWN=true|false (opt-in typed reads; default false),
#                TARN_DDL_JOB_STALE=SECS (ddl_job_stale in cluster.json: tests),
#                TARN_BUCKET_COUNT=N (bucket_count in cluster.json: buckets of a new cluster)
#                TARN_TOPOLOGY_LIMITS=JSON (gateway topology object: resource-pressure fixtures)
#   scripts/cluster.sh stop  [-d DIR]
#   scripts/cluster.sh status [-d DIR]
#   scripts/cluster.sh restart [-d DIR] INSTANCE...   (e.g. s2.1, tarn2: start a stopped instance again)
#   scripts/cluster.sh add-shard [-d DIR] I   (start the storage of a shard that is to join, id I, with
#                                              the cluster's replicas per shard; prints its replicas for
#                                              TARN ADD SHARD I '...')
#
# Storage instance (shard i, replica j) listens on 127.0.0.1:$((BASE + 10*i + j)).
# tarn-server k listens on 127.0.0.1:$((TARN_PORT + k - 1)).
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-tarantool}
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
CMD=${1:-}; shift || true
N=2; R=1; DIR=${TARN_DIR:-/tmp/tarn-cluster}; TPORT=4000; SERVERS=1; WAL=write; MEM=1024
THREADS=""; ACK=""; BASE=${TARN_BASE_PORT:-3300}; ITHREADS=1; UNIX=0; POOL=""; ISO=""; ENGINE=""
while getopts "n:r:d:p:s:w:m:t:ab:i:uP:I:e:" o; do
  case $o in
    n) N=$OPTARG;; r) R=$OPTARG;; d) DIR=$OPTARG;; p) TPORT=$OPTARG;; s) SERVERS=$OPTARG;;
    w) WAL=$OPTARG;; m) MEM=$OPTARG;; t) THREADS=$OPTARG;; a) ACK=--ack-on-prepare;; b) BASE=$OPTARG;;
    i) ITHREADS=$OPTARG;;
    u) UNIX=1;;
    P) POOL=$OPTARG;;
    I) ISO=$OPTARG;;
    e) ENGINE=$OPTARG;;
    *) echo "bad option"; exit 2;;
  esac
done
shift $((OPTIND - 1))
PUSHDOWN=${TARN_QUERY_PUSHDOWN:-false}
case "$PUSHDOWN" in true|false) ;; *) echo 'TARN_QUERY_PUSHDOWN must be true or false' >&2; exit 2;; esac

port_of() { echo $((BASE + 10 * $1 + $2)); }

wait_port() {
  for _ in $(seq 300); do (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null && return 0; sleep 0.1; done
  echo "port $1 did not open" >&2; return 1
}

peers_json() {
  local out="{" i j sep=""
  for i in $(seq 1 "$N"); do
    out+="$sep\"$i\":["; local s2=""
    for j in $(seq 1 "$R"); do out+="$s2\"127.0.0.1:$(port_of "$i" "$j")\""; s2=","; done
    out+="]"; sep=","
  done
  echo "$out}"
}

replicas_json() {
  local i=$1 out="[" j sep=""
  if [ "$R" -gt 1 ]; then
    for j in $(seq 1 "$R"); do out+="$sep\"tarn:tarn@127.0.0.1:$(port_of "$i" "$j")\""; sep=","; done
  fi
  echo "$out]"
}

start_storage() {
  local i=$1 j=$2 p; p=$(port_of "$i" "$j")
  mkdir -p "$DIR/s$i.$j"
  # The instance's command, kept in s<i>.<j>.sh for `restart`.
  {
    printf 'cd %q || exit 1\nexec env' "$DIR/s$i.$j"
    printf ' %q' TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$p TARN_WORKDIR="$DIR/s$i.$j" \
      TARN_REPLICAS="$(replicas_json "$i")" TARN_PEERS="$(peers_json)" TARN_MEMORY_MB=$MEM \
      TARN_WAL_MODE=$WAL TARN_LOG_LEVEL=${TARN_LOG_LEVEL:-5} TARN_IPROTO_THREADS=$ITHREADS \
      TARN_VINYL_MEMORY_MB=${TARN_VINYL_MEMORY_MB:-128} TARN_VINYL_CACHE_MB=${TARN_VINYL_CACHE_MB:-128} \
      TARN_VINYL_TIMEOUT=${TARN_VINYL_TIMEOUT:-0.25} \
      TARN_VINYL_READ_THREADS=${TARN_VINYL_READ_THREADS:-} TARN_HOST_INSTANCES=$((N * R)) \
      TARN_VINYL_WRITE_THREADS=${TARN_VINYL_WRITE_THREADS:-} \
      TARN_VINYL_PAGE_SIZE=${TARN_VINYL_PAGE_SIZE:-} TARN_VINYL_RANGE_SIZE=${TARN_VINYL_RANGE_SIZE:-} \
      TARN_VINYL_BLOOM_FPR=${TARN_VINYL_BLOOM_FPR:-} \
      TARN_CHECK_YIELDS=${TARN_CHECK_YIELDS:-} TARN_TEST_HOOKS=${TARN_TEST_HOOKS:-} \
      TARN_CHECK_LOCKS=${TARN_CHECK_LOCKS:-} \
      TARN_DUMP_EVERY=${TARN_DUMP_EVERY:-} TARN_LOAD_WINDOW=${TARN_LOAD_WINDOW:-} TARN_NATIVE=${TARN_NATIVE:-} \
      TARN_LISTEN_UNIX=$([ "$UNIX" = 1 ] && echo "$DIR/s$i.$j.sock") \
      "$T" "$ROOT/lua/storage.lua"
    printf ' >> %q 2>&1 < /dev/null\n' "$DIR/s$i.$j.log"
  } > "$DIR/s$i.$j.sh"
  bash "$DIR/s$i.$j.sh" &
  echo $! > "$DIR/s$i.$j.pid"
}

write_config() {
  local i j sep="" out="{\"shards\":["
  for i in $(seq 1 "$N"); do
    out+="$sep{\"id\":$i,\"replicas\":["; local s2=""
    for j in $(seq 1 "$R"); do
      if [ "$UNIX" = 1 ]; then out+="$s2\"unix/:$DIR/s$i.$j.sock\""; else out+="$s2\"127.0.0.1:$(port_of "$i" "$j")\""; fi
      s2=","
    done
    out+="]}"; sep=","
  done
  out+="]"
  out+=",\"query_pushdown\":$PUSHDOWN"
  [ -n "${TARN_DDL_JOB_STALE:-}" ] && out+=",\"ddl_job_stale\":$TARN_DDL_JOB_STALE"
  [ -n "${TARN_BUCKET_COUNT:-}" ] && out+=",\"bucket_count\":$TARN_BUCKET_COUNT"
  [ -n "${TARN_TOPOLOGY_LIMITS:-}" ] && out+=",\"topology\":$TARN_TOPOLOGY_LIMITS"
  echo "$out}" > "$DIR/cluster.json"
}

# Wait until shard i has a writable leader with the Tarn schema.
wait_leader() {
  local i=$1 j ok=0
  for _ in $(seq 600); do
    for j in $(seq 1 "$R"); do
      if "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$(port_of "$i" "$j")" < /dev/null > /dev/null 2>&1; then ok=1; break; fi
    done
    [ $ok = 1 ] && return 0; sleep 0.2
  done
  echo "shard $i has no writable leader; see $DIR/s$i.*.log" >&2; return 1
}

# Wait until every replica of shard i has joined, so that workloads start on
# whole replica sets: a replica still joining a new shard that already takes
# synchronous writes has been seen to stop in its first checkpoint ("can't
# initialize storage: timed out").
wait_joined() {
  local i=$1 j ok
  [ "$R" -gt 1 ] || return 0
  for j in $(seq 1 "$R"); do
    ok=0
    for _ in $(seq 300); do
      if "$T" "$ROOT/scripts/joined.lua" "tarn:tarn@127.0.0.1:$(port_of "$i" "$j")" "$R" < /dev/null > /dev/null 2>&1; then ok=1; break; fi
      sleep 0.2
    done
    [ $ok = 1 ] || { echo "storage s$i.$j did not finish joining its shard; see $DIR/s$i.$j.log" >&2; return 1; }
  done
}

# SQL server k (its command kept in tarn<k>.sh for `restart`).
start_server() {
  local k=$1 tp=$((TPORT + $1 - 1))
  {
    printf 'exec env TARN_LOG=%q %q --config %q --listen 127.0.0.1:%s --node-id %s' \
      "${TARN_LOG:-info}" "$TARN" "$DIR/cluster.json" "$tp" "$k"
    printf ' %s' ${THREADS:+--threads $THREADS} ${POOL:+--pool $POOL} ${ISO:+--isolation $ISO} \
      ${ENGINE:+--default-engine $ENGINE} $ACK
    printf ' >> %q 2>&1 < /dev/null\n' "$DIR/tarn$k.log"
  } > "$DIR/tarn$k.sh"
  bash "$DIR/tarn$k.sh" &
  echo $! > "$DIR/tarn$k.pid"
  wait_port "$tp" || { tail -5 "$DIR/tarn$k.log"; return 1; }
}

case "$CMD" in
  start)
    mkdir -p "$DIR"
    echo "N=$N R=$R BASE=$BASE TPORT=$TPORT SERVERS=$SERVERS MEM=$MEM WAL=$WAL ITHREADS=$ITHREADS UNIX=$UNIX" > "$DIR/params"
    for i in $(seq 1 "$N"); do for j in $(seq 1 "$R"); do start_storage "$i" "$j"; done; done
    for i in $(seq 1 "$N"); do for j in $(seq 1 "$R"); do wait_port "$(port_of "$i" "$j")" || exit 1; done; done
    write_config
    for i in $(seq 1 "$N"); do wait_leader "$i" || exit 1; done
    for i in $(seq 1 "$N"); do wait_joined "$i" || exit 1; done
    for k in $(seq 1 "$SERVERS"); do start_server "$k" || exit 1; done
    echo "cluster up: $N shard(s) x $R replica(s); SQL on 127.0.0.1:$TPORT..$((TPORT + SERVERS - 1)); dir $DIR"
    ;;
  stop)
    for f in "$DIR"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; done
    sleep 0.5
    for f in "$DIR"/*.pid; do [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null; done
    [ "${KEEP:-0}" = 1 ] || rm -rf "$DIR"
    ;;
  restart)
    # shellcheck disable=SC1091
    . "$DIR/params"
    for n in "$@"; do
      [ -f "$DIR/$n.sh" ] || { echo "no instance $n in $DIR" >&2; exit 1; }
      [ -f "$DIR/$n.pid" ] && kill -0 "$(cat "$DIR/$n.pid")" 2>/dev/null && { echo "$n is running" >&2; exit 1; }
      bash "$DIR/$n.sh" &
      echo $! > "$DIR/$n.pid"
      case $n in
        tarn*) wait_port $((TPORT + ${n#tarn} - 1)) || exit 1;;
        *) i=${n#s}; j=${i#*.}; i=${i%.*}; wait_port "$(port_of "$i" "$j")" || exit 1;;
      esac
    done
    ;;
  add-shard)
    # shellcheck disable=SC1091
    . "$DIR/params"
    I=${1:?shard id}
    # (its peers: every shard up to it; the cluster's shards learn its
    # replicas from the membership once it joins)
    N=$I
    for j in $(seq 1 "$R"); do start_storage "$I" "$j" > /dev/null; done
    for j in $(seq 1 "$R"); do wait_port "$(port_of "$I" "$j")" || exit 1; done
    wait_leader "$I" || exit 1
    wait_joined "$I" || exit 1
    sep=""
    for j in $(seq 1 "$R"); do printf '%s127.0.0.1:%s' "$sep" "$(port_of "$I" "$j")"; sep=","; done
    echo
    ;;
  status)
    for f in "$DIR"/*.pid; do
      [ -f "$f" ] || continue
      if kill -0 "$(cat "$f")" 2>/dev/null; then echo "$(basename "$f" .pid) up"; else echo "$(basename "$f" .pid) DOWN"; fi
    done
    ;;
  *) sed -n 2,17p "$0"; exit 2;;
esac
