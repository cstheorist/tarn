#!/bin/bash
# Start / stop a local Tarn cluster: N shards x R Tarantool replicas each, plus
# one or more tarn-server SQL front ends.
#
#   scripts/cluster.sh start [-n SHARDS] [-r REPLICAS] [-d DIR] [-p TARN_PORT]
#                            [-s SERVERS] [-w WAL_MODE] [-m MEMORY_MB] [-t THREADS]
#                            [-a]  (ack-on-prepare)  [-u]  (SQL servers reach storage via Unix sockets)
#   scripts/cluster.sh stop  [-d DIR]
#   scripts/cluster.sh status [-d DIR]
#
# Storage instance (shard i, replica j) listens on 127.0.0.1:$((BASE + 10*i + j)).
# tarn-server k listens on 127.0.0.1:$((TARN_PORT + k - 1)).
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-/opt/src/tarantool/build/src/tarantool}
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
CMD=${1:-}; shift || true
N=2; R=1; DIR=${TARN_DIR:-/tmp/tarn-cluster}; TPORT=4000; SERVERS=1; WAL=write; MEM=1024
THREADS=""; ACK=""; BASE=${TARN_BASE_PORT:-3300}; ITHREADS=1; UNIX=0; POOL=""
while getopts "n:r:d:p:s:w:m:t:ab:i:uP:" o; do
  case $o in
    n) N=$OPTARG;; r) R=$OPTARG;; d) DIR=$OPTARG;; p) TPORT=$OPTARG;; s) SERVERS=$OPTARG;;
    w) WAL=$OPTARG;; m) MEM=$OPTARG;; t) THREADS=$OPTARG;; a) ACK=--ack-on-prepare;; b) BASE=$OPTARG;;
    i) ITHREADS=$OPTARG;;
    u) UNIX=1;;
    P) POOL=$OPTARG;;
    *) echo "bad option"; exit 2;;
  esac
done

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
  ( cd "$DIR/s$i.$j" && TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$p TARN_WORKDIR="$DIR/s$i.$j" \
      TARN_REPLICAS="$(replicas_json "$i")" TARN_PEERS="$(peers_json)" TARN_MEMORY_MB=$MEM \
      TARN_WAL_MODE=$WAL TARN_LOG_LEVEL=${TARN_LOG_LEVEL:-5} TARN_IPROTO_THREADS=$ITHREADS \
      TARN_LISTEN_UNIX=$([ "$UNIX" = 1 ] && echo "$DIR/s$i.$j.sock") \
      exec "$T" "$ROOT/lua/storage.lua" >> "$DIR/s$i.$j.log" 2>&1 < /dev/null ) &
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
  out+="]}"
  echo "$out" > "$DIR/cluster.json"
}

case "$CMD" in
  start)
    mkdir -p "$DIR"
    echo "N=$N R=$R BASE=$BASE TPORT=$TPORT SERVERS=$SERVERS" > "$DIR/params"
    for i in $(seq 1 "$N"); do for j in $(seq 1 "$R"); do start_storage "$i" "$j"; done; done
    for i in $(seq 1 "$N"); do for j in $(seq 1 "$R"); do wait_port "$(port_of "$i" "$j")" || exit 1; done; done
    write_config
    # Wait until every shard has a writable leader with the Tarn schema.
    for i in $(seq 1 "$N"); do
      ok=0
      for _ in $(seq 600); do
        for j in $(seq 1 "$R"); do
          if "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$(port_of "$i" "$j")" < /dev/null > /dev/null 2>&1; then ok=1; break; fi
        done
        [ $ok = 1 ] && break; sleep 0.2
      done
      [ $ok = 1 ] || { echo "shard $i has no writable leader; see $DIR/s$i.*.log" >&2; exit 1; }
    done
    for k in $(seq 1 "$SERVERS"); do
      tp=$((TPORT + k - 1))
      TARN_LOG=${TARN_LOG:-info} "$TARN" --config "$DIR/cluster.json" --listen 127.0.0.1:$tp --node-id "$k" \
        ${THREADS:+--threads $THREADS} ${POOL:+--pool $POOL} $ACK >> "$DIR/tarn$k.log" 2>&1 < /dev/null &
      echo $! > "$DIR/tarn$k.pid"
      wait_port "$tp" || { tail -5 "$DIR/tarn$k.log"; exit 1; }
    done
    echo "cluster up: $N shard(s) x $R replica(s); SQL on 127.0.0.1:$TPORT..$((TPORT + SERVERS - 1)); dir $DIR"
    ;;
  stop)
    for f in "$DIR"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; done
    sleep 0.5
    for f in "$DIR"/*.pid; do [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null; done
    [ "${KEEP:-0}" = 1 ] || rm -rf "$DIR"
    ;;
  status)
    for f in "$DIR"/*.pid; do
      [ -f "$f" ] || continue
      if kill -0 "$(cat "$f")" 2>/dev/null; then echo "$(basename "$f" .pid) up"; else echo "$(basename "$f" .pid) DOWN"; fi
    done
    ;;
  *) sed -n 2,12p "$0"; exit 2;;
esac
