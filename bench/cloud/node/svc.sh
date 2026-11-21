#!/bin/bash
# Minimal process supervision on a benchmark node (no systemd needed, so the
# same commands work on the VMs and in the local rehearsal).
#
#   svc.sh start NAME CMD [ARG...]    start detached; env from NAME.env if present
#   svc.sh restart NAME               start NAME again with the command it was started with
#   svc.sh stop NAME                  SIGTERM, then SIGKILL after 15 s
#   svc.sh kill9 NAME                 SIGKILL now (fault injection)
#   svc.sh pid NAME                   print the pid if running
#   svc.sh stopall                    stop every service
#
# State: /data/run/svc/NAME.{cmd,pid,env}; output: /data/run/logs/NAME.log
set -u
D=/data/run/svc L=/data/run/logs
mkdir -p "$D" "$L"
op=${1:-}; name=${2:-}

alive() { [ -f "$D/$1.pid" ] && kill -0 "$(cat "$D/$1.pid")" 2> /dev/null; }

launch() {
  local n=$1
  # A new session, so that the service outlives this SSH session.
  setsid bash -c '
    ulimit -n 1048576 2> /dev/null || ulimit -n 65536
    ulimit -c unlimited
    [ -f "$2" ] && { set -a; . "$2"; set +a; }
    echo "=== start $(date -u +%FT%TZ)" >> "$3"
    exec bash "$1" >> "$3" 2>&1 < /dev/null' _ "$D/$n.cmd" "$D/$n.env" "$L/$n.log" &
  echo $! > "$D/$n.pid"
  sleep 0.2
  alive "$n" || { echo "$n exited at once:"; tail -20 "$L/$n.log"; return 1; }
}

case $op in
  start)
    shift 2
    alive "$name" && { echo "$name already running"; exit 1; }
    { printf 'exec'; printf ' %q' "$@"; echo; } > "$D/$name.cmd"
    launch "$name"
    ;;
  restart)
    alive "$name" && { echo "$name already running"; exit 1; }
    [ -f "$D/$name.cmd" ] || { echo "no service $name"; exit 1; }
    launch "$name"
    ;;
  stop)
    alive "$name" || exit 0
    p=$(cat "$D/$name.pid"); kill "$p"
    for _ in $(seq 150); do kill -0 "$p" 2> /dev/null || exit 0; sleep 0.1; done
    kill -9 "$p" 2> /dev/null; sleep 0.5
    ;;
  kill9)
    alive "$name" && kill -9 "$(cat "$D/$name.pid")"
    ;;
  pid)
    alive "$name" && cat "$D/$name.pid"
    ;;
  stopall)
    for f in "$D"/*.pid; do [ -f "$f" ] && "$0" stop "$(basename "$f" .pid)"; done
    ;;
  *) sed -n '2,12p' "$0"; exit 2;;
esac
