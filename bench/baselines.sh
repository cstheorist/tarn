#!/bin/bash
# Start/stop single-node baseline servers with comparable durability settings
# (redo log written at commit, fsynced about once a second -- the same
# guarantee as Tarantool's wal_mode=write that Tarn's storage uses here).
#
#   bench/baselines.sh start mariadb|mysql   -> listens on 3407 / 3408, user sb (no password)
#   bench/baselines.sh stop  mariadb|mysql
set -u
CMD=$1 WHICH=$2
BASE=${BASELINE_DIR:-/tmp/tarn-baselines}
ISO=${ISOLATION:-REPEATABLE-READ}
mkdir -p "$BASE"
case $WHICH in
  mariadb) PORT=3407; BIN=/usr/sbin/mariadbd; CLIENT=/usr/bin/mariadb;;
  mysql)   PORT=3408; BIN=/opt/mysql/bin/mysqld; CLIENT=/usr/bin/mariadb;;
  *) echo "mariadb or mysql"; exit 2;;
esac
D=$BASE/$WHICH
SOCK=$D/mysqld.sock

write_cnf() {
  cat > "$D/my.cnf" <<EOF
[mysqld]
datadir=$D/data
port=$PORT
socket=$SOCK
pid-file=$D/mysqld.pid
bind-address=127.0.0.1
user=root
skip-log-bin
innodb_buffer_pool_size=1G
innodb_flush_log_at_trx_commit=2
innodb_doublewrite=0
max_connections=2000
max_prepared_stmt_count=100000
performance_schema=OFF
transaction_isolation=$ISO
EOF
  if [ "$WHICH" = mariadb ]; then
    echo "innodb_log_file_size=512M" >> "$D/my.cnf"
    echo "query_cache_type=0" >> "$D/my.cnf"
  else
    echo "innodb_redo_log_capacity=1G" >> "$D/my.cnf"
    echo "mysqlx=OFF" >> "$D/my.cnf"
  fi
}

case $CMD in
  start)
    mkdir -p "$D"
    write_cnf
    if [ ! -d "$D/data" ]; then
      if [ "$WHICH" = mariadb ]; then
        mariadb-install-db --defaults-file="$D/my.cnf" --auth-root-authentication-method=normal \
          > "$D/init.log" 2>&1 || { tail "$D/init.log"; exit 1; }
      else
        "$BIN" --defaults-file="$D/my.cnf" --initialize-insecure > "$D/init.log" 2>&1 || { tail "$D/init.log"; exit 1; }
      fi
    fi
    "$BIN" --defaults-file="$D/my.cnf" > "$D/server.log" 2>&1 &
    for _ in $(seq 300); do [ -S "$SOCK" ] && break; sleep 0.1; done
    sleep 0.5
    "$CLIENT" -S "$SOCK" -u root -e "CREATE USER IF NOT EXISTS 'sb'@'%' IDENTIFIED BY ''; GRANT ALL ON *.* TO 'sb'@'%'; CREATE USER IF NOT EXISTS 'sb'@'localhost' IDENTIFIED BY ''; GRANT ALL ON *.* TO 'sb'@'localhost';" \
      || { tail "$D/server.log"; exit 1; }
    echo "$WHICH up on 127.0.0.1:$PORT ($("$CLIENT" -S "$SOCK" -u root -N -e 'select version()'), isolation $ISO)"
    ;;
  stop)
    [ -f "$D/mysqld.pid" ] && kill "$(cat "$D/mysqld.pid")" 2>/dev/null
    for _ in $(seq 100); do [ -f "$D/mysqld.pid" ] || break; sleep 0.2; done
    [ "${KEEP:-0}" = 1 ] || rm -rf "$D"
    ;;
esac
