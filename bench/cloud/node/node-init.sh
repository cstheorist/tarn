#!/bin/bash
# Startup script of every benchmark VM (GCE runs it as root at each boot).
# Prepares /data (the local NVMe SSD where there is one) and the kernel
# settings the three systems' deployment guides ask for. Idempotent.
set -u
exec >> /var/log/tarn-bench-init.log 2>&1
echo "init $(date -u +%FT%TZ)"

# /data: the local SSD, ext4, no atime; a directory on the boot disk otherwise.
ssd=/dev/disk/by-id/google-local-nvme-ssd-0
mkdir -p /data
if [ -e "$ssd" ] && ! mountpoint -q /data; then
  if ! blkid "$ssd" > /dev/null 2>&1; then
    # Initialise the inode tables and journal now, not in the background
    # during the benchmark.
    mkfs.ext4 -F -q -E lazy_itable_init=0,lazy_journal_init=0 -m 0 "$ssd"
  fi
  mount -o noatime,nodiscard "$ssd" /data
fi
chmod 1777 /data

# Kernel and limits (TiDB, YugabyteDB and Tarantool guides agree on these).
cat > /etc/sysctl.d/90-tarn-bench.conf << 'EOF'
net.core.somaxconn = 32768
net.core.netdev_max_backlog = 32768
net.ipv4.tcp_max_syn_backlog = 32768
net.ipv4.ip_local_port_range = 10000 65000
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_syncookies = 0
vm.swappiness = 0
vm.overcommit_memory = 1
fs.file-max = 4194304
kernel.core_pattern = /data/core.%e.%p
EOF
sysctl -q --system
echo never > /sys/kernel/mm/transparent_hugepage/enabled || true
echo never > /sys/kernel/mm/transparent_hugepage/defrag || true
cat > /etc/security/limits.d/90-tarn-bench.conf << 'EOF'
*  soft  nofile  1048576
*  hard  nofile  1048576
*  soft  nproc   1048576
*  hard  nproc   1048576
*  soft  core    unlimited
*  hard  core    unlimited
*  soft  stack   10240
EOF
# Many parallel SSH sessions from the control node.
if ! grep -q '^MaxStartups 200' /etc/ssh/sshd_config; then
  printf 'MaxStartups 200\nMaxSessions 200\n' >> /etc/ssh/sshd_config
  systemctl reload ssh 2> /dev/null || systemctl reload sshd 2> /dev/null || true
fi
# YugabyteDB's initdb needs en_US.UTF-8 (generated from the image's locales
# package; workers have no internet access).
if ! locale -a 2> /dev/null | grep -qi '^en_US.utf8$'; then
  locale-gen en_US.UTF-8 > /dev/null 2>&1 || true
fi
# Nothing else should run during the benchmark.
systemctl stop unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer 2> /dev/null || true
systemctl disable unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer 2> /dev/null || true
# Clocks: GCE's metadata NTP via chrony is the default; record the state.
chronyc tracking 2> /dev/null | head -4 || true

touch /var/run/tarn-bench-ready
echo "ready $(date -u +%FT%TZ)"
