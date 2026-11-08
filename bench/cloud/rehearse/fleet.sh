#!/bin/bash
# A fake fleet on one Linux machine, for rehearsing a run without the cloud:
# every host is a network namespace with its own IP on a bridge, its own
# sshd, iptables and private /data (a bind mount in its own mount namespace).
# The root namespace plays the control node. hosts.json has the format
# tarn-bench writes, so orchestrate.py runs unchanged (with REHEARSAL=1).
#
#   bench/cloud/rehearse/fleet.sh up DIR      DIR/hosts.json, DIR/id_ed25519
#   bench/cloud/rehearse/fleet.sh down DIR
#
# Hosts: 1 shard x 3 replicas in three fake zones, two SQL nodes, one load
# generator. Needs root, iproute2, iptables, openssh-server.
set -euo pipefail
CMD=${1:?up|down}; DIR=$(mkdir -p "${2:?DIR}" && cd "$2" && pwd)
BR=br-tb NET=10.42.0
# name role zone ip-suffix [shard replica | index]
HOSTS="s1r1:storage:z-a:11:1:1 s1r2:storage:z-b:12:1:2 s1r3:storage:z-c:13:1:3 sql1:sql:z-a:21:1 sql2:sql:z-b:22:2 load1:load:z-b:31:1"

down() {
  for h in $HOSTS; do
    n=${h%%:*}
    [ -f "$DIR/$n/sshd.pid" ] && kill "$(cat "$DIR/$n/sshd.pid")" 2> /dev/null || true
    ip netns pids "$n" 2> /dev/null | xargs -r kill -9 2> /dev/null || true
    ip netns del "$n" 2> /dev/null || true
  done
  [ -f "$DIR/control-sshd.pid" ] && kill "$(cat "$DIR/control-sshd.pid")" 2> /dev/null || true
  ip link del "$BR" 2> /dev/null || true
}

if [ "$CMD" = down ]; then down; echo "fleet down"; exit 0; fi

down
id bench > /dev/null 2>&1 || useradd -m -s /bin/bash bench
echo 'bench ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/bench
install -d -o bench -g bench -m 700 /home/bench/.ssh
[ -f "$DIR/id_ed25519" ] || ssh-keygen -q -t ed25519 -N "" -C rehearsal -f "$DIR/id_ed25519"
install -o bench -g bench -m 600 "$DIR/id_ed25519.pub" /home/bench/.ssh/authorized_keys
mkdir -p /run/sshd /data && chmod 1777 /data
cat > "$DIR/sshd_config" << EOF
Port 22
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
UsePAM no
PermitRootLogin no
MaxStartups 200
MaxSessions 200
Subsystem sftp internal-sftp
HostKey /etc/ssh/ssh_host_ed25519_key
EOF
[ -f /etc/ssh/ssh_host_ed25519_key ] || ssh-keygen -q -t ed25519 -N "" -f /etc/ssh/ssh_host_ed25519_key

ip link add "$BR" type bridge
ip addr add "$NET.1/24" dev "$BR"
ip link set "$BR" up
/usr/sbin/sshd -f "$DIR/sshd_config" -o "ListenAddress=$NET.1" -o "PidFile=$DIR/control-sshd.pid"

for h in $HOSTS; do
  IFS=: read -r n role zone sfx a b <<< "$h"
  mkdir -p "$DIR/$n/data" "$DIR/$n/tmp"; chmod 1777 "$DIR/$n/data" "$DIR/$n/tmp"
  ip netns add "$n"
  ip link add "v-$n" type veth peer name eth0 netns "$n"
  ip link set "v-$n" master "$BR" up
  ip -n "$n" addr add "$NET.$sfx/24" dev eth0
  ip -n "$n" link set eth0 up
  ip -n "$n" link set lo up
  ip -n "$n" route add default via "$NET.1"
  # sshd in its own mount namespace, where /data and /tmp are this host's
  # (servers keep instance locks in /tmp, as on separate machines).
  ip netns exec "$n" unshare --mount --propagation private bash -c "
    mount --bind '$DIR/$n/data' /data
    mount --bind '$DIR/$n/tmp' /tmp
    exec /usr/sbin/sshd -f '$DIR/sshd_config' -o 'ListenAddress=$NET.$sfx' -o 'PidFile=$DIR/$n/sshd.pid'"
done

python3 - "$DIR" "$HOSTS" "$NET" << 'EOF'
import json, sys
d, hosts, net = sys.argv[1], sys.argv[2].split(), sys.argv[3]
out = [{"name": "control", "role": "control", "zone": "z-a", "machine": "rehearsal", "internal_ip": f"{net}.1"}]
for h in hosts:
    n, role, zone, sfx, *rest = h.split(":")
    e = {"name": n, "role": role, "zone": zone, "machine": "rehearsal", "internal_ip": f"{net}.{sfx}"}
    if role == "storage":
        e.update(shard=int(rest[0]), replica=int(rest[1]))
    else:
        e["index"] = int(rest[0])
    out.append(e)
json.dump({"topology": "rehearsal", "region": "rehearsal", "zones": ["z-a", "z-b", "z-c"], "rf": 3,
           "shards": 1, "hosts": out, "run_id": "rehearsal", "ssh_user": "bench"},
          open(f"{d}/hosts.json", "w"), indent=1)
EOF
chmod 644 "$DIR/hosts.json"
echo "fleet up: $(echo "$HOSTS" | wc -w) hosts + control; $DIR/hosts.json"
