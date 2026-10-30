# Production profile

Use `scripts/production.py` and `tarn-server --production` for the explicit
Linux, 64-bit production profile. The ordinary `--shard`, `--allow-all`,
`--user NAME:PASSWORD` and `lua/storage.lua` defaults are development settings;
they emit a development warning. They are not evidence of durable, encrypted,
authenticated deployment. A production gateway refuses development credential
and durability overrides, plaintext storage endpoints and incomplete inventory.

The profile requires odd RF >= 3, distinct declared failure domains per shard,
fsync WAL on every replica, majority synchronous quorum, strict election
fencing, and one ready leader per shard. The gateway checks every endpoint's
shard ID, distinct instance UUID, common replicaset UUID, WAL, quorum, local
profile and domain before opening its SQL listener. It probes actual access
rights too. Failed health checks exit nonzero. Membership changes require
updating and distributing the TLS route/domain inventory first.

A zone label is an operator assertion. Neither a configuration check nor three
processes on one host proves independent power, disks, networks, or clocks.
Place replicas in genuinely independent failure domains, keep at least a
majority available, and verify the inventory against the hosting control plane.
Use storage that honors fsync through the whole device/controller stack; fsync
cannot repair a device that acknowledges volatile writes as durable.

## Transport and accounts

MySQL uses native SSLRequest negotiation, TLS 1.2/1.3, certificate-based server
identity verification at the client, and required mysql_native_password
accounts inside TLS. The server only advertises SSL when an identity loads;
production refuses a plaintext handshake before authentication. `SHOW STATUS
LIKE 'Ssl_%'` reports the actual protocol and cipher. Optional
`mysql_tls.client_ca_file` requires a client certificate as well as the password.
All SQL accounts currently have Tarn's existing SQL capabilities; this profile
does not add per-database SQL GRANT/REVOKE or tenant isolation.

Tarantool Community Edition does not provide native binary/replication TLS.
`tarn-tls` supplies mutual TLS on **every network storage link**, including
replication, recovery and clock sampling. Tarantool listens only on a Unix
socket in a mode-0700 service-owned directory. Per-peer Unix sockets connect
the Lua clients to verified remote TLS endpoints. Certificates must match the
host/IP in `tls://HOST:PORT`; there is no name override, insecure verifier,
plaintext fallback, or reconnect/replay within an application stream. The local
service UID and root can access these private sockets and are trusted.
[Upstream TLS availability](https://www.tarantool.io/en/doc/latest/singlepage/).

The fixed `tarn_service` account has SESSION/USAGE, READ only on SQL data
spaces, and EXECUTE only on the explicit Lua/native RPC allowlist. It cannot
EVAL, call arbitrary globals, read system spaces, write tuples directly, manage
users, invoke test hooks, or call the native state installer. The account is
trusted to run SQL's data/DDL/cluster operations through these RPCs; these are
privileged APIs, not an untrusted end-user sandbox. Functions use Tarantool's
setuid procedure mechanism; their bodies and the service binaries are trusted.
The separate `tarn_repl` account has only session/usage and the replication
role. Production removes inherited/public, guest and legacy `tarn` grants.
Existing unexpected function definitions cause refusal, not a permissive
fallback. Internal net.box clients request no schema-view privileges. Development
rolling upgrades retain the predecessor EVAL role probe for mixed storage;
production disables that fallback, and the switch cannot be set in JSON.

## Provision and start

1. Install release `tarn-server`, `tarn-tls`, the pinned Tarantool version,
   Lua sources and, optionally, `libtarn_storage.so`. Give each node its own
   unprivileged service UID, persistent data directory, private runtime
   directory, and service supervisor. Keep the runtime directory on local
   storage. The profile checks ownership/modes and never unlinks a socket for
   you; after a crash, verify the old owner is stopped before removing stale
   sockets. Set supervisor restart/backoff and OS process/file-descriptor limits.
2. Issue a storage CA and distinct node/gateway identities. Node certificates
   need serverAuth and clientAuth EKUs and the node's DNS/IP SAN. The gateway
   needs a clientAuth storage identity and a separate serverAuth MySQL
   identity matching the SQL address. Use your managed CA/secret store;
   distribute only the required trust roots and identities to each host.
3. Create two **different**, random 32..128-byte base64url storage secrets in
   `service.secret` and `replication.secret`. Create a private JSON account map
   in `mysql-users.json`, for example `{"operator":"<random password>"}`;
   each SQL password must have at least 20 bytes. Write secrets through a
   secret-store file projection or private descriptor/file, never argv or
   inline cluster JSON. Keys and secret files must be regular files owned by
   the service UID with no group/other access (0600 or 0400). Final symlinks
   are rejected; project by atomic rename of private regular files. Do not put
   secrets in shell history, logs or public artifacts.
4. Copy and edit [the inventory](../deploy/production/inventory.example.json).
   Set actual DNS names, domains, paths, host prerequisites, memory pools,
   client admission and a unique gateway node ID. The example is a starting
   policy, not a capacity recommendation. Render on a trusted deployment host:

   ```sh
   python3 scripts/production.py render inventory.json generated
   ```

   Distribute each node's `NAME.storage.json`, `NAME.tls.json`, required Lua
   files, secrets and certificates to its host at the configured paths. The
   service/replication secrets must agree across the shard deployment.
   Distribute `cluster.json` and `gateway.production.json` to each gateway.
5. Under the service UID, supervise these two commands on each storage host:

   ```sh
   tarn-tls /etc/tarn/generated/s1a.tls.json
   python3 scripts/production.py storage /etc/tarn/generated/s1a.storage.json
   ```

   Start all replicas; the configured first replica bootstraps the schema.
   Set `TARN_NATIVE` to the installed `libtarn_storage.so`, or `0` to use Lua.
   Other inherited `TARN_*` development switches are rejected by the launcher.
   Production data directories must be fresh or already migrated to this
   profile. For a development installation, stop external access, take and
   validate a backup, migrate account/function grants offline, then validate
   the production profile before reopening access. Never reuse a publicly
   reachable development listener as an in-place security migration.
6. Run checks and start the gateway:

   ```sh
   python3 scripts/production.py health /etc/tarn/generated --gateway /usr/local/bin/tarn-server
   tarn-server --config /etc/tarn/generated/cluster.json --production /etc/tarn/generated/gateway.production.json --listen 0.0.0.0:3306
   ```

   Firewall the storage TLS ports to authorized peers/gateways; expose only the
   SQL TLS listener to applications. Run health checks under the service UID
   at deployment and periodically. A nonzero exit is actionable; retain the
   reason rather than changing the profile to pass it.

## Resource and clock checks

The gateway and storage check online CPUs, Linux cgroup-v2 CPU quota when
present, physical/cgroup memory limits, and available disk on their data
filesystem against explicit positive minima. Storage memory pools must fit
within 75% of the declared minimum and observed memory. Keep one storage
instance per declared host budget, or account for the aggregate separately.
The combined SQL server and encoded-response accounting budgets must leave 25% of declared host memory
as headroom. TLS socket envelopes are charged before admitting a connection;
result charges remain held until encrypted output flushes. Minima are startup
prerequisites, not reservations or a bound on process RSS,
query plans/results, allocator overhead, WAL growth, bridge buffers or page
cache. Size admission and pools using the actual workload; monitor process,
container, disk and RPC pressure continuously. Nested/custom cgroup mounts and
host-wide sharing need independent supervisor/control-plane validation.

The existing immutable clock policy, drift/gap checks, peer certification,
quorum freshness and fail-closed storage guards remain active. Leader health
requires a current clock certificate; connectivity and ro=false are not a
clock proof. Operate independent time synchronization and monitor external
clock accuracy; agreeing peers cannot detect common-mode time errors. A TLS
certificate validity failure is separate from Tarn's transaction clock policy.

## Certificate and secret rotation

Use overlapping trust and rolling restarts: distribute old+new CA bundles to
all SQL clients, storage bridges and gateways; restart consumers to load that
bundle; issue/install new leaf chains and private keys by atomic rename;
restart bridges one replica at a time, waiting for deployment health and
replication rejoin between replicas; restart gateways with draining/transaction
handling appropriate to the application. Once every connection uses a new
identity, remove the old roots and roll consumers again. Keep a majority live
and check SAN/EKU/validity before each step (`openssl x509 -checkend 86400`).
A running process snapshots its TLS configuration; replacing a PEM file alone
does not reload it or revoke existing connections. For a compromised key,
close/drain affected connections and remove its trust, rather than assuming a
file replacement revokes sessions. A cut connection can leave a mutation
uncertain; honor 1180/40003 and reconcile its outcome, never blindly replay it.
Storage-password rotation currently needs a coordinated maintenance window for
the fixed accounts and process restarts; there is no dual-password rollout.

The automated fixture exercises CA overlap, leaf renewal and old-root
retirement. It also checks wrong CA/name, expired certificates, missing client
identity, plaintext refusal, bad passwords, private secret modes, RPC/data
permissions, a coalesced SSLRequest/ClientHello, RF3 recovery and an observed
lost mutation reply with exact single-application readback.

## Driver matrix and costs

`python3 tests/production.py --bin target/release --work /tmp/tarn-production --native target/release/libtarn_storage.so --require-drivers`
runs the real deployment and retains source hashes/configurations/results.
The required matrix covers PyMySQL (verified TLS/auth/DDL/transactions),
MySQL Connector/Python (verified TLS/native-auth switch/binary prepares), and
MariaDB or MySQL CLI (CA/name verification/auth/query). Use CA and identity
verification in each client; enabling encryption alone does not authenticate
the server. Other drivers need their own compatibility/verification run.

The fixture separates MySQL encryption, storage/replication encryption, and
fsync/write WAL. It keeps RF3, RPC, connection count, data shape and transport
hops matched, alternates MySQL modes, and retains three repetitions and raw
latencies, including connection/handshake time. The plaintext bridge example
is loopback-only and explicitly unsafe; a test-only wrapper varies WAL before
startup. Neither is a supported production override, and the production
health gate rejects these controls. A one-host sequential counter/read study
measures local cost, not independent-failure durability, throughput capacity,
production latency, or a universal percentage. Use repeated matched release
measurements on deployment hardware before making those claims.
