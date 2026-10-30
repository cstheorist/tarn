#!/usr/bin/env python3
"""Live production-profile security, RF3 recovery, driver and matched-cost fixture.
Linux + OpenSSL + Tarantool 3.9 + PyMySQL. No production credentials are used.
Run: python3 tests/production.py --bin target/release --work /tmp/tarn-production
The fixture's failure-domain names are logical labels on one host, not
physical placement evidence. Every subprocess, config, and result is retained.
"""

import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import statistics
import struct
import subprocess
import sys
import time

import pymysql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import production as deploy


def run(cmd, **kw):
    r = subprocess.run(
        [str(x) for x in cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kw
    )
    if r.returncode:
        raise RuntimeError(
            "subprocess failed: "
            + str(cmd[0])
            + ": "
            + r.stderr.decode(errors="replace")[-4000:]
        )
    return r


def private(path, text):
    path.write_text(text)
    path.chmod(0o600)


def pki(d):
    d.mkdir(mode=0o700)
    for ca in ("ca", "other"):
        run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-keyout",
                d / (ca + ".key"),
                "-out",
                d / (ca + ".pem"),
                "-subj",
                "/CN=Tarn fixture " + ca,
                "-days",
                "2",
            ]
        )
        (d / (ca + ".key")).chmod(0o600)
    for name, san, days in [
        ("server", "DNS:localhost,IP:127.0.0.1", "2"),
        ("wrong", "DNS:wrong.invalid", "2"),
        ("expired", "DNS:localhost,IP:127.0.0.1", "0"),
    ]:
        run(
            [
                "openssl",
                "req",
                "-new",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-keyout",
                d / (name + ".key"),
                "-out",
                d / (name + ".csr"),
                "-subj",
                "/CN=" + name,
            ]
        )
        (d / (name + ".key")).chmod(0o600)
        (d / (name + ".ext")).write_text(
            "subjectAltName=" + san + "\nextendedKeyUsage=serverAuth,clientAuth\n"
        )
        run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                d / (name + ".csr"),
                "-CA",
                d / "ca.pem",
                "-CAkey",
                d / "ca.key",
                "-CAcreateserial",
                "-out",
                d / (name + ".pem"),
                "-days",
                days,
                "-extfile",
                d / (name + ".ext"),
            ]
        )


class Fixture:
    def __init__(self, args):
        self.args = args
        self.work = args.work.resolve()
        if self.work.exists():
            raise ValueError(
                "fixture work directory must be new (retained evidence is never overwritten)"
            )
        self.work.mkdir(mode=0o700, parents=True)
        self.bin = args.bin.resolve()
        self.children = []
        self.nodes = []
        self.checks = []
        self.result = {
            "checks": self.checks,
            "cost": [],
            "source": os.getenv("SOURCE_REVISION", "unavailable-in-container"),
            "placement": "single-host logical RF3 fixture; no physical failure-domain claim",
            "gateway_build": str(self.bin),
            "commands": [],
            "versions": {},
        }
        self.cert = self.work / "cert"
        pki(self.cert)
        self.password = secrets.token_urlsafe(32)
        private(self.work / "service.secret", secrets.token_urlsafe(32) + "\n")
        private(self.work / "replication.secret", secrets.token_urlsafe(32) + "\n")
        private(self.work / "users.json", json.dumps({"test": self.password}))
        self.result["binaries"] = {
            str(p): __import__("hashlib").sha256(p.read_bytes()).hexdigest()
            for p in [
                self.bin / "tarn-server",
                self.bin / "tarn-tls",
                self.bin / "examples/security-cost",
                self.bin / "examples/unsafe-plaintext-bridge",
            ]
            if p.exists()
        }
        if args.native:
            self.result["native_sha256"] = (
                __import__("hashlib").sha256(args.native.read_bytes()).hexdigest()
            )
        self.port = args.port
        self.base = args.storage_port
        host = dict(
            min_cpus=1,
            min_memory_bytes=256 * 1024**2,
            min_free_disk_bytes=64 * 1024**2,
            data_path=str(self.work),
        )
        tls = dict(
            ca_file=str(self.cert / "ca.pem"),
            cert_file=str(self.cert / "server.pem"),
            key_file=str(self.cert / "server.key"),
        )
        self.server_tls = dict(cert_file=tls["cert_file"], key_file=tls["key_file"])
        for i in range(3):
            node = self.work / ("n" + str(i))
            node.mkdir(mode=0o700)
            self.nodes.append(
                dict(
                    name="n" + str(i),
                    uri=f"tls://127.0.0.1:{self.base+i}",
                    shard_id=1,
                    failure_domain="logical-fixture-" + str(i),
                    socket_dir=str(node / "run"),
                    workdir=str(node / "data"),
                    cert_file=tls["cert_file"],
                    key_file=tls["key_file"],
                )
            )
            deploy.private_dir(node / "run")
            deploy.private_dir(node / "data")
        inv = dict(
            nodes=self.nodes,
            tls=tls,
            mysql_tls=self.server_tls,
            users_file=str(self.work / "users.json"),
            service_password_file=str(self.work / "service.secret"),
            replication_password_file=str(self.work / "replication.secret"),
            memtx_mb=64,
            vinyl_memory_mb=16,
            vinyl_cache_mb=0,
            storage_host=host,
            gateway_host=host,
            bridge_max_connections=256,
            gateway_max_connections=32,
            gateway_node_id=139,
            pool_size=2,
        )
        deploy.dump(self.work / "inventory.json", inv)
        self.config = self.work / "config"
        deploy.render(self.work / "inventory.json", self.config)
        self.result["versions"]["openssl"] = (
            run(["openssl", "version"]).stdout.decode().strip()
        )
        self.result["versions"]["tarantool"] = (
            run(["tarantool", "-v"]).stdout.decode().splitlines()[0]
        )
        self.result["versions"]["pymysql"] = getattr(pymysql, "__version__", "unknown")
        self.result["source_files"] = {
            str(p.relative_to(ROOT)): __import__("hashlib")
            .sha256(p.read_bytes())
            .hexdigest()
            for pattern in (
                "crates/**/*.rs",
                "crates/**/Cargo.toml",
                "lua/*.lua",
                "scripts/production.py",
                "tests/production.py",
                "Cargo.toml",
                "Cargo.lock",
            )
            for p in sorted(ROOT.glob(pattern))
        }

    def spawn(self, name, cmd, env=None):
        self.result["commands"].append(dict(name=name, argv=list(map(str, cmd))))
        out = (self.work / (name + ".log")).open("ab")
        p = subprocess.Popen(list(map(str, cmd)), stdout=out, stderr=out, env=env)
        out.close()
        self.children.append(p)
        return p

    def record(self, name, details=True):
        print("PASS", name, flush=True)
        self.checks.append(dict(name=name, result=details))
        self.save()

    def save(self):
        deploy.dump(self.work / "result.json", self.result)

    def context(self, ca="ca.pem", identity=False):
        c = ssl.create_default_context(cafile=str(self.cert / ca))
        if identity:
            c.load_cert_chain(self.cert / "server.pem", self.cert / "server.key")
        return c

    def sql(self, password=None, tls=True, ca="ca.pem"):
        return pymysql.connect(
            host="127.0.0.1",
            port=self.port,
            user="test",
            password=self.password if password is None else password,
            ssl=self.context(ca) if tls else None,
            connect_timeout=3,
            read_timeout=8,
            write_timeout=8,
            autocommit=True,
        )

    def query(self, sql, c=None):
        own = c is None
        c = c or self.sql()
        try:
            with c.cursor() as q:
                q.execute(sql)
                return q.fetchall()
        finally:
            if own:
                c.close()

    def reject(self, name, fn):
        try:
            value = fn()
        except (OSError, RuntimeError, pymysql.MySQLError) as e:
            self.record(name, type(e).__name__)
            return
        if hasattr(value, "close"):
            value.close()
        raise AssertionError("negative control accepted: " + name)

    def health(self):
        try:
            return run(
                [
                    self.bin / "tarn-server",
                    "--config",
                    self.config / "cluster.json",
                    "--production",
                    self.config / "gateway.production.json",
                    "--check-deployment",
                    "--threads",
                    "2",
                ]
            )
        except RuntimeError as e:
            (self.work / "last-health.log").write_text(str(e))
            raise

    def wait(self, name, fn, seconds=90):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            try:
                value = fn()
                if value is not False:
                    return value
            except Exception as e:
                last = e
            time.sleep(0.2)
        probe = self.work / "diagnostic.lua"
        probe.write_text(
            "local n=require('net.box');local j=require('json');local f=assert(io.open(arg[2]));local p=f:read('*l');f:close();local c=n.connect(arg[1],{fetch_schema=false,user='tarn_service',password=p,wait_connected=1});for _,fn in ipairs({'tarn_deployment_status','tarn_clock_status'}) do local ok,r=pcall(c.call,c,fn,{}, {timeout=1});print(fn,j.encode({ok,r})) end;c:close();os.exit()"
        )
        with (self.work / "timeout-diagnostics.log").open("ab") as out:
            for n in self.nodes:
                subprocess.run(
                    [
                        "tarantool",
                        str(probe),
                        "unix/:" + n["socket_dir"] + "/storage.sock",
                        str(self.work / "service.secret"),
                    ],
                    stdout=out,
                    stderr=out,
                    timeout=5,
                )
        raise AssertionError(name + ": " + str(last))

    def start(self):
        self.bridges = [
            self.spawn(
                "bridge-" + n["name"],
                [self.bin / "tarn-tls", self.config / (n["name"] + ".tls.json")],
            )
            for n in self.nodes
        ]
        env = {k: v for k, v in os.environ.items() if not k.startswith("TARN_")}
        env["TARN_NATIVE"] = (
            str(self.args.native.resolve()) if self.args.native else "0"
        )
        self.storages = [
            self.spawn(
                "storage-" + n["name"],
                [
                    sys.executable,
                    ROOT / "scripts/production.py",
                    "storage",
                    self.config / (n["name"] + ".storage.json"),
                ],
                env,
            )
            for n in self.nodes
        ]
        self.wait("all fsync replicas ready", self.health)
        self.record(
            "RF3 fsync identity, majority quorum, strict fencing, clock readiness and resource preflight"
        )
        self.gateway = self.spawn(
            "gateway",
            [
                self.bin / "tarn-server",
                "--config",
                self.config / "cluster.json",
                "--production",
                self.config / "gateway.production.json",
                "--listen",
                f"0.0.0.0:{self.port}",
                "--threads",
                "2",
            ],
            dict(os.environ, RUST_MIN_STACK="33554432"),
        )
        self.wait("SQL listener", lambda: self.query("SELECT 1"))

    def coalesced_ssl_request(self):
        import hashlib

        raw = socket.create_connection(("127.0.0.1", self.port), timeout=3)

        def exact(n):
            b = bytearray()
            while len(b) < n:
                part = raw.recv(n - len(b))
                assert part
                b.extend(part)
            return bytes(b)

        header = exact(4)
        greeting = exact(int.from_bytes(header[:3], "little"))
        pos = greeting.index(0, 1) + 1 + 4
        salt = greeting[pos : pos + 8] + greeting[pos + 27 : pos + 39]
        assert len(salt) == 20
        incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
        tls = self.context().wrap_bio(
            incoming, outgoing, server_side=False, server_hostname="127.0.0.1"
        )
        try:
            tls.do_handshake()
        except ssl.SSLWantReadError:
            pass
        caps = 1 | (1 << 9) | (1 << 11) | (1 << 15) | (1 << 19)
        request = struct.pack("<IIB23s", caps, 16 * 1024**2, 45, b"\0" * 23)
        raw.sendall(bytes([32, 0, 0, 1]) + request + outgoing.read())

        def flush():
            data = outgoing.read()
            if data:
                raw.sendall(data)

        while True:
            try:
                tls.do_handshake()
                flush()
                break
            except ssl.SSLWantReadError:
                flush()
                incoming.write(raw.recv(65536))
        a = hashlib.sha1(self.password.encode()).digest()
        b = hashlib.sha1(a).digest()
        mix = hashlib.sha1(salt + b).digest()
        response = bytes(x ^ y for x, y in zip(a, mix))
        payload = (
            request + b"test\0" + bytes([20]) + response + b"mysql_native_password\0"
        )
        tls.write(len(payload).to_bytes(3, "little") + b"\2" + payload)
        flush()

        def encrypted_exact(n):
            b = bytearray()
            while len(b) < n:
                try:
                    b.extend(tls.read(n - len(b)))
                except ssl.SSLWantReadError:
                    flush()
                    incoming.write(raw.recv(65536))
            return bytes(b)

        h = encrypted_exact(4)
        reply = encrypted_exact(int.from_bytes(h[:3], "little"))
        assert reply[0] == 0, reply
        tls.write(bytes([1, 0, 0, 0, 14]))
        flush()
        h = encrypted_exact(4)
        assert encrypted_exact(int.from_bytes(h[:3], "little"))[0] == 0
        raw.close()
        self.record(
            "SSLRequest and ClientHello coalesced in one TCP write preserve TLS bytes and authenticate"
        )

    def security(self):
        self.coalesced_ssl_request()
        self.reject("plaintext SQL refused", lambda: self.sql(tls=False))
        self.reject(
            "wrong SQL password refused over TLS",
            lambda: self.sql(password="incorrect"),
        )
        self.reject("untrusted SQL CA refused", lambda: self.sql(ca="other.pem"))
        self.record(
            "PyMySQL verified TLS query and native-password authentication",
            self.query("SHOW STATUS LIKE 'Ssl_%'"),
        )
        with self.sql() as c:
            self.query("CREATE DATABASE IF NOT EXISTS secure_test", c)
            self.query("USE secure_test", c)
            for engine in ("memtx", "vinyl"):
                self.query(
                    f"CREATE TABLE {engine}_t (id INT PRIMARY KEY, n INT, KEY by_n(n)) ENGINE={engine}",
                    c,
                )
                self.query(f"INSERT INTO {engine}_t VALUES (1,10)", c)
                self.query("START TRANSACTION", c)
                assert self.query(f"SELECT * FROM {engine}_t WHERE id=1", c) == (
                    (1, 10),
                )
                self.query(f"UPDATE {engine}_t SET n=n+1 WHERE id=1", c)
                self.query("COMMIT", c)
                self.query(f"ALTER TABLE {engine}_t ADD COLUMN extra INT DEFAULT 0", c)
                assert self.query(f"SELECT * FROM {engine}_t ORDER BY n", c) == (
                    (1, 11, 0),
                )
            self.record(
                "authenticated TLS DDL, native data reads and transactions on memtx and vinyl"
            )
        # Drive storage directly through net.box over private sockets for
        # exact authorization checks; the mTLS network test follows below.
        probe = self.work / "acl.lua"
        probe.write_text("""local json=require('json'); local net=require('net.box');
local f=assert(io.open(arg[2])); local password=f:read('*l'); f:close()
local c=net.connect(arg[1],{fetch_schema=false,user='tarn_service',password=password,wait_connected=5})
assert(c:is_connected()); assert(type(c:call('tarn_role'))=='boolean')
local info=c:call('tarn_info'); if not info.ro then print(json.encode(info.native)) end
for _, fn in ipairs({function() c:eval('return 1') end,
 function() c:call('box.schema.user.create',{'intruder'}) end,
 function() c:call('tarn_clock_test_time',{0,0}) end,
 function() c:call('box.session.user') end}) do assert(not pcall(fn),'service privilege escape') end
local guest=net.connect(arg[1],{fetch_schema=false,wait_connected=.3}); if guest:is_connected() then assert(not pcall(guest.call,guest,'tarn_role')) end; guest:close()
local repl=net.connect(arg[1],{fetch_schema=false,user='tarn_repl',password=(function() local x=assert(io.open(arg[3])); local p=x:read('*l');x:close();return p end)(),wait_connected=5})
assert(repl:is_connected()); assert(not pcall(repl.call,repl,'tarn_role')); assert(not pcall(repl.eval,repl,'return 1')); repl:close();c:close();os.exit(0)
""")
        # Any replica supports raw role diagnostics and the same ACL schema.
        native_info = []
        for n in self.nodes:
            r = run(
                [
                    "tarantool",
                    probe,
                    "unix/:" + n["socket_dir"] + "/storage.sock",
                    self.work / "service.secret",
                    self.work / "replication.secret",
                ]
            )
            if r.stdout.strip():
                native_info.append(json.loads(r.stdout))
        assert len(native_info) == 1, native_info
        self.result["native_state"] = native_info[0]
        if self.args.native:
            assert (
                native_info[0]
                and native_info[0]["ready"] == 1
                and (
                    native_info[0]["served"]
                    + native_info[0]["writes"]
                    + native_info[0]["commits"]
                )
                > 0
            ), native_info
        self.record(
            "service denies EVAL, arbitrary CALL, test hooks, system reads and raw writes; guest denied; replication account cannot execute SQL RPCs"
        )

        def no_client_identity():
            with socket.create_connection(("127.0.0.1", self.base), timeout=3) as raw:
                with self.context().wrap_socket(raw, server_hostname="127.0.0.1") as s:
                    if s.recv(128):
                        raise AssertionError("unauthenticated storage greeting leaked")
                    raise ssl.SSLError("client identity required")

        self.reject(
            "storage mTLS refuses a client without identity before IPROTO greeting",
            no_client_identity,
        )
        with socket.create_connection(("127.0.0.1", self.base), timeout=3) as raw:
            with self.context(identity=True).wrap_socket(
                raw, server_hostname="127.0.0.1"
            ) as s:
                assert b"Tarantool" in s.recv(128)
        self.record("storage verified mTLS delivers IPROTO greeting")
        # A raw TCP client receives no plaintext greeting and cannot downgrade.
        with socket.create_connection(("127.0.0.1", self.base), timeout=3) as s:
            s.sendall(b"not a TLS ClientHello\n")
            assert b"Tarantool" not in s.recv(128)
        self.record("storage plaintext/downgrade refused")
        self.config_controls()
        self.drivers()

    def config_controls(self):
        def bad(name, path, change, cmd):
            old = path.read_bytes()
            v = json.loads(old)
            change(v)
            deploy.dump(path, v)
            try:
                p = subprocess.run(
                    list(map(str, cmd)),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=15,
                )
                assert p.returncode != 0, name
                (self.work / ("reject-" + name + ".log")).write_bytes(
                    p.stdout + p.stderr
                )
            finally:
                path.write_bytes(old)
            self.record(name)

        cmd = [
            self.bin / "tarn-server",
            "--config",
            self.config / "cluster.json",
            "--production",
            self.config / "gateway.production.json",
            "--check-deployment",
            "--threads",
            "2",
        ]
        path = self.config / "cluster.json"
        bad(
            "RF1 configuration rejected",
            path,
            lambda c: c["shards"][0].update(replicas=c["shards"][0]["replicas"][:1]),
            cmd,
        )
        bad(
            "plaintext storage configuration rejected",
            path,
            lambda c: c["shards"][0]["replicas"].__setitem__(0, "127.0.0.1:1"),
            cmd,
        )
        bad(
            "inline storage secret rejected",
            path,
            lambda c: c.update(password="unsafe-inline-password"),
            cmd,
        )
        p = self.config / "gateway.production.json"
        bad(
            "duplicate failure domains rejected",
            p,
            lambda c: c["failure_domains"].update(
                {n["uri"]: "same-host" for n in self.nodes}
            ),
            cmd,
        )
        bad(
            "unachievable CPU requirement rejected",
            p,
            lambda c: c["host"].update(min_cpus=100000),
            cmd,
        )
        bad(
            "unachievable memory requirement rejected",
            p,
            lambda c: c["host"].update(min_memory_bytes=2**60),
            cmd,
        )
        bad(
            "unachievable disk requirement rejected",
            p,
            lambda c: c["host"].update(min_free_disk_bytes=2**60),
            cmd,
        )
        secret = self.work / "service.secret"
        secret.chmod(0o644)
        try:
            assert (
                subprocess.run(
                    list(map(str, cmd)), stdout=subprocess.PIPE, stderr=subprocess.PIPE
                ).returncode
                != 0
            )
        finally:
            secret.chmod(0o600)
        self.record("public secret permissions rejected")
        # Present wrong-name and expired server identities from an independent
        # mTLS bridge, then point one gateway replica at that listener.
        for offset, name in enumerate(("wrong", "expired"), 10):
            extra = self.work / (name + ".bridge.json")
            deploy.dump(
                extra,
                dict(
                    max_connections=8,
                    clients=[],
                    servers=[
                        dict(
                            listen=f"127.0.0.1:{self.base+offset}",
                            upstream_unix=self.nodes[0]["socket_dir"] + "/storage.sock",
                            tls=dict(
                                cert_file=str(self.cert / (name + ".pem")),
                                key_file=str(self.cert / (name + ".key")),
                                client_ca_file=str(self.cert / "ca.pem"),
                            ),
                        )
                    ],
                ),
            )
            self.spawn(name + "-bridge", [self.bin / "tarn-tls", extra])
            time.sleep(0.15)
            ctx = self.context(identity=True)
            self.reject(
                name + " storage server identity rejected",
                lambda: ctx.wrap_socket(
                    socket.create_connection(
                        ("127.0.0.1", self.base + offset), timeout=3
                    ),
                    server_hostname="127.0.0.1",
                ),
            )

    def drivers(self):
        matrix = [
            dict(
                driver="PyMySQL",
                status="passed",
                operations="TLS verify identity, auth, DDL, transactions, memtx/vinyl",
            )
        ]
        try:
            import mysql.connector
        except ImportError:
            if self.args.require_drivers:
                raise
            matrix.append(dict(driver="mysql-connector-python", status="not installed"))
        else:
            c = mysql.connector.connect(
                host="127.0.0.1",
                port=self.port,
                user="test",
                password=self.password,
                ssl_ca=str(self.cert / "ca.pem"),
                ssl_verify_cert=True,
                ssl_verify_identity=True,
            )
            q = c.cursor(prepared=True)
            q.execute("SELECT %s + %s", (2, 3))
            assert q.fetchall() == [(5,)]
            q.close()
            c.close()
            matrix.append(
                dict(
                    driver="mysql-connector-python",
                    version=mysql.connector.__version__,
                    status="passed",
                    operations="verified TLS, native auth switch, binary prepared statement",
                )
            )
        cli = shutil.which("mariadb") or shutil.which("mysql")
        if cli:
            # Secret stays in a private option file, never argv/environment.
            opts = self.work / "driver.cnf"
            private(
                opts,
                "[client]\nuser=test\npassword="
                + self.password
                + "\nhost=127.0.0.1\nport="
                + str(self.port)
                + "\nssl-ca="
                + str(self.cert / "ca.pem")
                + "\nssl-verify-server-cert\n",
            )
            out = run(
                [cli, "--defaults-extra-file=" + str(opts), "-N", "-e", "SELECT 6*7"]
            ).stdout
            assert out.strip() == b"42", out
            matrix.append(
                dict(
                    driver="MariaDB CLI/libmariadb",
                    version=run([cli, "--version"]).stdout.decode().strip(),
                    status="passed",
                    operations="verified TLS, auth, query",
                )
            )
        elif self.args.require_drivers:
            raise AssertionError("MariaDB CLI required")
        self.result["drivers"] = matrix
        self.record("client driver matrix", matrix)

    def recovery(self):
        # Kill the actual leader identified by the bounded role RPC. No
        # mutation is retried during the interruption.
        probe = self.work / "leader.lua"
        probe.write_text(
            "local n=require('net.box'); local f=assert(io.open(arg[2])); local p=f:read('*l'); f:close(); local c=n.connect(arg[1],{fetch_schema=false,user='tarn_service',password=p,wait_connected=2}); print(c:call('tarn_role') and 'ro' or 'rw'); c:close();os.exit()"
        )
        leader = None
        for i, n in enumerate(self.nodes):
            if (
                run(
                    [
                        "tarantool",
                        probe,
                        "unix/:" + n["socket_dir"] + "/storage.sock",
                        self.work / "service.secret",
                    ]
                ).stdout.strip()
                == b"rw"
            ):
                leader = i
        assert leader is not None
        self.query("UPDATE secure_test.memtx_t SET n=n+1 WHERE id=1")
        self.storages[leader].kill()
        self.storages[leader].wait()
        self.wait(
            "confirmed fsync data survives leader loss",
            lambda: self.query("SELECT n FROM secure_test.memtx_t WHERE id=1")
            == ((12,),),
        )
        self.record("confirmed commit survives RF3 leader SIGKILL with TLS routing")
        # The killed Unix socket remains. Remove only our fixture-owned
        # storage socket, then restart that exact node as a voter/candidate.
        Path(self.nodes[leader]["socket_dir"], "storage.sock").unlink(missing_ok=True)
        n = self.nodes[leader]
        env = {k: v for k, v in os.environ.items() if not k.startswith("TARN_")}
        env["TARN_NATIVE"] = (
            str(self.args.native.resolve()) if self.args.native else "0"
        )
        self.storages[leader] = self.spawn(
            "storage-" + n["name"] + "-restart",
            [
                sys.executable,
                ROOT / "scripts/production.py",
                "storage",
                self.config / (n["name"] + ".storage.json"),
            ],
            env,
        )
        self.wait("rejoined fsync replica healthy", self.health)
        self.record(
            "killed replica recovers WAL, rejoins encrypted replication and passes deployment checks"
        )

    def rotation(self):
        # Old+new trust first, then leaves, then retire old trust. All
        # restarts are rolling; no mutation is replayed during the cuts.
        ca = self.cert / "ca.pem"
        old = ca.read_text()
        new = (self.cert / "other.pem").read_text()
        run(
            [
                "openssl",
                "req",
                "-new",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-keyout",
                self.cert / "renewed.key",
                "-out",
                self.cert / "renewed.csr",
                "-subj",
                "/CN=renewed",
            ]
        )
        (self.cert / "renewed.key").chmod(0o600)
        run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                self.cert / "renewed.csr",
                "-CA",
                self.cert / "other.pem",
                "-CAkey",
                self.cert / "other.key",
                "-CAcreateserial",
                "-out",
                self.cert / "renewed.pem",
                "-days",
                "2",
                "-extfile",
                self.cert / "server.ext",
            ]
        )

        def bridges():
            for i, n in enumerate(self.nodes):
                self.bridges[i].terminate()
                self.bridges[i].wait(timeout=10)
                for r in self.nodes:
                    if r is not n:
                        Path(n["socket_dir"], r["name"] + ".sock").unlink(
                            missing_ok=True
                        )
                self.bridges[i] = self.spawn(
                    "rotated-bridge-" + n["name"],
                    [self.bin / "tarn-tls", self.config / (n["name"] + ".tls.json")],
                )
                self.wait("rolling bridge rotation", self.health)

        def gateway():
            self.gateway.terminate()
            self.gateway.wait(timeout=10)
            self.gateway = self.spawn(
                "rotated-gateway",
                [
                    self.bin / "tarn-server",
                    "--config",
                    self.config / "cluster.json",
                    "--production",
                    self.config / "gateway.production.json",
                    "--listen",
                    f"0.0.0.0:{self.port}",
                    "--threads",
                    "2",
                ],
                dict(os.environ, RUST_MIN_STACK="33554432"),
            )
            self.wait(
                "rotated SQL TLS",
                lambda: self.query("SELECT n FROM secure_test.memtx_t WHERE id=1"),
            )

        private(ca, old + new)
        bridges()
        gateway()
        os.replace(self.cert / "renewed.key", self.cert / "server.key")
        os.replace(self.cert / "renewed.pem", self.cert / "server.pem")
        bridges()
        gateway()
        private(ca, new)
        gateway()
        bridges()
        self.record(
            "rolling CA overlap, leaf renewal, and old-root retirement on MySQL and storage TLS"
        )

    def lost_reply(self):
        # Use the existing framed fault proxy between private Unix sockets;
        # both network hops still negotiate verified mutual TLS.
        probe = self.work / "leader.lua"
        leader = None
        for n in self.nodes:
            if (
                run(
                    [
                        "tarantool",
                        probe,
                        "unix/:" + n["socket_dir"] + "/storage.sock",
                        self.work / "service.secret",
                    ]
                ).stdout.strip()
                == b"rw"
            ):
                leader = n
        assert leader
        d = self.work / "fault"
        d.mkdir(mode=0o700)
        witness = d / "reply.bin"
        arm = d / "armed"
        env = dict(
            os.environ,
            TARN_FAULT_WITNESS_FILE=str(witness),
            TARN_PROXY_ARM_FILE=str(arm),
        )
        self.spawn(
            "tls-fault-proxy",
            [
                sys.executable,
                ROOT / "tests/proxy.py",
                "unix/:" + str(d / "proxy.sock"),
                "unix/:" + str(d / "upstream.sock"),
                "tarn_storage.ts_autocommit" if self.args.native else "tarn_autocommit",
                "update",
            ],
            env,
        )
        tls = json.loads((self.config / "cluster.json").read_text())["storage_tls"]
        bridge = d / "bridge.json"
        deploy.dump(
            bridge,
            dict(
                max_connections=16,
                servers=[
                    dict(
                        listen=f"127.0.0.1:{self.base+20}",
                        upstream_unix=str(d / "proxy.sock"),
                        tls=dict(self.server_tls, client_ca_file=tls["ca_file"]),
                    )
                ],
                clients=[
                    dict(
                        listen_unix=str(d / "upstream.sock"),
                        upstream=leader["uri"],
                        tls=tls,
                    )
                ],
            ),
        )
        self.spawn("tls-fault-bridge", [self.bin / "tarn-tls", bridge])
        self.wait(
            "fault sockets bound",
            lambda: (d / "proxy.sock").exists() and (d / "upstream.sock").exists(),
        )
        with socket.create_connection(("127.0.0.1", self.base + 20), timeout=3) as raw:
            with self.context(identity=True).wrap_socket(
                raw, server_hostname="127.0.0.1"
            ) as s:
                assert b"Tarantool" in s.recv(128)
        cfg = json.loads((self.config / "cluster.json").read_text())
        cfg["pool_size"] = 1
        cfg["shards"][0]["replicas"] = [f"tls://127.0.0.1:{self.base+20}"]
        deploy.dump(d / "cluster.json", cfg)
        deploy.dump(d / "mysql.json", self.server_tls)
        port = self.port + 2
        gateway = self.spawn(
            "tls-fault-gateway",
            [
                self.bin / "tarn-server",
                "--config",
                d / "cluster.json",
                "--listen",
                f"127.0.0.1:{port}",
                "--users-file",
                self.work / "users.json",
                "--mysql-tls",
                d / "mysql.json",
                "--threads",
                "2",
            ],
            dict(os.environ, RUST_MIN_STACK="33554432"),
        )

        def connect():
            return pymysql.connect(
                host="127.0.0.1",
                port=port,
                user="test",
                password=self.password,
                ssl=self.context(),
                autocommit=True,
                connect_timeout=3,
                read_timeout=15,
            )

        self.wait("TLS uncertainty gateway", lambda: connect().close())
        before = self.query("SELECT n FROM secure_test.memtx_t WHERE id=1")[0][0]
        with connect() as c:
            self.query("SELECT 1", c)
            arm.touch()
            sqlstates = []
            original = pymysql.err.raise_mysql_exception

            def capture(data):
                if len(data) >= 9 and data[3:4] == b"#":
                    sqlstates.append(data[4:9].decode())
                original(data)

            pymysql.err.raise_mysql_exception = capture
            try:
                try:
                    self.query("UPDATE secure_test.memtx_t SET n=n+1 WHERE id=1", c)
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1180 and sqlstates == ["40003"], (e, sqlstates)
                    self.result["lost_reply_error"] = dict(
                        message=str(e), sqlstate=sqlstates[0]
                    )
                else:
                    raise AssertionError("lost storage reply was reported successful")
            finally:
                pymysql.err.raise_mysql_exception = original
        assert witness.exists(), "matched mutation reply not observed"
        frame = witness.read_bytes()
        # Tarantool's reply header begins map(TYPE=0, SYNC=...). Exact
        # successful status is witnessed before the proxy closes the stream.
        assert frame[0] == 0xCE and frame[5] in (0x82, 0x83) and frame[6] == 0, frame[
            :16
        ]
        marker = frame[7]
        status = (
            marker
            if marker < 0x80
            else int.from_bytes(
                frame[8 : 8 + {0xCC: 1, 0xCD: 2, 0xCE: 4, 0xCF: 8}[marker]], "big"
            )
        )
        assert status == 0, frame[:16]
        assert self.query("SELECT n FROM secure_test.memtx_t WHERE id=1") == (
            (before + 1,),
        )
        gateway.terminate()
        gateway.wait(timeout=10)
        self.record(
            "TLS lost mutation reply reports 1180/40003; successful storage reply witnessed; exact single increment, no replay"
        )

    def cost(self):
        # Three matched repetitions for each mode, same RF/data/client count,
        # no latency sleep. This is deliberately a local cost study, not a
        # throughput/production capacity claim. WAL write is development.
        # Run against this same secured deployment for TLS baseline; plaintext
        # client mode uses a development SQL listener with identical fsync RF3
        # and storage mTLS. Durability is varied separately on all replicas.
        devport = self.port + 1
        opts = self.work / "mysql-tls.json"
        deploy.dump(opts, self.server_tls)
        dev = self.spawn(
            "development-cost-gateway",
            [
                self.bin / "tarn-server",
                "--config",
                self.config / "cluster.json",
                "--listen",
                f"127.0.0.1:{devport}",
                "--users-file",
                self.work / "users.json",
                "--mysql-tls",
                opts,
                "--threads",
                "2",
            ],
            dict(os.environ, RUST_MIN_STACK="33554432"),
        )

        def connect(tls):
            return pymysql.connect(
                host="127.0.0.1",
                port=devport,
                user="test",
                password=self.password,
                ssl=self.context() if tls else None,
                autocommit=True,
                connect_timeout=3,
                read_timeout=10,
            )

        self.wait("matched cost listener", lambda: connect(True).close())
        for rep in range(3):
            for encrypted in ([False, True] if rep % 2 == 0 else [True, False]):
                with connect(encrypted) as c:
                    with c.cursor() as q:
                        for _ in range(10):
                            q.execute("SELECT 1")
                            q.fetchall()
                        samples = []
                        for _ in range(self.args.operations):
                            start = time.perf_counter_ns()
                            q.execute("SELECT n FROM secure_test.memtx_t WHERE id=1")
                            q.fetchall()
                            samples.append((time.perf_counter_ns() - start) / 1e6)
                        self.result["cost"].append(
                            dict(
                                repetition=rep,
                                query="point read",
                                mysql_tls=encrypted,
                                storage_mtls=True,
                                wal="fsync",
                                rf=3,
                                clients=1,
                                operations=len(samples),
                                latency_ms=samples,
                                p50_ms=statistics.median(samples),
                                p95_ms=sorted(samples)[int(0.95 * (len(samples) - 1))],
                            )
                        )
        dev.terminate()
        dev.wait(timeout=10)
        self.record(
            "matched MySQL TLS cost (alternating order, three repetitions; full samples retained)"
        )

    def transport_cost(self):
        # A test-only wrapper changes box.cfg's WAL mode before first cfg;
        # all roles, quorums, data, procedures and clock policy stay matched.
        # The plaintext bridge example has identical TCP/Unix hops and copy
        # buffers. Production clients must refuse that counterfactual mode.
        self.gateway.terminate()
        self.gateway.wait(timeout=10)
        for p in self.storages + self.bridges:
            p.terminate()
        for p in self.storages + self.bridges:
            p.wait(timeout=10)
        wrapper = self.work / "cost-storage.lua"
        wrapper.write_text(
            "local real=box.cfg;box.cfg=function(c)c.wal_mode=arg[2];return real(c)end;dofile(arg[1])"
        )
        source_env = {k: v for k, v in os.environ.items() if not k.startswith("TARN_")}
        leader_probe = self.work / "cost-leader.lua"
        leader_probe.write_text(
            "local n=require('net.box');local f=assert(io.open(arg[2]));local p=f:read('*l');f:close();local c=n.connect(arg[1],{fetch_schema=false,user='tarn_service',password=p,wait_connected=1});local ro=c:call('tarn_role');local clock=c:call('tarn_clock_status');print(not ro and clock.read_ready and 'ready' or 'wait');c:close();os.exit()"
        )
        # Reverse the factor order on alternate WAL modes; retain each of
        # three repetitions and connection/handshake time separately.
        for wal, encrypted in [
            ("fsync", True),
            ("fsync", False),
            ("write", False),
            ("write", True),
        ]:
            label = wal + ("-tls" if encrypted else "-plain")
            for n in self.nodes:
                for p in Path(n["socket_dir"]).glob("*.sock"):
                    p.unlink()
            bridge = (
                self.bin / "tarn-tls"
                if encrypted
                else self.bin / "examples/unsafe-plaintext-bridge"
            )
            bridges = [
                self.spawn(
                    "cost-" + label + "-bridge-" + n["name"],
                    [bridge, self.config / (n["name"] + ".tls.json")],
                )
                for n in self.nodes
            ]
            storages = []
            for n in self.nodes:
                config = self.config / (n["name"] + ".storage.json")
                c = json.loads(config.read_text())
                env = dict(
                    source_env,
                    TARN_PROFILE="production",
                    TARN_PRODUCTION_CONFIG=str(config),
                    TARN_SHARD_ID=str(c["shard_id"]),
                    TARN_WORKDIR=c["workdir"],
                    TARN_INSTANCE_NAME="shard"
                    + str(c["shard_id"])
                    + "_"
                    + config.stem.replace(".", "_"),
                    TARN_REPLICAS=json.dumps(c["replicas"]),
                    TARN_PEERS=json.dumps(c["peers"]),
                    TARN_NATIVE=(
                        str(self.args.native.resolve()) if self.args.native else "0"
                    ),
                )
                storages.append(
                    self.spawn(
                        "cost-" + label + "-storage-" + n["name"],
                        ["tarantool", wrapper, ROOT / "lua/storage.lua", wal],
                        env,
                    )
                )

            def ready():
                for n in self.nodes:
                    r = run(
                        [
                            "tarantool",
                            leader_probe,
                            "unix/:" + n["socket_dir"] + "/storage.sock",
                            self.work / "service.secret",
                        ]
                    )
                    if r.stdout.strip() == b"ready":
                        return n
                return False

            leader = self.wait("cost leader ready " + label, ready)
            if not encrypted:
                self.reject(
                    "production gateway rejects plaintext counterfactual transport",
                    self.health,
                )
            addr = leader["uri"] if encrypted else leader["uri"].removeprefix("tls://")
            cfg = dict(
                addr=addr,
                password_file=str(self.work / "service.secret"),
                tls=(
                    json.loads((self.config / "cluster.json").read_text())[
                        "storage_tls"
                    ]
                    if encrypted
                    else None
                ),
                operations=self.args.operations,
                repetitions=3,
            )
            path = self.work / ("cost-" + label + ".json")
            deploy.dump(path, cfg)
            result = json.loads(run([self.bin / "examples/security-cost", path]).stdout)
            result.update(
                wal=wal,
                storage_mtls=encrypted,
                rf=3,
                profile="unsafe isolated matched cost control",
                peers="same RF3 identities; clock certification uses the same selected bridge",
                transport_hops="TCP to bridge, private Unix to storage",
            )
            self.result.setdefault("storage_durability_cost", []).append(result)
            self.save()
            for p in storages + bridges:
                p.terminate()
            for p in storages + bridges:
                p.wait(timeout=10)
        self.record(
            "matched storage mTLS x fsync/write costs; RF3, same hops, three repetitions, raw samples and no replay"
        )

    def close(self):
        for p in reversed(self.children):
            if p.poll() is None:
                p.terminate()
        for p in reversed(self.children):
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        self.save()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--work", type=Path, required=True)
    p.add_argument("--bin", type=Path, default=ROOT / "target/release")
    p.add_argument("--native", type=Path)
    p.add_argument("--port", type=int, default=43439)
    p.add_argument("--storage-port", type=int, default=43391)
    p.add_argument("--skip-cost", action="store_true")
    p.add_argument("--operations", type=int, default=100)
    p.add_argument("--require-drivers", action="store_true")
    args = p.parse_args()
    f = Fixture(args)
    try:
        f.start()
        f.security()
        f.recovery()
        f.rotation()
        f.lost_reply()
        if not args.skip_cost:
            f.cost()
            f.transport_cost()
        f.result["status"] = "passed"
    except BaseException as e:
        f.result["status"] = "failed"
        f.result["failure"] = repr(e)
        raise
    finally:
        f.close()


if __name__ == "__main__":
    main()
