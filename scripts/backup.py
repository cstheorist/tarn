#!/usr/bin/env python3
"""Local physical Tarn backup/restore agent. Requires Python 3 and Tarantool 3.9.

Run on the storage host: the administrative connection and source directory must
name the same instance. A completed manifest is published only after every listed
file is fsynced and the owned GC lease is released. No user mutation is replayed.
"""
import argparse
import hashlib
import json
import math
import os
import pathlib
import selectors
import stat
import subprocess
import sys
import time
import uuid

CHUNK = 1024 * 1024
FORMAT = "tarn-file-backup-v1"
MAGIC = {".snap": b"SNAP", ".xlog": b"XLOG", ".vylog": b"VYLOG", ".run": b"RUN", ".index": b"INDEX"}


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def relative(name):
    p = pathlib.PurePosixPath(name)
    if not name or p.is_absolute() or ".." in p.parts or "\\" in name or p.suffix not in MAGIC:
        raise ValueError("invalid backup file path: " + name)
    return pathlib.Path(*p.parts)


def regular(root, rel):
    if root.is_symlink() or not root.is_dir():
        raise ValueError("invalid backup file root")
    p = root
    for part in rel.parts:
        p = p / part
        if stat.S_ISLNK(p.lstat().st_mode):
            raise ValueError("symlink in backup path: " + str(rel))
    if not stat.S_ISREG(p.stat().st_mode):
        raise ValueError("not a regular file: " + str(rel))
    return p


class Pace:
    def __init__(self, mib):
        self.rate = mib * CHUNK
        self.start = time.monotonic()
        self.total = 0

    def charge(self, count):
        self.total += count
        if self.rate:
            delay = self.total / self.rate - (time.monotonic() - self.start)
            if delay > 0:
                time.sleep(delay)


class Bridge:
    def __init__(self, tarantool, uri_env, timeout=40):
        env = dict(os.environ, TARN_BACKUP_URI=os.environ[uri_env])
        self.p = subprocess.Popen([tarantool, str(pathlib.Path(__file__).with_name("backup_rpc.lua"))],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
        self.pending = b""
        self.timeout = timeout

    def call(self, op, *args):
        self.p.stdin.write((json.dumps({"op": op, "args": args, "timeout": self.timeout - 10}) + "\n").encode())
        self.p.stdin.flush()
        deadline = time.monotonic() + self.timeout
        with selectors.DefaultSelector() as selector:
            selector.register(self.p.stdout, selectors.EVENT_READ)
            while b"\n" not in self.pending:
                if not selector.select(max(0, deadline - time.monotonic())):
                    raise TimeoutError("backup RPC outcome unknown: " + op)
                data = os.read(self.p.stdout.fileno(), 65536)
                if not data:
                    raise RuntimeError("backup bridge closed; RPC outcome unknown: " + op)
                self.pending += data
                if len(self.pending) > 8 * CHUNK:
                    raise ValueError("backup RPC response exceeds 8 MiB")
        line, self.pending = self.pending.split(b"\n", 1)
        result = json.loads(line)
        if not result["ok"]:
            raise RuntimeError(result["error"])
        return result.get("result")

    def close(self):
        self.p.stdin.close()
        try:
            self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.p.terminate()
            self.p.wait(timeout=5)


def copy_file(src, dst, pace, source_uuid=None, target_uuid=None, check=None):
    before = src.stat()
    digest, output_digest = hashlib.sha256(), hashlib.sha256()
    count, checked_at = 0, time.monotonic()
    dst.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as inp, dst.open("xb") as out:
        os.chmod(dst, 0o600)
        while True:
            block = inp.read(CHUNK)
            if not block:
                break
            digest.update(block)
            if count == 0:
                header, sep, _ = block.partition(b"\n\n")
                if not sep or header.split(b"\n", 1)[0] != MAGIC[src.suffix] or header.splitlines()[1] != b"0.13":
                    raise ValueError("unrecognized checkpoint header: " + str(src))
                expected = b"Instance: " + source_uuid.encode() if source_uuid else None
                if expected and header.splitlines().count(expected) != 1:
                    raise ValueError("checkpoint UUID differs from manifest: " + str(src))
                if target_uuid:
                    pos = block.index(expected) + len(b"Instance: ")
                    block = block[:pos] + target_uuid.encode() + block[pos + 36:]
            out.write(block)
            output_digest.update(block)
            count += len(block)
            pace.charge(len(block))
            if check is not None and time.monotonic() - checked_at >= 1:
                check()
                checked_at = time.monotonic()
        out.flush()
        os.fsync(out.fileno())
    if count == 0:
        raise ValueError("empty checkpoint file: " + str(src))
    after = src.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or count != before.st_size:
        raise ValueError("backup source file changed during copy: " + str(src))
    return {"size": count, "sha256": digest.hexdigest(), "output_sha256": output_digest.hexdigest()}


def begin(output):
    output = output.absolute()
    output.mkdir(mode=0o700)  # Never overwrite, merge or erase an existing directory.
    (output / "INCOMPLETE").write_text("No complete backup/restore manifest has been published.\n")
    sync_dir(output)
    return output


def publish(output, name, manifest):
    p = output / (name + ".partial")
    with p.open("x") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    # Persist nested directory entries as well as file contents.
    for parent, _, _ in os.walk(output, topdown=False):
        sync_dir(parent)
    p.rename(output / name)
    (output / "INCOMPLETE").unlink()
    sync_dir(output)
    sync_dir(output.parent)


def create(args):
    out = begin(pathlib.Path(args.output))
    source = pathlib.Path(args.source).resolve(strict=True)
    pace, token = Pace(args.rate_mib), str(uuid.uuid4())
    bridge = Bridge(args.tarantool, args.uri_env)
    held = True  # Even a lost START reply may have acquired our lease.
    try:
        info = bridge.call("start", token, args.ttl, args.seed_uuid)
        if source != pathlib.Path(info["source_dir"]).resolve():
            raise ValueError("run on the source host with its exact source directory")
        names = info.pop("files")
        files, seen = [], set()
        for name in names:
            rel = relative(name)
            if str(rel) in seen:
                raise ValueError("duplicate backup file")
            seen.add(str(rel))
            src = regular(source, rel)
            item = copy_file(src, out / "files" / rel, pace, info["source_uuid"],
                             check=lambda: bridge.call("check", token))
            item.pop("output_sha256")
            files.append(dict(item, path=rel.as_posix()))
            bridge.call("check", token)
        if not any(pathlib.Path(x["path"]).suffix == ".snap" for x in files):
            raise ValueError("backup has no checkpoint")
        bridge.call("check", token)
        # Do not retry STOP if its reply is lost. The TTL bounds the remaining pin.
        held = False
        bridge.call("stop", token)
        manifest = dict(info, format=FORMAT, files=files, bytes=pace.total,
                        elapsed_s=time.monotonic() - pace.start, rate_mib=args.rate_mib,
                        created_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        publish(out, "manifest.json", manifest)
        return manifest
    finally:
        try:
            if held:
                bridge.call("stop", token)
        finally:
            bridge.close()


def restore(args):
    root = pathlib.Path(args.backup).resolve(strict=True)
    if (root / "INCOMPLETE").exists():
        raise ValueError("incomplete backup")
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("format") != FORMAT:
        raise ValueError("unknown backup format")
    source_uuid = str(uuid.UUID(manifest["source_uuid"]))
    target_uuid = args.seed_uuid
    if target_uuid and (target_uuid != manifest.get("seed_uuid") or target_uuid == source_uuid):
        raise ValueError("seed UUID must be the distinct identity registered in this backup")
    files, seen = manifest["files"], set()
    if not files or len(files) > 1000000:
        raise ValueError("invalid backup file count")
    if not any(pathlib.Path(item["path"]).suffix == ".snap" for item in files):
        raise ValueError("backup has no checkpoint")
    for item in files:
        rel = relative(item["path"])
        if str(rel) in seen:
            raise ValueError("duplicate backup file")
        seen.add(str(rel))
        regular(root / "files", rel)
    out, pace, restored = begin(pathlib.Path(args.output)), Pace(args.rate_mib), []
    if getattr(args, 'quarantine', None) is not None:
        # The cluster controller installs its startup fence before the local
        # INCOMPLETE marker can ever be removed, including after a crash.
        with (out / 'tarn-maintenance.json').open('x') as f:
            json.dump(args.quarantine, f)
            f.flush()
            os.fsync(f.fileno())
        sync_dir(out)
    for item in files:
        rel = relative(item["path"])
        result = copy_file(regular(root / "files", rel), out / rel, pace, source_uuid, target_uuid)
        if result["sha256"] != item["sha256"] or result["size"] != item["size"]:
            raise ValueError("backup checksum/size mismatch: " + str(rel))
        restored.append(dict(path=rel.as_posix(), size=result["size"], sha256=result["output_sha256"]))
    result = dict(format=FORMAT, source_uuid=source_uuid, instance_uuid=target_uuid or source_uuid,
                  replicaset_uuid=manifest["replicaset_uuid"], seed=bool(target_uuid),
                  files=restored, bytes=pace.total, elapsed_s=time.monotonic() - pace.start,
                  rate_mib=args.rate_mib)
    if target_uuid:
        # Startup keeps a new seed out of elections until an operator has
        # checked WAL catch-up and archives this small identity marker.
        with (out / "seed.json").open("x") as f:
            json.dump({"instance_uuid": target_uuid, "source_uuid": source_uuid,
                       "replicaset_uuid": manifest["replicaset_uuid"]}, f)
            f.flush()
            os.fsync(f.fileno())
    publish(out, "restore.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create", help="copy the files pinned by box.backup.start")
    create_parser.add_argument("--source", required=True)
    create_parser.add_argument("--uri-env", default="TARN_BACKUP_URI")
    create_parser.add_argument("--tarantool", default=os.getenv("TARANTOOL", "tarantool"))
    create_parser.add_argument("--ttl", type=float, default=3600)
    restore_parser = sub.add_parser("restore", help="verify and restore to a new stopped data directory")
    restore_parser.add_argument("--backup", required=True)
    for p in (create_parser, restore_parser):
        p.add_argument("--output", required=True)
        p.add_argument("--rate-mib", type=float, default=64, help="aggregate file-copy MiB/s; 0 is unlimited")
        p.add_argument("--seed-uuid", type=lambda value: str(uuid.UUID(value)))
    args = parser.parse_args()
    if not math.isfinite(args.rate_mib) or args.rate_mib < 0:
        parser.error("rate must be finite and nonnegative")
    if args.command == "create" and (not math.isfinite(args.ttl) or not 1 <= args.ttl <= 86400):
        parser.error("TTL must be from 1 through 86400 seconds")
    result = create(args) if args.command == "create" else restore(args)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, TimeoutError, KeyError) as error:
        print("backup: " + str(error), file=sys.stderr)
        sys.exit(1)
