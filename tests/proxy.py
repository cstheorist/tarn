#!/usr/bin/env python3
"""Fault-injecting TCP proxy for tests/faults.sh: forwards IPROTO between a
tarn-server and one storage node. The first request that contains every
MATCH (default: an UPDATE through tarn_autocommit; tests/faults.sh names the
procedure that makes it) is delivered to the storage node, which
executes and commits it; the proxy then swallows the reply and closes the
connection, so the SQL server cannot know whether the statement ran.

    proxy.py LISTEN_PORT UPSTREAM_PORT [MATCH...]
"""
import socket
import os
import sys
import threading
import time

LP, UP = int(sys.argv[1]), int(sys.argv[2])
MATCH = [m.encode() for m in (sys.argv[3:] or ["tarn_autocommit", "update"])]
REPLY = os.environ.get("TARN_FAULT_REPLY", "drop")
if REPLY not in ("drop", "invalid-type", "oversized-frame", "stall"):
    raise ValueError("TARN_FAULT_REPLY must be drop, invalid-type, oversized-frame or stall")
state = {"left": int(os.environ.get("TARN_FAULT_COUNT", "1"))}
lock = threading.Lock()
ARM_FILE = os.environ.get("TARN_PROXY_ARM_FILE")


def matching_sync(data):
    """Read the matched request's integer-only IPROTO header, without dependencies."""
    def uint(p, at):
        marker = p[at]
        if marker < 0x80:
            return marker, at + 1
        width = {0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8}[marker]
        end = at + 1 + width
        return int.from_bytes(p[at + 1:end], "big"), end

    at = 0
    while at < len(data):
        size, begin = uint(data, at)
        end = begin + size
        p = data[begin:end]
        if end <= len(data) and all(m in p for m in MATCH):
            assert 0x80 <= p[0] < 0x90, "unexpected request header"
            pos, sync = 1, None
            for _ in range(p[0] & 15):
                key, pos = uint(p, pos)
                value, pos = uint(p, pos)
                if key == 1:
                    sync = value
            assert sync is not None
            return sync
        at = end
    raise ValueError("matched request is not a complete IPROTO frame")


def invalid_reply(sync):
    # A plausible retryable procedure error in an unsupported response type.
    # Accepting this as a terminal OK could replay the already committed UPDATE.
    body = b'\x81\x30\x91\x93\xa5error\xcd\x04\xbd\xb1injected conflict'
    payload = b'\x82\x00\x2a\x01\xcf' + sync.to_bytes(8, "big") + body
    return b'\xce' + len(payload).to_bytes(4, "big") + payload


def client_packet(src):
    # Tarn emits the canonical u32 length prefix. Frame requests here so a
    # fragmented/coalesced TCP read cannot bypass a pressure-test trigger.
    def exact(n):
        out = bytearray()
        while len(out) < n:
            chunk = src.recv(n - len(out))
            if not chunk:
                if not out:
                    return b""
                raise OSError("truncated request")
            out.extend(chunk)
        return bytes(out)
    prefix = exact(5)
    if not prefix:
        return b""
    if len(prefix) != 5 or prefix[0] != 0xce:
        raise OSError("unexpected request prefix")
    size = int.from_bytes(prefix[1:], "big")
    if size > 128 * 1024 * 1024:
        raise OSError("oversized proxy request")
    return prefix + exact(size)


def pump(src, dst, conn, c2s):
    try:
        while True:
            data = client_packet(src) if c2s else src.recv(65536)
            if not data:
                break
            if c2s:
                with lock:
                    trig = (state["left"] > 0 and (not ARM_FILE or os.path.isfile(ARM_FILE))
                            and all(m in data for m in MATCH))
                    if trig:
                        state["left"] -= 1
                        conn["drop"] = True
                dst.sendall(data)
                if trig and REPLY == "stall":
                    print("proxy: withholding replies on an open connection", flush=True)
                    continue
                if trig:
                    time.sleep(0.5)  # the storage node executes and commits meanwhile
                    if REPLY == "invalid-type":
                        src.sendall(invalid_reply(matching_sync(data)))
                        print("proxy: injected invalid response type after execution", flush=True)
                    if REPLY == "oversized-frame":
                        # Above the gateway's 32 MiB frame cap; advertise only
                        # the length and leave the socket open. This is a sent
                        # mutation, so resource refusal must retain uncertainty.
                        src.sendall(b'\xce' + (64 << 20).to_bytes(4, "big"))
                        print("proxy: injected oversized frame after execution", flush=True)
                        time.sleep(2)
                    print("proxy: swallowed the reply, closing the connection", flush=True)
                    break
            elif not conn.get("drop"):
                dst.sendall(data)
    except OSError:
        pass
    finally:
        conn["close"]()


def handle(cs):
    us = socket.create_connection(("127.0.0.1", UP))
    conn = {}

    def close():
        for s in (cs, us):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass

    conn["close"] = close
    threading.Thread(target=pump, args=(cs, us, conn, True), daemon=True).start()
    threading.Thread(target=pump, args=(us, cs, conn, False), daemon=True).start()


ls = socket.socket()
ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("127.0.0.1", LP))
ls.listen(64)
while True:
    c, _ = ls.accept()
    handle(c)
