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
if REPLY not in ("drop", "invalid-type"):
    raise ValueError("TARN_FAULT_REPLY must be drop or invalid-type")
state = {"left": 1}
lock = threading.Lock()


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


def pump(src, dst, conn, c2s):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            if c2s:
                with lock:
                    trig = state["left"] > 0 and all(m in data for m in MATCH)
                    if trig:
                        state["left"] -= 1
                        conn["drop"] = True
                dst.sendall(data)
                if trig:
                    time.sleep(0.5)  # the storage node executes and commits meanwhile
                    if REPLY == "invalid-type":
                        src.sendall(invalid_reply(matching_sync(data)))
                        print("proxy: injected invalid response type after execution", flush=True)
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
