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
state = {"left": 1}
lock = threading.Lock()
ARM_FILE = os.environ.get("TARN_PROXY_ARM_FILE")


def pump(src, dst, conn, c2s):
    try:
        while True:
            data = src.recv(65536)
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
                if trig:
                    time.sleep(0.5)  # the storage node executes and commits meanwhile
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
