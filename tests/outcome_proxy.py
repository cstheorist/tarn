#!/usr/bin/env python3
"""Owned, frame-aware fault proxy for the outcome contract tests.

Control is a local JSON file {token, match, fault}. A token injects once,
after consuming the exact successful backend reply (except before_send).
Every completed matching mutation is recorded, including an illegal replay.
No user-supplied traffic or services belong in this fixture.
"""
import argparse
import json
from pathlib import Path
import socket
import threading
import time

import msgpack


def exact(sock, n):
    out = bytearray()
    while len(out) < n:
        part = sock.recv(n - len(out))
        if not part:
            raise EOFError
        out.extend(part)
    return bytes(out)


def frame(sock):
    prefix = exact(sock, 5)
    assert prefix[0] == 0xce, "fixture requires canonical IPROTO frame lengths"
    size = int.from_bytes(prefix[1:], "big")
    assert size <= 128 * 1024 * 1024
    data = exact(sock, size)
    # IPROTO AUTH carries arbitrary scramble bytes as a legacy MP_STR.
    unpack = msgpack.Unpacker(raw=False, strict_map_key=False, unicode_errors='surrogateescape')
    unpack.feed(data)
    header, body = next(unpack), next(unpack, {})
    return prefix + data, header, body


def reply(sync, body):
    data = msgpack.packb({0: 0, 1: sync}) + msgpack.packb(body)
    return b'\xce' + len(data).to_bytes(4, 'big') + data


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--listen', type=int, required=True)
    ap.add_argument('--upstream', type=int, required=True)
    ap.add_argument('--plan', type=Path, required=True)
    args = ap.parse_args()
    lock, used = threading.Lock(), set()

    def plan():
        try:
            return json.loads(args.plan.read_text())
        except FileNotFoundError:
            return None

    def record(**event):
        with lock:
            print(json.dumps(event, sort_keys=True), flush=True)

    def handle(client):
        backend = socket.create_connection(('127.0.0.1', args.upstream), timeout=5)
        backend.settimeout(None)
        pending, pending_lock = {}, threading.Lock()

        def close():
            for sock in (client, backend):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()

        def requests():
            try:
                while True:
                    data, header, body = frame(client)
                    p = plan()
                    name = body.get(0x22, '')
                    if p and header.get(0) == 10 and p['match'] in name:
                        with pending_lock:
                            pending[header[1]] = (p, name)
                        if p['fault'] == 'before_send':
                            with lock:
                                inject = p['token'] not in used
                                used.add(p['token'])
                            if inject:
                                record(token=p['token'], stage='before_send', procedure=name, sync=header[1])
                                break
                    backend.sendall(data)
            except (OSError, EOFError):
                pass
            finally:
                close()

        def responses():
            held_token = None
            try:
                client.sendall(exact(backend, 128))
                while True:
                    data, header, body = frame(backend)
                    with pending_lock:
                        match = pending.pop(header.get(1), None)
                    if match:
                        p, name = match
                        result = body.get(0x30, [])
                        committed = (header.get(0) == 0 and len(result) == 1
                                     and isinstance(result[0], list) and result[0][:1] == ['ok'])
                        record(token=p['token'], stage='reply', procedure=name, sync=header[1],
                               committed=committed, status=result[0][0] if result and result[0] else None)
                        with lock:
                            inject = committed and p['token'] not in used
                            if inject:
                                used.add(p['token'])
                        if inject:
                            fault = p['fault']
                            record(token=p['token'], stage='injected', fault=fault)
                            if fault == 'lost':
                                break
                            if fault == 'hold':
                                held_token = p['token']
                                deadline = time.monotonic() + 12
                                while time.monotonic() < deadline and plan() == p:
                                    time.sleep(.01)
                                break
                            replacement = {
                                'malformed_error': {0x30: [['error', 1213, 'injected', 'extra']]},
                                'malformed_handback': {0x30: [['lua', 'unexpected']]},
                                'extra_return': {0x30: [['error', 1213, 'injected'], None]},
                                'missing_data': {},
                                'invalid_code': {0x30: [['error', 65536 + 1213, 'injected']]},
                                'unknown': {0x30: [['error', 1180, 'injected native outcome unknown']]},
                            }[fault]
                            data = reply(header[1], replacement)
                    client.sendall(data)
            except (OSError, EOFError):
                pass
            finally:
                close()
                if held_token is not None:
                    record(token=held_token, stage='retired')

        threading.Thread(target=requests, daemon=True).start()
        threading.Thread(target=responses, daemon=True).start()

    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', args.listen))
        listener.listen(64)
        while True:
            client, _ = listener.accept()
            handle(client)


if __name__ == '__main__':
    main()
