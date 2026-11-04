"""Real TCP regression for held responses during backend listener loss."""
import json
from pathlib import Path
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import msgpack


class OutcomeProxy(unittest.TestCase):
    def test_refused_new_backend_preserves_held_session(self):
        with tempfile.TemporaryDirectory() as temp, socket.socket() as backend_listener:
            backend_listener.bind(('127.0.0.1', 0))
            backend_listener.listen()
            backend_listener.settimeout(5)
            upstream = backend_listener.getsockname()[1]
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
            plan = Path(temp) / 'plan.json'
            token = 'held-success'
            plan.write_text(json.dumps({'token': token, 'match': 'tarn_autocommit', 'fault': 'hold'}))
            child = subprocess.Popen(
                [sys.executable, str(Path(__file__).with_name('outcome_proxy.py')),
                 '--listen', str(port), '--upstream', str(upstream), '--plan', str(plan)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            events, output = queue.Queue(), []

            def read_output():
                for line in child.stdout:
                    output.append(line)
                    try:
                        events.put(json.loads(line))
                    except ValueError:
                        pass

            reader = threading.Thread(target=read_output, daemon=True)
            reader.start()

            def receive(sock, size):
                received = bytearray()
                while len(received) < size:
                    part = sock.recv(size - len(received))
                    self.assertTrue(part, 'unexpected EOF')
                    received.extend(part)
                return bytes(received)

            def event(stage):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    try:
                        item = events.get(timeout=.1)
                    except queue.Empty:
                        continue
                    if item.get('stage') == stage:
                        self.assertEqual(item['token'], token)
                        return item
                self.fail(f'missing {stage} event; child={child.poll()}: {"".join(output)}')

            try:
                deadline = time.monotonic() + 5
                while True:
                    try:
                        client = socket.create_connection(('127.0.0.1', port), timeout=.1)
                        break
                    except ConnectionRefusedError:
                        if child.poll() is not None or time.monotonic() >= deadline:
                            self.fail(f'proxy did not start: {"".join(output)}')
                        time.sleep(.01)
                with client, backend_listener.accept()[0] as backend:
                    client.settimeout(5)
                    backend.settimeout(5)
                    greeting = b'X' * 128
                    backend.sendall(greeting)
                    self.assertEqual(receive(client, len(greeting)), greeting)
                    request = msgpack.packb({0: 10, 1: 7}) + msgpack.packb({0x22: 'tarn_autocommit'})
                    request = b'\xce' + len(request).to_bytes(4, 'big') + request
                    client.sendall(request)
                    self.assertEqual(receive(backend, len(request)), request)
                    response = msgpack.packb({0: 0, 1: 7}) + msgpack.packb({0x30: [['ok']]})
                    backend.sendall(b'\xce' + len(response).to_bytes(4, 'big') + response)
                    self.assertTrue(event('reply')['committed'])
                    self.assertEqual(event('injected')['fault'], 'hold')

                    # Existing backend stream stays owned and open; only its listener disappears.
                    backend_listener.close()
                    for _ in range(2):
                        with socket.create_connection(('127.0.0.1', port), timeout=5) as newcomer:
                            self.assertEqual(newcomer.recv(1), b'')
                        self.assertIsNone(child.poll(), ''.join(output))
                    client.settimeout(.2)
                    with self.assertRaises(socket.timeout):
                        client.recv(1)
                    plan.unlink()
                    event('retired')
                    client.settimeout(5)
                    self.assertEqual(client.recv(1), b'')
                    self.assertIsNone(child.poll(), ''.join(output))
            finally:
                if child.poll() is None:
                    child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                reader.join(timeout=5)
                child.stdout.close()


if __name__ == '__main__':
    unittest.main()
