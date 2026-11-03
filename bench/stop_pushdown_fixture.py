#!/usr/bin/env python3
"""Stop only this Linux study fixture's processes, keeping PID identity pinned.

Do not call cluster.sh stop first: it removes the ownership PID files before
waiting. Logs, data and launch scripts are retained for artifact collection.
"""
import argparse
import os
from pathlib import Path
import select
import signal
import stat
import time


def fixture_process(fixture, name, pid):
    """Check a live PID against the fixture's exact launch paths before signaling."""
    proc = Path('/proc') / str(pid)
    if proc.stat().st_uid != os.getuid():
        raise ValueError(f'{name}: process belongs to another user')
    args = (proc / 'cmdline').read_bytes().split(b'\0')
    args = [os.fsdecode(arg) for arg in args if arg]
    script = str(fixture / f'{name}.sh')
    if name.startswith('s'):
        storage = fixture / name
        # Tarantool changes its argv/process title after startup. The exact
        # private instance directory plus executable identifies it without
        # relying on the original script argument surviving that rewrite.
        valid = ((proc / 'cwd').resolve() == storage
                 and (proc / 'exe').resolve().name == 'tarantool')
    else:
        config = str(fixture / 'cluster.json')
        valid = any(args[i:i+2] == ['--config', config] for i in range(len(args)-1))
    # A just-launched shell may not yet have exec'd its storage/gateway.
    shell = bool(args) and Path(args[0]).name in ('bash', 'sh') and script in args[1:]
    if not (valid or shell):
        raise ValueError(f'{name}: PID {pid} does not identify this fixture')


def stop(fixture, timeout=15):
    fixture = Path(fixture).absolute()
    if not fixture.exists():
        return
    if fixture.resolve() != fixture or fixture.stat().st_uid != os.getuid():
        raise ValueError('fixture must be an owned directory without symlinks')
    handles = []
    try:
        # Validate every handle before signaling any process. pidfds ensure a
        # PID reused after capture can never receive our TERM/KILL signals.
        for path in sorted(fixture.glob('*.pid')):
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                raise ValueError(f'invalid ownership file: {path}')
            name = path.stem
            if not (name.startswith('s') or name.startswith('tarn')):
                raise ValueError(f'unrecognized fixture process: {name}')
            text = path.read_text()
            pid = int(text.strip())
            if pid <= 1 or pid == os.getpid():
                raise ValueError(f'invalid fixture PID: {pid}')
            try:
                fd = os.pidfd_open(pid)
            except ProcessLookupError:
                path.unlink()
                continue
            handles.append((fd, path, text))
            ready = select.poll()
            ready.register(fd, select.POLLIN)
            if not ready.poll(0):
                try:
                    fixture_process(fixture, name, pid)
                except FileNotFoundError:
                    if not ready.poll(0):
                        raise
        pending = {fd for fd, _, _ in handles}
        for sig, seconds in ((signal.SIGTERM, timeout), (signal.SIGKILL, 5)):
            for fd in pending:
                try:
                    signal.pidfd_send_signal(fd, sig)
                except ProcessLookupError:
                    pass
            poller = select.poll()
            for fd in pending:
                poller.register(fd, select.POLLIN)
            deadline = time.monotonic() + seconds
            while pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                for fd, _ in poller.poll(max(1, int(remaining * 1000))):
                    pending.discard(fd)
                    poller.unregister(fd)
            if not pending:
                break
        if pending:
            raise TimeoutError('owned fixture processes did not exit; refusing measurement startup')
        for _, path, text in handles:
            if path.exists() and path.read_text() == text:
                path.unlink()
    finally:
        for fd, _, _ in handles:
            os.close(fd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('fixture', type=Path)
    stop(parser.parse_args().fixture)
