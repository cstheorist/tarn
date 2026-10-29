#!/usr/bin/env python3
"""Injected administrative transport failures retain uncertainty without secrets."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import traceback
from unittest.mock import patch

repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo / 'tests/sql'))
import kvtest
import kvfaults
import pymysql

secret = 'synthetic-never-a-live-password-c4e149631'
command = ['tarantool', 'lazyrpc.lua', 'owned:1', 'eval', 'create-reader', json.dumps(['reader', secret])]
cases = [
    ('outer_timeout', subprocess.TimeoutExpired(command, 12), False),
    ('outer_process_error', subprocess.CalledProcessError(1, command, stderr=secret), False),
    ('os_error', FileNotFoundError(2, 'missing', secret), False),
    ('nested_timeout', RuntimeError('storage eval: subprocess.TimeoutExpired: Command ' + repr(command)), False),
    ('truncated_wrapper', RuntimeError('private command tail ' + secret + ' final'), True),
]
results = []
for name, error, redacted in cases:
    with patch.object(kvtest, '_admin_ev', side_effect=error) as called:
        try:
            kvtest.ev('owned:1', 'create-reader', ['reader', secret], stage='create_reader', sensitive=(secret,))
            raise AssertionError('exception was swallowed')
        except RuntimeError as rejected:
            rendered = traceback.format_exc()
            assert secret not in rendered, name
            assert rejected.__suppress_context__, name
            if redacted:
                assert '<redacted>' in rendered
            else:
                assert 'unknown_no_replay' in rendered and 'create_reader' in rendered
            if getattr(error, 'cmd', None) is not None:
                assert 'command_sha256' in rendered
            results.append({'case': name, 'passed': True, 'rendered_sha256': hashlib.sha256(rendered.encode()).hexdigest()})
        assert called.call_count == 1

for error in (RuntimeError('Clock safety unavailable 1213'), pymysql.err.OperationalError(1213, 'definite database refusal')):
    with patch.object(kvtest, '_admin_ev', side_effect=error) as called:
        try:
            kvtest.ev('owned:1', 'actual-db-read', [])
            raise AssertionError('database error was swallowed')
        except Exception as retained:
            assert retained is error
        assert called.call_count == 1
    results.append({'case': 'database_semantics_' + type(error).__name__, 'passed': True})

assert kvfaults.ev is kvtest.ev
results.append({'case': 'fault_driver_uses_same_boundary', 'passed': True})
try:
    raise subprocess.TimeoutExpired(command, 12)
except subprocess.TimeoutExpired:
    assert secret in traceback.format_exc()  # Synthetic legacy leakage control.
results.append({'case': 'raw_exception_negative_control_detects_leak', 'passed': True, 'synthetic_only': True})
receipt = {'scope': 'Offline injected exceptions only; no live credentials, processes, network or database calls',
           'passed': len(results), 'failed': 0, 'cases': results,
           'source_sha256': {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in (repo/'tests/sql/kvtest.py', repo/'tests/sql/kvfaults.py')},
           'control_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
print(json.dumps(receipt, indent=2))
