#!/usr/bin/env python3
"""Run isolated companion contracts; keep logs and expected failing controls."""
import argparse
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--jar', type=Path, required=True)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
source = Path(__file__).resolve().parent / 'rc-cost-models'
expected_failures = {
    'Bug_PlanGuardSkipSpace': 'NoUnsafeAcceptance',
    'Bug_PlanGuardYield': 'NoUnsafeAcceptance',
    'Bug_ConfirmationCleanupEarlyRetirement': 'PendingIsTracked',
    'Bug_ConfirmationCleanupEarlyUnlock': 'NoUnlockBeforeSettlement',
    'Bug_ConfirmationCleanupStaleRead': 'NoStaleLockRead',
    'Witness_ConfirmationCleanupCallbackFinish': 'NoWaiterFinishBeforeCallerResume',
}
args.out.mkdir(parents=True, exist_ok=False)
for path in source.iterdir():
    if path.suffix in ('.tla', '.cfg'):
        shutil.copyfile(path, args.out / path.name)
for config in sorted(args.out.glob('*.cfg')):
    module = 'PlanGuardBatch' if 'PlanGuard' in config.stem else 'ConfirmationCleanup'
    log = config.with_suffix('.out')
    with log.open('w') as output:
        result = subprocess.run(['java', '-XX:+UseParallelGC', '-Xmx512m', '-cp', str(args.jar.resolve()),
                                 'tlc2.TLC', '-workers', '1', '-config', config.name, module],
                                cwd=args.out, stdout=output, stderr=subprocess.STDOUT, timeout=120)
    text = log.read_text()
    if config.stem.startswith(('Bug_', 'Witness_')):
        expected = expected_failures[config.stem]
        assert result.returncode != 0 and f'Invariant {expected} is violated' in text, (config.name, text[-4000:])
    else:
        assert result.returncode == 0 and 'No error has been found' in text, (config.name, text[-4000:])
    print(config.name + ': expected result', flush=True)
