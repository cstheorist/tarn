#!/usr/bin/env python3
"""Apply the #186 final-plan guard experiment to an owned source copy only.

The production checkout is never patched by this module. The caller copies
``lua`` (and any fixture scripts) into a directory without Git metadata, then
passes that directory to ``apply``. Both exact source anchors are validated
before either file is replaced. No barrier, clock check, control-space check,
writable check, per-space property check or direct F.space call is removed.

Only one mode value is reused, inside the existing nonyielding final plan
check. It cannot survive a yield or another invocation. The runner must retain
the original/candidate source hashes and independently verify runtime flags.
"""
from pathlib import Path
import os
import re
import stat
import tempfile


_SPACE = b"""    function F.space(sp)
        local mode = F.mode()
        if mode == 3 then unavailable('unsupported replicated configuration') end
        if mode == 2 and (not sp.is_sync or not sp.state.is_sync) then
            unavailable('space ' .. sp.name .. ' is not synchronous')
        end
    end
"""
_BATCH_SPACE = b"""    local function space_mode(sp, mode)
        if mode == 3 then unavailable('unsupported replicated configuration') end
        if mode == 2 and (not sp.is_sync or not sp.state.is_sync) then
            unavailable('space ' .. sp.name .. ' is not synchronous')
        end
    end
    function F.space(sp) return space_mode(sp, F.mode()) end
    -- Experiment only: the mode belongs to this nonyielding final check.
    -- Do not retain it across a barrier, data access, yield or invocation.
    function F.plans(plans, space_pos)
        local mode = F.controls()
        c.writable()
        for _, plan in ipairs(plans) do space_mode(plan[space_pos], mode) end
    end
"""
_PLANS = b"""function BK.fresh_plans(plans)
    M.fresh.clock_check(); M.fresh.controls(); writable()
    for _, p in ipairs(plans) do M.fresh.space(p[P_SPACE]) end
end
"""
_BATCH_PLANS = b"""function BK.fresh_plans(plans)
    M.fresh.clock_check(); M.fresh.plans(plans, P_SPACE)
end
"""
_PLAN_LAYOUT = b'local P_SPACE, P_PK, P_OLD, P_NEW, P_RES, P_SKIP = 1, 2, 3, 4, 5, 6'


def _once(source: bytes, anchor: bytes, label: str) -> None:
    count = source.count(anchor)
    if count != 1:
        raise ValueError(f'{label}: expected one exact source anchor, found {count}')


def _replace_file(path: Path, source: bytes) -> None:
    """Replace the copied inode; do not mutate an accidentally hard-linked source."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + '.', delete=False) as output:
            temporary = Path(output.name)
            output.write(source)
        temporary.chmod(stat.S_IMODE(path.stat().st_mode))
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def apply(source_root: Path) -> None:
    """Patch an owned non-Git source copy, refusing stale or ambiguous inputs.

    Validation failures leave both files byte-for-byte unchanged. A filesystem
    failure can leave an incomplete candidate copy; the caller must discard it
    rather than run it. Reapplying the experiment is an error, not a no-op.
    """
    root = Path(source_root).resolve(strict=True)
    if (root / '.git').exists() or (root / '.git').is_symlink():
        raise ValueError('experiment requires an owned source copy without .git')
    lua = root / 'lua'
    if lua.is_symlink():
        raise ValueError('experiment refuses a symlinked lua directory')
    fresh, tarn = lua / 'tarn_fresh.lua', lua / 'tarn.lua'
    for path in (fresh, tarn):
        if path.is_symlink():
            raise ValueError(f'experiment refuses symlinked source: {path.name}')
        if not path.is_file():
            raise ValueError(f'experiment requires a regular source file: {path.name}')
    fresh_source, tarn_source = fresh.read_bytes(), tarn.read_bytes()
    _once(fresh_source, _SPACE, 'F.space')
    _once(tarn_source, _PLANS, 'BK.fresh_plans')
    _once(tarn_source, _PLAN_LAYOUT, 'P_SPACE layout')
    # A later implementation could introduce these names outside our anchors.
    # Refuse it instead of silently shadowing or replacing another helper.
    if re.search(rb'\bspace_mode\b|\bF\s*[.:]\s*plans\b', fresh_source):
        raise ValueError('freshness source already contains an experiment helper name')
    _replace_file(fresh, fresh_source.replace(_SPACE, _BATCH_SPACE, 1))
    _replace_file(tarn, tarn_source.replace(_PLANS, _BATCH_PLANS, 1))
