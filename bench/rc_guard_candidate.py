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
    -- F.space with the mode a caller read (F.mode) in the same check, with
    -- no yield since (BK.fresh_spaces).
    function F.space_in(sp, mode)
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
    -- F.space with the mode a caller read (F.mode) in the same check, with
    -- no yield since (BK.fresh_spaces).
    function F.space_in(sp, mode) return space_mode(sp, mode) end
    -- The mode belongs only to this nonyielding final check.
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


def apply(source_root: Path, variant: str = 'candidate') -> None:
    """Generate a scalar control or batched candidate from either exact source.

    Validate both files before replacing either inode. Discard the copy after
    any filesystem failure. Matching variants are validated and left intact.
    """
    if variant not in ('control', 'candidate'):
        raise ValueError('unknown guard variant')
    source_root = Path(source_root)
    if source_root.is_symlink():
        raise ValueError('experiment refuses a symlinked source root')
    root = source_root.resolve(strict=True)
    if any((p / '.git').exists() or (p / '.git').is_symlink()
           for p in (root, *root.parents)):
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
    _once(tarn_source, _PLAN_LAYOUT, 'P_SPACE layout')
    # Only the exact scalar or production batched pair is admitted. Reject
    # mixed states, drift, duplicate definitions and helpers outside the block.
    choices = ((_SPACE, _PLANS, 'control'), (_BATCH_SPACE, _BATCH_PLANS, 'candidate'))
    matches = [(f, t, name) for f, t, name in choices
               if fresh_source.count(f) == 1 and tarn_source.count(t) == 1]
    if len(matches) != 1:
        raise ValueError('expected one exact paired scalar or batched source anchor')
    old_f, old_t, current = matches[0]
    remainder = fresh_source.replace(old_f, b'', 1)
    if re.search(rb'\bspace_mode\b|\bF\s*[.:]\s*plans\b|function\s+F\s*[.:]\s*space\b|F\s*\.\s*space\s*=', remainder):
        raise ValueError('freshness source contains an ambiguous guard helper name')
    if re.search(rb'function\s+BK\s*[.:]\s*fresh_plans\b|'
                 rb'BK\s*[.:]\s*fresh_plans\s*=', tarn_source.replace(old_t, b'', 1)):
        raise ValueError('ambiguous BK.fresh_plans definition')
    if current == variant:
        return
    new_f, new_t, _ = next(c for c in choices if c[2] == variant)
    _replace_file(fresh, fresh_source.replace(old_f, new_f, 1))
    _replace_file(tarn, tarn_source.replace(old_t, new_t, 1))
