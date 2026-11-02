"""Apply the experimental RC confirmation cleanup to a disposable source copy.

This changes neither admission nor commit dispatch. Only the existing confirmed
commit callback retires its in-flight marker before it grants queued row locks.
The cleanup after box.commit (including its failure path) remains idempotent.
Do not invoke this on the working production source tree.
"""
from pathlib import Path


_FUNCTION = 'function tarn_commit_rc(txid, writes, gen, fence, wb)\n'
_BEFORE = '''            if r == nil then
                box.on_commit(function() pl_release_all(txid) end)
                local okc, cerr = pcall(box.commit)
                inflight_end(seq)
                if not okc then error(cerr) end
                M.stats.commit_rc = M.stats.commit_rc + 1
'''
_AFTER = '''            if r == nil then
                box.on_commit(function()
                    -- Experimental: this callback already certifies visibility.
                    -- Retire the confirmed attempt before granting its waiters.
                    inflight_end(seq)
                    pl_release_all(txid)
                end)
                local okc, cerr = pcall(box.commit)
                inflight_end(seq)
                if not okc then error(cerr) end
                M.stats.commit_rc = M.stats.commit_rc + 1
'''
_FINAL_CLEANUP = '''    if seq then inflight_end(seq) end
    if not ok then
        if box.is_in_txn() then box.rollback() end
'''


def apply(source_root: Path) -> None:
    """Patch exactly the expected RC function, refusing drift or repeat use."""
    path = source_root / 'lua' / 'tarn.lua'
    source = path.read_text()
    if source.count(_FUNCTION) != 1:
        raise ValueError('expected exactly one tarn_commit_rc definition')
    start = source.index(_FUNCTION)
    end = source.find('\nend\n', start)
    if end < 0:
        raise ValueError('cannot locate the end of tarn_commit_rc')
    end += len('\nend\n')
    body = source[start:end]
    if body.count(_BEFORE) != 1 or body.count(_FINAL_CLEANUP) != 1:
        raise ValueError('RC confirmation or failure cleanup differs from the expected source')
    candidate = body.replace(_BEFORE, _AFTER, 1)
    path.write_text(source[:start] + candidate + source[end:])
