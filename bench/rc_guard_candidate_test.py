#!/usr/bin/env python3
"""Source-bound controls for the copy-only #186 guard transformation."""
from pathlib import Path
import os
import re
import shutil
import tempfile
import unittest

from rc_guard_candidate import apply


ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('tarn_fresh.lua', 'tarn.lua')


class Candidate(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rc186-guard-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / 'lua').mkdir()
        for name in TARGETS:
            shutil.copy2(ROOT / 'lua' / name, self.root / 'lua' / name)
        apply(self.root, 'control')
        self.protected = self.root / 'lua' / 'tarn_engine.lua'
        self.protected.write_bytes(b'protected source must remain unchanged\n')

    def snapshot(self):
        return {p.name: p.read_bytes() for p in (self.root / 'lua').iterdir() if p.is_file()}

    def assert_refused(self, pattern):
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, pattern):
            apply(self.root)
        self.assertEqual(self.snapshot(), before, 'validation must precede both writes')

    def test_current_source_changes_only_the_two_final_guard_blocks(self):
        before = self.snapshot()
        apply(self.root)
        after = self.snapshot()
        self.assertEqual(set(before), set(after))
        self.assertEqual(after['tarn_engine.lua'], before['tarn_engine.lua'])
        old, new = before['tarn_fresh.lua'], after['tarn_fresh.lua']
        start, end = b'    function F.space(sp)\n', b'    -- These ordinary Lua guards run'
        old_prefix, tail = old.split(start, 1)
        old_body, old_suffix = tail.split(end, 1)
        new_prefix, tail = new.split(b'    local function space_mode(sp, mode)\n', 1)
        new_body, new_suffix = tail.split(end, 1)
        self.assertEqual(new_prefix, old_prefix, 'mode, controls and clock code changed')
        self.assertEqual(new_suffix, old_suffix, 'native/stream/barrier code changed')
        self.assertTrue(new_body.startswith(old_body.removeprefix(b'        local mode = F.mode()\n')),
                        'every original per-space check must remain exact')
        self.assertIn(b'function F.space(sp) return space_mode(sp, F.mode()) end', new_body)
        self.assertRegex(new_body, rb'function F\.plans\(plans, space_pos\)\n'
                         rb'        local mode = F\.controls\(\)\n'
                         rb'        c\.writable\(\)\n'
                         rb'        for _, plan in ipairs\(plans\) do space_mode\(plan\[space_pos\], mode\) end\n'
                         rb'    end\n$')
        old_guard = re.search(rb'function BK\.fresh_plans\(plans\).*?\nend\n', before['tarn.lua'], re.S)
        new_guard = re.search(rb'function BK\.fresh_plans\(plans\).*?\nend\n', after['tarn.lua'], re.S)
        self.assertEqual(new_guard[0], b'function BK.fresh_plans(plans)\n'
                         b'    M.fresh.clock_check(); M.fresh.plans(plans, P_SPACE)\nend\n')
        self.assertEqual(before['tarn.lua'].replace(old_guard[0], b'', 1),
                         after['tarn.lua'].replace(new_guard[0], b'', 1),
                         'lock entry, fresh_spaces, commit and protected paths changed')

    def test_stale_space_anchor(self):
        path = self.root / 'lua' / 'tarn_fresh.lua'
        path.write_bytes(path.read_bytes().replace(b'function F.space(sp)', b'function F.space(space)', 1))
        self.assert_refused('exact paired')

    def test_stale_plan_anchor_leaves_fresh_source_untouched(self):
        path = self.root / 'lua' / 'tarn.lua'
        path.write_bytes(path.read_bytes().replace(b'function BK.fresh_plans(plans)',
                                                  b'function BK.fresh_plans(planned)', 1))
        self.assert_refused('exact paired')

    def test_duplicate_space_anchor(self):
        path = self.root / 'lua' / 'tarn_fresh.lua'
        source = path.read_bytes()
        block = source.split(b'    function F.space(sp)\n', 1)[1].split(b'    -- These ordinary', 1)[0]
        path.write_bytes(source + b'    function F.space(sp)\n' + block)
        self.assert_refused('exact paired')

    def test_duplicate_plan_anchor(self):
        path = self.root / 'lua' / 'tarn.lua'
        source = path.read_bytes()
        block = re.search(rb'function BK\.fresh_plans\(plans\).*?\nend\n', source, re.S)[0]
        path.write_bytes(source + block)
        self.assert_refused('exact paired')

    def test_changed_plan_layout(self):
        path = self.root / 'lua' / 'tarn.lua'
        source = path.read_bytes().replace(b'P_RES, P_SKIP = 1, 2, 3, 4, 5, 6',
                                            b'P_RES, P_SKIP = 2, 1, 3, 4, 5, 6', 1)
        path.write_bytes(source)
        self.assert_refused('P_SPACE layout: expected one exact')

    def test_existing_helper_names(self):
        path = self.root / 'lua' / 'tarn_fresh.lua'
        source = path.read_bytes()
        for extra in (b'function F.plans(p) end\n', b'F . plans = function(p) end\n',
                      b'local function space_mode(sp) end\n'):
            with self.subTest(extra=extra):
                path.write_bytes(source + extra)
                self.assert_refused('ambiguous guard helper name')

    def test_matching_variant_is_validated_and_kept_exact(self):
        before = self.snapshot()
        apply(self.root, 'control')
        self.assertEqual(self.snapshot(), before)
        apply(self.root)
        before = self.snapshot()
        apply(self.root)
        self.assertEqual(self.snapshot(), before)

    def test_reverse_control_restores_exact_scalar_source(self):
        scalar = self.snapshot()
        apply(self.root)
        batched = self.snapshot()
        apply(self.root, 'control')
        self.assertEqual(self.snapshot(), scalar)
        apply(self.root)
        self.assertEqual(self.snapshot(), batched)
        source = {name: (ROOT / 'lua' / name).read_bytes() for name in TARGETS}
        expected = batched if b'function F.plans(' in source['tarn_fresh.lua'] else scalar
        self.assertEqual({name: expected[name] for name in TARGETS}, source)

    def test_mixed_and_drifted_batched_sources_refused_before_writes(self):
        scalar = self.snapshot()
        apply(self.root)
        batched = self.snapshot()
        path = self.root / 'lua/tarn.lua'
        path.write_bytes(scalar['tarn.lua'])
        self.assert_refused('exact paired')
        path.write_bytes(batched['tarn.lua'])
        fresh = self.root / 'lua/tarn_fresh.lua'
        fresh.write_bytes(batched['tarn_fresh.lua'].replace(b'local mode = F.controls()',
                                                         b'local mode = F.mode()', 1))
        self.assert_refused('exact paired')

    def test_reverse_replaces_both_hardlinked_inodes(self):
        apply(self.root)
        saved = {}
        for name in TARGETS:
            source = self.root / 'lua' / name
            link = self.root / name
            os.link(source, link)
            saved[name] = (link, link.read_bytes(), source.stat().st_mode)
        apply(self.root, 'control')
        for name, (link, original, mode) in saved.items():
            self.assertEqual(link.read_bytes(), original)
            self.assertNotEqual((self.root / 'lua' / name).read_bytes(), original)
            self.assertEqual((self.root / 'lua' / name).stat().st_mode, mode)

    def test_batched_anchor_drift_and_duplicates_refuse_control(self):
        apply(self.root)
        original = self.snapshot()
        fresh = self.root / 'lua/tarn_fresh.lua'
        tarn = self.root / 'lua/tarn.lua'
        for mutate in (
            lambda: fresh.write_bytes(original['tarn_fresh.lua'].replace(b'        c.writable()\n', b'', 1)),
            lambda: fresh.write_bytes(original['tarn_fresh.lua'] + b'function F.plans(p) end\n'),
            lambda: tarn.write_bytes(original['tarn.lua'] + b'function BK.fresh_plans(p) end\n'),
        ):
            for name in TARGETS:
                (self.root / 'lua' / name).write_bytes(original[name])
            mutate()
            before = self.snapshot()
            with self.assertRaises(ValueError):
                apply(self.root, 'control')
            self.assertEqual(self.snapshot(), before)

    def test_symlinked_root_is_refused(self):
        link = self.root / 'linked-root'
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlinked source root'):
            apply(link)


    def test_git_checkout_and_worktree_are_refused(self):
        marker = self.root / '.git'
        marker.write_text('gitdir: /unused\n')
        self.assert_refused('without .git')
        marker.unlink()
        marker.mkdir()
        self.assert_refused('without .git')

    def test_symlinked_source_is_refused(self):
        path = self.root / 'lua' / 'tarn_fresh.lua'
        path.unlink()
        path.symlink_to(ROOT / 'lua' / 'tarn_fresh.lua')
        self.assert_refused('symlinked source')

    def test_symlinked_lua_directory_is_refused(self):
        lua = self.root / 'lua'
        shutil.rmtree(lua)
        lua.symlink_to(ROOT / 'lua', target_is_directory=True)
        self.assert_refused('symlinked lua directory')

    def test_replacement_does_not_mutate_hardlinked_input(self):
        path = self.root / 'lua' / 'tarn_fresh.lua'
        saved = self.root / 'saved-source.lua'
        os.link(path, saved)
        original = saved.read_bytes()
        mode = path.stat().st_mode
        apply(self.root)
        self.assertEqual(saved.read_bytes(), original)
        self.assertNotEqual(path.read_bytes(), original)
        self.assertEqual(path.stat().st_mode, mode)


if __name__ == '__main__':
    unittest.main()
