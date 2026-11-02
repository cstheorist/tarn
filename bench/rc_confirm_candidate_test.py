#!/usr/bin/env python3
"""Fail-closed controls for applying the experimental source-copy patch."""
from pathlib import Path
import tempfile
import unittest

import rc_confirm_candidate as candidate


class CandidatePatch(unittest.TestCase):
    def fixture(self, source):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / 'lua').mkdir()
        path = root / 'lua' / 'tarn.lua'
        path.write_text(source)
        return root, path

    def source(self):
        return ('-- prefix stays unchanged\n' + candidate._FUNCTION
                + candidate._BEFORE + candidate._FINAL_CLEANUP
                + '    end\n    return res\nend\n-- suffix stays unchanged\n')

    def test_changes_only_confirmed_callback(self):
        source = self.source()
        root, path = self.fixture(source)
        candidate.apply(root)
        self.assertEqual(path.read_text(), source.replace(candidate._BEFORE, candidate._AFTER))
        self.assertIn(candidate._FINAL_CLEANUP, path.read_text())

    def test_reapplying_is_refused_without_writing(self):
        root, path = self.fixture(self.source())
        candidate.apply(root)
        applied = path.read_text()
        with self.assertRaises(ValueError):
            candidate.apply(root)
        self.assertEqual(path.read_text(), applied)

    def test_missing_final_failure_cleanup_is_refused_without_writing(self):
        source = self.source().replace(candidate._FINAL_CLEANUP, '')
        root, path = self.fixture(source)
        with self.assertRaises(ValueError):
            candidate.apply(root)
        self.assertEqual(path.read_text(), source)

    def test_ambiguous_or_drifted_source_is_refused_without_writing(self):
        for source in (self.source() * 2,
                       self.source().replace('local okc, cerr = pcall(box.commit)', 'box.commit()'),
                       self.source().replace(candidate._FUNCTION, 'function renamed()\n')):
            with self.subTest(source=source):
                root, path = self.fixture(source)
                with self.assertRaises(ValueError):
                    candidate.apply(root)
                self.assertEqual(path.read_text(), source)


if __name__ == '__main__':
    unittest.main()
