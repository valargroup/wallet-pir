#!/usr/bin/env python3
"""Check the regression case re-cut against the frozen specification it replaces."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
FROZEN = ROOT / 'server/transparent-regression/fixtures/mainnet-cases.json'
_spec = importlib.util.spec_from_file_location(
    'recut', ROOT / 'ops/scripts/recut-regression-cases.py')
recut = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recut)


class RecutTest(unittest.TestCase):
    def setUp(self):
        self.previous = json.loads(FROZEN.read_text())

    def test_roles_reproduce_the_frozen_specification(self):
        # The classification is only trustworthy because it regenerates the
        # reviewed file exactly; a misclassified checkpoint fails here.
        again = recut.build(
            self.previous, recut.PREVIOUS_ANCHOR, recut.PREVIOUS_CUTOFF, 0)
        self.assertEqual(again, self.previous)

    def test_scripts_are_carried_over_untouched(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        for new, old in zip(cases, self.previous):
            self.assertEqual(new['id'], old['id'])
            self.assertEqual(new['scripts'], old['scripts'])
            self.assertEqual(new['profile'], old['profile'])

    def test_boundary_probes_follow_the_cutoff(self):
        cases = {c['id']: c for c in recut.build(self.previous, 3479900, 3270771, 0)}
        self.assertEqual(
            cases['multi-script-self-transfer']['heights'][:3],
            [3270770, 3270771, 3270772])
        self.assertEqual(
            cases['old-receive-recent-spend']['heights'][1:3], [3270770, 3270771])
        self.assertEqual(cases['recent-birthday']['required_from'], 3270771)

    def test_history_pinned_heights_do_not_move(self):
        cases = {c['id']: c for c in recut.build(self.previous, 3479900, 3270771, 0)}
        self.assertEqual(
            cases['offline-receive-spend']['heights'][:5],
            [163548, 163726, 3439126, 3465622, 3472534])
        # Real coinbase outputs, not a tip offset.
        self.assertEqual(cases['coinbase']['heights'][:2], [3473678, 3473679])

    def test_every_case_terminates_at_the_anchor(self):
        for tail in (0, 1, 3):
            for case in recut.build(self.previous, 3479900, 3270771, tail):
                self.assertEqual(case['heights'][-1], 3479900)

    def test_tail_checkpoints_land_in_the_new_span(self):
        # recent-birthday's own anchor-1 checkpoint also falls in this span, so
        # compare against the sampled heights rather than counting the range.
        sampled = recut.tail_heights(3479900, 2)
        self.assertTrue(all(recut.PREVIOUS_ANCHOR < h < 3479900 for h in sampled))
        for case in recut.build(self.previous, 3479900, 3270771, 2):
            for height in sampled:
                self.assertIn(height, case['heights'], case['id'])

    def test_no_tail_checkpoints_when_not_requested(self):
        sampled = recut.tail_heights(3479900, 2)
        for case in recut.build(self.previous, 3479900, 3270771, 0):
            for height in sampled:
                self.assertNotIn(height, case['heights'], case['id'])

    def test_check_accepts_a_recut_specification(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        recut.check(cases, 3479900, 3270771)

    def test_check_refuses_a_checkpoint_above_the_anchor(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        cases[0]['heights'][-1] = 3479901
        with self.assertRaises(RuntimeError):
            recut.check(cases, 3479900, 3270771)

    def test_check_refuses_unsorted_or_duplicated_heights(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        cases[0]['heights'] = [5, 4, 3479900]
        with self.assertRaises(RuntimeError):
            recut.check(cases, 3479900, 3270771)

    def test_check_refuses_a_checkpoint_before_the_birthday(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        target = next(c for c in cases if c['id'] == 'reused-pages')
        target['heights'] = [1] + target['heights']
        with self.assertRaises(RuntimeError):
            recut.check(cases, 3479900, 3270771)

    def test_check_refuses_a_noncanonical_script(self):
        cases = recut.build(self.previous, 3479900, 3270771, 1)
        cases[0]['scripts'] = ['00' * 25]
        with self.assertRaises(RuntimeError):
            recut.check(cases, 3479900, 3270771)

    def test_build_refuses_an_unknown_case_set(self):
        with self.assertRaises(RuntimeError):
            recut.build(self.previous[:-1], 3479900, 3270771, 0)

    def test_build_refuses_a_changed_profile(self):
        altered = json.loads(json.dumps(self.previous))
        altered[0]['profile'] = 'something-else'
        with self.assertRaises(RuntimeError):
            recut.build(altered, 3479900, 3270771, 0)

    def test_tail_sampling_refuses_too_short_a_span(self):
        with self.assertRaises(RuntimeError):
            recut.tail_heights(recut.PREVIOUS_ANCHOR + 1, 2)

    def test_hazards_flag_the_moved_birthday_only_when_the_cutoff_moves(self):
        moved = ' '.join(recut.hazards(3479900, 3270771))
        self.assertIn('required_from moves', moved)
        held = ' '.join(recut.hazards(3479900, recut.PREVIOUS_CUTOFF))
        self.assertNotIn('required_from moves', held)


if __name__ == '__main__':
    unittest.main()
