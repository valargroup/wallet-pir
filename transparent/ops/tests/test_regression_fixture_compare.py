#!/usr/bin/env python3
"""Check the pre-freeze comparison of a re-cut regression fixture."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
_spec = importlib.util.spec_from_file_location(
    'compare', ROOT / 'transparent/ops/scripts/compare-regression-fixtures.py')
compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(compare)

P2PKH = '76a914' + '11' * 20 + '88ac'


def state(events=0, utxos=0, balance=0):
    return {
        'events': [f'event-{i}' for i in range(events)],
        'utxos': {str(i): 1 for i in range(utxos)},
        'spends': {},
        'history': {},
        'confirmed_balance': balance,
    }


def checkpoint(height, hash_=None, **kw):
    return {
        'anchor': {'height': height, 'hash': hash_ or f'{height:064x}'},
        'expected': state(**kw),
    }


def fixture(anchor, cutoff, cases, map_sha='a' * 64):
    heights = {str(h) for case in cases for h in
               [c['anchor']['height'] for c in case['checkpoints']]}
    heights.add(str(anchor))
    return {
        'schema': 'transparent-regression-v1',
        'map_sha256': map_sha,
        'cutoff_height': cutoff,
        'accepted_headers': {h: f'{int(h):064x}' for h in heights},
        'cases': cases,
    }


def case(case_id='small-active', profile='small-active', scripts=None,
         checkpoints=None):
    return {
        'id': case_id,
        'profile': profile,
        'scripts': scripts if scripts is not None else [P2PKH],
        'required_from': 0,
        'checkpoints': checkpoints or [
            checkpoint(100, events=1, utxos=1, balance=500),
            checkpoint(200, events=1, utxos=1, balance=500),
        ],
    }


class CompareTest(unittest.TestCase):
    def setUp(self):
        self.previous = fixture(200, 50, [case()])
        self.next_ = fixture(
            300, 60,
            [case(checkpoints=[
                checkpoint(100, events=1, utxos=1, balance=500),
                checkpoint(300, events=1, utxos=1, balance=500),
            ])],
            map_sha='b' * 64)

    def run_compare(self, previous=None, nxt=None, tolerance=0.25):
        return compare.compare(previous or self.previous, nxt or self.next_,
                               tolerance)

    def test_clean_recut_has_no_findings(self):
        blocking, review, _ = self.run_compare()
        self.assertEqual(blocking, [])
        self.assertEqual(review, [])

    def test_shared_checkpoint_with_changed_state_blocks(self):
        nxt = copy.deepcopy(self.next_)
        # Same height 100, same hash, different reduction: sealed history moved.
        nxt['cases'][0]['checkpoints'][0]['expected'] = state(
            events=2, utxos=1, balance=500)
        blocking, _, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('different expected state' in b for b in blocking),
                        blocking)

    def test_shared_checkpoint_with_changed_hash_reports_reorg(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['checkpoints'][0]['anchor']['hash'] = 'f' * 64
        blocking, _, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('reorganised' in b for b in blocking), blocking)

    def test_a_reorg_is_not_also_reported_as_a_state_difference(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['checkpoints'][0]['anchor']['hash'] = 'f' * 64
        nxt['cases'][0]['checkpoints'][0]['expected'] = state(events=9)
        blocking, _, _ = self.run_compare(nxt=nxt)
        self.assertEqual(len(blocking), 1, blocking)

    def test_changed_accepted_header_blocks(self):
        nxt = copy.deepcopy(self.next_)
        nxt['accepted_headers']['100'] = 'e' * 64
        blocking, _, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('disagree on the chain' in b for b in blocking),
                        blocking)

    def test_anchor_must_advance(self):
        blocking, _, _ = self.run_compare(nxt=copy.deepcopy(self.previous))
        self.assertTrue(any('not above' in b for b in blocking), blocking)

    def test_same_anchor_schema_replacement_is_explicit(self):
        nxt = copy.deepcopy(self.previous)
        nxt['map_sha256'] = 'b' * 64
        self.assertTrue(compare.compare(self.previous, nxt, .25)[0])
        blocking, review, _ = compare.compare(self.previous, nxt, .25, same_anchor=True)
        self.assertEqual((blocking, review), ([], []))

    def test_same_anchor_mode_refuses_changed_or_missing_case_context(self):
        for change in ['birthday', 'checkpoint', 'state', 'cutoff', 'identity', 'header']:
            with self.subTest(change=change):
                nxt = copy.deepcopy(self.previous)
                nxt['map_sha256'] = 'b' * 64
                if change == 'birthday':
                    nxt['cases'][0]['required_from'] = 1
                elif change == 'checkpoint':
                    nxt['cases'][0]['checkpoints'].pop(0)
                elif change == 'state':
                    nxt['cases'][0]['checkpoints'][0]['expected']['confirmed_balance'] += 1
                elif change == 'cutoff':
                    nxt['cutoff_height'] += 1
                elif change == 'identity':
                    nxt['map'] = {'genesis_hash': 'f' * 64}
                else:
                    nxt['accepted_headers']['200'] = 'f' * 64
                self.assertTrue(compare.compare(self.previous, nxt, .25, same_anchor=True)[0])

    def test_same_anchor_mode_refuses_advancing_or_regressing_anchors(self):
        for anchor in [199, 201]:
            nxt = copy.deepcopy(self.previous)
            nxt['accepted_headers'].pop('200')
            nxt['accepted_headers'][str(anchor)] = f'{anchor:064x}'
            self.assertTrue(compare.compare(self.previous, nxt, .25, same_anchor=True)[0])

    def test_unchanged_map_is_flagged_for_review(self):
        nxt = copy.deepcopy(self.next_)
        nxt['map_sha256'] = self.previous['map_sha256']
        _, review, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('gates the same publication' in r for r in review),
                        review)

    def test_unused_case_that_became_active_is_flagged(self):
        previous = fixture(200, 50, [case('unused-p2pkh', 'unused', checkpoints=[
            checkpoint(100), checkpoint(200)])])
        nxt = fixture(300, 60, [case('unused-p2pkh', 'unused', checkpoints=[
            checkpoint(100), checkpoint(300, events=3, utxos=1, balance=7)])],
            map_sha='b' * 64)
        _, review, _ = self.run_compare(previous, nxt)
        self.assertTrue(any('was 0 at the old anchor' in r for r in review),
                        review)

    def test_large_anchor_state_move_is_flagged(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['checkpoints'][-1]['expected'] = state(
            events=100, utxos=1, balance=500)
        _, review, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('events 1 -> 100' in r for r in review), review)

    def test_small_anchor_state_move_is_within_tolerance(self):
        previous = fixture(200, 50, [case(checkpoints=[
            checkpoint(100, events=100), checkpoint(200, events=100)])])
        nxt = fixture(300, 60, [case(checkpoints=[
            checkpoint(100, events=100), checkpoint(300, events=110)])],
            map_sha='b' * 64)
        _, review, _ = self.run_compare(previous, nxt)
        self.assertEqual(review, [])

    def test_profile_and_script_changes_are_flagged(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['profile'] = 'unused'
        nxt['cases'][0]['scripts'] = [P2PKH, '76a914' + '22' * 20 + '88ac']
        _, review, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('profile' in r for r in review), review)
        self.assertTrue(any('script set changed' in r for r in review), review)

    def test_added_and_dropped_cases_are_flagged(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['id'] = 'brand-new'
        _, review, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('dropped' in r for r in review), review)
        self.assertTrue(any('new case' in r for r in review), review)

    def test_no_shared_checkpoint_is_flagged(self):
        nxt = copy.deepcopy(self.next_)
        nxt['cases'][0]['checkpoints'][0] = checkpoint(150, events=1, utxos=1,
                                                       balance=500)
        _, review, _ = self.run_compare(nxt=nxt)
        self.assertTrue(any('no checkpoint height in common' in r for r in review),
                        review)

    def metadata_pair(self):
        """A legacy fixture and its v3 re-export: same events plus metadata."""
        legacy = [{'script': P2PKH, 'event': f'{i:02x}' * 87} for i in (1, 2)]
        extended = [{'script': e['script'], 'event': e['event'] + '2802'}
                    for e in legacy]
        previous = copy.deepcopy(self.previous)
        nxt = copy.deepcopy(self.next_)
        previous['cases'][0]['checkpoints'][0]['expected']['events'] = legacy
        nxt['cases'][0]['checkpoints'][0]['expected']['events'] = extended
        return previous, nxt

    def test_appended_event_metadata_blocks_unless_explicit(self):
        previous, nxt = self.metadata_pair()
        blocking, _, _ = compare.compare(previous, nxt, .25)
        self.assertTrue(any('different expected state' in b for b in blocking),
                        blocking)
        blocking, review, notes = compare.compare(
            previous, nxt, .25, event_metadata=True)
        self.assertEqual((blocking, review), ([], []))
        self.assertTrue(any('legacy 87-byte' in n for n in notes), notes)

    def test_event_metadata_mode_still_compares_everything_else(self):
        for change in ['legacy', 'event', 'balance', 'spends', 'history']:
            with self.subTest(change=change):
                previous, nxt = self.metadata_pair()
                expected = nxt['cases'][0]['checkpoints'][0]['expected']
                if change == 'legacy':
                    expected['events'][0]['event'] = 'ff' + expected['events'][0]['event'][2:]
                elif change == 'event':
                    expected['events'].pop()
                elif change == 'balance':
                    expected['confirmed_balance'] += 1
                elif change == 'spends':
                    expected['spends']['x:0'] = {}
                else:
                    expected['history']['x'] = {}
                blocking, _, _ = compare.compare(
                    previous, nxt, .25, event_metadata=True)
                self.assertTrue(
                    any('different expected state' in b for b in blocking),
                    blocking)

    def test_event_metadata_mode_refuses_misuse(self):
        previous, nxt = self.metadata_pair()
        # The previous fixture already carries metadata: truncating would hide
        # a metadata change.
        carried = copy.deepcopy(previous)
        carried['cases'][0]['checkpoints'][0]['expected']['events'] = copy.deepcopy(
            nxt['cases'][0]['checkpoints'][0]['expected']['events'])
        blocking, _, _ = compare.compare(carried, nxt, .25, event_metadata=True)
        self.assertTrue(any('already carries event metadata' in b for b in blocking),
                        blocking)
        short = nxt['cases'][0]['checkpoints'][0]['expected']['events']
        short[0]['event'] = short[0]['event'][:100]
        blocking, _, _ = compare.compare(previous, nxt, .25, event_metadata=True)
        self.assertTrue(any('shorter than the legacy' in b for b in blocking),
                        blocking)

    def test_unsupported_schema_is_refused(self):
        bad = ROOT / 'ops/tests/_bad_fixture.json'
        bad.write_text(json.dumps({'schema': 'something-else'}))
        try:
            with self.assertRaises(RuntimeError):
                compare.load(bad)
        finally:
            bad.unlink()

    def test_frozen_fixture_compares_with_itself_without_state_differences(self):
        frozen = compare.load(
            ROOT / 'transparent/tools/transparent-regression/fixtures/mainnet.json')
        blocking, _, _ = compare.compare(frozen, frozen, 0.25)
        # The degenerate self-comparison flags only the non-advancing anchor.
        self.assertEqual(
            [b for b in blocking if 'not above' not in b], [])


if __name__ == '__main__':
    unittest.main()
