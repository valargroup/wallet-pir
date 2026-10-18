"""Exhaustive check of the membership and actuation protocol (model/membership.py).

Every configuration's whole reachable state space must keep every invariant,
reach its goals (so a clean run is not vacuous) and take the transitions that
matter; every deliberate protocol bug must be caught.
"""
import concurrent.futures
import os
import sys
import time
import unittest

from model import membership as MM


def workers():
    try:
        return max(1, int(os.environ.get('TRANSPARENT_SIM_WORKERS', min(4, os.cpu_count() or 1))))
    except ValueError:
        return 1


def run_tasks(tasks):
    if workers() == 1:
        return [MM.run_task(t) for t in tasks]
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers()) as pool:
            return list(pool.map(MM.run_task, tasks))
    except (OSError, NotImplementedError, concurrent.futures.process.BrokenProcessPool):
        return [MM.run_task(t) for t in tasks]


class MembershipModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tasks = [('config', name) for name in MM.CONFIGS] + [('mutant', name) for name in MM.MUTANTS]
        started = time.monotonic()
        cls.results = {r['task']: r for r in run_tasks(tasks)}
        cls.elapsed = time.monotonic() - started

    def test_every_configuration_keeps_every_invariant(self):
        total = 0
        for name in MM.CONFIGS:
            result = self.results[('config', name)]
            total += result['states']
            with self.subTest(config=name):
                self.assertEqual(result['violations'], [])
                self.assertGreater(result['states'], 10_000)
                goals = set(MM.GOALS[name](MM.CONFIGS[name]))
                self.assertEqual(set(result['reached']), goals)
                missing = set(MM.LABELS[name]) - set(result['labels'])
                self.assertEqual(missing, set())
        print(f'\nmembership model: {len(MM.CONFIGS)} configurations, {total} states, '
              f'{len(MM.MUTANTS)} mutants, {self.elapsed:.1f} s on {workers()} processes', file=sys.stderr)

    def test_every_mutant_is_caught(self):
        expected = {
            'stop-guard-at-entry-only': 'with fewer than two others serving',
            'no-drain-filter': 'routed while an enrolled replica is routed',
            'drain-filter-always': 'router has no recent replica',
            'no-generation-check': 'routed without attesting the active publication',
            'break-before-make': 'before e serves',
            'boot-before-enroll': 'booted before it was enrolled',
            'destroy-before-retire': 'destroyed e before it was retired',
        }
        self.assertEqual(set(expected), set(MM.MUTANTS))
        for name, text in expected.items():
            with self.subTest(mutant=name):
                violations = self.results[('mutant', name)]['violations']
                self.assertTrue(any(text in v for v, _ in violations), violations)

    def test_initial_states_are_consistent(self):
        for name, config in MM.CONFIGS.items():
            model = MM.Model(config)
            state = model.initial()
            self.assertEqual(model.check(state), [], name)
            serving = [m for m in model.recent if model.serving(state, m)]
            self.assertGreaterEqual(len(serving), 2, name)

    def test_every_configuration_is_the_single_archive_owner_topology(self):
        # One static owner holds the whole archive and is enrolled and routed
        # from the start; every other member is a recent replica.
        for name, config in MM.CONFIGS.items():
            model = MM.Model(config)
            state = model.initial()
            self.assertEqual([m for m in model.members if m not in model.recent], [MM.ARCHIVE], name)
            self.assertEqual(state[MM.INTENT][MM.ARCHIVE], MM.ENROLLED, name)
            self.assertTrue(state[MM.RENDERED] & MM.bit(MM.ARCHIVE), name)


if __name__ == '__main__':
    unittest.main()
