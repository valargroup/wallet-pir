import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('qualification', Path(__file__).parents[1] / 'scripts/enhance-qualification.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class EvidenceGates(unittest.TestCase):
    def setUp(self):
        self.fixture = {'passed': True, 'retained_session_queries': True,
                        'serving_seconds': 21600, 'publications': 300}
        self.samples = {'worker': [dict(time=i * 5, active='active', restarts=0,
            planned_outage=False, **{'memory.current': 5 * 1024**3,
                'memory.peak': 6 * 1024**3, 'memory.events': {'oom_kill': 0},
                'swap_io': {'pswpin': 0, 'pswpout': 0}}) for i in range(4321)]}

    def assess(self):
        return module.assess(self.samples, self.fixture, True, 21600)

    def test_complete_pair_evidence_passes(self):
        self.assertEqual(self.assess(), [])

    def test_missing_or_short_fixture_is_not_accepted(self):
        self.fixture['serving_seconds'] = 21599
        self.assertTrue(self.assess())
        self.fixture = {}
        self.assertTrue(self.assess())

    def test_oom_is_not_hidden_by_a_later_restart(self):
        self.samples['worker'][50]['memory.events'] = {'oom_kill': 1}
        self.assertTrue(any('memory kill' in s for s in self.assess()))

    def test_only_the_planned_outage_is_exempt(self):
        sample = self.samples['worker'][100]
        sample.update(active='inactive', planned_outage=True)
        self.assertEqual(self.assess(), [])
        sample['planned_outage'] = False
        self.assertTrue(any('unexpected service outage' in s for s in self.assess()))

    def test_memory_and_sampling_gaps_fail(self):
        self.samples['worker'][0]['memory.peak'] = 7 * 1024**3
        del self.samples['worker'][100:110]
        reasons = self.assess()
        self.assertTrue(any('peak memory' in s for s in reasons))
        self.assertTrue(any('sampling gap' in s for s in reasons))

    def test_sustained_swap_io_fails(self):
        for i, sample in enumerate(self.samples['worker']):
            sample['swap_io'] = {'pswpin': i, 'pswpout': 0}
        self.assertTrue(any('sustained swap' in s for s in self.assess()))


if __name__ == '__main__':
    unittest.main()
