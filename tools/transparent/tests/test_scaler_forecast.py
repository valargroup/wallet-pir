"""Forecast: daily samples, least squares and days to the recent tier's limits."""
import unittest

from scaler_fixtures import NOW, member, snapshot
from scaler import forecast as F

DAY = F.DAY


def snap(resident, memory, budget=100.0, limit=100.0):
    members = {'a': member(role='archive-owner'),
               'r1': member(cache_resident_bytes=resident, cache_budget_bytes=budget,
                            cgroup_memory_bytes=memory, cgroup_memory_max_bytes=limit),
               'r2': member(cache_resident_bytes=resident / 2, cache_budget_bytes=budget,
                            cgroup_memory_bytes=memory / 2, cgroup_memory_max_bytes=limit),
               'r3': member(serving=False, cache_resident_bytes=99.0, cache_budget_bytes=100.0)}
    return snapshot(members=members)


class ForecastTests(unittest.TestCase):
    def test_observe_takes_the_worst_serving_recent_replica(self):
        self.assertEqual(F.observe(snap(60.0, 70.0)), (0.6, 0.7))
        self.assertEqual(F.observe(snapshot(members={'r': member(cache_budget_bytes=0)})), (None, None))

    def test_one_sample_per_day_keeping_the_maximum(self):
        samples = F.update([], snap(50.0, 60.0), NOW)
        samples = F.update(samples, snap(55.0, 58.0), NOW + 60)
        self.assertEqual(len(samples), 1)
        self.assertEqual((samples[0]['resident_ratio'], samples[0]['memory_ratio']), (0.55, 0.6))
        samples = F.update(samples, snap(52.0, 61.0), NOW + DAY)
        self.assertEqual(len(samples), 2)
        unchanged = F.update(samples, snapshot(members={}), NOW + DAY)
        self.assertEqual(unchanged, samples)

    def test_keeps_fourteen_days(self):
        samples = []
        for day in range(20):
            samples = F.update(samples, snap(50.0 + day, 50.0), NOW + day * DAY)
        self.assertEqual(len(samples), 14)
        self.assertEqual(samples[0]['resident_ratio'], 0.56)

    def test_least_squares_recovers_a_line(self):
        intercept, slope = F.fit([(0, 1.0), (1, 3.0), (2, 5.0), (3, 7.0)])
        self.assertAlmostEqual(intercept, 1.0)
        self.assertAlmostEqual(slope, 2.0)
        intercept, slope = F.fit([(0, 0.0), (1, 1.0), (2, 0.0), (3, 1.0)])
        self.assertAlmostEqual(slope, 0.2)
        self.assertIsNone(F.fit([(1, 1.0)]))
        self.assertIsNone(F.fit([(1, 1.0), (1, 2.0)]))

    def test_days_until_the_cache_budget(self):
        # 50% growing one point a day: 85% is 35 days after day 0, 26 after day 9.
        samples = []
        for day in range(10):
            samples = F.update(samples, snap(50.0 + day, 10.0), NOW + day * DAY)
        now = NOW + 9 * DAY
        self.assertAlmostEqual(F.days_until(samples, 'resident_ratio', 0.85, now), 26.0, places=6)
        self.assertIsNone(F.days_until(samples, 'memory_ratio', 0.90, now))  # flat
        result = F.forecast(samples, now)
        self.assertEqual(result['days_to_recent_budget'], 26.0)
        self.assertEqual(result['days_to_cache_budget'], 26.0)
        self.assertIsNone(result['days_to_memory_max'])
        self.assertTrue(result['alert'])
        self.assertFalse(F.forecast(samples, now, alert_days=20)['alert'])

    def test_sooner_limit_wins_and_edge_cases(self):
        samples = []
        for day in range(5):
            samples = F.update(samples, snap(10.0 + day, 80.0 + 2 * day), NOW + day * DAY)
        now = NOW + 4 * DAY
        result = F.forecast(samples, now)
        self.assertAlmostEqual(result['days_to_memory_max'], 1.0)
        self.assertEqual(result['days_to_recent_budget'], result['days_to_memory_max'])
        self.assertLess(result['days_to_memory_max'], result['days_to_cache_budget'])
        over = F.update([], snap(90.0, 10.0), NOW)
        self.assertEqual(F.days_until(over, 'resident_ratio', 0.85, NOW), 0.0)
        self.assertIsNone(F.days_until(F.update([], snap(50.0, 10.0), NOW), 'resident_ratio', 0.85, NOW))
        falling = []
        for day in range(4):
            falling = F.update(falling, snap(60.0 - day, 10.0), NOW + day * DAY)
        self.assertIsNone(F.forecast(falling, NOW + 3 * DAY)['days_to_recent_budget'])
        self.assertFalse(F.forecast([], NOW)['alert'])


if __name__ == '__main__':
    unittest.main()
