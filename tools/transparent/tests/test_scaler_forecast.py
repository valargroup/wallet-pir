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
