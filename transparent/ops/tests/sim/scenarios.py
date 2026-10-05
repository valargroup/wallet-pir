"""The simulator's scenario suite: deterministic, seeded, over 200 scenarios.

A fixed set of fault-free scenarios with an accurate capacity model carries
the reaction and scale-back checks; a seeded random set mixes every trace
with every fault and with capacity models that are wrong in both directions.
"""
import random

from . import traces as T
from .fleet import ARCHIVE, HOUR, MINUTE, Scenario

FAULTS = ('crash', 'never_warms', 'slow_status', 'burst', 'reconciler_stall', 'publisher_stall',
          'publisher_down', 'fence', 'archive_crash', 'maintenance')


def fixed():
    out = []

    def add(trace, hours, **extra):
        out.append(Scenario(len(out) + 1, trace, hours, **extra))
    for qps in (0.5, 4, 8, 12, 16, 22, 28, 34):
        add(T.Flat(qps), 3)
    for before, after in ((4, 13), (4, 18), (4, 24), (8, 18), (8, 24), (13, 30), (2, 36), (24, 40)):
        add(T.Step(before, after, HOUR), 2.5)
    for high in (13, 18, 24, 30):
        for up in (45 * MINUTE, 90 * MINUTE):
            add(T.UpDown(3, high, up, up + 2 * HOUR), 5.5)
    for high in (12, 20, 30, 40):
        add(T.Diurnal(2, high, period=8 * HOUR), 12)
    add(T.Diurnal(2, 30, period=24 * HOUR), 26)
    for peak in (20, 30, 45):
        for width in (60, 180, 600):
            add(T.Spike(3, peak, (HOUR, 3 * HOUR), width), 5)
    for low, high, ramp, rest in ((2, 28, 40 * MINUTE, 50 * MINUTE), (4, 20, 20 * MINUTE, 20 * MINUTE),
                                  (2, 35, HOUR, 2 * HOUR)):
        add(T.Sawtooth(low, high, ramp, rest), 8)
    return out


def random_trace(rng):
    kind = rng.choice(('flat', 'step', 'up-down', 'diurnal', 'spike', 'sawtooth'))
    if kind == 'flat':
        return T.Flat(rng.uniform(0.5, 36)), rng.uniform(2, 4)
    if kind == 'step':
        return T.Step(rng.uniform(1, 12), rng.uniform(8, 40), rng.uniform(0.5, 1.5) * HOUR), rng.uniform(2.5, 4)
    if kind == 'up-down':
        up = rng.uniform(0.5, 1.5) * HOUR
        return T.UpDown(rng.uniform(1, 6), rng.uniform(12, 36), up, up + rng.uniform(1, 2) * HOUR), 5.5
    if kind == 'diurnal':
        return T.Diurnal(rng.uniform(1, 4), rng.uniform(10, 40), period=rng.choice((4, 6)) * HOUR), 8
    if kind == 'spike':
        return T.Spike(rng.uniform(1, 6), rng.uniform(15, 45), (rng.uniform(0.5, 1.5) * HOUR,),
                       rng.choice((60, 180, 600))), 3.5
    return T.Sawtooth(rng.uniform(1, 4), rng.uniform(15, 35), rng.uniform(20, 60) * MINUTE,
                      rng.uniform(20, 90) * MINUTE), 5


def randomized(count, seed=2026, first=1000):
    rng = random.Random(seed)
    out = []
    for i in range(count):
        trace, hours = random_trace(rng)
        faults = rng.sample(FAULTS, rng.choice((0, 1, 1, 2, 2, 3)))
        if 'crash' in faults and rng.random() < 0.3:
            faults = {**{f: True for f in faults}, 'crash': 2}
        capacity = rng.choice((1.0, 1.0, 1.0, 0.6, 0.45, 1.4))
        out.append(Scenario(first + i, trace, hours, faults, capacity))
    return out


def single_owner():
    """A small set with one archive owner holding the whole archive (0-76):
    its outage withdraws the fleet exactly as either of two owners' did."""
    one = ARCHIVE[:1]
    out = [Scenario(3001, T.Flat(8), 3, archive=one),
           Scenario(3002, T.Step(4, 24, HOUR), 2.5, archive=one),
           Scenario(3003, T.UpDown(3, 24, 45 * MINUTE, 165 * MINUTE), 5.5, archive=one),
           Scenario(3004, T.Step(4, 18, HOUR), 3, ('archive_crash',), archive=one),
           Scenario(3005, T.Flat(12), 3, ('archive_crash', 'crash'), archive=one)]
    for scenario in randomized(5, seed=2027, first=3100):
        scenario.archive = one
        scenario.name += '-1owner'
        out.append(scenario)
    return out


def suite():
    fixed_scenarios = fixed()
    return fixed_scenarios + randomized(max(0, 210 - len(fixed_scenarios)))


def run_entry(index):
    """Run the suite's `index`th scenario; a small picklable summary for worker processes."""
    from .check import run
    scenario = suite()[index]
    result = run(scenario)
    return {'name': scenario.name, 'hours': scenario.hours, 'violations': result.violations,
            'requests': [e[2] for e in result.requests], 'flags': sorted(result.flags)}
