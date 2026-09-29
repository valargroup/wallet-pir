"""Capacity forecast for the recent tier: alert-only, never a request.

Every recent replica holds every recent shard, so the recent tier outgrows its
hosts in two ways: the cache (resident bytes against the cache budget) and the
process (cgroup memory against its limit). Once per UTC day the scaler keeps
the worst ratio of each over the serving recent replicas; a least-squares line
through up to 14 daily samples says how many days remain until the cache
reaches 85% of its budget or memory reaches 90% of its limit.

`update(samples, snapshot, now)` returns the new sample list (stored in the
scaler state); `forecast(samples, now)` returns the status fields, where
`days_to_recent_budget` is the sooner of the two limits. Both are pure.
"""
from __future__ import annotations

import math

DAY = 86400.0
KEEP_DAYS = 14
BUDGET_THRESHOLD = 0.85
MEMORY_THRESHOLD = 0.90


def day_of(unix):
    return int(unix // DAY)


def observe(snapshot):
    """Worst (resident/budget, memory/max) over serving recent replicas; None when unknown."""
    resident, memory = [], []
    for member in (snapshot.get('members') or {}).values():
        if member.get('role') != 'recent-replica' or not member.get('serving'):
            continue
        used, budget = member.get('cache_resident_bytes'), member.get('cache_budget_bytes')
        if used is not None and budget:
            resident.append(used / budget)
        current, limit = member.get('cgroup_memory_bytes'), member.get('cgroup_memory_max_bytes')
        if current is not None and limit:
            memory.append(current / limit)
    return (max(resident) if resident else None, max(memory) if memory else None)


def update(samples, snapshot, now):
    """Fold today's observation into the daily samples (keeping the day's maximum)."""
    samples = [dict(s) for s in samples or [] if isinstance(s, dict) and 'day' in s]
    resident, memory = observe(snapshot)
    if resident is None and memory is None:
        return _trim(samples, now)
    today = day_of(now)
    entry = next((s for s in samples if s['day'] == today), None)
    if entry is None:
        entry = {'day': today, 'unix': now, 'resident_ratio': None, 'memory_ratio': None}
        samples.append(entry)
    for key, value in (('resident_ratio', resident), ('memory_ratio', memory)):
        if value is not None:
            entry[key] = value if entry.get(key) is None else max(entry[key], value)
            entry['unix'] = now
    return _trim(samples, now)


def _trim(samples, now):
    horizon = day_of(now) - KEEP_DAYS + 1
    return sorted((s for s in samples if s['day'] >= horizon), key=lambda s: s['day'])


def fit(points):
    """Least-squares (intercept, slope) through `(x, y)`; None with fewer than two distinct x."""
    if len(points) < 2:
        return None
    n = len(points)
    mean_x = math.fsum(x for x, _ in points) / n
    mean_y = math.fsum(y for _, y in points) / n
    sxx = math.fsum((x - mean_x) ** 2 for x, _ in points)
    if sxx <= 0:
        return None
    sxy = math.fsum((x - mean_x) * (y - mean_y) for x, y in points)
    slope = sxy / sxx
    return mean_y - slope * mean_x, slope


def days_until(samples, key, threshold, now):
    """Days from `now` until the fitted trend of `key` reaches `threshold`.

    0 when the latest sample is already at or above it; None when there are
    fewer than two samples or the trend is flat or falling.
    """
    points = [(s['unix'] / DAY, s[key]) for s in samples if s.get(key) is not None]
    if not points:
        return None
    if points[-1][1] >= threshold:
        return 0.0
    line = fit(points)
    if line is None:
        return None
    intercept, slope = line
    if slope <= 0:
        return None
    at = (threshold - intercept) / slope
    return max(0.0, at - now / DAY)


def forecast(samples, now, alert_days=30.0):
    budget = days_until(samples, 'resident_ratio', BUDGET_THRESHOLD, now)
    memory = days_until(samples, 'memory_ratio', MEMORY_THRESHOLD, now)
    soonest = min((d for d in (budget, memory) if d is not None), default=None)
    def rounded(value):
        return None if value is None else round(value, 1)
    # The contract's single number is the sooner of the two limits.
    return {'days_to_recent_budget': rounded(soonest),
            'days_to_cache_budget': rounded(budget),
            'days_to_memory_max': rounded(memory),
            'samples': len(samples),
            'alert': soonest is not None and soonest < alert_days}
