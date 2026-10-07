#!/usr/bin/env python3
"""Before/after summary of display block-to-serving time and history prewarm.

Run from this evidence directory: python3 harness/analyze.py > summary.json
"""
import gzip
import json
from datetime import datetime, timezone

# The worker restart: before is the 24 h up to the deploy's start, after is
# every cycle activated once the restarted worker was serving.
DEPLOY_START_MS = 1791384340000  # 2026-10-07T14:45:40Z
DEPLOY_DONE_MS = 1791384447000  # 2026-10-07T14:47:27Z
BEFORE_FROM_MS = DEPLOY_START_MS - 86_400_000


def q(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p * (len(values) - 1))))]


def dist(values, scale=1.0):
    if not values:
        return None
    return {
        "n": len(values),
        "p50": round(q(values, 0.5) * scale, 3),
        "p95": round(q(values, 0.95) * scale, 3),
        "max": round(max(values) * scale, 3),
    }


def cycles(lo, hi):
    fresh, prepare, ship, count = [], [], [], 0
    with gzip.open("raw/cycles.jsonl.gz", "rt") as f:
        for line in f:
            e = json.loads(line)
            if not lo <= e["activated_ms"] < hi:
                continue
            if not any(w.get("role") == "recent-replica" for w in e.get("workers", [])):
                continue
            count += 1
            fresh += e["freshness_ms"]
            prepare.append(e["prepare_ms"])
            ship.append(e["ship_ms"])
    return {
        "cycles": count,
        "freshness_s": {**dist(fresh, 1e-3), "over_20s": sum(x > 20_000 for x in fresh)},
        "prepare_s": dist(prepare, 1e-3),
        "ship_s": dist(ship, 1e-3),
    }


def history(lo, hi):
    values = []
    for line in open("raw/history-prewarm.txt"):
        stamp, seconds = line.split()
        ms = datetime.fromisoformat(stamp).astimezone(timezone.utc).timestamp() * 1000
        if lo <= ms < hi:
            values.append(float(seconds))
    return dist(values)


def bursts(path):
    """CPU-seconds and cores per display burst (display above 0.1 core)."""
    rows = []
    for line in open(path):
        parts = line.split()
        if len(parts) == 3:
            rows.append((float(parts[0]), int(parts[1]), int(parts[2])))
    out, cur, prev = [], None, None
    for t, d, h in rows:
        if prev:
            if (d - prev[1]) / 1e6 / (t - prev[0]) > 0.1:
                if cur is None:
                    cur = [prev[0], 0, 0, t]
                cur[1] += d - prev[1]
                cur[2] += h - prev[2]
                cur[3] = t
            elif cur:
                out.append(cur)
                cur = None
        prev = (t, d, h)
    return [
        {
            "seconds": round(b[3] - b[0], 1),
            "display_cpu_s": round(b[1] / 1e6, 1),
            "display_cores": round(b[1] / 1e6 / (b[3] - b[0]), 2),
            "history_cpu_s": round(b[2] / 1e6, 1),
            "history_cores": round(b[2] / 1e6 / (b[3] - b[0]), 2),
        }
        for b in out
    ]


print(json.dumps({
    "before": {**cycles(BEFORE_FROM_MS, DEPLOY_START_MS),
               "history_prewarm_s": history(BEFORE_FROM_MS, DEPLOY_START_MS)},
    "after": {**cycles(DEPLOY_DONE_MS, 1 << 62),
              "history_prewarm_s": history(DEPLOY_DONE_MS, 1 << 62)},
    "cpu_bursts_before": bursts("raw/cpu-before.txt"),
    "cpu_bursts_after": bursts("raw/cpu-after.txt"),
}, indent=1))
