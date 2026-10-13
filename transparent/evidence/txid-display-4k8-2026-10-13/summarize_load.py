#!/usr/bin/env python3
"""Summarise one txid-rate window (JSONL) and the k8-01 worker samples taken over it.

Usage: summarize_load.py RATE_JSONL [SAMPLES_JSONL]
"""
import json, sys, collections

def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    return round(xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 3)

def dist(xs):
    return {"n": len(xs), "p50": pct(xs, 50), "p95": pct(xs, 95), "p99": pct(xs, 99), "max": round(max(xs), 3) if xs else None}

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
ev = collections.Counter(r["event"] for r in rows)
look = [r for r in rows if r["event"] == "lookup"]
errs = [r for r in rows if r["event"] == "error"]
missed = collections.Counter(r.get("reason") for r in rows if r["event"] == "missed_slot")
lu = [r["unix"] for r in look]
span = (max(lu) - min(lu)) if lu else 0
out = {
    "events": dict(ev),
    "lookups": len(look),
    "exact": sum(1 for r in look if r["exact"]),
    "not_exact": sum(1 for r in look if not r["exact"]),
    "errors": len(errs),
    "error_kinds": dict(collections.Counter((e.get("kind"), e.get("status")) for e in errs)),
    "missed_slots": dict(missed),
    "span_seconds": round(span, 1),
    "achieved_rate": round(len(look) / span, 2) if span else None,
    "stale_retries": sum(r.get("stale_retries", 0) for r in look),
    "total_seconds": {},
    "http_seconds_warm": dist([r["http_seconds"] for r in look if not r["cold"]]),
    "schedule_lag_p99": pct([r["schedule_lag_seconds"] for r in look], 99),
    "bytes_warm_found_median": {"up": pct([r["up"] for r in look if not r["cold"] and r["expected"] == "found"], 50),
                                "down": pct([r["down"] for r in look if not r["cold"] and r["expected"] == "found"], 50)},
}
groups = collections.defaultdict(list)
for r in look:
    tier = "archive" if r["sealed"] else "recent"
    groups["all"].append(r["total_seconds"])
    groups[f"{tier}{'-cold' if r['cold'] else '-warm'}"].append(r["total_seconds"])
    groups["warm" if not r["cold"] else "cold"].append(r["total_seconds"])
    if r["expected"] != "found":
        groups["absent"].append(r["total_seconds"])
out["total_seconds"] = {k: dist(v) for k, v in sorted(groups.items())}
if errs:
    out["first_errors"] = [e.get("error", "")[:200] for e in errs[:5]]
if len(sys.argv) > 2 and lu:
    lo, hi = min(lu), max(lu)
    s = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
    inside = [x for x in s if lo <= x["unix"] <= hi + 15]
    if len(inside) >= 2:
        a, b = inside[0], inside[-1]
        dt = b["unix"] - a["unix"]
        cpu = [(y["cpu_ns"] - x["cpu_ns"]) / 1e9 / (y["unix"] - x["unix"]) for x, y in zip(inside, inside[1:])]
        out["worker"] = {
            "cpu_cores_mean": round((b["cpu_ns"] - a["cpu_ns"]) / 1e9 / dt, 2),
            "cpu_cores_max_15s": round(max(cpu), 2),
            "mem_gb_max": round(max(x["mem"] for x in inside) / 1e9, 2),
            "load1_max": max(float(x["load"].split()[0]) for x in inside),
            "samples": len(inside),
        }
print(json.dumps(out, indent=1))
