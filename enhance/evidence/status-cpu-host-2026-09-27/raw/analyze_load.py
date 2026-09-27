"""Gate: 20 QPS Status live load; p99 < 2000 ms, p50 < 700 ms, zero failures."""
import collections
import json
import sys

rows = [json.loads(l) for l in open(sys.argv[1]).read().splitlines() if l.endswith("}")]
results = collections.Counter(r["result"] for r in rows)
errors = collections.Counter(str(r["error"])[:80] for r in rows if r["error"])
ok = sorted(r["duration_ms"] for r in rows if r["result"] == "correct")
alld = sorted(r["duration_ms"] if r["duration_ms"] is not None else float("inf") for r in rows)


def q(xs, p):
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


span = (max(r["scheduled_ms"] for r in rows) - min(r["scheduled_ms"] for r in rows)) / 1000
age = sorted(r["observation_age_ms"] for r in rows if r.get("observation_age_ms") is not None)
lag = max(r.get("start_lag_ms") or 0 for r in rows)
summary = {
    "arrivals": len(rows),
    "span_s": round(span, 1),
    "offered_qps": round(len(rows) / span, 2) if span else None,
    "results": dict(results),
    "errors": dict(errors),
    "p50_ms_all": q(alld, 0.50),
    "p99_ms_all": q(alld, 0.99),
    "max_ms_all": alld[-1] if alld else None,
    "p50_ms_correct": q(ok, 0.50),
    "p99_ms_correct": q(ok, 0.99),
    "observation_age_ms_p50": q(age, 0.5),
    "observation_age_ms_max": age[-1] if age else None,
    "max_start_lag_ms": lag,
    "generations": len({r["generation"] for r in rows if r.get("generation")}),
}
nonc = len(rows) - results.get("correct", 0)
summary["gate_pass"] = (
    nonc == 0
    and summary["p99_ms_all"] < 2000
    and summary["p50_ms_all"] < 700
    and summary["offered_qps"] >= 19.5
)
print(json.dumps(summary, indent=1))
