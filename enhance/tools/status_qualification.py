#!/usr/bin/env python3
"""Fail-closed assessment of a Status PIR production qualification capture.

Inputs are JSONL emitted by an independent live-source load campaign. Missing
evidence is a failed gate. The current synthetic `status-pir probe` deliberately
cannot pass this assessment.
"""

import argparse
import json
import math
from pathlib import Path

SECONDS = 21_600
QPS = 20
P99_MS = 1_000
FRESHNESS_MS = 20_000


def records(path: Path):
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"{path}:{line_number}: expected JSON object")
                yield value


def assess(summary, requests, publications, resources):
    failures = []
    expected_geometry = {"protocol": "status-pir-v2-q48", "rows": 8192,
                         "slots": 256, "slot_bytes": 40, "columns": 6144,
                         "row_bytes": 12288, "database_bytes": 100663296}
    if any(summary.get(key) != value for key, value in expected_geometry.items()):
        failures.append("capture does not identify the compact v2 protocol and geometry")
    if summary.get("source") != "live" or summary.get("oracle_source") != "independent":
        failures.append("live source and independent answer oracle are required")
    start = summary.get("run_started_ms")
    end = summary.get("run_ended_ms")
    if (
        not isinstance(start, int)
        or not isinstance(end, int)
        or end - start < SECONDS * 1_000
    ):
        failures.append("run timestamps do not span six hours")
    if summary.get("seconds", 0) < SECONDS or summary.get("offered", 0) < SECONDS * QPS:
        failures.append("six hours at 20 offered QPS were not captured")

    seen = set()
    latencies = []
    for request in requests:
        arrival = request.get("arrival")
        if not isinstance(arrival, int) or arrival < 0 or arrival in seen:
            failures.append("request arrivals are missing, duplicated, or invalid")
            break
        seen.add(arrival)
        scheduled = request.get("scheduled_ms")
        completed = request.get("completed_ms")
        if (
            not isinstance(start, int)
            or not isinstance(end, int)
            or not isinstance(scheduled, int)
            or not isinstance(completed, int)
            or not start <= scheduled <= completed <= end
            or abs(scheduled - (start + arrival * 1_000 / QPS)) > 1
        ):
            failures.append("request timing does not match the open-loop schedule")
            break
        if request.get("result") != "correct":
            failures.append("an incorrect, failed, or unstarted lookup was recorded")
            break
        duration = request.get("duration_ms")
        age = request.get("observation_age_ms")
        if not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
            failures.append("request duration is missing or invalid")
            break
        if not isinstance(age, (int, float)) or not math.isfinite(age) or not 0 <= age <= FRESHNESS_MS:
            failures.append("a response used a missing, future, or stale observation")
            break
        latencies.append(duration)
    if len(seen) != summary.get("offered") or seen != set(range(len(seen))):
        failures.append("per-arrival evidence does not cover the offered workload")
    p99 = None
    if latencies:
        latencies.sort()
        p99 = latencies[math.ceil(0.99 * len(latencies)) - 1]
        if p99 > P99_MS:
            failures.append("complete lookup p99 exceeds one second")
    else:
        failures.append("no correct lookups were recorded")

    # Each observation has exactly one terminal disposition. Supersession may
    # coalesce mempool snapshots, but cannot erase an observation or reset its
    # deadline repeatedly until a later publication happens to pass.
    observations = {}
    for publication in publications:
        identity = publication.get("observation_id")
        if not isinstance(identity, int) or isinstance(identity, bool) or identity < 0 or identity in observations:
            failures.append("observation identities are missing, duplicated, or invalid")
            continue
        observations[identity] = publication
    publication_count = len(observations)
    if summary.get("source_observations") != publication_count:
        failures.append("source observation totals do not match publication evidence")
    kinds = set()
    queryable_times = []
    for identity, observation in observations.items():
        observed = observation.get("source_observed_ms")
        if (not isinstance(observed, int) or not isinstance(start, int)
                or not isinstance(end, int) or not start <= observed <= end
                or observation.get("kind") not in ("block", "mempool", "unchanged")):
            failures.append("invalid source observation record")
            continue
        successor = observation
        visited = {identity}
        while successor.get("disposition") == "superseded":
            next_id = successor.get("superseded_by")
            next_record = observations.get(next_id) if isinstance(next_id, int) else None
            if (next_record is None or next_id in visited or next_id <= successor["observation_id"]
                    or not isinstance(next_record.get("source_observed_ms"), int)
                    or next_record["source_observed_ms"] < successor["source_observed_ms"]):
                failures.append("supersession chain is missing, cyclic, or out of order")
                successor = {}
                break
            visited.add(next_id)
            successor = next_record
        ready = successor.get("queryable_ms")
        if (successor.get("disposition") != "published"
                or successor.get("oracle_match") is not True
                or successor.get("canonical_continuity") is not True
                or successor.get("complete_mempool") is not True
                or not isinstance(ready, int) or not observed <= ready <= end
                or ready - observed > FRESHNESS_MS):
            failures.append("an observation was lost, incorrect, or starved beyond twenty seconds")
        if observation.get("disposition") == "published" and isinstance(ready, int):
            kinds.add(observation.get("kind"))
            queryable_times.append(ready)
    if not {"block", "mempool"}.issubset(kinds):
        failures.append("concurrent block and mempool publication evidence is missing")
    times = sorted(set(queryable_times))
    if (not times or not isinstance(start, int) or not isinstance(end, int)
            or times[0] - start > FRESHNESS_MS or end - times[-1] > FRESHNESS_MS
            or any(b - a > FRESHNESS_MS for a, b in zip(times, times[1:]))):
        failures.append("live publication evidence does not continuously span the run")

    resource_count = 0
    first_sample = None
    last_sample = None
    for sample in resources:
        resource_count += 1
        sampled = sample.get("sampled_ms")
        if (
            not isinstance(sampled, int)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or not start <= sampled <= end
            or (last_sample is not None and not 0 < sampled - last_sample <= 60_000)
        ):
            failures.append("resource sampling has missing or invalid timing")
            break
        first_sample = sampled if first_sample is None else first_sample
        last_sample = sampled
        if sample.get("oom_events") != 0 or sample.get("swap_bytes") != 0:
            failures.append("OOM or swap use was observed, or its counters are missing")
            break
    if resource_count < SECONDS // 60:
        failures.append("resource samples cover less than one sample per minute")
    if first_sample is None or first_sample - start > 60_000 or end - last_sample > 60_000:
        failures.append("resource samples do not span the run")

    return {
        "passed": not failures,
        "failures": sorted(set(failures)),
        "offered": summary.get("offered"),
        "recorded_arrivals": len(seen),
        "p99_ms": p99,
        "publications": publication_count,
        "resource_samples": resource_count,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--publications", type=Path, required=True)
    parser.add_argument("--resources", type=Path, required=True)
    args = parser.parse_args()
    summaries = [row for row in records(args.summary) if row.get("phase") == "load"]
    if len(summaries) != 1:
        parser.error("--summary must contain exactly one load result")
    result = assess(
        summaries[0],
        records(args.requests),
        records(args.publications),
        records(args.resources),
    )
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
