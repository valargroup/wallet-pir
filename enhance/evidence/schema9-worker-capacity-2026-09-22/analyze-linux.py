#!/usr/bin/env python3
"""Summarize memory and correctness scope of the isolated Linux runtime fixture."""
import argparse
import json
from pathlib import Path
import re


def percentile(values, fraction):
    import math
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--soft-gib", type=float, default=6)
    args = parser.parse_args()
    summaries = json.loads((args.results / "summary.json").read_text())
    output = []
    for summary in summaries:
        name = summary["name"]
        recorded_high = summary.get("final", {}).get("MemoryHigh")
        if recorded_high and int(recorded_high) != int(args.soft_gib * 1024**3):
            raise ValueError(f"{name}: --soft-gib does not match recorded MemoryHigh")
        records = [json.loads(line) for line in (args.results / (name + ".jsonl")).read_text().splitlines()
                   if line.startswith("{")]
        samples = [json.loads(line) for line in (args.results / (name + ".cgroup.jsonl")).read_text().splitlines()]
        start = next((row for row in records if row.get("event") == "start"), {})
        complete = next((row for row in records if row.get("event") == "complete"), None)
        timing_path = args.results / (name + ".time")
        timing = timing_path.read_text() if timing_path.exists() else ""
        rss = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", timing)
        publications = [row for row in records if row.get("event") == "candidate_overlap"]
        prep = [row["seconds"] for row in records if row.get("event") == "prepared" and row.get("revision", 0) > 0]
        kernel_peak = max((s.get("memory.stat", {}).get("kernel", 0) for s in samples), default=0)
        rss_peak = int(rss.group(1)) * 1024 if rss else None
        resident_budget = max(rss_peak or 0, summary["max_anon_bytes"]) + kernel_peak
        outcome = {
            "name": name, "domains": start.get("domains"), "active": start.get("active"),
            "transition_extra_units": start.get("transition_extra_units", 0),
            "completed": complete is not None and summary.get("stopped") is None
                         and summary["final"].get("ExecMainStatus") == "0"
                         and summary["final"].get("Result") == "success",
            "wall_seconds": summary["wall_seconds"],
            "peak_rss_bytes": rss_peak,
            "peak_cgroup_bytes": summary["peak_cgroup_bytes"],
            "max_sampled_anon_bytes": summary["max_anon_bytes"],
            "max_sampled_anon_plus_kernel_bytes": max((s.get("memory.stat", {}).get("anon", 0)
                                                      + s.get("memory.stat", {}).get("kernel", 0)
                                                      for s in samples), default=0),
            "max_sampled_kernel_bytes": kernel_peak,
            "conservative_resident_plus_kernel_bytes": resident_budget,
            "soft_limit_bytes": int(args.soft_gib * 1024**3),
            "resident_headroom_to_soft_bytes": int(args.soft_gib * 1024**3) - resident_budget,
            "chosen_guard_bytes": 512 * 1024**2,
            "resident_guard_met": rss_peak is not None and resident_budget <= (args.soft_gib - .5) * 1024**3,
            "events": summary["events"],
            "publications": len(publications),
            "sealed_exact_evaluations": sum(row.get("evaluations", 0) for row in records
                                             if row.get("event") == "sealed_serving"),
            "concurrent_exact_evaluations": sum(row.get("concurrent_evaluations", 0) for row in records
                                                if row.get("event") in ["candidate_overlap", "transition_surcharge_overlap"]),
            "max_cached_units": max((row.get("cached_units", 0) for row in records), default=0),
            "frontier_prepare_p50_seconds": percentile(prep, .5),
            "frontier_prepare_p95_seconds": percentile(prep, .95),
            "stopped": summary.get("stopped"),
            "scope": "Worker intermediate correctness and memory; not encrypted end-to-end loan or production qualification",
        }
        outcome["capacity_candidate_met"] = (outcome["completed"] and outcome["resident_guard_met"]
                                             and not any(outcome["events"][key]
                                                         for key in ["max", "oom", "oom_kill"]))
        output.append(outcome)
    rendered = json.dumps(output, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
