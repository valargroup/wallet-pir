#!/usr/bin/env python3
"""Summarizes runs/ into summary.json: python3 summarize.py > summary.json"""
import glob, json, os, statistics as st

here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runs")
out = {"schema": "transparent-prewarm-hint-summary-v1", "fills": {}}
for fill in ["10.2", "13.5", "15.9"]:
    entry = {}
    for which in ["base", "final"]:
        runs = [json.load(open(f)) for f in sorted(glob.glob(f"{here}/{fill}-{which}-r*.json"))]
        rounds = [r for run in runs for r in run["rounds"]]
        table = lambda name: [t["seconds"] for r in rounds for t in r["tables"] if t["table"] == name]
        totals = [r["seconds"] for r in rounds]
        entry[which] = {
            "two_table_seconds": {"median": st.median(totals), "min": min(totals), "max": max(totals), "n": len(totals)},
            "directory_median_seconds": st.median(table("directory")),
            "pages_median_seconds": st.median(table("pages")),
            "peak_rss_bytes": [run["peak_rss_bytes"] for run in runs],
            "public_params_sha256": {d["table"]: d["public_params_sha256"] for d in runs[0]["public_params"]},
            "public_params_stable_across_runs": all(run["public_params"] == runs[0]["public_params"] for run in runs),
        }
    entry["public_params_equal"] = entry["base"]["public_params_sha256"] == entry["final"]["public_params_sha256"]
    entry["two_table_change"] = entry["final"]["two_table_seconds"]["median"] / entry["base"]["two_table_seconds"]["median"] - 1
    entry["hints"] = {}
    for line in open(f"{here}/{fill}-hints.jsonl"):
        h = json.loads(line)
        entry["hints"][h["table"]] = {
            "used_blocks": h["used_blocks"], "hints_equal": h["hints_equal"],
            "reference_median_seconds": st.median(h["reference_hint_seconds"]),
            "batched_median_seconds": st.median(h["batched_hint_seconds"]),
            "public_params_sha256": h["public_params_sha256"],
        }
    out["fills"][fill] = entry
print(json.dumps(out, indent=1))
