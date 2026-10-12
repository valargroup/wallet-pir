#!/usr/bin/env python3
"""Tip catch-up bills across catch-up-anchors.sh's anchors, with pairs.py's model.

For each layout and catch-up class: the per-wallet total MB (with the
decoupled run cache) pooled over every anchor, as mean / p50 / p95 / p99,
plus the spread of per-anchor means (min-max) and round trips p50 / p99.

Usage: catch-up-summary.py OUT
"""
import collections
import glob
import importlib.util
import json
import os
import sys

spec = importlib.util.spec_from_file_location(
    "pairs", os.path.join(os.path.dirname(os.path.abspath(__file__)), "pairs.py"))
pairs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pairs)

LAYOUTS = ["recent-4k-8k", "coupled-2k-8k", "bulk-recent-4k-8k", "decoupled-4k-8k", "decoupled-8k-8k"]
CLASSES = ["catch-up-1d", "catch-up-7d", "catch-up-30d"]


def main():
    out = sys.argv[1]
    pooled = collections.defaultdict(list)
    means = collections.defaultdict(list)
    trips = collections.defaultdict(list)
    anchors = sorted(int(os.path.basename(p)[7:-5]) for p in glob.glob(os.path.join(out, "sample-*.json")))
    for end in anchors:
        sample = json.load(open(os.path.join(out, f"sample-{end}.json")))
        for layout in LAYOUTS:
            c = pairs.parse(os.path.join(out, f"{end}.{layout}.txt"))
            start = min(s["start"] for s in c["shards"].values())
            bills = collections.defaultdict(list)
            for client in sample["clients"]:
                low = client["required_from"]
                if not client["scripts"] or low < start or low > end:
                    continue
                b = pairs.bill(c, client["scripts"], low, end,
                               cached_before=low if c["kind"] == "decoupled" else None)
                bills[client["class"]].append(b)
            for k, v in bills.items():
                pooled[layout, k] += [b["total_bytes"] / 1e6 for b in v]
                trips[layout, k] += [b["round_trips"] for b in v]
                means[layout, k].append(sum(b["total_bytes"] for b in v) / len(v) / 1e6)
    print(f"anchors: {', '.join(map(str, anchors))}\n")
    print("| Layout | " + " | ".join(CLASSES) + " |")
    print("|---|" + "---|" * len(CLASSES))
    for layout in LAYOUTS:
        cells = []
        for k in CLASSES:
            v = pooled[layout, k]
            m = means[layout, k]
            cells.append("%.3f; %.2f / %.2f / %.2f; anchors %.3f–%.3f; trips %d / %d" % (
                sum(v) / len(v), pairs.percentile(v, 50), pairs.percentile(v, 95),
                pairs.percentile(v, 99), min(m), max(m),
                pairs.percentile(trips[layout, k], 50), pairs.percentile(trips[layout, k], 99)))
        print(f"| {layout} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
