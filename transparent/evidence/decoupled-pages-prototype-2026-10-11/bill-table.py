#!/usr/bin/env python3
"""Markdown bill tables over census logs, using pairs.py's model unchanged.

One row per census log. One-shot cells are total MB per wallet as
p50 / p95 / p99, and round trips as p50 / p99. Catch-up cells use the run
cache for decoupled layouts (pairs.py's `cached_before`).

Usage: bill-table.py --sample sample.json --anchor H [--classes a,b] census.txt ...
"""
import argparse
import collections
import importlib.util
import json
import os

spec = importlib.util.spec_from_file_location(
    "pairs", os.path.join(os.path.dirname(os.path.abspath(__file__)), "pairs.py"))
pairs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pairs)


def cell(values):
    return "%.2f / %.2f / %.2f" % tuple(pairs.percentile(values, p) / 1e6 for p in (50, 95, 99))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", required=True)
    parser.add_argument("--anchor", type=int, required=True)
    parser.add_argument("--classes")
    parser.add_argument("census", nargs="+")
    args = parser.parse_args()
    sample = json.load(open(args.sample))
    rows = []
    classes = set()
    for path in args.census:
        c = pairs.parse(path)
        start = min(s["start"] for s in c["shards"].values())
        end = min(max(s["end"] for s in c["shards"].values()), args.anchor)
        one, catch = collections.defaultdict(list), collections.defaultdict(list)
        for client in sample["clients"]:
            if not client["scripts"] or max(client["required_from"], start) > end:
                continue
            low = max(client["required_from"], start)
            label = client["class"] + (" (clipped)" if client["required_from"] < start else "")
            b = pairs.bill(c, client["scripts"], low, end)
            one[label].append(b)
            if client["class"].startswith("catch-up") and client["required_from"] >= start:
                catch[label].append(pairs.bill(c, client["scripts"], low, end, cached_before=low)
                                    if c["kind"] == "decoupled" else b)
        classes.update(one)
        name = os.path.basename(path)[:-4]
        rows.append((name, one, catch))
    wanted = args.classes.split(",") if args.classes else sorted(classes)
    print("| Layout | " + " | ".join(wanted) + " |")
    print("|---|" + "---|" * len(wanted))
    for name, one, _ in rows:
        print(f"| {name} | " + " | ".join(
            cell([b["total_bytes"] for b in one[k]]) if one.get(k) else "—" for k in wanted) + " |")
    print("\nRound trips p50 / p99\n")
    print("| Layout | " + " | ".join(wanted) + " |")
    print("|---|" + "---|" * len(wanted))
    for name, one, _ in rows:
        print(f"| {name} | " + " | ".join(
            "%d / %d" % (pairs.percentile([b["round_trips"] for b in one[k]], 50),
                         pairs.percentile([b["round_trips"] for b in one[k]], 99))
            if one.get(k) else "—" for k in wanted) + " |")
    catch_classes = [k for k in wanted if k.startswith("catch-up")]
    if catch_classes:
        print("\nTip catch-up with the run cache (MB p50 / p95 / p99; mean)\n")
        print("| Layout | " + " | ".join(catch_classes) + " |")
        print("|---|" + "---|" * len(catch_classes))
        for name, _, catch in rows:
            print(f"| {name} | " + " | ".join(
                cell([b["total_bytes"] for b in catch[k]]) + "; %.3f" % (
                    sum(b["total_bytes"] for b in catch[k]) / len(catch[k]) / 1e6)
                if catch.get(k) else "—" for k in catch_classes) + " |")


if __name__ == "__main__":
    main()
