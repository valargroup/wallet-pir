#!/usr/bin/env python3
"""Directory-only wallet bills, and the paged tail, with pairs.py's model unchanged.

A wallet is *directory-only* in a layout when none of its scripts needs a page
query over the evaluated range: in coupled layouts, no page fragments in any
queried shard; in decoupled ones, no run walked (with the run cache for tip
catch-up). Its bill is then filters + choice tables + setups + directory
queries.

The headline compares the same wallets across layouts: per class, the wallets
that are directory-only in *every* listed layout (the common set). Each
layout's own directory-only share is shown alongside, because a layout can
move wallets into or out of the paged tail.

Paged wallets (paged in that layout) are reported only as a bounded tail:
count, bytes p99 / max and round trips p99 / max. The sanity bound, chosen
here and not a product requirement: tail bytes p99 and max at most 1.25x the
baseline layout's (the first listed), and round trips at most
MAX_PAGE_CHILDREN + 2 = 34.

Modes:
  one-shot:  directory-only.py --sample S --anchor H census.txt ...
  catch-up:  directory-only.py --anchors DIR --layouts a,b,...   (catch-up-anchors.sh output;
             wallets pooled over every anchor, decoupled walks use the run cache)
"""
import argparse
import collections
import glob
import importlib.util
import json
import os

spec = importlib.util.spec_from_file_location(
    "pairs", os.path.join(os.path.dirname(os.path.abspath(__file__)), "pairs.py"))
pairs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pairs)

BOUND_RATIO = 1.25
BOUND_TRIPS = 34


def bills_for(c, clients, anchor, cache):
    """{(key, class): (bill, paged)} for every client in range."""
    start = min(s["start"] for s in c["shards"].values())
    end = min(max(s["end"] for s in c["shards"].values()), anchor)
    out = {}
    for key, client in clients:
        if not client["scripts"]:
            continue
        low = max(client["required_from"], start)
        if low > end or (cache and client["required_from"] < start):
            continue
        label = client["class"] + (" (clipped)" if client["required_from"] < start else "")
        cached = low if cache and c["kind"] == "decoupled" else None
        b = pairs.bill(c, client["scripts"], low, end, cached_before=cached)
        out[key, label] = (b, b["paged_pairs"] > 0 or b["page_queries"] > 0)
    return out


def q(values, p):
    return pairs.percentile(values, p)


def report(layouts, per_layout):
    """per_layout: {layout: {(key, class): (bill, paged)}}"""
    classes = sorted({cls for bills in per_layout.values() for _, cls in bills})
    keys = set.intersection(*(set(b) for b in per_layout.values()))
    common = {k for k in keys if not any(per_layout[l][k][1] for l in layouts)}
    print("Directory-only wallets common to every layout: total MB per wallet, "
          "p50 / p95 / p99 (mean); n = wallets in the common set\n")
    print("| Layout | " + " | ".join(classes) + " |")
    print("|---|" + "---|" * len(classes))
    for layout in layouts:
        cells = []
        for cls in classes:
            v = [per_layout[layout][k][0]["total_bytes"] / 1e6 for k in common if k[1] == cls]
            cells.append("%.3f / %.3f / %.3f (%.3f)" % (q(v, 50), q(v, 95), q(v, 99), sum(v) / len(v))
                         if v else "—")
        print(f"| {layout} | " + " | ".join(cells) + " |")
    print("| n (common) | " + " | ".join(str(sum(1 for k in common if k[1] == cls)) for cls in classes) + " |")
    print("\nShare of wallets that are directory-only in each layout (of wallets in range)\n")
    print("| Layout | " + " | ".join(classes) + " | all |")
    print("|---|" + "---|" * (len(classes) + 1))
    for layout in layouts:
        bills = per_layout[layout]
        cells = []
        for cls in classes + [None]:
            ks = [k for k in bills if cls is None or k[1] == cls]
            cells.append("%.1f%% of %d" % (100 * sum(not bills[k][1] for k in ks) / len(ks), len(ks))
                         if ks else "—")
        print(f"| {layout} | " + " | ".join(cells) + " |")
    print("\nPaged tail (wallets that need pages in that layout, all classes)\n")
    print("| Layout | Wallets | Bytes MB p99 / max | Round trips p99 / max | Within bound |")
    print("|---|---:|---|---|---|")
    base = None
    for layout in layouts:
        paged = [b for b, p in per_layout[layout].values() if p]
        v = [b["total_bytes"] / 1e6 for b in paged]
        t = [b["round_trips"] for b in paged]
        if not paged:
            print(f"| {layout} | 0 | — | — | yes |")
            continue
        p99, mx = q(v, 99), max(v)
        if base is None:
            base = (p99, mx)
        ok = p99 <= BOUND_RATIO * base[0] and mx <= BOUND_RATIO * base[1] and max(t) <= BOUND_TRIPS
        print(f"| {layout} | {len(paged)} | {p99:.2f} / {mx:.2f} | {q(t, 99)} / {max(t)} | "
              f"{'yes' if ok else 'no'} ({p99 / base[0]:.2f}x / {mx / base[1]:.2f}x) |")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample")
    parser.add_argument("--anchor", type=int)
    parser.add_argument("--anchors")
    parser.add_argument("--layouts")
    parser.add_argument("census", nargs="*")
    args = parser.parse_args()
    per_layout = {}
    if args.anchors:
        layouts = args.layouts.split(",")
        per_layout = {layout: {} for layout in layouts}
        for path in sorted(glob.glob(os.path.join(args.anchors, "sample-*.json"))):
            end = int(os.path.basename(path)[7:-5])
            clients = [((end, i), c) for i, c in enumerate(json.load(open(path))["clients"])]
            for layout in layouts:
                c = pairs.parse(os.path.join(args.anchors, f"{end}.{layout}.txt"))
                per_layout[layout].update(bills_for(c, clients, end, cache=True))
        print(f"Tip catch-up with the run cache, pooled over anchors in {os.path.basename(args.anchors)}\n")
    else:
        layouts = [os.path.basename(p)[:-4] for p in args.census]
        clients = list(enumerate(json.load(open(args.sample))["clients"]))
        for layout, path in zip(layouts, args.census):
            per_layout[layout] = bills_for(pairs.parse(path), clients, args.anchor, cache=False)
        print("One-shot over the window\n")
    report(layouts, per_layout)


if __name__ == "__main__":
    main()
