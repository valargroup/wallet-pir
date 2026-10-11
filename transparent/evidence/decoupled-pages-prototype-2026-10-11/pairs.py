#!/usr/bin/env python3
"""Per-wallet private-query bills under a census's shards, for the frozen sample.

Reads one or more `shard-census --sample` outputs, coupled (today's layout,
`per_shard` / `per_shard_tail` / `sample` rows) or decoupled (`per_directory` /
`per_child` / `sample_chain` rows), and prices every sampled wallet's recovery
over the census window at the `prices` row the census printed.

Model (derived arithmetic, not a measurement):

- A wallet's range is [max(required_from, window start), min(anchor, window end)].
  Ranges that begin before the window are clipped to it and labelled so.
- Filters: every shard overlapping the range, at ~2.5 B per script. Choice
  tables: every matched shard, at ~1.245 bits per script.
- Coupled, per matched (script, shard): one directory query, then the
  script's page fragments in that shard; 2 round trips when paged, else 1.
- Decoupled, per matched (script, directory shard): one directory query, then
  the chain newest to oldest. A run of K fragments costs K page queries; round
  trips are 1 + runs + (1 if the oldest walked run has K > 1).
- Setup: one per (shard, table) queried, per segment.
- Every query costs upload + response per segment of the table it asks.
- Catch-up with cache (decoupled only, catch-up classes): the wallet synced
  through required_from - 1. Directory shards ending before required_from are
  not queried; in later ones the walk stops at the first run in a page shard
  that ended before required_from. Coupled catch-up equals the one-shot bill
  over the same range, because a sealed coupled shard is never re-queried.

Usage: pairs.py --sample sample.json --anchor H census.txt [census.txt ...]
"""
import argparse
import collections
import json

FILTER_BYTES_PER_SCRIPT = 2.5
CHOICE_BITS_PER_SCRIPT = 1.245


def parse(path):
    c = {"path": path, "kind": None, "shards": {}, "children": collections.defaultdict(dict),
         "pairs": collections.defaultdict(dict), "prices": None}
    for line in open(path):
        f = line.strip().split("\t")
        tag = f[0]
        if tag in ("per_shard", "per_shard_tail") and f[1].isdigit():
            c["kind"] = "coupled"
            sid = int(f[1])
            if sid in c["shards"]:
                continue  # later policies of a default sweep
            c["shards"][sid] = dict(start=int(f[2]), end=int(f[3]), scripts=int(f[5]),
                                    page_rows=int(f[6]), reason=f[9],
                                    dir_bytes=int(f[10]), dir_segments=int(f[11]),
                                    page_segments=int(f[12]))
        elif tag == "sample":
            c["pairs"][f[2]].setdefault(int(f[1]), int(f[3]))
        elif tag == "per_directory" and f[1].isdigit():
            c["kind"] = "decoupled"
            c["shards"][int(f[1])] = dict(start=int(f[2]), end=int(f[3]), scripts=int(f[5]),
                                          dir_bytes=int(f[6]), children=int(f[9]),
                                          dir_segments=int(f[11]), reason=f[12])
        elif tag == "per_child" and f[1].isdigit():
            c["children"][int(f[1])][int(f[2])] = dict(start=int(f[3]), end=int(f[4]),
                                                       page_rows=int(f[6]),
                                                       page_segments=int(f[11]), reason=f[12])
        elif tag == "sample_chain":
            runs = []
            if len(f) > 4 and f[4]:
                for run in f[4].split(","):
                    j, k, n = map(int, run.split(":"))
                    runs.append((j, k, n))
            c["pairs"][f[2]][int(f[1])] = runs
        elif tag == "prices" and c["prices"] is None:
            c["prices"] = dict(zip(f[1::2], map(int, f[2::2])))
    if not c["shards"] or c["prices"] is None:
        raise SystemExit(f"{path}: no shards or prices; was it run with --per-shard/--decoupled?")
    return c


def percentile(values, p):
    values = sorted(values)
    if not values:
        return 0
    index = max(0, min(len(values) - 1, -(-int(p * len(values)) // 100) - 1))
    return values[index]


def bill(c, scripts, low, high, cached_before=None):
    """One wallet's bill over [low, high]; cached_before enables the decoupled run cache."""
    p = c["prices"]
    dir_cost = p["directory_query"] + p["response"]
    page_cost = p["page_query"] + p["response"]
    out = collections.Counter()
    overlapping = [sid for sid, s in c["shards"].items() if s["end"] >= low and s["start"] <= high]
    out["filter_bytes"] = sum(c["shards"][sid]["scripts"] for sid in overlapping) * FILTER_BYTES_PER_SCRIPT
    matched = set()
    depth = 0
    setups = set()
    for script in scripts:
        for sid, value in c["pairs"].get(script, {}).items():
            shard = c["shards"][sid]
            if sid not in overlapping:
                continue
            if cached_before is not None and shard["end"] < cached_before:
                continue
            matched.add(sid)
            out["directory_queries"] += 1
            out["query_bytes"] += dir_cost * shard["dir_segments"]
            setups.add((sid, "directory", shard["dir_segments"]))
            if c["kind"] == "coupled":
                fragments = value
                if fragments:
                    out["page_queries"] += fragments
                    out["query_bytes"] += page_cost * fragments * shard["page_segments"]
                    setups.add((sid, "pages", shard["page_segments"]))
                    out["paged_pairs"] += 1
                depth = max(depth, 2 if fragments else 1)
                continue
            walked = []
            for j, k, n in reversed(value):  # newest run first
                child = c["children"][sid][j]
                if cached_before is not None and child["end"] < cached_before:
                    break
                walked.append((j, k))
            for j, k in walked:
                segments = c["children"][sid][j]["page_segments"]
                out["page_queries"] += k
                out["query_bytes"] += page_cost * k * segments
                setups.add((sid, f"pages-{j}", segments))
            if walked:
                out["paged_pairs"] += 1
                out["runs_walked"] += len(walked)
                depth = max(depth, 1 + len(walked) + (1 if walked[-1][1] > 1 else 0))
            else:
                depth = max(depth, 1)
    out["setup_bytes"] = sum(segments for _, _, segments in setups) * p["setup"]
    out["choice_bytes"] = sum(c["shards"][sid]["scripts"] for sid in matched) * CHOICE_BITS_PER_SCRIPT / 8
    out["matched_shards"] = len(matched)
    out["round_trips"] = depth
    out["total_bytes"] = out["query_bytes"] + out["setup_bytes"] + out["filter_bytes"] + out["choice_bytes"]
    return out


def summarize(label, bills, keys):
    n = len(bills)
    cells = [label, str(n)]
    for key in keys:
        values = [b[key] for b in bills]
        cells.append("%s/%s/%s/%s" % tuple(
            _fmt(key, v) for v in (sum(values) / n, percentile(values, 50),
                                   percentile(values, 95), percentile(values, 99))))
    print("\t".join(cells))


def _fmt(key, value):
    if key.endswith("bytes"):
        return f"{value / 1e6:.3f}"
    return f"{value:.2f}" if isinstance(value, float) else str(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", required=True)
    parser.add_argument("--anchor", type=int, required=True)
    parser.add_argument("census", nargs="+")
    args = parser.parse_args()
    sample = json.load(open(args.sample))
    keys = ["directory_queries", "page_queries", "query_bytes", "total_bytes", "round_trips",
            "matched_shards"]
    for path in args.census:
        c = parse(path)
        start = min(s["start"] for s in c["shards"].values())
        end = min(max(s["end"] for s in c["shards"].values()), args.anchor)
        print(f"\n== {path} ({c['kind']}, heights {start}-{end}, {len(c['shards'])} shards"
              + (f", {sum(len(v) for v in c['children'].values())} page shards" if c["children"] else "")
              + ")")
        print("class\twallets\t" + "\t".join(f"{k} mean/p50/p95/p99" for k in keys)
              + "\t(MB for bytes)")
        by_class = collections.defaultdict(list)
        catch_up = collections.defaultdict(list)
        for client in sample["clients"]:
            if not client["scripts"]:
                continue
            low = max(client["required_from"], start)
            if low > end:
                continue
            clipped = client["required_from"] < start
            label = client["class"] + (" (clipped to window)" if clipped else "")
            by_class[label].append(bill(c, client["scripts"], low, end))
            if client["class"].startswith("catch-up") and not clipped:
                if c["kind"] == "decoupled":
                    catch_up[label].append(bill(c, client["scripts"], low, end, cached_before=low))
                else:
                    catch_up[label].append(by_class[label][-1])
        for label in sorted(by_class):
            summarize(label, by_class[label], keys)
        if catch_up:
            print("catch-up with cache (the wallet synced through required_from - 1)")
            for label in sorted(catch_up):
                summarize(label, catch_up[label], keys)


if __name__ == "__main__":
    main()
