#!/usr/bin/env python3
"""Summarizes results.jsonl from run.sh into summary.json and Markdown tables.

    summarize.py <results.jsonl> <publication-dir> > summary.md
"""
import json
import os
import statistics
import sys

VARIANTS = ["baseline", "p8-19x2", "p8-27x1"]
TIER = {"decoupled-32k-32k": "archive", "decoupled-2k-8k": "recent"}

results, publication = sys.argv[1], sys.argv[2]
rows = [json.loads(line) for line in open(results)]
errors = [r for r in rows if r.get("error")]
rows = [r for r in rows if not r.get("error")]


def sealed(path):
    manifest = json.load(open(os.path.join(os.path.dirname(path), "manifest.json")))
    if os.path.basename(path).startswith("directory"):
        return manifest["sealed"]
    index = int(os.path.basename(path).split("-")[1].split(".")[0])
    return next(c["sealed"] for c in manifest["page_children"] if c["index"] == index)


def med(values):
    return statistics.median(values) if values else None


def pick(variant, tier=None, table=None):
    return [r for r in rows if r["variant"] == variant
            and (tier is None or TIER[r["geometry"]] == tier)
            and (table is None or r["table"] == table)]


summary = {"errors": len(errors), "groups": {}, "totals": {}, "certificates": {}}
out = []
out.append(f"Runs: {len(rows)} recorded, {len(errors)} failed.\n")

# Per-table medians by tier, table kind and variant.
out.append("| Tier | Table | Variant | Tables | Upload (prefix) B | Response B | Published B | Held MiB | Build s | Answer p50 / p99 ms | Paired p50 ms |")
out.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for tier in ["archive", "recent"]:
    for table in ["directory", "pages"]:
        for variant in VARIANTS:
            g = pick(variant, tier, table)
            if not g:
                continue
            res = [r["result"] for r in g]
            entry = {
                "tables": len(g),
                "upload_bytes": med([x["upload_bytes"] for x in res]),
                "upload_prefix_bytes": med([x["upload_prefix_bytes"] for x in res]),
                "response_bytes": med([x["response_bytes"] for x in res]),
                "published_bytes": med([x["published_bytes"] for x in res]),
                "held_mib": med([x["held_bytes"] / 2**20 for x in res]),
                "build_seconds": med([x["build_seconds"] for x in res]),
                "answer_p50_ms": med([x["answer_single"]["p50_ms"] for x in res]),
                "answer_p99_ms": max(x["answer_single"]["p99_ms"] for x in res),
                "paired_p50_ms": med([x["answer_paired"]["p50_ms"] for x in res]),
            }
            if variant != "baseline":
                entry["scan_p50_ms"] = med([x["scan"]["p50_ms"] for x in res])
                entry["packing_p50_ms"] = med([x["packing"]["p50_ms"] for x in res])
            summary["groups"][f"{tier}/{table}/{variant}"] = entry
            out.append(
                f"| {tier} | {table} | {variant} | {entry['tables']} | {entry['upload_bytes']:,.0f} ({entry['upload_prefix_bytes']:,.0f}) "
                f"| {entry['response_bytes']:,.0f} | {entry['published_bytes']:,.0f} | {entry['held_mib']:.1f} | {entry['build_seconds']:.2f} "
                f"| {entry['answer_p50_ms']:.2f} / {entry['answer_p99_ms']:.2f} | {entry['paired_p50_ms']:.2f} |")

# Fleet totals: memory a worker holds for every table, cold build with one
# build slot, the tail rebuild, and a full pass over every directory shard.
# Every table of a tier and kind has one shape, so held bytes, upload and
# build time are per-shape figures; the archive was timed on a sample, and
# totals scale the per-kind median to every table of the publication.
counts = {}
for line in open(os.path.join(os.path.dirname(results), "tables.tsv")):
    geometry, table, path, _ = line.rstrip("\n").split("\t")
    counts.setdefault((TIER[geometry], table), []).append(path)
out.append("\n| Tier | Variant | Tables (timed) | Held GiB | Peak-process max GiB | Cold build s | Open-tail rebuild s | Directory pass upload KB |")
out.append("|---|---|---:|---:|---:|---:|---:|---:|")
for tier in ["archive", "recent"]:
    for variant in VARIANTS:
        g = pick(variant, tier)
        if not g:
            continue
        held = build = directory_pass = 0.0
        for table in ["directory", "pages"]:
            kind = [r["result"] for r in g if r["table"] == table]
            n = len(counts.get((tier, table), []))
            if not kind:
                continue
            held += n * med([x["held_bytes"] for x in kind])
            build += n * med([x["build_seconds"] for x in kind])
            if table == "directory":
                directory_pass += n * med([x["upload_bytes"] for x in kind])
        tail = [r["result"]["build_seconds"] for r in g if not sealed(r["path"])]
        t = {
            "tables": sum(len(v) for (k, _), v in counts.items() if k == tier),
            "timed": len(g),
            "held_gib": held / 2**30,
            "peak_max_gib": max(r["peak_rss_bytes"] for r in g) / 2**30,
            "cold_build_seconds": build,
            "open_tail_rebuild_seconds": sum(tail),
            "open_tail_tables": len(tail),
            "directory_pass_kb": directory_pass / 1e3,
        }
        summary["totals"][f"{tier}/{variant}"] = t
        out.append(f"| {tier} | {variant} | {t['tables']} ({t['timed']}) | {t['held_gib']:.2f} | {t['peak_max_gib']:.2f} | {t['cold_build_seconds']:.0f} "
                   f"| {t['open_tail_rebuild_seconds']:.2f} ({t['open_tail_tables']} tables) | {t['directory_pass_kb']:,.0f} |")

# Certificates.
out.append("\n| Tier | Table | Key | Query bits | Tables | Min bits | Median bits | Below 50 | Below 78 | Below 128 | Analyze s (median) |")
out.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for tier in ["archive", "recent"]:
    for table in ["directory", "pages"]:
        for gadget in ["19x2", "27x1"]:
            g = pick(f"certify-{gadget}", tier, table)
            if not g:
                continue
            for bits in sorted({r["result"]["query_bits"] for r in g}):
                h = [r for r in g if r["result"]["query_bits"] == bits]
                scores = [r.get("certified_failure_bits") for r in h]
                valid = [s for s in scores if isinstance(s, int)]
                c = {
                    "tables": len(h), "min_bits": min(valid) if valid else None,
                    "median_bits": med(valid),
                    **{f"below_{floor}": sum(1 for s in scores if not isinstance(s, int) or s < floor) for floor in (50, 78, 128)},
                    "analyze_seconds": med([r["result"]["analyze_seconds"] for r in h]),
                    "worst": min(h, key=lambda r: r.get("certified_failure_bits") if isinstance(r.get("certified_failure_bits"), int) else -1)["path"],
                }
                summary["certificates"][f"{tier}/{table}/{gadget}/q{bits}"] = c
                out.append(f"| {tier} | {table} | {gadget} | {bits} | {c['tables']} | {c['min_bits']} | {c['median_bits']} | {c['below_50']} | {c['below_78']} | {c['below_128']} | {c['analyze_seconds']:.1f} |")

json.dump(summary, open(os.path.join(os.path.dirname(results), "summary.json"), "w"), indent=1)
print("\n".join(out))
