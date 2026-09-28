"""Summarize the paired single-lookup measurement.

Reads report-{off,all}-{1,2}.json and prints, per variant, class and
concurrency: syncs, exact/failed/mismatched counts, mean payload per sync by
stage, directory queries per sync, and sync-time percentiles. Means divide by
all attempted syncs (n), so failures stay in the denominator. Percentiles are
reported per repetition, never pooled or added.
"""
import json
import sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
STAGES = ["filters", "manifest", "setup_directory", "setup_pages", "query_directory", "query_pages"]

rows = []
for path in sorted(root.glob("report-*.json")):
    variant, rep = path.stem.split("-")[1:3]
    report = json.load(open(path))
    for step in report["steps"]:
        for name, c in sorted(step["classes"].items()):
            n = c["n"]
            if not n:
                continue
            stages = c["stages"]
            total = sum(s["bytes_up"] + s["bytes_down"] for s in stages.values())
            per = {k: (stages[k]["bytes_up"] + stages[k]["bytes_down"]) / n for k in STAGES if k in stages}
            rows.append({
                "variant": variant, "rep": rep, "concurrency": step["concurrency"], "class": name,
                "n": n, "exact": c["exact"], "failed": c["failed"], "mismatched": c["mismatched"],
                "total": total / n,
                "directory_queries": stages.get("query_directory", {}).get("calls", 0) / n,
                "page_queries": stages.get("query_pages", {}).get("calls", 0) / n,
                "p50": c["sync_seconds"]["p50"], "p95": c["sync_seconds"]["p95"],
                **{f"{k}_bytes": v for k, v in per.items()},
            })

json.dump(rows, open(root / "summary.json", "w"), indent=1)
cols = ["variant", "rep", "concurrency", "class", "n", "exact", "failed", "mismatched",
        "total", "query_directory_bytes", "manifest_bytes", "directory_queries", "page_queries", "p50", "p95"]
print("\t".join(cols))
for r in sorted(rows, key=lambda r: (r["class"], r["concurrency"], r["variant"], r["rep"])):
    print("\t".join(
        f"{r.get(c, 0):.0f}" if isinstance(r.get(c), float) and c not in ("p50", "p95", "directory_queries", "page_queries")
        else f"{r.get(c, 0):.3f}" if isinstance(r.get(c), float) else str(r.get(c, ""))
        for c in cols))

# Paired comparison: pool both repetitions' byte means (weighted by n) per
# class and concurrency. Byte means are additive; latency percentiles are not
# pooled.
print("\nclass\tconcurrency\toff_total\tall_total\tchange\toff_dirq\tall_dirq\toff_manifest\tall_manifest")
keys = sorted({(r["class"], r["concurrency"]) for r in rows})
for name, conc in keys:
    def pooled(variant, field):
        sel = [r for r in rows if r["class"] == name and r["concurrency"] == conc and r["variant"] == variant]
        n = sum(r["n"] for r in sel)
        return sum(r[field] * r["n"] for r in sel) / n if n else float("nan")
    off, on = pooled("off", "total"), pooled("all", "total")
    print(f"{name}\t{conc}\t{off:.0f}\t{on:.0f}\t{100 * (on / off - 1):+.1f}%\t"
          f"{pooled('off', 'directory_queries'):.3f}\t{pooled('all', 'directory_queries'):.3f}\t"
          f"{pooled('off', 'manifest_bytes'):.0f}\t{pooled('all', 'manifest_bytes'):.0f}")
