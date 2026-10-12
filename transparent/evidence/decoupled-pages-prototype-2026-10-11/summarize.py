#!/usr/bin/env python3
"""Shard-side comparison table from run-census.sh logs.

For each `WINDOW.LAYOUT.txt` under OUT: shard (and page-shard) counts, seal
reasons, directory fill by bytes and by script count per shard (p50 / max),
page fill, chain lengths and pinned plaintext, plus the run's peak RSS and
wall time from its `.time` report.

Coupled fill uses the publisher's capacities: scripts against rows x 58
minimal entries, directory bytes against rows x 4,092 payload bytes, page rows
against the policy's page capacity. Decoupled fill is the census's own
`script_fill` / `byte_fill` columns.

Usage: summarize.py OUT
"""
import collections
import glob
import os
import re
import sys


def pct(values, p):
    values = sorted(values)
    if not values:
        return float("nan")
    return values[max(0, min(len(values) - 1, -(-int(p * len(values)) // 100) - 1))]


def time_report(path):
    out = {}
    if os.path.exists(path):
        text = open(path).read()
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
        out["rss_gb"] = int(m.group(1)) / 1e6 if m else None
        m = re.search(r"Elapsed \(wall clock\) time.*: (\S+)", text)
        out["wall"] = m.group(1) if m else None
    return out


def coupled(path):
    rows, tail, dir_rows, page_cap, pinned = [], [], None, None, None
    for line in open(path):
        f = line.strip().split("\t")
        if f[0] == "per_shard" and f[1].isdigit():
            rows.append(f)
        elif f[0] == "per_shard_tail":
            tail.append(f)
        elif f[0] == "prices":
            dir_rows = int(f[2])
            page_cap = int(f[6])
        elif "pinned plaintext per shard" in line:
            pinned = float(line.split("fleet ~")[1].split()[0])
    scripts_cap = dir_rows * 58
    bytes_cap = dir_rows * 4092
    reasons = collections.Counter(f[9] for f in rows)
    script_fill = [int(f[5]) / scripts_cap for f in rows]
    byte_fill = [int(f[10]) / bytes_cap for f in rows]
    page_fill = [int(f[6]) / page_cap for f in rows]
    return dict(shards=len(rows), tail=len(tail), children=None, reasons=dict(reasons),
                script_fill=(pct(script_fill, 50), max(script_fill, default=0)),
                byte_fill=(pct(byte_fill, 50), max(byte_fill, default=0)),
                page_fill=(pct(page_fill, 50), max(page_fill, default=0)),
                pinned_gb=pinned)


def decoupled(path):
    dirs, children, chains, pinned, page_fill_line = [], [], {}, None, None
    for line in open(path):
        f = line.strip().split("\t")
        if f[0] == "per_directory" and f[1].isdigit():
            dirs.append(f)
        elif f[0] == "per_child" and f[1].isdigit():
            children.append(f)
        elif f[0] == "chain_length":
            chains[int(f[1])] = int(f[2])
        elif line.strip().startswith("pinned"):
            pinned = float(line.split("total")[1].split()[0])
        elif line.strip().startswith("page fill"):
            page_fill_line = line.strip()[len("page fill"):].strip()
        elif line.strip().startswith("decoupled:"):
            m = re.search(r"page_rows: Limit \{ target: \d+, capacity: (\d+)", line)
            page_cap = int(m.group(1))
    sealed = [f for f in dirs if f[12] != "tail"]
    reasons = collections.Counter(f[12] for f in sealed)
    child_reasons = collections.Counter(f[12] for f in children)
    script_fill = [float(f[7]) for f in sealed]
    byte_fill = [float(f[8]) for f in sealed]
    child_fill = [int(f[6]) / page_cap for f in children if f[12] != "tail"]
    paged = sum(v for k, v in chains.items() if k > 0)

    def chain_pct(p):
        want, seen = -(-p * paged // 100), 0
        for k in sorted(chains):
            if k == 0:
                continue
            seen += chains[k]
            if seen >= want:
                return k
        return 0
    return dict(shards=len(sealed), tail=len(dirs) - len(sealed), children=len(children),
                reasons=dict(reasons), child_reasons=dict(child_reasons),
                script_fill=(pct(script_fill, 50), max(script_fill, default=0)),
                byte_fill=(pct(byte_fill, 50), max(byte_fill, default=0)),
                page_fill=(pct(child_fill, 50), max(child_fill, default=0)),
                page_fill_line=page_fill_line,
                chains=(chain_pct(50), chain_pct(95), chain_pct(99), max(chains, default=0)),
                pinned_gb=pinned)


def main():
    out = sys.argv[1]
    for path in sorted(glob.glob(os.path.join(out, "*.txt"))):
        name = os.path.basename(path)[:-4]
        if name == "jobs":
            continue
        kind = decoupled if ".decoupled-" in name else coupled
        try:
            r = kind(path)
        except Exception as error:  # an unfinished or failed run
            print(f"{name}\tunreadable: {error}")
            continue
        r.update(time_report(path[:-4] + ".time"))
        cells = [name, f"shards {r['shards']}+{r['tail']} tail"]
        if r["children"] is not None:
            cells.append(f"page shards {r['children']}")
        cells.append("seal " + ", ".join(f"{k} {v}" for k, v in sorted(r["reasons"].items())))
        if r.get("child_reasons"):
            cells.append("child seal " + ", ".join(f"{k} {v}" for k, v in sorted(r["child_reasons"].items())))
        cells.append("dir script fill p50/max %.1f%%/%.1f%%" % tuple(100 * x for x in r["script_fill"]))
        cells.append("dir byte fill p50/max %.1f%%/%.1f%%" % tuple(100 * x for x in r["byte_fill"]))
        cells.append("page rows fill p50/max %.1f%%/%.1f%%" % tuple(100 * x for x in r["page_fill"]))
        if r.get("page_fill_line"):
            cells.append("page fill " + r["page_fill_line"])
        if r.get("chains"):
            cells.append("runs p50/p95/p99/max %d/%d/%d/%d" % r["chains"])
        cells.append(f"pinned {r['pinned_gb']} GB")
        if r.get("rss_gb") is not None:
            cells.append(f"rss {r['rss_gb']:.1f} GB, wall {r['wall']}")
        else:
            cells.append("still running")
        print("\t".join(cells))


if __name__ == "__main__":
    main()
