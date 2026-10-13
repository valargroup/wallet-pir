# Joins plan, served setup digests, reports and checker verdicts into
# raw/results.tsv and raw/summary.json.
import json, statistics, re
plan = [l.rstrip("\n").split("\t") for l in open("inputs/plan.tsv")]
served = {l.split("\t")[1]: l.split("\t")[2] for l in open("inputs/served-setup.tsv")}
times = {}
for l in open("raw/native.log"):
    m = re.match(r"([\d.]+) s (\d+) KiB raw/reports/(\S+)", l)
    if m: times[m.group(3)] = (float(m.group(1)), int(m.group(2)))
rows, tiers = [], {}
for f in plan:
    sid, tier, dig = f[0], f[1], f[2]
    n = f"s{int(sid):03d}-{dig[:12]}"
    r = json.load(open(f"raw/reports/{n}.report.json"))
    v = json.load(open(f"raw/verdicts/{n}.verdict.json"))
    a = v["actual_profile"]; bits = a["certified_failure_bits"]
    assert r["served_public_checked"] and r["served_public_sha256"] == served[dig]
    assert r["database_sha256"] == "rows_sha256:" + f[7]
    assert (a["p_bits"], a["published_mask_bits"], a["query_bits"], a["response_bits"]) == (8, 22, 28, 10)
    t = times.get(n, (None, None))
    rows.append([sid, tier, dig, f[3], f[4], f[5], bits, a["meets_78"], a["meets_128"], r["served_public_sha256"], t[0], t[1]])
    tiers.setdefault(tier, []).append(bits)
with open("raw/results.tsv", "w") as o:
    o.write("shard\ttier\tmanifest_digest\tstart_height\tend_height\trecords\tcertified_failure_bits\tmeets_78\tmeets_128\tserved_public_sha256\tseconds\tpeak_kib\n")
    for r in rows: o.write("\t".join(map(str, r)) + "\n")
summary = {t: {"tables": len(b), "min": min(b), "median": statistics.median(b), "max": max(b),
               "below_50": sum(x < 50 for x in b), "below_78": sum(x < 78 for x in b), "below_128": sum(x < 128 for x in b)}
           for t, b in tiers.items()}
secs = [r[10] for r in rows if r[10]]; kib = [r[11] for r in rows if r[11]]
summary["tool_run"] = {"seconds_median": statistics.median(secs), "seconds_max": max(secs), "peak_rss_kib_max": max(kib), "parallel": 4}
json.dump(summary, open("raw/summary.json", "w"), indent=1)
print(json.dumps(summary))
