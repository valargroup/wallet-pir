"""Matched recent (script, shard) pairs per sampled wallet under a census's boundaries.

Like the single-lookup census's pairs.py, but reads every per_shard row of the
first policy and treats the remainder up to the anchor as the tail. A pair is a
sampled script with an event in a shard ending at or after required_from; filter
false positives are not counted.
"""
import collections, json, struct, sys

census, sample_path, journal = sys.argv[1:4]
start, anchor = 3262749, 3473686
bounds, seen = [], set()
for line in open(census):
    f = line.strip().split("\t")
    if f[0] == "per_shard" and f[1].isdigit():
        if f[1] in seen:
            break
        seen.add(f[1]); bounds.append((int(f[1]), int(f[2]), int(f[3])))
    elif bounds and not line.startswith("  per_shard"):
        break
bounds.append((bounds[-1][0] + 1, bounds[-1][2] + 1, anchor))
tail_id = bounds[-1][0]
sample = json.load(open(sample_path))
want = {bytes.fromhex(s) for c in sample["clients"] for s in c["scripts"]}
blocks = open(journal + "/blocks.bin", "rb").read(); events = open(journal + "/events.bin", "rb").read()
active = collections.defaultdict(set); b = 0
for i in range(len(blocks) // 48):
    h = start + i
    while h > bounds[b][2]: b += 1
    off, cnt = struct.unpack("<QQ", blocks[i*48+32:i*48+48]); p = off
    for _ in range(cnt):
        n = struct.unpack("<H", events[p:p+2])[0]; s = events[p+2:p+2+n]; p += 2 + n + 96
        if s in want: active[s].add(bounds[b][0])
end = {sid: e for sid, _, e in bounds}
per = collections.defaultdict(lambda: [0, 0, 0, 0])
for c in sample["clients"]:
    low = max(c["required_from"], start); t = per[c["class"]]; t[2] += 1; shards = set()
    for s in c["scripts"]:
        for sid in active.get(bytes.fromhex(s), ()):
            if end[sid] >= low:
                t[0 if sid != tail_id else 1] += 1; shards.add(sid)
    t[3] += len(shards)
print(f"shards\t{len(bounds)}")
print("class\tclients\tsealed_pairs\ttail_pairs\tmatched_shards")
for k, (se, ta, n, sh) in sorted(per.items()):
    print(f"{k}\t{n}\t{se/n:.3f}\t{ta/n:.3f}\t{sh/n:.3f}")
