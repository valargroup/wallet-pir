"""Matched recent (script, shard) pairs per sampled wallet, tail versus sealed.

Reads the recent journal slice, the recent-8k census boundaries and the pinned
workload sample. A pair is a sampled script with at least one event in a recent
shard whose end is at or after the wallet's required_from. Filter false
positives are not counted.
"""
import collections
import json
import struct
import sys

census, sample_path, journal = sys.argv[1:4]
start = 3262749
sample = json.load(open(sample_path))
bounds = []
for line in open(census):
    fields = line.split("\t")
    if line.startswith("  per_shard\t") and fields[1].isdigit():
        bounds.append((int(fields[1]), int(fields[2]), int(fields[3])))
# The first policy's thirteen sealed shards, then the tail the census reports
# as shard 173 through the anchor.
bounds = bounds[:13] + [(173, bounds[12][2] + 1, 3473686)]
want = {bytes.fromhex(s) for c in sample["clients"] for s in c["scripts"]}
blocks = open(journal + "/blocks.bin", "rb").read()
events = open(journal + "/events.bin", "rb").read()
active = collections.defaultdict(set)
shard = 0
for i in range(len(blocks) // 48):
    height = start + i
    while height > bounds[shard][2]:
        shard += 1
    offset, count = struct.unpack("<QQ", blocks[i * 48 + 32 : i * 48 + 48])
    p = offset
    for _ in range(count):
        n = struct.unpack("<H", events[p : p + 2])[0]
        script = events[p + 2 : p + 2 + n]
        p += 2 + n + 96
        if script in want:
            active[script].add(bounds[shard][0])
per = collections.defaultdict(lambda: [0, 0, 0, 0])
for client in sample["clients"]:
    low = max(client["required_from"], start)
    totals = per[client["class"]]
    totals[2] += 1
    shards = set()
    for s in client["scripts"]:
        for sid in active.get(bytes.fromhex(s), ()):
            if bounds[sid - 160][2] >= low:
                totals[0 if sid != 173 else 1] += 1
                shards.add(sid)
    totals[3] += len(shards)
print("class\tclients\tsealed_pairs_per_client\ttail_pairs_per_client\ttail_fraction\tmatched_shards_per_client")
for name, (sealed, tail, n, shards) in sorted(per.items()):
    print(f"{name}\t{n}\t{sealed / n:.3f}\t{tail / n:.3f}\t{tail / max(1, sealed + tail):.3f}\t{shards / n:.3f}")
