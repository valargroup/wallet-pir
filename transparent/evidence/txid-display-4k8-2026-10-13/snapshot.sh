#!/usr/bin/env bash
# Hardlink the worker's active txid-4k8 publication into the measurement
# directory and write inputs/plan.tsv from its map and manifests, checking
# every table file against its manifest digest.
set -euo pipefail
M=/root/txid-4k8-measure; cd $M; mkdir -p inputs
date -u +%FT%TZ >inputs/snapshot-started
cp /opt/transparent-txid-display/worker/active.json inputs/active.json
map=$(python3 -c 'import json;print(json.load(open("inputs/active.json"))["map_sha256"])')
src=/srv/transparent-txid-display/publications/$map
rm -rf tables; cp -al $src tables
cp $src/txid-shards.json inputs/txid-shards.json
test "$(sha256sum inputs/txid-shards.json | cut -d' ' -f1)" = "$map"
python3 - <<'PY'
import hashlib, json
m = json.load(open("inputs/txid-shards.json")); out = open("inputs/plan.tsv", "w")
for s in m["shards"]:
    d = s["manifest_digest"]; man = json.load(open(f"tables/{d}/manifest.json"))
    assert hashlib.sha256(open(f"tables/{d}/manifest.json", "rb").read()).hexdigest() == d
    assert man["geometry"] == "txid-4k8" and len(man["buckets"]) == 1
    b = man["buckets"][0]; seg = b["directory_segments"]
    assert len(seg) == 1, f"shard {s['shard_id']} has {len(seg)} segments"
    h = hashlib.sha256(open(f"tables/{d}/directory-0.0.bin", "rb").read()).hexdigest()
    assert h == seg[0]["sha256"] and seg[0]["rows"] == 4096
    out.write("\t".join(map(str, [s["shard_id"], "sealed" if s["sealed"] else "recent", d, s["start_height"],
              s["end_height"], s["records"], b["placement_seed"], h, man["schema"]])) + "\n")
print("tables", len(m["shards"]))
PY
date -u +%FT%TZ >inputs/snapshot-finished
