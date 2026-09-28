#!/usr/bin/env bash
# Publish each archive slice as v8 archive-wide shards, then certify every page
# and directory segment with its measured query term, and record column L1.
set -uo pipefail
B=/root/v8-target/release
CERT=/root/.cargo/git/checkouts/ipir-sp-ecbe94d2bf94a34c/1f2aec6/reinspiring/tools/security/certify_native.py
OUT=/root/archive-measure; mkdir -p $OUT
declare -A PARENT=( [s84]=000000000438b241a7724f27e9689c1a98603bb6d2d425ea26892763b56d9ed1 [s94]=0000000000b2c88d7e4643003589debf6f0bc5146e34725f50a6ea0948a8b2a4 [s157]=000000000071a171904a4f12fea48016e17a82ee16ef74a87972caca8c863ed4 )
for s in s84 s94 s157; do
  rm -rf /root/archive-v8/$s
  $B/shard-publish --data-dir /root/archive-slices/$s --output /root/archive-v8/$s --recent-geometry archive-wide \
    --zakura-cookie /dev/null --parent-block-hash ${PARENT[$s]} --range-profile zcash-transparent-range-v2 \
    --directory-choice all > $OUT/$s.publish.log 2>&1 || { echo "$s publish failed"; tail -3 $OUT/$s.publish.log; continue; }
  for dir in /root/archive-v8/$s/*/; do
    for table in pages directory; do
      for seg in $dir$table.*.bin; do
        [ -f "$seg" ] || continue
        name=$OUT/$s-$(basename $(dirname $seg) | cut -c1-8)-$(basename $seg .bin)
        /root/v8-target/release/examples/native_certificate segment --geometry archive-wide --table $table --rows-bin "$seg" > $name.report.json 2> $name.err
        python3 $CERT $name.report.json > $name.certificate.json 2>&1 || true
        /root/venv-measure/bin/python - "$seg" "$name" <<PY
import sys, numpy as np, json
seg, name = sys.argv[1], sys.argv[2]
rows = 65536 if "pages" in seg else 32768
a = np.fromfile(seg, dtype="<u2").reshape(rows, 2048).astype(np.uint64)
l1 = a.sum(axis=0)
nz = (a != 0).mean()
cert = json.load(open(name + ".certificate.json")) if open(name + ".certificate.json").read().strip().startswith("{") else {}
print(json.dumps({"segment": seg, "rows": rows, "max_column_l1": int(l1.max()), "mean_column_l1": float(l1.mean()),
  "worst_case_l1": 65535 * rows, "max_fraction_of_worst": float(l1.max()) / (65535 * rows), "nonzero_u16_fraction": float(nz),
  "certified_failure_bits": cert.get("certified_failure_bits")}))
PY
      done
    done
  done
done
