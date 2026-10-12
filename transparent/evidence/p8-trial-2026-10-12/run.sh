#!/usr/bin/env bash
# Runs p8_trial over every table of a copied v12 publication, one process per
# table and variant, appending to results.jsonl and skipping work already
# recorded. Certificates go to reports/ and are checked by certify_native.py.
#   run.sh <publication-dir> <p8_trial binary> <certify_native.py> <out-dir>
set -euo pipefail
pub=$1 bin=$2 checker=$3 out=$4
mkdir -p "$out/reports" "$out/certify"
results=$out/results.jsonl
touch "$results"
python3 - "$pub" > "$out/tables.tsv" <<'PY'
import json, glob, os, sys
pub = sys.argv[1]
for m in sorted(glob.glob(os.path.join(pub, "*/manifest.json"))):
    j = json.load(open(m)); d = os.path.dirname(m); g = j["geometry"]
    seg = j["directory_segments"][0]
    print(g, "directory", os.path.join(d, "directory.0.bin"), seg["rows"], sep="\t")
    for c in j["page_children"]:
        rows = c["page_segments"][0]["rows"]
        q = rows if len(c["page_segments"]) != 1 else min(-(-max(c["occupancy"]["page_rows"], 1) // 2048) * 2048, rows)
        print(g, "pages", os.path.join(d, f"pages-{c['index']}.0.bin"), q, sep="\t")
PY
variants=${VARIANTS:-"baseline p8-19x2 p8-27x1 certify-19x2 certify-27x1"}
while IFS=$'\t' read -r geometry table path query_rows; do
  for variant in $variants; do
    key="$variant $path"
    grep -qF "\"variant\":\"$variant\"" "$results" && grep -F "\"variant\":\"$variant\"" "$results" | grep -qF "\"path\":\"$path\"" && continue
    report=$out/reports/$(basename "$(dirname "$path")" | cut -c1-16)-$(basename "$path" .0.bin)-${variant#certify-}-q${QUERY_BITS:-28}.json
    if ! line=$(TRANSPARENT_BUILD_THREADS=8 "$bin" --variant "$variant" --geometry "$geometry" --table "$table" \
        --rows-bin "$path" --queries "${QUERIES:-10}" --query-rows "$query_rows" \
        --query-bits "${QUERY_BITS:-28}" --report "$report" 2>>"$out/stderr.log"); then
      echo "{\"variant\":\"$variant\",\"path\":\"$path\",\"error\":true}" >> "$results"; continue
    fi
    if [[ $variant == certify-* ]]; then
      verdict=$out/certify/$(basename "$report")
      python3 "$checker" --require-bits 128 "$report" > "$verdict" 2>>"$out/stderr.log" || true
      bits=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["actual_profile"]["certified_failure_bits"]))' "$verdict" 2>/dev/null || echo null)
      line=${line%\}}",\"certified_failure_bits\":$bits}"
    fi
    echo "$line" >> "$results"
    echo "$key done"
  done
done < "$out/tables.tsv"
