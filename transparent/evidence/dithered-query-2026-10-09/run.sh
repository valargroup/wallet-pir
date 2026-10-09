#!/usr/bin/env bash
# Regenerates every report and certificate in this directory.
# Usage: transparent/evidence/dithered-query-2026-10-09/run.sh <certify_native.py>
# The checker must be ipir-sp d76e61a's reinspiring/tools/security/certify_native.py
# (SHA-256 9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7),
# in place in that checkout: it reads reinspiring/src/native_gaussian_cdf.txt
# relative to itself.
set -euo pipefail
cd "$(dirname "$0")/../../.."
certify=${1:?path to reinspiring/tools/security/certify_native.py (ipir-sp d76e61a)}
out=transparent/evidence/dithered-query-2026-10-09
cargo build --locked --profile release-fast -p transparent-shard-server --example native_certificate
tool="${CARGO_TARGET_DIR:-target}/release-fast/examples/native_certificate"

# certify <name>: certify_native.py exits non-zero below 128 bits; keep its
# verdict either way, as the earlier screens did.
certify() {
  python3 "$certify" "$out/$1.report.json" >"$out/$1.certificate.json" || true
}

# Every table a published shard may name, at its own setup and masks, for a
# splitmix64 full-range database and an all-0xffff one, with the worst-case
# query term; each at 49-bit nearest and 44-bit dithered query rounding.
tables="recent-8k:directory recent-8k:pages recent-4k:directory recent-4k:pages
recent-4k-8k:directory recent-4k-8k:pages archive-32k:directory archive-32k:pages
archive-wide:directory archive-wide:pages txid-2k:txdirectory txid-4k:txdirectory"
for entry in $tables; do
  geometry=${entry%%:*}
  table=${entry##*:}
  for fill in random max; do
    for rounding in nearest dithered; do
      name="synthetic-$geometry-$table-$fill-$rounding"
      "$tool" synthetic --geometry "$geometry" --table "$table" --fill "$fill" \
        --query-rounding "$rounding" >"$out/$name.report.json"
      certify "$name"
    done
  done
done

# Segment mode with the query term measured from the table, on the same
# reproducible pseudo-random archive-wide page segment as the 2026-09-28 screen
# (not a published snapshot; the 256 MiB input is regenerated, not retained).
segment=$(mktemp)
trap 'rm -f "$segment"' EXIT
python3 -c "import random,sys; sys.stdout.buffer.write(random.Random(1).randbytes(65536*4096))" \
  >"$segment"
for rounding in nearest dithered; do
  name="segment-measured-archive-wide-pages-seed1-$rounding"
  "$tool" segment --geometry archive-wide --table pages --rows-bin "$segment" \
    --query-rounding "$rounding" >"$out/$name.report.json"
  certify "$name"
done

python3 "$out/summarize.py" "$out" >"$out/results.json"
(cd "$out" && shasum -a 256 manifest.json run.sh summarize.py results.json ./*.report.json ./*.certificate.json >SHA256SUMS)
