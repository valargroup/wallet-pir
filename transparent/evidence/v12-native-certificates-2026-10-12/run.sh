#!/usr/bin/env bash
# Regenerates every report and certificate in this directory.
# Usage: transparent/evidence/v12-native-certificates-2026-10-12/run.sh <certify_native.py>
# The checker must be ipir-sp d76e61a's reinspiring/tools/security/certify_native.py
# (SHA-256 9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7),
# in place in that checkout: it reads reinspiring/src/native_gaussian_cdf.txt
# relative to itself. Run from the wallet-pir checkout with cargo on PATH.
set -euo pipefail
cd "$(dirname "$0")/../../.."
certify=${1:?path to reinspiring/tools/security/certify_native.py (ipir-sp d76e61a)}
out=transparent/evidence/v12-native-certificates-2026-10-12
cargo build --locked --profile release-fast -p transparent-shard-server --example native_certificate
tool="${CARGO_TARGET_DIR:-target}/release-fast/examples/native_certificate"

# certify <name>: certify_native.py exits non-zero below its --require-bits
# (128 by default); keep its verdict either way.
certify() {
  python3 "$certify" "$out/$1.report.json" >"$out/$1.certificate.json" || true
}

# Every table a v12 shard may name, at its own setup and masks, for a
# splitmix64 full-range database and an all-0xffff one, with the worst-case
# query term; each at 49-bit nearest and 44-bit dithered query rounding.
tables="decoupled-2k-8k:directory decoupled-2k-8k:pages
decoupled-32k-32k:directory decoupled-32k-32k:pages"
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

python3 "$out/summarize.py" "$out" >"$out/results.json"
(cd "$out" && sha256sum run.sh summarize.py results.json ./*.report.json ./*.certificate.json >SHA256SUMS)
