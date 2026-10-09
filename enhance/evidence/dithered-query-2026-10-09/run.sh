#!/usr/bin/env bash
# Regenerates every report and certificate in this directory.
# Usage: enhance/evidence/dithered-query-2026-10-09/run.sh <certify_native.py>
# The checker must be ipir-sp d76e61a's reinspiring/tools/security/certify_native.py
# (SHA-256 9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7),
# in place in that checkout: it reads reinspiring/src/native_gaussian_cdf.txt
# relative to itself.
set -euo pipefail
cd "$(dirname "$0")/../../.."
certify=${1:?path to reinspiring/tools/security/certify_native.py (ipir-sp d76e61a)}
out=enhance/evidence/dithered-query-2026-10-09
cargo build --locked --profile release-fast -p enhance-pir-server --features native-reinspiring \
  --example native_certificate
tool="${CARGO_TARGET_DIR:-target}/release-fast/examples/native_certificate"

certify() {
  # Exits non-zero below 128 bits; keep the verdict either way.
  python3 "$certify" "$out/$1.report.json" >"$out/$1.certificate.json" || true
}

# Enhance shard 0 at every query-domain size, under its own masks and packing
# setup, for a splitmix64 full-range database and an all-0xffff one, with the
# worst-case query term; each at 49-bit nearest and 44-bit dithered rounding.
for rows in 4096 8192 16384 32768; do
  for fill in random max; do
    for rounding in nearest dithered; do
      name="enhance-shard0-$rows-$fill-$rounding"
      "$tool" enhance --fill "$fill" --rows "$rows" --shard 0 \
        --query-rounding "$rounding" >"$out/$name.report.json"
      certify "$name"
    done
  done
done

# Status, 8,192 x 6,144, under the masks and packing setup of the generation
# the 2026-09-26 certificates recorded (network and salt from its manifest),
# with synthetic contents and the worst-case query term.
manifest=enhance/evidence/native-certificate-2026-09-26/status-manifest.json
network=$(python3 -c "import json,sys; print(bytes(json.load(open(sys.argv[1]))['network']).hex())" "$manifest")
salt=$(python3 -c "import json,sys; print(bytes(json.load(open(sys.argv[1]))['salt']).hex())" "$manifest")
for fill in random max; do
  for rounding in nearest dithered; do
    name="status-gen203-$fill-$rounding"
    "$tool" status --fill "$fill" --network-hex "$network" --salt-hex "$salt" \
      --query-rounding "$rounding" >"$out/$name.report.json"
    certify "$name"
  done
done

python3 "$out/summarize.py" "$out" >"$out/results.json"
(cd "$out" && shasum -a 256 manifest.json run.sh summarize.py results.json ./*.report.json ./*.certificate.json >SHA256SUMS)
