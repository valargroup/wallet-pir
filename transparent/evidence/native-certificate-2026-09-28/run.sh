#!/usr/bin/env bash
# Regenerates every report and certificate in this directory.
# Usage: transparent/evidence/native-certificate-2026-09-28/run.sh <certify_native.py>
set -euo pipefail
cd "$(dirname "$0")/../../.."
certify=${1:?path to reinspiring/tools/security/certify_native.py (ipir-sp 1f2aec6)}
out=transparent/evidence/native-certificate-2026-09-28
cargo build --release -p transparent-shard-server --example native_certificate
for fill in random max; do
  for rows in 2048 4096 8192 32768 65536; do
    name="$out/synthetic-$fill-$rows"
    ./target/release/examples/native_certificate synthetic --rows "$rows" --fill "$fill" \
      >"$name.report.json"
    # certify_native.py exits non-zero below 128 bits; keep its verdict either way.
    python3 "$certify" "$name.report.json" >"$name.certificate.json" || true
  done
done

# Segment mode with the query term measured from the table, on a reproducible
# pseudo-random archive-wide page segment (not a published snapshot; the
# 256 MiB input is regenerated here rather than retained).
segment=$(mktemp)
trap 'rm -f "$segment"' EXIT
python3 -c "import random,sys; sys.stdout.buffer.write(random.Random(1).randbytes(65536*4096))" \
  >"$segment"
name="$out/segment-measured-archive-wide-pages-seed1"
./target/release/examples/native_certificate segment --geometry archive-wide --table pages \
  --rows-bin "$segment" >"$name.report.json"
python3 "$certify" "$name.report.json" >"$name.certificate.json" || true
