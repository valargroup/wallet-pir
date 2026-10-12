#!/bin/bash
# Export the v12 regression fixture from the live v3 journal (read-only) against the initial v12 map.
set -uo pipefail
D=/opt/transparent-publisher/v12/deploy-20261012
J=/srv/transparent-activity/full-v3/journal
{ cat $J/meta.json; od -An -tu8 -N16 $J/checkpoint.bin; } > $D/s3-journal-at-start.txt
cp /srv/transparent-activity/full-v12/publications/initial/shards.json $D/s3-map.json
nice -n 19 ionice -c3 /usr/bin/time -v -o $D/s3.time /srv/wallet-pir-build-fb90519b/target/release-fast/regression-export \
  --data-dir $J --map $D/s3-map.json --cases $D/mainnet-cases-v12.json \
  --cutoff-height 3076108 --source-sha fb90519b --out $D/mainnet-v12.json > $D/s3.stdout 2> $D/s3.stderr
echo "S3_EXIT $?"
