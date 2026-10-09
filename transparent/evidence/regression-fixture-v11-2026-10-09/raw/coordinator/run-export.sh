# usage: run-export.sh <source sha>; read-only journal replay into a new fixture file.
sha=$1
cd /root/claude-regression-v11
cat /srv/transparent-activity/full-v3/journal/meta.json > journal-at-start.txt
od -An -tu8 -N16 /srv/transparent-activity/full-v3/journal/checkpoint.bin >> journal-at-start.txt
echo "start $(date -u +%FT%TZ)" > export.time
/usr/bin/time -v /srv/claude-regression-export-$sha/target/release-fast/regression-export \
  --data-dir /srv/transparent-activity/full-v3/journal \
  --map /root/claude-regression-v11/shards.json \
  --cases /root/claude-regression-v11/mainnet-cases.json \
  --cutoff-height 3289805 --source-sha $sha \
  --out /root/claude-regression-v11/mainnet-v11.json > export.stdout 2> export.stderr
echo "exit $? end $(date -u +%FT%TZ)" >> export.time
