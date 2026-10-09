# usage: run.sh <label> [extra args]
label=$1; shift
cd /root/claude-cutoff-569f68e6
echo "start $(date -u +%FT%TZ)" > $label.time
/usr/bin/time -v /srv/wallet-pir-build-569f68e6/target/release-fast/shard-cutoff \
  --data-dir /srv/transparent-activity/full-v3/journal "$@" --months 6 \
  --archive-geometry archive-wide --zakura-cookie /root/.cache/zakura/.cookie \
  --source-sha 569f68e6 --out /root/claude-cutoff-569f68e6/cutoff-$label.json > $label.stdout 2> $label.stderr
echo "exit $? end $(date -u +%FT%TZ)" >> $label.time
