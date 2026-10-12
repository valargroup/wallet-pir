#!/bin/bash
# Plan the initial v12 assignment and stage it on the v12 host (both workers).
set -euo pipefail
V=/opt/transparent-publisher/v12; D=$V/deploy-20261012; B=$V/releases/fb90519b/bin
P=/srv/transparent-activity/full-v12/publications/initial
M=1261e06fb7aee008b41bebc3fe53c7007f1ff5d84bb4a7cd56065e7f43fa611f
A=$V/state/$M.assignment.json
K="ssh -i /opt/transparent-publisher/credentials/deploy-ssh -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -o BatchMode=yes"
REMOTE=/srv/transparent-pir/v12/publications/$M
cp $V/roster.json $V/state/$M.roster.json
$B/shard-assign plan --shard-dir $P --roster $V/state/$M.roster.json --recent-from-height 3076108 --headroom 0.05 --out-assignment $A --source-sha fb90519b
$B/shard-assign check --help >/dev/null 2>&1 || true
for w in transparent-pir-v12-archive transparent-pir-v12-recent; do
  $B/shard-assign files --shard-dir $P --assignment $A --worker-id $w > $D/files-$w.txt
  echo "$w files $(wc -l < $D/files-$w.txt)"
done
sort -u $D/files-*.txt > $D/files-all.txt
$K root@10.142.0.9 "mkdir -p $REMOTE"
nice -n 10 rsync -a --ignore-existing --files-from $D/files-all.txt -e "$K" $P/ root@10.142.0.9:$REMOTE/
$K root@10.142.0.9 "cat > $REMOTE/assignment.json.next && mv $REMOTE/assignment.json.next $REMOTE/assignment.json" < $A
$K root@10.142.0.9 "du -sh $REMOTE; ls $REMOTE | wc -l"
echo S2_DONE
