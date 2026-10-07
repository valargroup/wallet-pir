#!/bin/zsh
# fetch.sh NAME: copy a window's raw output from the bench and the controller timeline tail (read-only).
D=/Users/roman/.config/wallet-pir-deploy/txid-display/measure-20qps-20261007; n=$1
mkdir -p $D/runs $D/timeline
scp -q -r -o BatchMode=yes roman-ipir-bench-8vcpu:/root/txid-measure-d191f86b/runs/$n $D/runs/
$D/tools/sshp.sh coordinator 'tail -n 4000 /srv/zakura/txid-display-poc/root/timeline.jsonl' > $D/timeline/$n.jsonl
