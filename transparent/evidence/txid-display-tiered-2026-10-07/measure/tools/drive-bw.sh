#!/bin/zsh
D=/Users/roman/.config/wallet-pir-deploy/txid-display/measure-20qps-20261007
t=$(date -j -u -f '%Y-%m-%dT%H:%M:%S' $1 +%s)
while [ $(date -u +%s) -lt $t ]; do sleep 2; done
if ls $D/TRIPPED >/dev/null 2>&1; then echo "REFUSED: TRIPPED present"; exit 3; fi
echo "{\"unix\":$(date +%s),\"event\":\"window_start\",\"name\":\"BW-bandwidth\"}" >> $D/events.jsonl
ssh -o BatchMode=yes -o ServerAliveInterval=30 roman-ipir-bench-8vcpu '/root/txid-measure-d191f86b/run-bandwidth.sh; cat /root/txid-measure-d191f86b/runs/BW-bandwidth/exits.txt; tail -5 /root/txid-measure-d191f86b/runs/BW-bandwidth/bandwidth.log'
echo "{\"unix\":$(date +%s),\"event\":\"window_end\",\"name\":\"BW-bandwidth\"}" >> $D/events.jsonl
$D/tools/fetch.sh BW-bandwidth
echo DRIVE_DONE
