#!/bin/zsh
# drive.sh START_UTC NAME UNIT TOTAL_RATE PROCS SECONDS [txid-rate args...]
D=/Users/roman/.config/wallet-pir-deploy/txid-display/measure-20qps-20261007
start=$1 n=$2; shift 2
t=$(date -j -u -f '%Y-%m-%dT%H:%M:%S' $start +%s)
while [ $(date -u +%s) -lt $t ]; do sleep 2; done
if ls $D/TRIPPED >/dev/null 2>&1; then echo "REFUSED: TRIPPED present"; exit 3; fi
echo "{\"unix\":$(date +%s),\"event\":\"window_start\",\"name\":\"$n\",\"args\":\"$*\"}" >> $D/events.jsonl
ssh -o BatchMode=yes -o ServerAliveInterval=30 roman-ipir-bench-8vcpu "/root/txid-measure-d191f86b/run-window.sh $n $* > /root/txid-measure-d191f86b/$n.out 2>&1; cat /root/txid-measure-d191f86b/runs/$n/exits.txt"
echo "{\"unix\":$(date +%s),\"event\":\"window_end\",\"name\":\"$n\"}" >> $D/events.jsonl
$D/tools/fetch.sh $n
ls $D/TRIPPED >/dev/null 2>&1 && echo "TRIPPED DURING OR AFTER WINDOW"
echo DRIVE_DONE $n
