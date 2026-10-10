# usage: worker-checks.sh <host> <id> <probe-log>
. /root/deploy-9d2cbda0/env.sh
h=$1; id=$2; probe=$3
curl -s http://$h:8093/v1/ready | python3 -c "import json,sys;r=json.load(sys.stdin);print('new_binary',r['binary_sha256']=='$NEWW', r['ready'], r['mode'], r['warm_runtimes'], r['target_runtimes'], r['runtime_cache'], 'prewarm_failed', r.get('prewarm_failed'), r['map_sha256'][:12])"
python3 $DEP/steps/membership.py
$W root@$h "sha256sum /usr/local/bin/shard-control /usr/local/bin/transparent-shard-server /proc/\$(systemctl show -p MainPID --value transparent-shard-server)/exe; systemctl show transparent-shard-server -p NRestarts -p MemoryCurrent -p ActiveEnterTimestamp; sha256sum $OUT/$id/*; grep MemAvailable /proc/meminfo"
[ -n "$probe" ] && awk '{print $2,$3,$4,$5,$6}' $probe | uniq -c
true
