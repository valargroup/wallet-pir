# Validation 1-3 and 8: identity, ready/warm, maps, memory/disk.
. /root/deploy-a317455e/env.sh
echo "== 1. worker identity"
for h in $R01 $R02 $A03; do
  $W root@$h "echo \$(hostname); sha256sum /usr/local/bin/transparent-shard-server /usr/local/bin/shard-control /proc/\$(systemctl show -p MainPID --value transparent-shard-server)/exe; systemctl show transparent-shard-server -p NRestarts -p MemoryCurrent -p MemoryHigh -p MemoryMax; grep -E '^(MemTotal|MemAvailable)' /proc/meminfo; df -B1 / | tail -1"
  $W root@$h "sed -n 's/^ExecStart=//p' /etc/systemd/system/transparent-shard-server.service | tr ' ' '\n' | sort" > /tmp/v-flags-now-$h
  $W root@$h "sed -n 's/^ExecStart=//p' $OUT/\$(hostname)/worker.service | tr ' ' '\n' | sort" > /tmp/v-flags-before-$h
  cmp -s /tmp/v-flags-now-$h /tmp/v-flags-before-$h && echo "$h unit flags equal baseline modulo order" || { echo "$h FLAGS DIFFER"; diff /tmp/v-flags-before-$h /tmp/v-flags-now-$h; }
  rm -f /tmp/v-flags-now-$h /tmp/v-flags-before-$h
done
echo "NEWW=$NEWW NEWS=$NEWS"
sha256sum /proc/$(systemctl show -p MainPID --value transparent-publish-controller)/exe; echo "NEWC=$NEWC"
echo "== 2. ready and warm"
for h in $R01 $R02 $A03; do curl -s http://$h:8093/v1/ready > $DEP/ready-after-$h.json; python3 -c "import json;r=json.load(open('$DEP/ready-after-$h.json'));print('$h',r['binary_sha256']=='$NEWW',r['ready'],r['mode'],r['warm_runtimes'],r['target_runtimes'],'prewarm_failed',r.get('prewarm_failed'),'write_failures',r['runtime_cache']['write_failures'],'map',r['map_sha256'][:12])"; done
curl -s 127.0.0.1:8094/v1/status | python3 -c "import json,sys;s=json.load(sys.stdin);print('controller',s['phase'],'map',s['map_sha256'][:12],s['public_height'],s['node_height'],s['freshness_seconds'])"
python3 $DEP/steps/membership.py
echo "== 3. maps"
curl -s https://transparent-pir.valargroup.dev/v1/shards > $DEP/map-public-after.json
curl -s https://enhance-pir.valargroup.dev/v1/filters/shards > $DEP/map-filters-after.json
cmp $DEP/map-public-after.json $DEP/map-filters-after.json && echo origins-agree; sha256sum $DEP/map-public-after.json
python3 $DEP/sealed-compare.py $DEP/baseline/map-public.json $DEP/map-public-after.json
A=/srv/transparent-activity/full-v11/publications/active.json
D=$(python3 -c "import json;print(json.load(open('$A'))['directory'])"); M=$(python3 -c "import json;print(json.load(open('$A'))['map_sha256'])")
echo "active $M"
for id in transparent-pir-recent-01 transparent-pir-recent-02 transparent-pir-archive-03; do
  $R/binaries/shard-assign files --shard-dir $D --assignment /opt/transparent-publisher/v11/state/$M.assignment.json --worker-id $id > /tmp/v-sa-$id.txt; echo "$id shard-assign rc=$? lines=$(wc -l < /tmp/v-sa-$id.txt)"; rm -f /tmp/v-sa-$id.txt
done
echo "== setup compare"
python3 $DEP/setup-compare.py $R01 $R02 || { echo retry-once; python3 $DEP/setup-compare.py $R01 $R02; }
echo "== 8. coordinator headroom"
df -h / /srv/zakura /srv/transparent-activity; free -g | sed -n 2p
echo "== scaler"
python3 -c "import json;s=json.load(open('/opt/transparent-publisher/v11/scaler/status.json'));print(s.get('mode'),s.get('decision'))"
test -e /opt/transparent-publisher/scaler/disabled && echo actuator-still-disabled
