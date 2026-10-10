# Swap the publish controller under the lock (stops the 5 QPS load), wait until both origins serve at the tip, restart the load.
. /root/deploy-9d2cbda0/env.sh
flock -n -E 75 $LOCK bash $DEP/steps/p6-swap-inner.sh; rc=$?
echo "swap rc=$rc"; [ $rc = 0 ] || { echo "swap failed; load left stopped"; exit $rc; }
bash $DEP/steps/p6-watch.sh | tail -4
sha256sum /proc/$(systemctl show -p MainPID --value transparent-publish-controller)/exe | cut -c1-16
jq -r .source_sha /opt/transparent-publisher/v11/controller.json
curl -s https://transparent-pir.valargroup.dev/v1/shards > $DEP/map-after-controller.json
python3 $DEP/sealed-compare.py $DEP/map-before-controller.json $DEP/map-after-controller.json
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/load-start-controller; systemctl is-active transparent-5qps-continuous
