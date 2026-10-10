# Pause the 5 QPS load, roll recent-01 then recent-02 (the other replica must serve), re-pin, restart the load.
set -u
. /root/deploy-9d2cbda0/env.sh
cp -p $LOAD/pins.json $DEP/pins.json.before
systemctl stop transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/load-stop-recents
test ! -e $LOAD/latched.json && echo stopped-clean
for id in transparent-pir-recent-01 transparent-pir-recent-02; do
  s=$(date +%s)
  flock -n -E 75 $LOCK python3 -B $DEP/roll-two-replica.py $S/roll-recent-replicas.py --fleet-config $FLEET --artifacts $R/binaries --out $OUT --workers $id --warm-seconds 900 --route-seconds 300 2>&1 | tee $DEP/roll-$id.log
  rc=${PIPESTATUS[0]}; echo "$id roll rc=$rc wall=$(( $(date +%s) - s ))s"; [ $rc = 0 ] || exit $rc
  bash $DEP/steps/health.sh
done
python3 $DEP/pin.py $NEWW transparent-pir-recent-01 transparent-pir-recent-02
systemctl start transparent-5qps-continuous; date -u +%FT%TZ | tee $DEP/load-start-recents; systemctl is-active transparent-5qps-continuous
