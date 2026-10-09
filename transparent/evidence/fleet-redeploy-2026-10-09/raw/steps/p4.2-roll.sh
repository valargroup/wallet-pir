. /root/deploy-a317455e/env.sh
s=$(date +%s)
flock -n -E 75 $LOCK python3 -B $DEP/roll-two-replica.py $S/roll-recent-replicas.py --fleet-config $FLEET --artifacts $R/binaries --out $OUT --workers transparent-pir-recent-02 --warm-seconds 900 --route-seconds 300 2>&1 | tee $DEP/roll-recent-02.log
rc=${PIPESTATUS[0]}; echo "rc=$rc wall=$(( $(date +%s) - s ))s"; exit $rc
