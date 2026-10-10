# Stage the 9d2cbda0 worker binary on every history worker and run --verify-only; no restart.
. /root/deploy-9d2cbda0/env.sh
for id in transparent-pir-recent-01 transparent-pir-recent-02 transparent-pir-archive-03; do
  s=$(date +%s); flock -n -E 75 $LOCK timeout 900 python3 -B $DEP/install-worker.py $S $FLEET $id $R/binaries $OUT/$id stage 2>&1 | tee $DEP/stage-$id.log; rc=${PIPESTATUS[0]}
  echo "$id rc=$rc seconds=$(( $(date +%s) - s ))"; [ $rc = 0 ] || exit $rc
done
