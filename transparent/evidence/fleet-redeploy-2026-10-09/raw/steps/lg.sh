#!/bin/bash
# usage: lg.sh <name> <bash script string>; logs command + output + exit code under $DEP/logs
. /root/deploy-a317455e/env.sh
name=$1; shift
n=$(ls $DEP/logs | wc -l); f=$(printf "%s/logs/%03d-%s.log" $DEP $n "$name")
{ echo "### start $(date -u +%FT%T.%3NZ)"; echo "### cmd:"; printf "%s\n" "$*"; echo "### output:"; } > "$f"
set -o pipefail
bash -c ". /root/deploy-a317455e/env.sh; $*" 2>&1 | tee -a "$f"; rc=${PIPESTATUS[0]}
echo "### end $(date -u +%FT%T.%3NZ) rc=$rc" | tee -a "$f"
exit $rc
