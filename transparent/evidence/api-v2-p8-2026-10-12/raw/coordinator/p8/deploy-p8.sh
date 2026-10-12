#!/bin/bash
# Serve the 8-bit history profile on v12 staging: new worker binaries and
# shard-assign, new runtime-cache directories, larger recent budgets, both
# workers restarted together. v11 and the v12 controller are not touched.
#
#   deploy-p8.sh <source-sha> plan|apply|rollback
set -euo pipefail
S=$1; MODE=$2
V=/opt/transparent-publisher/v12; R=$V/releases/$S; D=$V/deploy-p8-$S
BUILD=/srv/wallet-pir-build-p8/target/release
K="ssh -i /opt/transparent-publisher/credentials/deploy-ssh -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -o BatchMode=yes root@10.142.0.9"
W=/opt/transparent-pir/v12
mkdir -p "$D"
exec 9>/run/lock/wallet-pir-production.lock
flock -w 60 9 || { echo LOCK_BUSY; exit 1; }
echo "lock held $(date -u +%FT%TZ)"

case "$MODE" in
plan)
  mkdir -p "$R/bin"
  cp "$BUILD"/{shard-assign,shard-control,transparent-shard-server} "$R/bin/"
  (cd "$R/bin" && sha256sum shard-assign shard-control transparent-shard-server) > "$R/SHA256SUMS"
  cat "$R/SHA256SUMS"
  [ -f "$D/roster.before.json" ] || cp $V/roster.json "$D/roster.before.json"
  [ -f "$D/fleet.before.json" ] || cp $V/fleet.json "$D/fleet.before.json"
  python3 - "$D/roster.before.json" "$D/roster.json" <<'EOF'
import json, sys
roster = json.load(open(sys.argv[1]))
for w in roster:
    if w["id"] == "transparent-pir-v12-recent":
        w["cache_bytes"] = 8589934592
        w["memory_max"] = "10G"
json.dump(roster, open(sys.argv[2], "w"), indent=1)
EOF
  M=$(python3 -c 'import json; print(json.load(open("/opt/transparent-publisher/v12/state/active.json"))["map_sha256"])')
  P=$(python3 - "$M" <<'EOF'
import json, sys, glob
m = sys.argv[1]
for path in sorted(glob.glob("/opt/transparent-publisher/v12/state/*.request.json")):
    j = json.load(open(path))
    if j.get("map_sha256") == m:
        print(j.get("publication") or j.get("shard_dir") or j.get("directory") or "")
        break
EOF
)
  echo "active map $M publication ${P:-?}"
  if [ -n "$P" ] && [ -d "$P" ]; then
    "$R/bin/shard-assign" plan --shard-dir "$P" --roster "$D/roster.json" --recent-from-height 3076108 \
      --headroom 0.05 --out-assignment "$D/plan.assignment.json" --source-sha "$S"
    python3 -c 'import json,sys; a=json.load(open(sys.argv[1])); [print(w["id"], len(w["shards"]), w["estimated_resident_bytes"], w["cache_bytes"]) for w in a["workers"]]; print("unassigned", a["unassigned"])' "$D/plan.assignment.json"
  else
    echo "publication directory not found; request keys:"; python3 -c 'import json,sys,glob; print(sorted(json.load(open(sorted(glob.glob("/opt/transparent-publisher/v12/state/*.request.json"))[-1])).keys()))'
  fi
  $K "df -h /srv | tail -1; free -g | sed -n 2p"
  ;;
apply)
  test -f "$R/SHA256SUMS" && test -f "$D/roster.json"
  # Worker: keep the running binaries and units for rollback, then swap.
  $K "set -e; mkdir -p $W/bin-before-$S $W/units-before-$S
      [ -f $W/bin-before-$S/transparent-shard-server ] || cp -p $W/bin/transparent-shard-server $W/bin/shard-control $W/bin-before-$S/
      [ -f $W/units-before-$S/transparent-v12-recent.service ] || cp -p /etc/systemd/system/transparent-v12-archive.service /etc/systemd/system/transparent-v12-recent.service $W/units-before-$S/
      mkdir -p /srv/transparent-pir/v12/runtime-cache-archive-p8 /srv/transparent-pir/v12/runtime-cache-recent-p8"
  for b in transparent-shard-server shard-control; do
    $K "cat > $W/bin/$b.next && chmod 755 $W/bin/$b.next" < "$R/bin/$b"
  done
  $K "set -e; cd $W/bin; sha256sum transparent-shard-server.next shard-control.next"
  # Units: new cache directories; recent cache 8 GiB, disk 16 GiB, memory 9G/10G.
  $K "set -e
      sed -i 's#runtime-cache-archive #runtime-cache-archive-p8 #' /etc/systemd/system/transparent-v12-archive.service
      sed -i -e 's#runtime-cache-recent #runtime-cache-recent-p8 #' -e 's#--cache-bytes 6442450944#--cache-bytes 8589934592#' \
             -e 's#--runtime-cache-max-bytes 12884901888#--runtime-cache-max-bytes 17179869184#' \
             -e 's#^MemoryHigh=7G#MemoryHigh=9G#' -e 's#^MemoryMax=8G#MemoryMax=10G#' /etc/systemd/system/transparent-v12-recent.service
      grep -c runtime-cache-archive-p8 /etc/systemd/system/transparent-v12-archive.service
      grep -E -c 'runtime-cache-recent-p8 .*--runtime-cache-max-bytes 17179869184|--cache-bytes 8589934592' /etc/systemd/system/transparent-v12-recent.service
      grep -E '^Memory(High|Max)=' /etc/systemd/system/transparent-v12-recent.service"
  # Coordinator: the planner and roster the next publication uses.
  cp "$D/roster.json" $V/roster.json
  python3 - "$R/bin/shard-assign" <<'EOF'
import json, sys
path = "/opt/transparent-publisher/v12/fleet.json"
fleet = json.load(open(path))
fleet["assign_binary"] = sys.argv[1]
open(path + ".next", "w").write(json.dumps(fleet, indent=1))
EOF
  mv $V/fleet.json.next $V/fleet.json
  $K "set -e; cd $W/bin; mv transparent-shard-server.next transparent-shard-server; mv shard-control.next shard-control
      systemctl daemon-reload
      systemctl restart transparent-v12-archive.service transparent-v12-recent.service
      sleep 2; systemctl is-active transparent-v12-archive.service transparent-v12-recent.service"
  echo "restarted $(date -u +%FT%TZ)"
  ;;
rollback)
  $K "set -e; cp -p $W/bin-before-$S/transparent-shard-server $W/bin-before-$S/shard-control $W/bin/
      cp -p $W/units-before-$S/*.service /etc/systemd/system/
      systemctl daemon-reload
      systemctl restart transparent-v12-archive.service transparent-v12-recent.service"
  cp "$D/roster.before.json" $V/roster.json
  cp "$D/fleet.before.json" $V/fleet.json
  echo "rolled back $(date -u +%FT%TZ)"
  ;;
esac
