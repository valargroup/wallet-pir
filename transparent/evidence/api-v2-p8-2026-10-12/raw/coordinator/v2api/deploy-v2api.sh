#!/bin/bash
# Move the v12 fleet to wallet API v2 paths (/v2/shards, /v2/filters/shards,
# /v2/txid): new worker binaries, shard-assign, publish controller and fleet
# adapter, all from one source commit. v11 is not touched here.
#
#   deploy-v2api.sh <source-sha> build|apply|rollback
set -euo pipefail
S=$1; MODE=$2
V=/opt/transparent-publisher/v12; R=$V/releases/$S; D=$V/deploy-v2api-$S
SRC=/srv/wallet-pir-src-$S; TARGET=/srv/wallet-pir-build-p8/target
OPS=/srv/transparent-activity/ops/sources/v12-$S
K="ssh -i /opt/transparent-publisher/credentials/deploy-ssh -o UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts -o BatchMode=yes root@10.142.0.9"
W=/opt/transparent-pir/v12
CTL=/etc/systemd/system/transparent-v12-publish-controller.service.d
mkdir -p "$D"

case "$MODE" in
build)
  test -f "$SRC/Cargo.toml"
  cd "$SRC"
  CARGO_TARGET_DIR=$TARGET RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq' \
    CXXFLAGS=-mpclmul CFLAGS=-mpclmul nice -n 15 ionice -c3 /root/.cargo/bin/cargo build --locked --release -j 4 \
    -p transparent-filter-server -p transparent-shard-server --bins
  echo BUILD_DONE
  ;;
apply)
  exec 9>/run/lock/wallet-pir-production.lock
  flock -w 60 9 || { echo LOCK_BUSY; exit 1; }
  echo "lock held $(date -u +%FT%TZ)"
  install -d "$R/bin"
  for b in transparent-shard-server shard-control shard-assign transparent-publish-controller; do
    install -m 0755 "$TARGET/release/$b" "$R/bin/$b"
  done
  (cd "$R/bin" && sha256sum *) > "$R/SHA256SUMS"; cat "$R/SHA256SUMS"
  # The fleet adapter from the same commit.
  install -d "$OPS/transparent"
  rm -rf "$OPS/transparent/ops"; cp -a "$SRC/transparent/ops" "$OPS/transparent/ops"
  # Backups for rollback, taken once.
  [ -f "$D/fleet.before.json" ] || cp $V/fleet.json "$D/fleet.before.json"
  [ -f "$D/controller.before.json" ] || cp $V/controller.json "$D/controller.before.json"
  [ -f "$D/release.conf.before" ] || cp $CTL/release.conf "$D/release.conf.before"
  $K "set -e; mkdir -p $W/bin-before-$S
      [ -f $W/bin-before-$S/transparent-shard-server ] || cp -p $W/bin/transparent-shard-server $W/bin/shard-control $W/bin-before-$S/
      [ -f $W/txid.caddy.before-$S ] || cp -p /etc/caddy/v12-routes/txid.caddy $W/txid.caddy.before-$S"
  for b in transparent-shard-server shard-control; do
    $K "cat > $W/bin/$b.next && chmod 755 $W/bin/$b.next" < "$R/bin/$b"
  done
  $K "cd $W/bin && sha256sum transparent-shard-server.next shard-control.next"
  # v2 clients read the one-query display at /v2/txid; nothing forwards to v1.
  $K "cat > /etc/caddy/v12-routes/txid.caddy" <<'EOF'
# Wallet API v2: the one-query txid display site.
handle /v2/txid/* {
	reverse_proxy 10.142.0.18:8095
}
EOF
  python3 - "$R" "$OPS" <<'EOF'
import json, sys
release, ops = sys.argv[1:]
path = "/opt/transparent-publisher/v12/fleet.json"
fleet = json.load(open(path))
fleet["api_version"] = "v2"
fleet["assign_binary"] = release + "/bin/shard-assign"
open(path + ".next", "w").write(json.dumps(fleet, indent=1))
path = "/opt/transparent-publisher/v12/controller.json"
controller = json.load(open(path))
controller["fleet_command"] = ops + "/transparent/ops/scripts/transparent-live-fleet.py"
open(path + ".next", "w").write(json.dumps(controller, indent=2))
EOF
  mv $V/fleet.json.next $V/fleet.json
  mv $V/controller.json.next $V/controller.json
  printf '[Service]\nExecStart=\nExecStart=%s/bin/transparent-publish-controller --config /opt/transparent-publisher/v12/controller.json\n' "$R" > $CTL/release.conf
  $K "set -e; cd $W/bin; mv transparent-shard-server.next transparent-shard-server; mv shard-control.next shard-control
      systemctl restart transparent-v12-archive.service transparent-v12-recent.service
      sleep 2; systemctl is-active transparent-v12-archive.service transparent-v12-recent.service"
  systemctl daemon-reload
  systemctl restart transparent-v12-publish-controller
  sleep 3; systemctl is-active transparent-v12-publish-controller
  echo "applied $(date -u +%FT%TZ)"
  ;;
rollback)
  exec 9>/run/lock/wallet-pir-production.lock
  flock -w 60 9 || { echo LOCK_BUSY; exit 1; }
  $K "set -e; cp -p $W/bin-before-$S/transparent-shard-server $W/bin-before-$S/shard-control $W/bin/
      cp -p $W/txid.caddy.before-$S /etc/caddy/v12-routes/txid.caddy
      systemctl restart transparent-v12-archive.service transparent-v12-recent.service"
  cp "$D/fleet.before.json" $V/fleet.json
  cp "$D/controller.before.json" $V/controller.json
  cp "$D/release.conf.before" $CTL/release.conf
  systemctl daemon-reload
  systemctl restart transparent-v12-publish-controller
  echo "rolled back $(date -u +%FT%TZ)"
  ;;
esac
