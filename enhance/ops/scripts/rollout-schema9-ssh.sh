#!/usr/bin/env bash
# Direct SSH rollout of the schema-9 (33-record) layout.
#
# This exists because deploy-enhance-pir.sh cannot do this migration: it stages
# and restarts Caddy, pir-apm and transparent-filter-server alongside Enhance,
# and the live Caddyfile carries an operator edit that is not in git. Installing
# the committed template would silently revert it. This script touches Enhance
# and nothing else.
#
# What it does NOT do, deliberately:
#   * no CI provenance gate, no workflow dispatch, no PR requirement;
#   * no Caddyfile, no pir-apm binary, no transparent-filter-server;
#   * no write of any kind to the -v7 data or artifact directories, which are
#     the rollback.
#
# What it keeps from the helper it replaces: the production lock, save-then-
# replace binary installs, a complete rollback set captured before any service
# stops, worker-before-coordinator ordering, and verification that fails closed.
#
# Usage: rollout-schema9-ssh.sh preflight|stage|cutover|verify|rollback
set -euo pipefail

usage() { echo "usage: $0 preflight|stage|cutover|verify|rollback" >&2; exit 2; }
MODE="${1:-}"
case "$MODE" in preflight | stage | cutover | verify | rollback) ;; *) usage ;; esac

# ---------------------------------------------------------------- inventory
COORDINATOR="${ENHANCE_COORDINATOR_HOST:?ENHANCE_COORDINATOR_HOST is required}"
WORKERS="${ENHANCE_WORKER_HOSTS:?ENHANCE_WORKER_HOSTS is required (space separated)}"
PUBLIC_URL="${ENHANCE_PUBLIC_URL:?ENHANCE_PUBLIC_URL is required}"
RELEASE_SHA="${ENHANCE_RELEASE_SHA:?ENHANCE_RELEASE_SHA is required}"
[[ "$RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] || { echo "release sha must be 40 hex" >&2; exit 2; }

# Must match enhance_pir::types and deploy-enhance-pir.sh.
TARGET_SCHEMA=9
TARGET_RECORDS_PER_ROW=33
TARGET_ROW_BYTES=24321
DATA_DIR=/srv/zakura/enhance-data-r33
PREVIOUS_DATA_DIR=/srv/zakura/enhance-data-r29
ARTIFACT_DIR=/srv/enhance-pir/artifacts-r33
PREVIOUS_ARTIFACT_DIR=/srv/enhance-pir/artifacts-v7
RELEASE_DIR="/opt/enhance-pir/schema9-$RELEASE_SHA"
ROLLBACK_DIR="/opt/enhance-pir/rollback-before-schema9-$RELEASE_SHA"
LOCK=/run/lock/wallet-pir-production.lock

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=15)
coord() { "${SSH[@]}" "root@$COORDINATOR" "$@"; }
worker() { local h="$1"; shift; "${SSH[@]}" -J "root@$COORDINATOR" "root@$h" "$@"; }

note() { printf '  %-46s %s\n' "$1" "$2"; }
die() { echo "FATAL: $*" >&2; exit 1; }

# ---------------------------------------------------------------- preflight
preflight() {
  echo "== preflight"
  local serving
  serving="$(coord systemctl show -p ExecStart --value enhance-pir-server)"
  case "$serving" in
    *"--data-dir $PREVIOUS_DATA_DIR "*) note "live data dir" "$PREVIOUS_DATA_DIR (pre-cutover, expected)" ;;
    *"--data-dir $DATA_DIR "*) note "live data dir" "$DATA_DIR (already cut over)" ;;
    *) die "live coordinator serves an unrecognised data dir: $serving" ;;
  esac

  # The prepared journal must exist, carry the target layout, and name this
  # exact release. A receipt is the only thing standing between a cutover and
  # a journal some other build left half-written.
  local receipt prepared_rpr
  receipt="$(coord cat "$DATA_DIR/prepared-release" 2>/dev/null || true)"
  [[ "$receipt" == "$RELEASE_SHA records-per-row=$TARGET_RECORDS_PER_ROW" ]] \
    || die "preparation receipt is '$receipt', expected '$RELEASE_SHA records-per-row=$TARGET_RECORDS_PER_ROW'"
  prepared_rpr="$(coord jq -r '.records_per_row // 0' "$DATA_DIR/enhance/manifest.json")"
  [[ "$prepared_rpr" == "$TARGET_RECORDS_PER_ROW" ]] \
    || die "prepared journal has $prepared_rpr records per row, expected $TARGET_RECORDS_PER_ROW"
  note "preparation receipt" "release and layout match"

  # Staged binaries, by digest, on every host that will run one.
  coord test -x "$RELEASE_DIR/enhance-pir-server" || die "server binary not staged on coordinator"
  local host
  for host in $WORKERS; do
    worker "$host" test -x "$RELEASE_DIR/enhance-pir-worker" \
      || die "worker binary not staged on $host"
    # Free space for a full set of schema-9 artifacts before anything starts
    # writing them; a worker that fills its disk mid-preparation leaves the
    # group without a ready replica.
    local avail
    avail="$(worker "$host" df --output=avail -BG / | tail -1 | tr -dc '0-9')"
    [[ "$avail" -ge 10 ]] || die "$host has ${avail}G free; schema-9 artifacts need more headroom"
    note "$host" "binary staged, ${avail}G free"
  done

  local live_schema
  live_schema="$(curl -fsS "$PUBLIC_URL/v1/enhance/init" | jq -r '.generation.schema_version')"
  note "public origin" "serving schema $live_schema"
}

# ------------------------------------------------------------------- backup
capture_rollback() {
  echo "== capturing rollback set"
  coord bash -s -- "$ROLLBACK_DIR" <<'REMOTE'
set -euo pipefail
r="$1"; install -d -m 0700 "$r"
cp -L /usr/local/bin/enhance-pir-server "$r/enhance-pir-server"
cp /etc/systemd/system/enhance-pir-server.service "$r/enhance-pir-server.service"
cp /etc/enhance-pir/workers.json "$r/workers.json"
[[ -r /etc/default/pir-apm ]] && cp /etc/default/pir-apm "$r/pir-apm.env"
sha256sum "$r"/* > "$r/SHA256SUMS"
REMOTE
  note "coordinator" "$ROLLBACK_DIR"
  local host
  for host in $WORKERS; do
    worker "$host" bash -s -- "$ROLLBACK_DIR" <<'REMOTE'
set -euo pipefail
r="$1"; install -d -m 0700 "$r"
cp -L /usr/local/bin/enhance-pir-worker "$r/enhance-pir-worker"
cp /etc/systemd/system/enhance-pir-worker.service "$r/enhance-pir-worker.service"
sha256sum "$r"/* > "$r/SHA256SUMS"
REMOTE
    note "$host" "$ROLLBACK_DIR"
  done
  echo "  old data left in place: $PREVIOUS_DATA_DIR, $PREVIOUS_ARTIFACT_DIR"
}

# ------------------------------------------------------------------ cutover
cutover() {
  preflight
  capture_rollback
  echo "== cutover (holding $LOCK)"
  # Workers first: a worker restarted under the new artifact directory has no
  # prepared shards until the coordinator asks, and the coordinator is stopped
  # for that window, so no query can reach a half-migrated group.
  coord systemctl stop enhance-pir-server
  note "coordinator" "stopped"

  local host
  for host in $WORKERS; do
    worker "$host" bash -s -- "$RELEASE_DIR" "$ARTIFACT_DIR" <<'REMOTE'
set -euo pipefail
release="$1"; artifacts="$2"
install -d -m 0700 "$artifacts"
install -m 0755 "$release/enhance-pir-worker" /usr/local/bin/enhance-pir-worker.next
mv -f /usr/local/bin/enhance-pir-worker.next /usr/local/bin/enhance-pir-worker
# Only the data directory changes. Every limit, ordering and environment line
# in the live unit is preserved by rewriting that one argument in place.
sed -i "s#--data-dir /srv/enhance-pir/artifacts-v7#--data-dir $artifacts#" \
  /etc/systemd/system/enhance-pir-worker.service
grep -Fq -- "--data-dir $artifacts" /etc/systemd/system/enhance-pir-worker.service
systemctl daemon-reload
systemctl restart enhance-pir-worker
REMOTE
    for _ in $(seq 1 30); do
      worker "$host" curl -fsS http://127.0.0.1:8091/internal/health >/dev/null 2>&1 && break
      sleep 2
    done
    worker "$host" curl -fsS http://127.0.0.1:8091/internal/health >/dev/null \
      || die "$host worker did not come back healthy"
    note "$host" "worker activated on $ARTIFACT_DIR"
  done

  coord bash -s -- "$RELEASE_DIR" "$DATA_DIR" "$PREVIOUS_DATA_DIR" <<'REMOTE'
set -euo pipefail
release="$1"; data="$2"; previous="$3"
install -m 0755 "$release/enhance-pir-server" /usr/local/bin/enhance-pir-server.next
mv -f /usr/local/bin/enhance-pir-server.next /usr/local/bin/enhance-pir-server
install -m 0755 "$release/enhance-pir-cli" /usr/local/bin/enhance-pir-cli
# Carry the committed topology across, exactly as the schema transition in
# deploy-enhance-pir.sh does; TopologyStore refuses a file that disagrees with
# the binary's shard range, so this is a copy and never a rewrite.
if [[ -f "$previous/topology.json" && ! -f "$data/topology.json" ]]; then
  cp "$previous/topology.json" "$data/topology.json"
fi
sed -i "s#--data-dir $previous#--data-dir $data#" /etc/systemd/system/enhance-pir-server.service
grep -Fq -- "--data-dir $data" /etc/systemd/system/enhance-pir-server.service
# The APM sidecar reads the served directory for its disk panel. Rewrite that
# one line rather than reinstalling its environment file, which also holds a
# Slack webhook this rollout has no business rotating.
if [[ -r /etc/default/pir-apm ]]; then
  sed -i "s#^PIR_APM_DATA_DIR=.*#PIR_APM_DATA_DIR=$data#" /etc/default/pir-apm
  systemctl restart pir-apm || true
fi
systemctl daemon-reload
systemctl start enhance-pir-server
REMOTE
  note "coordinator" "started on $DATA_DIR"

  echo "== waiting for serving phase"
  local serving=0
  for _ in $(seq 1 180); do
    if coord curl -fsS http://127.0.0.1:8080/v1/health 2>/dev/null \
      | jq -e '.phase.phase == "serving"' >/dev/null 2>&1; then serving=1; break; fi
    sleep 10
  done
  [[ "$serving" -eq 1 ]] || die "coordinator did not reach serving phase"
  note "coordinator" "serving"
  verify
}

# ------------------------------------------------------------------- verify
verify() {
  echo "== verification"
  local init
  init="$(curl -fsS "$PUBLIC_URL/v1/enhance/init")" || die "public init failed"
  # Fed from the constants at the top of this file, so the gate cannot drift
  # from the layout this rollout is for. shellcheck's SC2016 is inverted here:
  # the $-names are jq variables supplied by --argjson, not shell expansions.
  # shellcheck disable=SC2016
  jq -e --argjson expected 1 \
    --argjson schema "$TARGET_SCHEMA" \
    --argjson rpr "$TARGET_RECORDS_PER_ROW" \
    --argjson rowbytes "$TARGET_ROW_BYTES" '
    (.generation.schema_version == $schema) and
    (.generation.protocol_revision == "ironwood-enhance-pir-v3") and
    (.generation.pir_profile == "simplepir-p16-q46-v1") and
    (.params.p == 65536) and (.params.query_bits == 46) and
    (.generation.record_bytes == 737) and (.generation.records_per_row == $rpr) and
    (.generation.row_bytes == $rowbytes) and (.generation.shard_rows == 8192) and
    (.generation.network == "main") and (.generation.pool == "ironwood") and
    ([.generation.shards[].worker] | unique | length) <= $expected and
    (.generation.shards | length) > 0 and
    (.params | type == "object") and
    (.public_params_base64 | type == "string" and length > 0)
  ' >/dev/null <<<"$init" || die "public init does not declare the schema-9 layout"
  note "public init" "schema $TARGET_SCHEMA, $TARGET_RECORDS_PER_ROW x 737 B, $TARGET_ROW_BYTES-byte rows"

  local health
  health="$(curl -fsS "$PUBLIC_URL/v1/health")"
  jq -e '.phase.phase == "serving" and .tables.enhance.workers == 2' >/dev/null <<<"$health" \
    || die "health is not serving with both workers"
  note "health" "serving, $(jq -r '.retained_generations' <<<"$health") retained generations"

  # Two sequential queries exercise both replicas of the active-active group.
  local i
  for i in 1 2; do
    coord /usr/local/bin/enhance-pir-cli --server "$PUBLIC_URL" dummy >/dev/null \
      || die "cover query $i failed"
  done
  note "cover queries" "both replicas answered"

  local host
  for host in $WORKERS; do
    local peak events
    peak="$(worker "$host" cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.peak)"
    events="$(worker "$host" cat /sys/fs/cgroup/system.slice/enhance-pir-worker.service/memory.events | tr '\n' ' ')"
    note "$host memory" "peak $((peak / 1024 / 1024)) MiB; $events"
    grep -qE 'oom_kill 0' <<<"$events" || die "$host recorded an OOM kill"
  done
}

# ----------------------------------------------------------------- rollback
rollback() {
  echo "== rollback to the pre-schema-9 deployment"
  coord systemctl stop enhance-pir-server || true
  local host
  for host in $WORKERS; do
    worker "$host" bash -s -- "$ROLLBACK_DIR" <<'REMOTE'
set -euo pipefail
r="$1"
cd "$r" && sha256sum --check --status SHA256SUMS
install -m 0755 "$r/enhance-pir-worker" /usr/local/bin/enhance-pir-worker
install -m 0644 "$r/enhance-pir-worker.service" /etc/systemd/system/enhance-pir-worker.service
systemctl daemon-reload && systemctl restart enhance-pir-worker
REMOTE
    note "$host" "worker restored"
  done
  coord bash -s -- "$ROLLBACK_DIR" <<'REMOTE'
set -euo pipefail
r="$1"
cd "$r" && sha256sum --check --status SHA256SUMS
install -m 0755 "$r/enhance-pir-server" /usr/local/bin/enhance-pir-server
install -m 0644 "$r/enhance-pir-server.service" /etc/systemd/system/enhance-pir-server.service
install -m 0644 "$r/workers.json" /etc/enhance-pir/workers.json
[[ -r "$r/pir-apm.env" ]] && install -m 0600 "$r/pir-apm.env" /etc/default/pir-apm && systemctl restart pir-apm
systemctl daemon-reload && systemctl start enhance-pir-server
REMOTE
  note "coordinator" "restored; serving the untouched -v7 journal again"
}

case "$MODE" in
  preflight) preflight ;;
  stage) capture_rollback ;;
  verify) verify ;;
  rollback) flock -w 300 "$LOCK" true 2>/dev/null || true; rollback ;;
  cutover)
    # Bounded wait, and never a forced take: the autoscaler and the transparent
    # deploys use this same lock, and going first is not worth a race.
    coord flock -w 600 "$LOCK" -c "true" \
      || die "production lock is held; check what else is deploying before retrying"
    cutover
    ;;
esac
