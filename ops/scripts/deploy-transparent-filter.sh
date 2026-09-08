#!/usr/bin/env bash
# Deploys the transparent filter service on its own.
#
# The filter service shipped inside deploy-enhance-pir.sh, so a filter change
# meant an Enhance rollout, and a filter binary that could not read the set its
# unit named failed that coordinated rollout. This script touches the filter
# service and nothing else: not the Enhance coordinator's units, not its
# Caddyfile, not any worker. It saves its rollback copies under the same
# /opt/enhance-pir/rollback paths the Enhance script uses, so either script's
# rollback restores the same artefact.
#
# The staged binary is asked to open the shard directory the unit will name
# before the running service is stopped, which is the check the coordinated
# rollout lacked: a schema the binary cannot read fails here, with the old
# service still answering.
set -euo pipefail

MODE="${1:-deploy}"

# ------------------------------------------------------------- jq programs
#
# Hoisted for ops/scripts/check-jq-contracts.sh, which compiles each one.
# JQ_FILTER_CHECK_ reads the staged binary's --check-shard-dir output;
# JQ_FILTER_HEALTH_ reads /v1/health on loopback.
readonly JQ_FILTER_CHECK_MAP='.map_sha256'
readonly JQ_FILTER_CHECK_SUMMARY='"\(.shards) shards, \(.start_height)-\(.covered_through), schema \(.schema), map \(.map_sha256)"'
readonly JQ_FILTER_HEALTH_SERVING='.phase == "serving" or .phase == "syncing" or .phase == "starting"'

jq_programs() {
  local name
  for name in JQ_FILTER_CHECK_MAP JQ_FILTER_CHECK_SUMMARY JQ_FILTER_HEALTH_SERVING; do
    printf '%s\t%s\0' "$name" "${!name}"
  done
}

fail() {
  echo "error: $*" >&2
  exit 1
}

require_env() {
  local name
  for name in "$@"; do
    [[ -n "${!name:-}" ]] || fail "$name is required"
  done
}

# ---------------------------------------------------------------- validation

validate_inputs() {
  require_env TRANSPARENT_FILTER_HOST TRANSPARENT_DEPLOY_USER TRANSPARENT_RELEASE_SHA \
    TRANSPARENT_ARTIFACT_DIR TRANSPARENT_FILTER_SHARD_DIR ENHANCE_PUBLIC_URL
  [[ "$TRANSPARENT_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] \
    || fail "TRANSPARENT_RELEASE_SHA must be a full commit SHA"
  [[ "$TRANSPARENT_FILTER_HOST" =~ ^[A-Za-z0-9._-]+$ ]] \
    || fail "TRANSPARENT_FILTER_HOST is not a plain host or address"
  [[ "$TRANSPARENT_DEPLOY_USER" =~ ^[A-Za-z0-9._-]+$ ]] \
    || fail "TRANSPARENT_DEPLOY_USER is not a plain user name"
  [[ "$TRANSPARENT_FILTER_SHARD_DIR" = /* ]] \
    || fail "TRANSPARENT_FILTER_SHARD_DIR must be absolute"
  [[ "$ENHANCE_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+$ ]] \
    || fail "ENHANCE_PUBLIC_URL must be https:// and a bare host"
  if [[ -n "${TRANSPARENT_PUBLIC_URL:-}" ]]; then
    [[ "$TRANSPARENT_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+$ ]] \
      || fail "TRANSPARENT_PUBLIC_URL must be https:// and a bare host"
  fi
  case "${TRANSPARENT_SSH_KEY_PATH:-}${TRANSPARENT_KNOWN_HOSTS_PATH:-}" in
    *[[:space:]]*) fail "SSH key and known_hosts paths must not contain spaces" ;;
  esac
}

# The unit names the set the service reads. Rendered from the committed unit
# by replacing its --shard-dir argument, so the deploy's input decides the set
# rather than whatever the committed file last said.
RENDERED_UNIT=""
render_unit() {
  local source="$TRANSPARENT_ARTIFACT_DIR/transparent-filter-server.service"
  [[ -r "$source" ]] || fail "no unit at $source"
  local unit="$TRANSPARENT_ARTIFACT_DIR/transparent-filter-server.service.rendered"
  sed -E "s|--shard-dir [^ ]+|--shard-dir $TRANSPARENT_FILTER_SHARD_DIR|" "$source" >"$unit"
  local execstarts
  execstarts="$(grep -c '^ExecStart=' "$unit" || true)"
  [[ "$execstarts" -eq 1 ]] || fail "rendered unit has $execstarts ExecStart lines, expected exactly 1"
  grep -q -- "--shard-dir $TRANSPARENT_FILTER_SHARD_DIR" "$unit" \
    || fail "unit does not name the shard set it is being deployed with"
  grep -qE '\\$' "$unit" && fail "unit uses line continuations; keep ExecStart on one line"
  [[ "$(sed -n 's/^ExecStart=//p' "$unit")" == /* ]] || fail "unit ExecStart is not an absolute path"
  RENDERED_UNIT="$unit"
}

ssh_opts() {
  printf '%s\n' -i "$TRANSPARENT_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$TRANSPARENT_KNOWN_HOSTS_PATH" \
    -o ConnectTimeout=10
}

host_ssh() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  # shellcheck disable=SC2029
  ssh "${opts[@]}" "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_FILTER_HOST" "$@"
}

# ------------------------------------------------------------------ preflight

preflight() {
  echo "== preflight $TRANSPARENT_FILTER_HOST"
  host_ssh bash -s -- "$TRANSPARENT_FILTER_SHARD_DIR" <<'REMOTE'
set -euo pipefail
[[ "$(uname -m)" == "x86_64" ]] || { echo "host is not x86_64" >&2; exit 1; }
for tool in curl jq sha256sum systemctl; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 1; }
done
sudo -n true || { echo "passwordless sudo is required" >&2; exit 1; }
sudo test -f "$1/shards.json" || { echo "$1 holds no shards.json" >&2; exit 1; }
systemctl is-active zakurad >/dev/null || { echo "zakurad is not active" >&2; exit 1; }
REMOTE
}

stage() {
  local staged="/tmp/transparent-filter-$TRANSPARENT_RELEASE_SHA"
  local -a opts
  mapfile -t opts < <(ssh_opts)
  render_unit
  echo "== stage binary and unit"
  host_ssh "mkdir -p $(printf %q "$staged")"
  scp "${opts[@]}" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-filter-server" \
    "$TRANSPARENT_ARTIFACT_DIR/SHA256SUMS" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_FILTER_HOST:$staged/"
  scp "${opts[@]}" "$RENDERED_UNIT" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_FILTER_HOST:$staged/unit.rendered"
  host_ssh bash -s -- "$staged" <<'REMOTE'
set -euo pipefail
cd "$1"
sha256sum -c SHA256SUMS --ignore-missing
chmod 0755 transparent-filter-server
REMOTE
}

# The staged binary opens the set the unit names. This is the whole reason
# the filter service has a deploy of its own.
verify_prepared() {
  echo "== verify prepared: the staged binary reads $TRANSPARENT_FILTER_SHARD_DIR"
  local check
  check="$(host_ssh bash -s -- "/tmp/transparent-filter-$TRANSPARENT_RELEASE_SHA" "$TRANSPARENT_FILTER_SHARD_DIR" <<'REMOTE'
set -euo pipefail
sudo "$1/transparent-filter-server" --shard-dir "$2" --check-shard-dir
REMOTE
)"
  echo "$check" | jq -r "$JQ_FILTER_CHECK_SUMMARY"
  STAGED_MAP_SHA256="$(echo "$check" | jq -er "$JQ_FILTER_CHECK_MAP")"
  if [[ -n "${TRANSPARENT_SHARD_SOURCE:-}" ]]; then
    # The set the retrieval fleet serves must be the set the filter origin
    # serves; the map digest is the identity both report.
    local fleet_map
    fleet_map="$(jq -c . "$TRANSPARENT_SHARD_SOURCE/shards.json" | tr -d '\n' | sha256sum | cut -d' ' -f1)"
    [[ "$fleet_map" == "$STAGED_MAP_SHA256" ]] \
      || fail "the filter set digests to $STAGED_MAP_SHA256, the fleet's set to $fleet_map"
  fi
}
STAGED_MAP_SHA256=""

# ------------------------------------------------------------------- activate

activate() {
  echo "== activate"
  host_ssh bash -s -- "/tmp/transparent-filter-$TRANSPARENT_RELEASE_SHA" "$TRANSPARENT_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
staged="$1"; sha="$2"
release="/opt/enhance-pir/releases/$sha"
rollback="/opt/enhance-pir/rollback"
sudo mkdir -p "$release" "$rollback"
# The same rollback paths the Enhance deploy uses, so either script's rollback
# restores the same artefact.
[[ -x /usr/local/bin/transparent-filter-server ]] && sudo cp -L /usr/local/bin/transparent-filter-server "$rollback/transparent-filter-server"
[[ -r /etc/systemd/system/transparent-filter-server.service ]] && sudo cp /etc/systemd/system/transparent-filter-server.service "$rollback/transparent-filter-server.service"
sudo install -m 0755 "$staged/transparent-filter-server" "$release/transparent-filter-server"
sudo install -m 0755 "$release/transparent-filter-server" /usr/local/bin/transparent-filter-server.next
sudo mv -f /usr/local/bin/transparent-filter-server.next /usr/local/bin/transparent-filter-server
sudo install -m 0644 "$staged/unit.rendered" /etc/systemd/system/transparent-filter-server.service
sudo systemctl daemon-reload
sudo systemctl enable transparent-filter-server
sudo systemctl restart transparent-filter-server
# Health, deliberately not ready: a fresh store backfills for a long time.
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8090/v1/health >/dev/null; then break; fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:8090/v1/health >/dev/null
sudo systemctl is-active --quiet transparent-filter-server
rm -rf "$staged"
REMOTE
}

rollback() {
  echo "== rollback" >&2
  host_ssh bash -s <<'REMOTE' || true
set -euo pipefail
rollback="/opt/enhance-pir/rollback"
if [[ -x "$rollback/transparent-filter-server" ]]; then
  sudo install -m 0755 "$rollback/transparent-filter-server" /usr/local/bin/transparent-filter-server
  [[ -r "$rollback/transparent-filter-server.service" ]] && sudo install -m 0644 "$rollback/transparent-filter-server.service" /etc/systemd/system/transparent-filter-server.service
  sudo systemctl daemon-reload
  sudo systemctl restart transparent-filter-server || true
fi
sudo journalctl -u transparent-filter-server -n 40 --no-pager || true
REMOTE
}

# -------------------------------------------------------------------- verify

verify() {
  echo "== verify"
  local health
  health="$(host_ssh "curl --fail --silent --max-time 10 http://127.0.0.1:8090/v1/health")" \
    || fail "the filter service does not answer on loopback"
  echo "$health" | jq -e "$JQ_FILTER_HEALTH_SERVING" >/dev/null || fail "filter service is not serving: $health"
  local loopback_map
  loopback_map="$(host_ssh "curl --fail --silent --max-time 10 http://127.0.0.1:8090/v1/filters/shards | sha256sum | cut -d' ' -f1")"
  [[ "$loopback_map" == "$STAGED_MAP_SHA256" ]] \
    || fail "the service serves map $loopback_map, the staged binary read $STAGED_MAP_SHA256"
  local public_map attempt
  for attempt in $(seq 1 12); do
    if public_map="$(curl --fail --silent --max-time 15 "$ENHANCE_PUBLIC_URL/v1/filters/shards" | sha256sum | cut -d' ' -f1)" \
      && [[ "$public_map" == "$loopback_map" ]]; then
      break
    fi
    [[ "$attempt" -lt 12 ]] || fail "the public filter origin serves $public_map, loopback $loopback_map"
    sleep 5
  done
  local status path
  for path in /v1/health /metrics; do
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 15 "$ENHANCE_PUBLIC_URL$path")"
    [[ "$status" == "404" ]] || fail "$path is reachable publicly (HTTP $status)"
  done
  if [[ -n "${TRANSPARENT_PUBLIC_URL:-}" ]]; then
    local retrieval_map
    retrieval_map="$(curl --fail --silent --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/shards" | sha256sum | cut -d' ' -f1)" \
      || fail "the retrieval origin did not answer"
    [[ "$retrieval_map" == "$public_map" ]] \
      || fail "the retrieval origin serves map $retrieval_map, the filter origin $public_map"
    echo "both origins serve map $public_map"
  else
    echo "filter origin serves map $public_map; operator routes 404"
  fi
}

# ----------------------------------------------------------------------- main

case "$MODE" in
  jq-programs)
    jq_programs
    ;;
  validate)
    validate_inputs
    if [[ -r "$TRANSPARENT_ARTIFACT_DIR/transparent-filter-server.service" ]]; then
      render_unit
      echo "unit renders to $RENDERED_UNIT, naming $TRANSPARENT_FILTER_SHARD_DIR"
    else
      echo "no unit in $TRANSPARENT_ARTIFACT_DIR; skipping the unit check"
    fi
    echo "inputs are valid"
    ;;
  preflight)
    validate_inputs
    preflight
    stage
    verify_prepared
    echo "preflight complete; the staged binary reads the set; nothing was activated"
    ;;
  deploy)
    validate_inputs
    preflight
    stage
    verify_prepared
    set -E
    trap 'rollback' ERR
    activate
    verify
    trap - ERR
    echo "deployed filter service $TRANSPARENT_RELEASE_SHA reading $TRANSPARENT_FILTER_SHARD_DIR"
    ;;
  *)
    fail "unknown mode $MODE (jq-programs, validate, preflight, deploy)"
    ;;
esac
