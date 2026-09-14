#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 jq-programs|validate|preflight|deploy" >&2
  exit 2
}

MODE="${1:-}"
case "$MODE" in
  jq-programs | validate | preflight | deploy) ;;
  *) usage ;;
esac

# ------------------------------------------------------------- jq programs
#
# The jq programs on the rollout path, named so ops/scripts/check-jq-contracts.sh
# can compile each one. A jq program that does not compile exits 3, and every one
# of these runs after the coordinator has been stopped -- the transparent script
# lost a deploy that way, to `join(\" \")` inside an interpolation, and two of
# these carry the same escaping shape: they are embedded in double-quoted bash
# strings passed to ssh, where the inner quotes had to be backslashed.
#
# Only the rollout-path programs are here. The inventory, topology and Caddyfile
# programs above are already compiled by `validate` mode, which CI runs on every
# push; these are not reachable without a live fleet.
#
# Compile-checked but not evaluated: the payloads are the coordinator's, and a
# fixture for them means standing up enhance-pir-server. Field-path drift here
# is still uncovered -- see docs and check-jq-contracts.sh.
readonly JQ_ENHANCE_WORKER_HOSTS='.[].replicas[].ssh_host'
readonly JQ_ENHANCE_WORKER_NAMES='.[].replicas[].name'
readonly JQ_ENHANCE_GROUP_COUNT='length'
readonly JQ_ENHANCE_GENERATION='.generation'
# Hoisting these two also removes the backslashed inner quotes they needed
# inside the ssh command string.
readonly JQ_ENHANCE_WORKER_OK='.status == "ok"'
readonly JQ_ENHANCE_COORDINATOR_SERVING='.phase.phase == "serving"'
# The `$expected` and `$old` below are jq variables, supplied by --argjson at
# the call site, so single quotes are exactly right and shellcheck's SC2016 --
# "expressions don't expand in single quotes" -- is the opposite of the advice
# this wants.
# shellcheck disable=SC2016
readonly JQ_ENHANCE_HEALTH_WORKERS='.phase.phase == "serving" and .tables.enhance.workers == $expected'
# shellcheck disable=SC2016
readonly JQ_ENHANCE_INIT_COMPLETE='(.generation.schema_version == 7) and
    (.generation.protocol_revision == "ironwood-enhance-pir-v2") and
    (.generation.record_bytes == 737) and (.generation.row_bytes == 6633) and
    (.generation.network == "main") and
    (.generation.pool == "ironwood") and
    (.generation.setup_seed | type == "number") and
    ([.generation.shards[].worker] | unique | length) <= $expected and
    (.generation.shards | length) > 0 and
    (.params | type == "object") and
    (.public_params_base64 | type == "string" and length > 0)'
# shellcheck disable=SC2016
readonly JQ_ENHANCE_NOT_REGRESSED='.anchor_height >= $old.anchor_height and
    .ironwood_tree_size >= $old.ironwood_tree_size'

# A waiver is an explicit, exact-release operator decision, never evidence of passed tests.
# This is the same acceptance contract used by enhance-autoscale.py.
# shellcheck disable=SC2016
readonly JQ_ENHANCE_RELEASE_ACCEPTED='.revision == $revision and .worker_size == "c-4" and .shards_per_group == 16 and (
    (.passed == true and .full_capacity == true and .failover == true and
     .online_append == true and .memory == true and .seconds >= 21600 and .publications >= 300)
    or (.acceptance == "operator" and .waive_qualification == true and .waive_initial_observation == true and
        ([.authorized_by, .authorized_at, .reason] | all(.[]; type == "string" and test("\\S"))))
)'

# Emitted as NUL-terminated NAME<TAB>PROGRAM records for the contract checker.
# NUL rather than newline because a program may itself span lines, and a
# line-delimited format would split one in half. Not meant to be read by eye.
# Listed explicitly, so a program added without being registered here is a
# visible omission rather than a silent gap in the check.
jq_programs() {
  local name
  for name in JQ_ENHANCE_WORKER_HOSTS JQ_ENHANCE_WORKER_NAMES JQ_ENHANCE_GROUP_COUNT \
    JQ_ENHANCE_GENERATION JQ_ENHANCE_WORKER_OK JQ_ENHANCE_COORDINATOR_SERVING \
    JQ_ENHANCE_HEALTH_WORKERS JQ_ENHANCE_INIT_COMPLETE JQ_ENHANCE_NOT_REGRESSED JQ_ENHANCE_RELEASE_ACCEPTED; do
    printf '%s\t%s\0' "$name" "${!name}"
  done
}

# Answered before the environment is required, so a checker can ask for the
# programs without a fleet's worth of variables set.
if [[ "$MODE" == "jq-programs" ]]; then
  jq_programs
  exit 0
fi

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment variable: $name" >&2
    exit 2
  fi
}

for name in ENHANCE_COORDINATOR_HOST ENHANCE_DEPLOY_USER ENHANCE_PUBLIC_URL ENHANCE_WORKERS_JSON; do
  require_env "$name"
done

if [[ ! "$ENHANCE_COORDINATOR_HOST" =~ ^[A-Za-z0-9.-]+$ ]]; then
  echo "invalid coordinator SSH host" >&2
  exit 2
fi
if [[ ! "$ENHANCE_DEPLOY_USER" =~ ^[a-z_][a-z0-9_-]*$ ]]; then
  echo "invalid deployment user" >&2
  exit 2
fi
if [[ ! "$ENHANCE_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?/?$ ]]; then
  echo "ENHANCE_PUBLIC_URL must be an HTTPS origin" >&2
  exit 2
fi

if ! jq -e --arg coordinator "$ENHANCE_COORDINATOR_HOST" '
  type == "array" and length >= 1 and
  all(.[];
    (keys | sort) == ["name", "replicas"] and
    (.name | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9-]*$")) and
    (.replicas | type == "array" and length == 2) and
    all(.replicas[];
      (keys | sort) == ["name", "service_url", "ssh_host"] and
      (.name | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9-]*$")) and
      (.ssh_host | type == "string" and test("^[A-Za-z0-9.-]+$")) and
      (.service_url | type == "string" and test("^https?://[A-Za-z0-9.-]+:[0-9]+/?$"))
    )
  ) and
  ([.[].name] | length == (unique | length)) and
  ([.[].replicas[].name] | length == (unique | length)) and
  ([.[].replicas[].ssh_host] | length == (unique | length)) and
  ([.[].replicas[].service_url | rtrimstr("/")] | length == (unique | length)) and
  all(.[].replicas[].ssh_host; . != $coordinator)
' >/dev/null <<<"$ENHANCE_WORKERS_JSON"; then
  echo "ENHANCE_WORKERS_JSON is invalid or contains duplicate groups or replicas" >&2
  exit 2
fi

SERVER_CONFIG="$(jq -cn --argjson enhance "$ENHANCE_WORKERS_JSON" '
  {
    groups: ($enhance | map({name, replicas: [.replicas[] | {name, url: (.service_url | rtrimstr("/"))}]}))
  }
')"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENHANCE_CADDYFILE="${ENHANCE_CADDYFILE:-$SCRIPT_DIR/../infra/digitalocean/production/deploy/Caddyfile}"
ENHANCE_APM_ENVIRONMENT="${ENHANCE_APM_ENVIRONMENT:-production}"
PUBLIC_HOST="${ENHANCE_PUBLIC_URL#https://}"
PUBLIC_HOST="${PUBLIC_HOST%%/*}"
PUBLIC_HOST="${PUBLIC_HOST%%:*}"

# The committed Caddyfile is a template keyed on ENHANCE_PUBLIC_HOST so the public
# hostname lives in one place (the GitHub Environment) rather than in git.
render_caddyfile() {
  local template="$1" output="$2"
  if [[ ! -r "$template" ]]; then
    echo "Caddyfile template is unreadable: $template" >&2
    exit 2
  fi
  if ! grep -q '^ENHANCE_PUBLIC_HOST {' "$template"; then
    echo "Caddyfile template must start its site block with 'ENHANCE_PUBLIC_HOST {'" >&2
    exit 2
  fi
  sed "s/ENHANCE_PUBLIC_HOST/$PUBLIC_HOST/g" "$template" >"$output"
  if ! grep -q "^$PUBLIC_HOST {" "$output"; then
    echo "rendered Caddyfile does not serve $PUBLIC_HOST" >&2
    exit 2
  fi
  if grep -q 'ENHANCE_PUBLIC_HOST' "$output"; then
    echo "rendered Caddyfile still contains the ENHANCE_PUBLIC_HOST placeholder" >&2
    exit 2
  fi
}

# Sealed shard ownership is a function of group order, so groups may only be
# extended at the end. Replica membership may change without moving shards.
# `-n` matters: without it jq
# waits for stdin and, given none, produces no result, which `-e` reports as
# failure for every input, including an unchanged inventory.
topology_is_append_only() {
  local old="$1" new="$2"
  jq -n -e --argjson old "$old" --argjson new "$new" '
    ($old.groups | type == "array") and
    ($new.groups | length) >= ($old.groups | length) and
    all(range(0; $old.groups | length); $new.groups[.].name == $old.groups[.].name)
  ' >/dev/null
}

if [[ "$MODE" == "validate" ]]; then
  extended="$(jq -c '.groups += [{name: "self-test", replicas: [{name: "self-test-a", url: "http://127.0.0.1:1"}]}]' <<<"$SERVER_CONFIG")"
  # Prepend rather than reverse: a one-entry inventory reversed is unchanged.
  reordered="$(jq -c '.groups = [{name: "self-test", replicas: [{name: "self-test-a", url: "http://127.0.0.1:1"}]}] + .groups' <<<"$SERVER_CONFIG")"
  replaced="$(jq -c '.groups[0].replicas[0].name = "replacement"' <<<"$SERVER_CONFIG")"
  topology_is_append_only "$SERVER_CONFIG" "$SERVER_CONFIG" || { echo "append-only check rejects an unchanged inventory" >&2; exit 1; }
  topology_is_append_only "$SERVER_CONFIG" "$extended" || { echo "append-only check rejects an appended group" >&2; exit 1; }
  topology_is_append_only "$SERVER_CONFIG" "$replaced" || { echo "append-only check rejects a replica replacement" >&2; exit 1; }
  if topology_is_append_only "$SERVER_CONFIG" "$reordered"; then echo "append-only check accepts a prepended inventory" >&2; exit 1; fi
  rendered="$(mktemp)"
  render_caddyfile "$ENHANCE_CADDYFILE" "$rendered"
  grep -q 'handle_path /apm\*' "$rendered" || { echo "Caddyfile does not route /apm to the sidecar" >&2; exit 1; }
  grep -q 'handle /metrics' "$rendered" || { echo "Caddyfile does not block /metrics" >&2; exit 1; }
  grep -q 'handle /v1/filters/\*' "$rendered" || { echo "Caddyfile does not route /v1/filters to the filter service" >&2; exit 1; }
  rm -f "$rendered"
  echo "$SERVER_CONFIG"
  exit 0
fi

for name in ENHANCE_RELEASE_SHA ENHANCE_ARTIFACT_DIR ENHANCE_SSH_KEY_PATH ENHANCE_KNOWN_HOSTS_PATH \
  ENHANCE_SERVER_SERVICE_FILE ENHANCE_WORKER_SERVICE_FILE ENHANCE_APM_SERVICE_FILE \
  TRANSPARENT_FILTER_SERVICE_FILE; do
  require_env "$name"
done
if [[ ! "$ENHANCE_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ENHANCE_RELEASE_SHA must be a full Git commit SHA" >&2
  exit 2
fi
for file in enhance-pir-server enhance-pir-worker enhance-pir-cli enhance-pir-qualify pir-apm transparent-filter-server; do
  if [[ ! -x "$ENHANCE_ARTIFACT_DIR/$file" ]]; then
    echo "missing executable deployment artifact: $file" >&2
    exit 2
  fi
done
# The Slack webhook is optional: without it the sidecar logs alerts to the
# journal. When present it must be a plain URL so it cannot break the env file.
if [[ -n "${PIR_APM_SLACK_WEBHOOK_URL:-}" && ! "$PIR_APM_SLACK_WEBHOOK_URL" =~ ^https://[A-Za-z0-9./_-]+$ ]]; then
  echo "PIR_APM_SLACK_WEBHOOK_URL is set but is not a plain https URL" >&2
  exit 2
fi
if [[ ! -r "$ENHANCE_SSH_KEY_PATH" || ! -r "$ENHANCE_KNOWN_HOSTS_PATH" ]]; then
  echo "SSH key or known-hosts file is unreadable" >&2
  exit 2
fi

SSH=(ssh -i "$ENHANCE_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$ENHANCE_KNOWN_HOSTS_PATH"
  -o ConnectTimeout=10)
SCP=(scp -q -i "$ENHANCE_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$ENHANCE_KNOWN_HOSTS_PATH"
  -o ConnectTimeout=10)

remote() {
  local host="$1"
  shift
  "${SSH[@]}" "$ENHANCE_DEPLOY_USER@$host" "$@"
}

copy_to() {
  local source="$1"
  local host="$2"
  local destination="$3"
  "${SCP[@]}" "$source" "$ENHANCE_DEPLOY_USER@$host:$destination"
}

mapfile -t WORKER_HOSTS < <(jq -r "$JQ_ENHANCE_WORKER_HOSTS" <<<"$ENHANCE_WORKERS_JSON")
mapfile -t WORKER_NAMES < <(jq -r "$JQ_ENHANCE_WORKER_NAMES" <<<"$ENHANCE_WORKERS_JSON")
REMOTE_STAGE="/tmp/enhance-pir-$ENHANCE_RELEASE_SHA"

preflight_host() {
  local host="$1"
  remote "$host" bash -s -- "$REMOTE_STAGE" <<'REMOTE'
set -euo pipefail
stage="$1"
[[ "$(uname -m)" == "x86_64" ]]
command -v curl >/dev/null
command -v jq >/dev/null
command -v sha256sum >/dev/null
command -v systemctl >/dev/null
if [[ "$(id -u)" -ne 0 ]]; then sudo -n true; fi
mkdir -p "$stage"
available_kb="$(df -Pk /opt | awk 'NR == 2 {print $4}')"
[[ "$available_kb" -ge 1048576 ]]
REMOTE
}

preflight_coordinator_extras() {
  remote "$ENHANCE_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
command -v caddy >/dev/null
systemctl is-enabled --quiet caddy
REMOTE
}

echo "Preflighting coordinator and ${#WORKER_HOSTS[@]} workers"
preflight_host "$ENHANCE_COORDINATOR_HOST"
preflight_coordinator_extras
for host in "${WORKER_HOSTS[@]}"; do
  preflight_host "$host"
done

# A matching qualification or explicit operator acceptance is required before services stop.
printf -v acceptance_arg '%q' "$JQ_ENHANCE_RELEASE_ACCEPTED"
remote "$ENHANCE_COORDINATOR_HOST" bash -s -- "$ENHANCE_RELEASE_SHA" "$acceptance_arg" <<'REMOTE'
set -euo pipefail
if ! jq -e --arg revision "$1" "$2" /etc/enhance-pir/qualification.json >/dev/null; then
  echo "Deployment requires qualification or explicit operator acceptance for the exact target revision" >&2
  exit 1
fi
REMOTE

# Once online topology is installed, it is authoritative. Refuse stale CI
# inventory before stopping any process; never override a committed append.
durable_config="$(remote "$ENHANCE_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
topology=/srv/zakura/enhance-data-v7/topology.json
[[ -r "$topology" ]] || topology=/srv/zakura/enhance-data/topology.json
if [[ -r "$topology" ]]; then
  jq -e 'if .pending != null then error("topology activation pending") else {groups: .groups} end' "$topology"
fi
REMOTE
)"
if [[ -n "$durable_config" ]] && ! jq -ne --argjson live "$durable_config" --argjson proposed "$SERVER_CONFIG" '$live.groups == $proposed.groups' >/dev/null; then
  echo "Deployment inventory differs from committed online topology; refresh ENHANCE_WORKERS_JSON before deployment" >&2
  exit 1
fi
existing_config="$(remote "$ENHANCE_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
if [[ -r /etc/enhance-pir/workers.json ]]; then
  cat /etc/enhance-pir/workers.json
fi
REMOTE
)"
if [[ -n "$existing_config" ]] && ! topology_is_append_only "$existing_config" "$SERVER_CONFIG"; then
  if [[ "${ENHANCE_ALLOW_TOPOLOGY_CHANGE:-false}" == "true" ]]; then
    # A deliberate, one-off change (for example removing a worker). Every
    # shard whose owner changes is rebuilt from the journal on the next publish.
    echo "WARNING: worker topology is not append-only; proceeding because ENHANCE_ALLOW_TOPOLOGY_CHANGE=true" >&2
    echo "  live: $existing_config" >&2
    echo "  new:  $SERVER_CONFIG" >&2
  else
    echo "worker topology is not append-only; refusing deployment (set allow_topology_change to override)" >&2
    exit 1
  fi
fi

server_config_file="$(mktemp)"
caddyfile_rendered="$(mktemp)"
apm_env_file="$(mktemp)"
cleanup() {
  rm -f "$server_config_file" "$caddyfile_rendered" "$apm_env_file"
}
trap cleanup EXIT
printf '%s\n' "$SERVER_CONFIG" >"$server_config_file"
render_caddyfile "$ENHANCE_CADDYFILE" "$caddyfile_rendered"
# mktemp creates the file 0600; the webhook value is never echoed anywhere else.
{
  echo "PIR_APM_SCRAPE_URL=http://127.0.0.1:8080"
  echo "PIR_APM_LISTEN=127.0.0.1:3002"
  echo "PIR_APM_METRICS_PATH=/metrics"
  echo "PIR_APM_HEALTH_PATH=/v1/health"
  echo "PIR_APM_READY_PATH=/ready"
  echo "PIR_APM_METRIC_PREFIX=enhance"
  echo "PIR_APM_ENDPOINTS=health,init,query"
  echo "PIR_APM_INFORMATIONAL_ENDPOINTS=health"
  echo "PIR_APM_PROCESSING_ENDPOINTS=query"
  echo "PIR_APM_LATENCY_P99_SECONDS=1.0"
  echo "PIR_APM_LATENCY_P99_OVERRIDES=query=5.0,init=2.0"
  echo "PIR_APM_TITLE=Enhance PIR APM"
  echo "PIR_APM_ENVIRONMENT=$ENHANCE_APM_ENVIRONMENT"
  echo "PIR_APM_DATA_DIR=/srv/zakura/enhance-data-v7"
  if [[ -n "${PIR_APM_SLACK_WEBHOOK_URL:-}" ]]; then
    echo "PIR_APM_SLACK_WEBHOOK_URL=$PIR_APM_SLACK_WEBHOOK_URL"
  fi
} >"$apm_env_file"

stage_file() {
  local source="$1"
  local host="$2"
  local name="$3"
  local mode="${4:-0644}"
  local digest
  digest="$(sha256sum "$source" | awk '{print $1}')"
  copy_to "$source" "$host" "$REMOTE_STAGE/$name"
  remote "$host" bash -s -- "$REMOTE_STAGE/$name" "$digest" "$mode" <<'REMOTE'
set -euo pipefail
printf '%s  %s\n' "$2" "$1" | sha256sum --check --status
chmod "$3" "$1"
REMOTE
}

# Keep the worker/qualification release on the coordinator beyond CI artifact expiry.
for artifact in enhance-pir-worker enhance-pir-qualify enhance-pir-cli SHA256SUMS revision enhance-autoscale.py enhance-runtime.py enhance-terraform.sh enhance-autoscale.service enhance-autoscale.timer autoscale.example.json enhance-infra.tar.gz; do
  stage_file "$ENHANCE_ARTIFACT_DIR/$artifact" "$ENHANCE_COORDINATOR_HOST" "$artifact"
done
stage_file "$ENHANCE_WORKER_SERVICE_FILE" "$ENHANCE_COORDINATOR_HOST" enhance-pir-worker.service
stage_file "$ENHANCE_ARTIFACT_DIR/enhance-pir-server" "$ENHANCE_COORDINATOR_HOST" enhance-pir-server
stage_file "$server_config_file" "$ENHANCE_COORDINATOR_HOST" workers.json
stage_file "$ENHANCE_SERVER_SERVICE_FILE" "$ENHANCE_COORDINATOR_HOST" enhance-pir-server.service
stage_file "$ENHANCE_ARTIFACT_DIR/pir-apm" "$ENHANCE_COORDINATOR_HOST" pir-apm
stage_file "$ENHANCE_APM_SERVICE_FILE" "$ENHANCE_COORDINATOR_HOST" pir-apm.service
stage_file "$apm_env_file" "$ENHANCE_COORDINATOR_HOST" pir-apm.env 0600
stage_file "$caddyfile_rendered" "$ENHANCE_COORDINATOR_HOST" Caddyfile
stage_file "$ENHANCE_ARTIFACT_DIR/transparent-filter-server" "$ENHANCE_COORDINATOR_HOST" transparent-filter-server
stage_file "$TRANSPARENT_FILTER_SERVICE_FILE" "$ENHANCE_COORDINATOR_HOST" transparent-filter-server.service

# Validate the staged Caddyfile before anything is installed, and show how it
# differs from the live one so a preflight run makes the upcoming change visible.
remote "$ENHANCE_COORDINATOR_HOST" bash -s -- "$REMOTE_STAGE" <<'REMOTE'
set -euo pipefail
stage="$1"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
as_root caddy validate --config "$stage/Caddyfile" --adapter caddyfile >/dev/null
if as_root test -r /etc/caddy/Caddyfile; then
  echo "Caddyfile changes (live -> staged):"
  as_root diff -u /etc/caddy/Caddyfile "$stage/Caddyfile" || true
else
  echo "No live Caddyfile; the staged one will be installed"
fi
REMOTE
for host in "${WORKER_HOSTS[@]}"; do
  stage_file "$ENHANCE_ARTIFACT_DIR/enhance-pir-worker" "$host" enhance-pir-worker
  stage_file "$ENHANCE_WORKER_SERVICE_FILE" "$host" enhance-pir-worker.service
done

if [[ "$MODE" == "preflight" ]]; then
  echo "Preflight and artifact verification succeeded; no services changed"
  exit 0
fi

coordinator_activated=0
activated_workers=()

show_logs() {
  remote "$ENHANCE_COORDINATOR_HOST" bash -s <<'REMOTE' || true
if [[ "$(id -u)" -eq 0 ]]; then
  journalctl -u enhance-pir-server --no-pager -n 80
  journalctl -u pir-apm --no-pager -n 40
  journalctl -u transparent-filter-server --no-pager -n 40
  journalctl -u caddy --no-pager -n 20
else
  sudo -n journalctl -u enhance-pir-server --no-pager -n 80
  sudo -n journalctl -u pir-apm --no-pager -n 40
  sudo -n journalctl -u transparent-filter-server --no-pager -n 40
  sudo -n journalctl -u caddy --no-pager -n 20
fi
REMOTE
  for host in "${WORKER_HOSTS[@]}"; do
    remote "$host" bash -s <<'REMOTE' || true
if [[ "$(id -u)" -eq 0 ]]; then
  journalctl -u enhance-pir-worker --no-pager -n 50
else
  sudo -n journalctl -u enhance-pir-worker --no-pager -n 50
fi
REMOTE
  done
}

coordinator_service() {
  local action="$1"
  remote "$ENHANCE_COORDINATOR_HOST" bash -s -- "$action" <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
if [[ "$1" == "stop" ]]; then
  as_root systemctl stop enhance-pir-server 2>/dev/null || true
else
  as_root systemctl "$1" enhance-pir-server
fi
REMOTE
}

rollback_worker() {
  local host="$1"
  remote "$host" bash -s <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
rollback=/opt/enhance-pir/rollback
as_root systemctl stop enhance-pir-worker 2>/dev/null || true
[[ -x "$rollback/enhance-pir-worker" ]] || exit 1
as_root install -m 0755 "$rollback/enhance-pir-worker" /usr/local/bin/enhance-pir-worker
[[ -r "$rollback/enhance-pir-worker.service" ]] && as_root install -m 0644 "$rollback/enhance-pir-worker.service" /etc/systemd/system/enhance-pir-worker.service
as_root systemctl daemon-reload
as_root systemctl enable --now enhance-pir-worker
REMOTE
}

rollback_coordinator() {
  remote "$ENHANCE_COORDINATOR_HOST" bash -s <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
rollback=/opt/enhance-pir/rollback
as_root systemctl stop enhance-pir-server || true
[[ -x "$rollback/enhance-pir-server" ]] && as_root install -m 0755 "$rollback/enhance-pir-server" /usr/local/bin/enhance-pir-server
[[ -r "$rollback/enhance-pir-server.service" ]] && as_root install -m 0644 "$rollback/enhance-pir-server.service" /etc/systemd/system/enhance-pir-server.service
if [[ -r "$rollback/workers.json" ]]; then
  as_root install -d -m 0755 /etc/enhance-pir
  as_root install -m 0644 "$rollback/workers.json" /etc/enhance-pir/workers.json
fi
# Sidecar and reverse proxy. A first deploy has no previous sidecar to restore,
# so disable it rather than leave it paging against a server without /metrics.
if [[ -x "$rollback/pir-apm" ]]; then
  as_root install -m 0755 "$rollback/pir-apm" /usr/local/bin/pir-apm
  [[ -r "$rollback/pir-apm.service" ]] && as_root install -m 0644 "$rollback/pir-apm.service" /etc/systemd/system/pir-apm.service
  if as_root test -r "$rollback/pir-apm.env"; then
    as_root install -m 0600 -o root -g root "$rollback/pir-apm.env" /etc/default/pir-apm
  fi
else
  as_root systemctl disable --now pir-apm 2>/dev/null || true
fi
# Transparent filter service. Independent of the PIR fleet, but rolled back with
# it so one release is one fleet state.
if [[ -x "$rollback/transparent-filter-server" ]]; then
  as_root install -m 0755 "$rollback/transparent-filter-server" /usr/local/bin/transparent-filter-server
  [[ -r "$rollback/transparent-filter-server.service" ]] && as_root install -m 0644 "$rollback/transparent-filter-server.service" /etc/systemd/system/transparent-filter-server.service
else
  as_root systemctl disable --now transparent-filter-server 2>/dev/null || true
fi
if as_root test -r "$rollback/Caddyfile"; then
  as_root install -m 0644 -o root -g root "$rollback/Caddyfile" /etc/caddy/Caddyfile
  as_root systemctl reload caddy || true
fi
as_root systemctl daemon-reload
[[ -x "$rollback/enhance-pir-server" ]] || exit 1
as_root systemctl enable --now enhance-pir-server
if [[ -x "$rollback/pir-apm" ]]; then
  as_root systemctl restart pir-apm || true
fi
REMOTE
}

rollback() {
  set +e
  echo "Deployment failed; restoring the previous fleet release" >&2
  for host in "${activated_workers[@]}"; do rollback_worker "$host"; done
  if [[ "$coordinator_activated" -eq 1 ]]; then
    rollback_coordinator
  else
    coordinator_service start || true
  fi
  show_logs
  set -e
}

activate_worker() {
  local host="$1"
  remote "$host" bash -s -- "$REMOTE_STAGE" "$ENHANCE_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
stage="$1"; sha="$2"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
release="/opt/enhance-pir/releases/$sha"
rollback=/opt/enhance-pir/rollback
as_root install -d -m 0755 "$release" "$rollback"
as_root install -m 0755 "$stage/enhance-pir-worker" "$release/enhance-pir-worker"
[[ -x /usr/local/bin/enhance-pir-worker ]] && as_root cp -L /usr/local/bin/enhance-pir-worker "$rollback/enhance-pir-worker"
[[ -r /etc/systemd/system/enhance-pir-worker.service ]] && as_root cp /etc/systemd/system/enhance-pir-worker.service "$rollback/enhance-pir-worker.service"
as_root systemctl stop enhance-pir-worker 2>/dev/null || true
as_root install -m 0755 "$release/enhance-pir-worker" /usr/local/bin/enhance-pir-worker.next
as_root mv -f /usr/local/bin/enhance-pir-worker.next /usr/local/bin/enhance-pir-worker
as_root install -m 0644 "$stage/enhance-pir-worker.service" /etc/systemd/system/enhance-pir-worker.service
as_root systemctl daemon-reload
as_root systemctl enable --now enhance-pir-worker
printf '%s\n' "$sha" | as_root tee /opt/enhance-pir/current-worker-release >/dev/null
REMOTE
  remote "$host" "curl --fail --silent --show-error http://127.0.0.1:8091/internal/health | jq -e '$JQ_ENHANCE_WORKER_OK' >/dev/null"
}

activate_coordinator() {
  remote "$ENHANCE_COORDINATOR_HOST" bash -s -- "$REMOTE_STAGE" "$ENHANCE_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
stage="$1"; sha="$2"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
release="/opt/enhance-pir/releases/$sha"
rollback=/opt/enhance-pir/rollback
as_root install -d -m 0755 "$release" "$rollback" /etc/enhance-pir
as_root install -m 0755 "$stage/enhance-pir-server" "$release/enhance-pir-server"
for artifact in enhance-pir-worker enhance-pir-qualify enhance-pir-cli; do
  as_root install -m 0755 "$stage/$artifact" "$release/$artifact"
done
for artifact in SHA256SUMS revision enhance-pir-worker.service; do
  as_root install -m 0644 "$stage/$artifact" "$release/$artifact"
done
as_root install -d -m 0755 /opt/enhance-pir/ops /opt/enhance-pir/infra/production
as_root install -m 0755 "$stage/enhance-autoscale.py" /opt/enhance-pir/ops/enhance-autoscale.py
as_root install -m 0755 "$stage/enhance-runtime.py" /opt/enhance-pir/ops/enhance-runtime.py
as_root install -m 0755 "$stage/enhance-terraform.sh" /opt/enhance-pir/ops/enhance-terraform.sh
as_root install -m 0644 "$stage/enhance-autoscale.service" /etc/systemd/system/enhance-autoscale.service
as_root install -m 0644 "$stage/enhance-autoscale.timer" /etc/systemd/system/enhance-autoscale.timer
as_root tar -xzf "$stage/enhance-infra.tar.gz" --strip-components=4 -C /opt/enhance-pir/infra/production
if ! as_root test -e /etc/enhance-pir/autoscale.json; then
  as_root install -m 0600 "$stage/autoscale.example.json" /etc/enhance-pir/autoscale.json
fi
# Timer activation is an explicit initial rollout gate, never a deploy side effect.
# Atomic pointer; controller also verifies it matches current-coordinator-release.
as_root ln -sfn "$release" /opt/enhance-pir/autoscale-release.next
as_root mv -Tf /opt/enhance-pir/autoscale-release.next /opt/enhance-pir/autoscale-release

[[ -x /usr/local/bin/enhance-pir-server ]] && as_root cp -L /usr/local/bin/enhance-pir-server "$rollback/enhance-pir-server"
[[ -r /etc/systemd/system/enhance-pir-server.service ]] && as_root cp /etc/systemd/system/enhance-pir-server.service "$rollback/enhance-pir-server.service"
[[ -r /etc/enhance-pir/workers.json ]] && as_root cp /etc/enhance-pir/workers.json "$rollback/workers.json"
as_root install -m 0755 "$release/enhance-pir-server" /usr/local/bin/enhance-pir-server.next
as_root mv -f /usr/local/bin/enhance-pir-server.next /usr/local/bin/enhance-pir-server
as_root install -m 0644 "$stage/workers.json" /etc/enhance-pir/workers.json
as_root install -m 0644 "$stage/enhance-pir-server.service" /etc/systemd/system/enhance-pir-server.service
as_root systemctl daemon-reload
as_root systemctl enable --now enhance-pir-server
printf '%s\n' "$sha" | as_root tee /opt/enhance-pir/current-coordinator-release >/dev/null

# pir-apm sidecar: same save-then-replace pattern as the server binary.
[[ -x /usr/local/bin/pir-apm ]] && as_root cp -L /usr/local/bin/pir-apm "$rollback/pir-apm"
[[ -r /etc/systemd/system/pir-apm.service ]] && as_root cp /etc/systemd/system/pir-apm.service "$rollback/pir-apm.service"
if as_root test -r /etc/default/pir-apm; then
  as_root install -m 0600 -o root -g root /etc/default/pir-apm "$rollback/pir-apm.env"
fi
if as_root test -r /etc/caddy/Caddyfile; then
  as_root install -m 0644 -o root -g root /etc/caddy/Caddyfile "$rollback/Caddyfile"
fi
as_root install -m 0755 "$stage/pir-apm" "$release/pir-apm"
as_root install -m 0755 "$release/pir-apm" /usr/local/bin/pir-apm.next
as_root mv -f /usr/local/bin/pir-apm.next /usr/local/bin/pir-apm
as_root install -m 0644 "$stage/pir-apm.service" /etc/systemd/system/pir-apm.service
as_root install -m 0600 -o root -g root "$stage/pir-apm.env" /etc/default/pir-apm
as_root caddy validate --config "$stage/Caddyfile" --adapter caddyfile >/dev/null
as_root install -m 0644 -o root -g root "$stage/Caddyfile" /etc/caddy/Caddyfile

# Transparent filter service: same save-then-replace pattern as the others. It
# binds loopback and the Caddyfile deliberately gives it no route.
[[ -x /usr/local/bin/transparent-filter-server ]] && as_root cp -L /usr/local/bin/transparent-filter-server "$rollback/transparent-filter-server"
[[ -r /etc/systemd/system/transparent-filter-server.service ]] && as_root cp /etc/systemd/system/transparent-filter-server.service "$rollback/transparent-filter-server.service"
as_root install -m 0755 "$stage/transparent-filter-server" "$release/transparent-filter-server"
as_root install -m 0755 "$release/transparent-filter-server" /usr/local/bin/transparent-filter-server.next
as_root mv -f /usr/local/bin/transparent-filter-server.next /usr/local/bin/transparent-filter-server
as_root install -m 0644 "$stage/transparent-filter-server.service" /etc/systemd/system/transparent-filter-server.service

as_root systemctl daemon-reload
as_root systemctl enable pir-apm
as_root systemctl restart pir-apm
as_root systemctl reload caddy
for _ in $(seq 1 10); do
  if curl --fail --silent http://127.0.0.1:3002/healthz >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:3002/healthz >/dev/null
as_root systemctl is-active --quiet pir-apm

as_root systemctl enable transparent-filter-server
as_root systemctl restart transparent-filter-server
# Health only, deliberately not /ready. A first deploy starts from an empty
# store and backfills tens of thousands of blocks, so it is legitimately not
# ready for a long time; gating the rollout on readiness would fail every
# initial deploy. This checks the service starts and answers.
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8090/v1/health >/dev/null; then break; fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:8090/v1/health >/dev/null
as_root systemctl is-active --quiet transparent-filter-server
REMOTE
}

old_session="$(curl --fail --silent --show-error "$ENHANCE_PUBLIC_URL/v1/enhance/init" || true)"
old_metadata="$(jq -c "$JQ_ENHANCE_GENERATION" <<<"$old_session" 2>/dev/null || true)"
if [[ -z "$old_metadata" || "$old_metadata" == "null" ]]; then
  # A rollback may still be serving the split setup API during migration.
  old_metadata="$(curl --fail --silent --show-error "$ENHANCE_PUBLIC_URL/v1/enhance/generation" || true)"
fi

# An actual schema transition needs this release's isolated preparation receipt.
# Later compatible schema-7 releases resume the journal already held by production.
old_schema=$(jq -r '.schema_version // 0' <<<"${old_metadata:-null}" 2>/dev/null || echo 0)
if [[ "$old_schema" != "7" ]]; then
remote "$ENHANCE_COORDINATOR_HOST" bash -s -- "$ENHANCE_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
[[ "$(as_root cat /srv/zakura/enhance-data-v7/prepared-release)" == "$1" ]]
as_root test -s /srv/zakura/enhance-data-v7/enhance/manifest.json
# Preserve append-only topology across the data-format transition.
if as_root test -f /srv/zakura/enhance-data/topology.json && ! as_root test -f /srv/zakura/enhance-data-v7/topology.json; then
  as_root cp /srv/zakura/enhance-data/topology.json /srv/zakura/enhance-data-v7/topology.json
fi
REMOTE
fi

if ! coordinator_service stop; then
  show_logs
  exit 1
fi

rollout_ok=1
for host in "${WORKER_HOSTS[@]}"; do
  activated_workers+=("$host")
  if ! activate_worker "$host"; then
    rollout_ok=0
    break
  fi
done
if [[ "$rollout_ok" -eq 1 ]]; then
  coordinator_activated=1
  if ! activate_coordinator; then rollout_ok=0; fi
fi

if [[ "$rollout_ok" -eq 1 ]]; then
  # Enhance ingests from Ironwood activation and resumes from its journal, so
  # this is normally quick. The generous ceiling covers a first deploy against
  # an empty data directory.
  serving=0
  for _ in $(seq 1 1800); do
    if remote "$ENHANCE_COORDINATOR_HOST" "curl --fail --silent http://127.0.0.1:8080/v1/health | jq -e '$JQ_ENHANCE_COORDINATOR_SERVING' >/dev/null"; then
      serving=1
      break
    fi
    sleep 10
  done
  [[ "$serving" -eq 1 ]] || rollout_ok=0
fi

if [[ "$rollout_ok" -eq 1 ]]; then
  health_json="$(curl --fail --silent --show-error "$ENHANCE_PUBLIC_URL/v1/health")" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  expected_workers="${#WORKER_HOSTS[@]}"
  if ! jq -e --argjson expected "$expected_workers" \
    "$JQ_ENHANCE_HEALTH_WORKERS" >/dev/null <<<"$health_json"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  new_session="$(curl --fail --silent --show-error "$ENHANCE_PUBLIC_URL/v1/enhance/init")" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  new_metadata="$(jq -c "$JQ_ENHANCE_GENERATION" <<<"$new_session")" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  expected_groups="$(jq "$JQ_ENHANCE_GROUP_COUNT" <<<"$ENHANCE_WORKERS_JSON")"
  if ! jq -e --argjson expected "$expected_groups" \
    "$JQ_ENHANCE_INIT_COMPLETE" >/dev/null <<<"$new_session"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 && -n "$old_metadata" ]]; then
  if ! jq -e --argjson old "$old_metadata" \
    "$JQ_ENHANCE_NOT_REGRESSED" >/dev/null <<<"$new_metadata"; then
    rollout_ok=0
  fi
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  # Two sequential queries exercise both replicas in every active-active group.
  for _ in 1 2; do
    "$ENHANCE_ARTIFACT_DIR/enhance-pir-cli" --server "$ENHANCE_PUBLIC_URL" dummy || rollout_ok=0
  done
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  worker_metrics="$(remote "$ENHANCE_COORDINATOR_HOST" "curl --fail --silent http://127.0.0.1:8080/metrics")" || rollout_ok=0
  worker_duration_metrics="$(grep '^enhance_worker_replica_request_duration_seconds_count{' <<<"$worker_metrics" || true)"
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  for worker in "${WORKER_NAMES[@]}"; do
    if ! grep -Fq "replica=\"$worker\"" <<<"$worker_duration_metrics"; then
      echo "worker query metrics missing for $worker" >&2
      rollout_ok=0
      break
    fi
  done
fi
# Readiness gate and sidecar exposure: /ready is loopback-only, /apm/ is public,
# and the raw /metrics exposition must stay behind Caddy's 404.
if [[ "$rollout_ok" -eq 1 ]]; then
  remote "$ENHANCE_COORDINATOR_HOST" "curl --fail --silent http://127.0.0.1:8080/ready >/dev/null" || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  apm_ok=0
  for _ in $(seq 1 10); do
    apm_html="$(curl --fail --silent --show-error "$ENHANCE_PUBLIC_URL/apm/" || true)"
    cards_ok=1
    for worker in "${WORKER_NAMES[@]}"; do
      grep -Fq "data-worker=\"$worker\"" <<<"$apm_html" || cards_ok=0
    done
    if grep -q '<title>' <<<"$apm_html" && [[ "$cards_ok" -eq 1 ]]; then
      apm_ok=1
      break
    fi
    sleep 3
  done
  [[ "$apm_ok" -eq 1 ]] || rollout_ok=0
fi
if [[ "$rollout_ok" -eq 1 ]]; then
  metrics_code="$(curl --silent --output /dev/null --write-out '%{http_code}' "$ENHANCE_PUBLIC_URL/metrics" || true)"
  [[ "$metrics_code" == "404" ]] || rollout_ok=0
fi

if [[ "$rollout_ok" -ne 1 ]]; then
  rollback
  exit 1
fi

echo "Enhance PIR fleet deployed successfully at $ENHANCE_RELEASE_SHA"
