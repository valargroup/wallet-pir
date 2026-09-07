#!/usr/bin/env bash
# Deploys the transparent PIR shard worker.
#
# Deliberately separate from deploy-enhance-pir.sh rather than a mode inside it.
# That script carries the one-time legacy rollback branches the repository
# guidance protects, and a fleet that shares no state with Enhance must not be
# able to disturb them. Nothing here touches the Enhance coordinator, its units,
# its Caddyfile, or the filter service.
#
# The worker is not published. Its firewall opens 8093 to the coordinator tag
# alone, because the wallet-facing API is not designed yet and the upstream
# binary defaults to loopback for exactly that reason. Verification therefore
# runs from the coordinator over the VPC, not from the internet.
#
# Shard bytes reach the worker by rsync from the coordinator, which is also the
# deploy runner, so the published set is local to it. This stands in for the
# coordinator push that the multi-worker build will add later.
set -euo pipefail

MODE="${1:-deploy}"

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
  require_env TRANSPARENT_WORKER_HOST TRANSPARENT_DEPLOY_USER TRANSPARENT_RELEASE_SHA \
    TRANSPARENT_ARTIFACT_DIR TRANSPARENT_SHARD_DIR TRANSPARENT_PUBLIC_URL
  [[ "$TRANSPARENT_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] \
    || fail "TRANSPARENT_RELEASE_SHA must be a full commit SHA"
  # HTTPS only. The public host is what Caddy requests a certificate for, so a
  # typo here becomes a rate-limited ACME failure rather than an obvious error.
  [[ "$TRANSPARENT_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+$ ]] \
    || fail "TRANSPARENT_PUBLIC_URL must be https:// and a bare host"
  [[ "$TRANSPARENT_WORKER_HOST" =~ ^[A-Za-z0-9._-]+$ ]] \
    || fail "TRANSPARENT_WORKER_HOST is not a plain host or address"
  [[ "$TRANSPARENT_DEPLOY_USER" =~ ^[A-Za-z0-9._-]+$ ]] \
    || fail "TRANSPARENT_DEPLOY_USER is not a plain user name"
  # An absolute path, because it is interpolated into remote commands and into
  # an rsync destination. A relative one would resolve against whatever the
  # remote shell's home happens to be.
  [[ "$TRANSPARENT_SHARD_DIR" = /* ]] \
    || fail "TRANSPARENT_SHARD_DIR must be absolute"
  # rsync's -e takes one string, so the ssh invocation is flattened into it and
  # a path containing a space would split into two arguments and fail somewhere
  # far from the cause. The runner never produces such a path; say so here
  # rather than debug it there.
  case "${TRANSPARENT_SSH_KEY_PATH:-}${TRANSPARENT_KNOWN_HOSTS_PATH:-}" in
    *[[:space:]]*) fail "SSH key and known_hosts paths must not contain spaces" ;;
  esac
}

# The published set has to be a set before it is shipped: a directory of shard
# directories plus the map that names them. Shipping a partial set would be
# refused at load by ShardSet::open, but it would be refused *after* the service
# had been stopped, which turns a bad input into an outage.
validate_shard_set() {
  local dir="$1"
  [[ -d "$dir" ]] || fail "shard set $dir does not exist"
  [[ -f "$dir/shards.json" ]] || fail "$dir has no shards.json"
  local named counted
  named="$(jq -er '.shards | length' "$dir/shards.json")" \
    || fail "$dir/shards.json is not a readable shard map"
  [[ "$named" -gt 0 ]] || fail "$dir/shards.json names no shards"

  # Every revision the map names must be on disk. This used to compare counts
  # instead, which was the same check while a set had exactly one directory per
  # shard -- and stopped being the same check when the publisher started leaving
  # a superseded tail beside the revision that replaced it. Counting would then
  # refuse a set the server loads happily, and it would refuse it *here*, in the
  # gate that exists to keep a bad input from becoming an outage.
  local digest missing=0
  while read -r digest; do
    [[ -d "$dir/$digest" ]] || { echo "  missing revision $digest"; missing=$((missing + 1)); }
  done < <(jq -er '.shards[].manifest_digest' "$dir/shards.json")
  [[ "$missing" -eq 0 ]] || fail "$dir is missing $missing of the $named revisions its map names"

  # Anything else on disk is a superseded revision the worker may still be
  # answering from, which is expected rather than an error. Reported, because an
  # unbounded number of them is how a set quietly grows without the map moving.
  counted="$(find "$dir" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
  local retained=$((counted - named))
  local first_digest
  first_digest="$(jq -r '.shards[0].manifest_digest // empty' "$dir/shards.json")"
  [[ -n "$first_digest" ]] || fail "$dir/shards.json entries carry no manifest digest"
  echo "shard set $dir: $named shards, $retained superseded revisions retained"
}

ssh_opts() {
  printf '%s\n' -i "$TRANSPARENT_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes \
    -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$TRANSPARENT_KNOWN_HOSTS_PATH" \
    -o ConnectTimeout=10
}

worker_ssh() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  # Callers that interpolate a path do so through printf %q, so the client-side
  # expansion shellcheck warns about here is both intended and already quoted.
  # Everything else is `bash -s` with the script on stdin, which does not expand
  # at all.
  # shellcheck disable=SC2029
  ssh "${opts[@]}" "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST" "$@"
}

# ------------------------------------------------------------------ preflight

preflight() {
  echo "== preflight $TRANSPARENT_WORKER_HOST"
  worker_ssh bash -s <<'REMOTE'
set -euo pipefail
[[ "$(uname -m)" == "x86_64" ]] || { echo "worker is not x86_64" >&2; exit 1; }
for tool in curl jq sha256sum systemctl rsync caddy; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 1; }
done
systemctl is-enabled caddy >/dev/null 2>&1 || { echo "caddy is not enabled" >&2; exit 1; }
sudo -n true || { echo "passwordless sudo is required" >&2; exit 1; }
free -g | awk '/^Mem:/ {print "memory: " $2 " GiB total, " $7 " GiB available"}'
REMOTE
}

# A shard set is gigabytes and the service holds it open, so check that the
# destination can take it *before* stopping anything. Measured against the
# source, plus the set already on the worker, since rsync stages alongside.
preflight_capacity() {
  local need have
  need="$(du -sm "$TRANSPARENT_SHARD_SOURCE" | cut -f1)"
  have="$(worker_ssh "df -Pm $(printf %q "$(dirname "$TRANSPARENT_SHARD_DIR")") | awk 'NR==2 {print \$4}'")"
  echo "shard set is ${need} MiB; worker has ${have} MiB free"
  # Twice the set plus a gigabyte: rsync writes temporaries beside the target.
  (( have > need * 2 + 1024 )) || fail "worker has too little space for the shard set"
}

# -------------------------------------------------------------------- staging

stage() {
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA"
  local -a opts
  mapfile -t opts < <(ssh_opts)
  echo "== stage binary, unit and Caddyfile"

  # The public host is substituted here rather than on the worker, so the file
  # that gets validated is byte-for-byte the file that gets installed.
  local host="${TRANSPARENT_PUBLIC_URL#https://}"
  local rendered="$TRANSPARENT_ARTIFACT_DIR/Caddyfile.rendered"
  sed "s/TRANSPARENT_PUBLIC_HOST/$host/" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-Caddyfile" >"$rendered"
  grep -q "TRANSPARENT_PUBLIC_HOST" "$rendered" \
    && fail "Caddyfile still carries an unsubstituted token"
  grep -q "^$host {" "$rendered" || fail "rendered Caddyfile does not name $host"

  worker_ssh "mkdir -p $(printf %q "$staged")"
  scp "${opts[@]}" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service" \
    "$TRANSPARENT_ARTIFACT_DIR/SHA256SUMS" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$staged/"
  scp "${opts[@]}" "$rendered" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$staged/Caddyfile"

  # Verified on the far side: a truncated copy that still looked like an ELF
  # binary would otherwise be installed and only fail at start. Caddy validates
  # its own config before anything replaces the running one, so a syntax error
  # is caught while the old config is still serving.
  worker_ssh bash -s -- "$staged" <<'REMOTE'
set -euo pipefail
cd "$1"
sha256sum -c SHA256SUMS --ignore-missing
chmod 0755 transparent-shard-server
sudo caddy validate --config Caddyfile --adapter caddyfile
REMOTE
}

# The set is immutable once published, so this is a plain mirror. --delete keeps
# a superseded set from accumulating beside the current one; the source is the
# authority, and anything on the worker that the map does not name would be
# refused at load anyway.
ship_shards() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  echo "== ship shard set ($TRANSPARENT_SHARD_SOURCE -> $TRANSPARENT_SHARD_DIR)"
  worker_ssh "sudo mkdir -p $(printf %q "$TRANSPARENT_SHARD_DIR") && sudo chown $(printf %q "$TRANSPARENT_DEPLOY_USER") $(printf %q "$TRANSPARENT_SHARD_DIR")"
  rsync -a --delete --info=stats1 \
    -e "ssh $(ssh_opts | tr '\n' ' ')" \
    "$TRANSPARENT_SHARD_SOURCE/" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$TRANSPARENT_SHARD_DIR/"
}

# ------------------------------------------------------------------- activate

activate() {
  echo "== activate"
  worker_ssh bash -s -- \
    "/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA" \
    "$TRANSPARENT_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
staged="$1"; sha="$2"
release="/opt/transparent-pir/releases/$sha"
sudo mkdir -p "$release" /opt/transparent-pir/rollback
sudo install -m 0755 "$staged/transparent-shard-server" "$release/"
sudo install -m 0644 "$staged/transparent-shard-server.service" "$release/"

# Keep whatever is running now, so a failed start has something to go back to.
if [[ -x /usr/local/bin/transparent-shard-server ]]; then
  sudo cp -f /usr/local/bin/transparent-shard-server /opt/transparent-pir/rollback/
fi
if [[ -f /etc/systemd/system/transparent-shard-server.service ]]; then
  sudo cp -f /etc/systemd/system/transparent-shard-server.service /opt/transparent-pir/rollback/
fi

sudo systemctl stop transparent-shard-server.service 2>/dev/null || true
# Installed beside the target and moved into place, so an interrupted copy
# never leaves a half-written binary at the path systemd will execute.
sudo install -m 0755 "$release/transparent-shard-server" /usr/local/bin/transparent-shard-server.next
sudo mv -f /usr/local/bin/transparent-shard-server.next /usr/local/bin/transparent-shard-server
sudo install -m 0644 "$release/transparent-shard-server.service" \
  /etc/systemd/system/transparent-shard-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now transparent-shard-server.service

# Caddy last, and reloaded rather than restarted, so an existing certificate and
# its in-flight connections survive. It was validated before the service was
# touched, so this is the step least likely to be the one that fails.
sudo cp -f /etc/caddy/Caddyfile /opt/transparent-pir/rollback/Caddyfile 2>/dev/null || true
sudo install -m 0644 "$staged/Caddyfile" /etc/caddy/Caddyfile
sudo systemctl reload caddy || sudo systemctl restart caddy

printf '%s\n' "$sha" | sudo tee /opt/transparent-pir/current-release >/dev/null
rm -rf "$staged"
REMOTE
}

rollback() {
  echo "== rollback" >&2
  worker_ssh bash -s <<'REMOTE' || true
set -euo pipefail
if [[ -x /opt/transparent-pir/rollback/transparent-shard-server ]]; then
  sudo install -m 0755 /opt/transparent-pir/rollback/transparent-shard-server \
    /usr/local/bin/transparent-shard-server
  if [[ -f /opt/transparent-pir/rollback/transparent-shard-server.service ]]; then
    sudo install -m 0644 /opt/transparent-pir/rollback/transparent-shard-server.service \
      /etc/systemd/system/transparent-shard-server.service
  fi
  sudo systemctl daemon-reload
  sudo systemctl restart transparent-shard-server.service || true
else
  # A first deploy has nothing to restore. Leave the host without the service
  # rather than restarting a binary that was never known to work.
  sudo systemctl disable --now transparent-shard-server.service || true
fi
sudo journalctl -u transparent-shard-server.service -n 60 --no-pager || true
REMOTE
}

# -------------------------------------------------------------------- verify

# Loading verifies every shard against its manifest digest, and a large set can
# take a while, so poll rather than assume the port is up when systemd returns.
verify() {
  echo "== verify"
  local url="http://$TRANSPARENT_WORKER_HOST:8093"
  local attempt health
  for attempt in $(seq 1 60); do
    if health="$(curl --fail --silent --max-time 10 "$url/v1/health" 2>/dev/null)"; then
      break
    fi
    [[ "$attempt" -lt 60 ]] || fail "worker never became healthy"
    sleep 10
  done
  echo "$health" | jq -e '.phase == "serving"' >/dev/null \
    || fail "worker is not serving: $health"

  local served expected
  served="$(echo "$health" | jq -er '.shards')"
  expected="$(jq -er '.shards | length' "$TRANSPARENT_SHARD_SOURCE/shards.json")"
  [[ "$served" -eq "$expected" ]] \
    || fail "worker serves $served shards, the published map names $expected"
  echo "serving $served shards"

  # init proves the parameters a wallet would validate are derivable, which the
  # health probe does not: health answers before any runtime has been built.
  local init
  init="$(curl --fail --silent --max-time 30 "$url/v1/shards/init")" \
    || fail "init did not answer"
  echo "$init" | jq -e '.directory_scheme and .pages_scheme and .covered_through' >/dev/null \
    || fail "init is missing scheme or coverage: $init"
  echo "$init" | jq -r '"covered_through \(.covered_through), \(.shards) shards, schema \(.schema)"'

  verify_public
}

# The public edge, over real TLS. A first deploy has to wait for Caddy to obtain
# a certificate, which needs DNS to have propagated and the ACME challenge to
# complete, so this polls rather than failing on the first refusal.
verify_public() {
  echo "== verify public edge at $TRANSPARENT_PUBLIC_URL"
  local attempt public
  for attempt in $(seq 1 30); do
    if public="$(curl --fail --silent --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/shards/init" 2>/dev/null)"; then
      break
    fi
    [[ "$attempt" -lt 30 ]] || fail "public endpoint never answered over TLS"
    sleep 10
  done
  echo "$public" | jq -e '.geometries | length > 0' >/dev/null \
    || fail "public init is not the shard service: $public"
  # Every geometry the worker serves must publish both tables' parameters. A
  # half-populated entry would be a client deriving one table against the other.
  echo "$public" \
    | jq -e 'all(.geometries[]; .name and .directory_scheme and .pages_scheme
                 and .directory_rows and .page_rows)' >/dev/null \
    || fail "public init declares an incomplete geometry: $public"

  # The edge must serve the same set as the worker, not a stale or different one.
  local public_shards
  public_shards="$(echo "$public" | jq -er '.shards')"
  [[ "$public_shards" -eq "$(jq -er '.shards | length' "$TRANSPARENT_SHARD_SOURCE/shards.json")" ]] \
    || fail "public edge serves $public_shards shards"

  # Operator surfaces must not be reachable from the internet. /v1/health is
  # served on the VPC and answered above; through the edge it must be a 404, or
  # the route is wider than it was meant to be.
  local health_status
  health_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/health")"
  [[ "$health_status" == "404" ]] \
    || fail "/v1/health is reachable publicly (HTTP $health_status); the route is too wide"

  # Same rule for the metrics and readiness surfaces, which are new. They report
  # cache occupancy, eviction and build rates -- operator information about how
  # the worker is coping, and a running commentary on how much traffic it is
  # taking. The Caddyfile allows only the routed prefixes, so this is a check
  # that the allowlist was not widened rather than a check on the binary.
  local operator_status
  for operator_path in /metrics /v1/ready; do
    operator_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      --max-time 15 "$TRANSPARENT_PUBLIC_URL$operator_path")"
    [[ "$operator_status" == "404" ]] \
      || fail "$operator_path is reachable publicly (HTTP $operator_status); the route is too wide"
  done

  # The public range filters. The service can serve these while the edge does
  # not route them, which is a silent half-deployment: the feature looks present
  # in the binary and is unreachable by any wallet. Compare the bytes against
  # the published filter rather than only checking the status, because a route
  # that reached the wrong backend would still answer 200.
  local filter_bytes published_bytes
  filter_bytes="$(curl --fail --silent --max-time 20 \
    --output /dev/null --write-out '%{size_download}' \
    "$TRANSPARENT_PUBLIC_URL/v1/filters/shards/0/filter")" \
    || fail "the public edge does not serve shard filters"
  published_bytes="$(wc -c <"$TRANSPARENT_SHARD_SOURCE/$(jq -er '.shards[0].manifest_digest' \
    "$TRANSPARENT_SHARD_SOURCE/shards.json")/filter.bin" | tr -d ' ')"
  [[ "$filter_bytes" -eq "$published_bytes" ]] \
    || fail "shard 0 filter is $filter_bytes bytes at the edge, $published_bytes as published"

  echo "public edge serving $public_shards shards; shard 0 filter $filter_bytes B; \
/v1/health, /metrics and /v1/ready correctly 404"
}

# ----------------------------------------------------------------------- main

case "$MODE" in
  validate)
    # Offline, for CI. Exercises the input checks without touching a host.
    validate_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    echo "inputs and shard set are valid"
    ;;
  preflight)
    validate_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    preflight
    preflight_capacity
    stage
    echo "preflight complete; nothing was activated"
    ;;
  deploy)
    validate_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    preflight
    preflight_capacity
    stage
    ship_shards
    trap 'rollback' ERR
    activate
    verify
    trap - ERR
    echo "deployed $TRANSPARENT_RELEASE_SHA"
    ;;
  *)
    fail "unknown mode $MODE (validate, preflight, deploy)"
    ;;
esac
