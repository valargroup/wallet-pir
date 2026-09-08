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

# ------------------------------------------------------------- jq programs
#
# Every jq program this script runs, named rather than inline, so that
# `jq-programs` mode can hand them to a checker. Two classes of bug have shipped
# here that no offline check could see, and both are the reason these are
# variables:
#
#   * A field moved. `.directory_scheme` became `.geometries[].directory_scheme`
#     when a set became able to mix geometries. verify_public was migrated;
#     verify was not, and it would have failed *after* activate.
#   * A program did not compile. `join(\" \")` inside a jq interpolation, inside
#     single-quoted bash: the backslashes reach jq literally, jq is in code
#     context where \" is not an escape, and it exits 3 -- failing a deploy
#     whose every substantive check had already passed.
#
# Note that shellcheck cannot look inside a single-quoted jq program -- it sees
# one opaque word -- and `validate` mode reaches only the map programs, so
# nothing exercised the rest.
# Hoisting them lets ops/scripts/check-jq-contracts.sh compile each one and run
# it against a payload serialized by the server's own types. The names carry the
# payload, which is how the checker knows which fixture to feed each.
#
# Keep the prefixes: JQ_MAP_ reads shards.json, JQ_HEALTH_ reads /v1/health,
# JQ_INIT_ reads /v1/shards/init from either the VPC or the public edge -- both
# serve the same document.

# shards.json, the published map. Serialized by ShardMap in
# pir/transparent-filter/src/wire.rs.
readonly JQ_MAP_SHARD_COUNT='.shards | length'
readonly JQ_MAP_DIGESTS='.shards[].manifest_digest'
readonly JQ_MAP_FIRST_DIGEST='.shards[0].manifest_digest'
readonly JQ_MAP_GEOMETRIES='[.shards[].geometry] | unique | join(",")'

# /v1/health on the VPC. An inline serde_json::json! in
# server/transparent-shard-server/src/service.rs, so nothing in the type system
# catches a rename here.
readonly JQ_HEALTH_SERVING='.phase == "serving"'
readonly JQ_HEALTH_SHARDS='.shards'

# /v1/shards/init. InitResponse and GeometryInit in the same file.
readonly JQ_INIT_COMPLETE='(.geometries | length > 0) and .covered_through and .map_sha256'
readonly JQ_INIT_GEOMETRY_NAMES='[.geometries[].name] | sort | join(",")'
readonly JQ_INIT_SHARDS='.shards'
readonly JQ_INIT_HAS_GEOMETRIES='.geometries | length > 0'
# Every geometry the worker serves must publish both tables' parameters. A
# half-populated entry would be a client deriving one table against the other.
readonly JQ_INIT_GEOMETRY_COMPLETE='all(.geometries[]; .name and .directory_scheme and .pages_scheme and .directory_rows and .page_rows)'
readonly JQ_INIT_SUMMARY='"covered_through \(.covered_through), \(.shards) shards, schema \(.schema), geometries \([.geometries[].name] | join(" "))"'

# Emitted as NUL-terminated NAME<TAB>PROGRAM records for the contract checker.
# NUL rather than newline because a program may itself span lines, and a
# line-delimited format would split one in half. Not meant to be read by eye.
# Listed explicitly, so a program added without being registered here is a
# visible omission rather than a silent gap in the check.
jq_programs() {
  local name
  for name in JQ_MAP_SHARD_COUNT JQ_MAP_DIGESTS JQ_MAP_FIRST_DIGEST JQ_MAP_GEOMETRIES \
    JQ_HEALTH_SERVING JQ_HEALTH_SHARDS \
    JQ_INIT_COMPLETE JQ_INIT_GEOMETRY_NAMES JQ_INIT_SHARDS JQ_INIT_HAS_GEOMETRIES \
    JQ_INIT_GEOMETRY_COMPLETE JQ_INIT_SUMMARY; do
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
  local set_name
  set_name="$(basename "$TRANSPARENT_SHARD_SOURCE")"
  [[ -n "$set_name" && "$set_name" != "/" && "$set_name" != "." && "$set_name" != ".." ]] \
    || fail "cannot derive a set directory name from $TRANSPARENT_SHARD_SOURCE"
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
  named="$(jq -er "$JQ_MAP_SHARD_COUNT" "$dir/shards.json")" \
    || fail "$dir/shards.json is not a readable shard map"
  [[ "$named" -gt 0 ]] || fail "$dir/shards.json names no shards"

  # Every revision the map names must be on disk. This used to compare counts
  # instead, which was the same check while a set had exactly one directory per
  # shard -- and stopped being the same check when the publisher started leaving
  # a superseded tail beside the revision that replaced it. Counting would then
  # refuse a set the server loads happily, and it would refuse it *here*, in the
  # gate that exists to keep a bad input from becoming an outage.
  # Read into an array rather than piping into `while`: a process substitution
  # is invisible to both `set -e` and `pipefail`, so a jq that failed used to
  # yield an empty stream, run the loop body zero times, leave missing=0, and
  # pass the very gate that exists to stop a bad set becoming an outage.
  local digest missing=0
  local -a digests=()
  mapfile -t digests < <(jq -er "$JQ_MAP_DIGESTS" "$dir/shards.json") \
    || fail "$dir/shards.json does not list manifest digests"
  [[ "${#digests[@]}" -eq "$named" ]] \
    || fail "$dir/shards.json names $named shards but lists ${#digests[@]} digests"
  for digest in "${digests[@]}"; do
    [[ -d "$dir/$digest" ]] || { echo "  missing revision $digest"; missing=$((missing + 1)); }
  done
  [[ "$missing" -eq 0 ]] || fail "$dir is missing $missing of the $named revisions its map names"

  # Anything else on disk is a superseded revision the worker may still be
  # answering from, which is expected rather than an error. Reported, because an
  # unbounded number of them is how a set quietly grows without the map moving.
  counted="$(find "$dir" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
  local retained=$((counted - named))
  local first_digest
  first_digest="$(jq -r "$JQ_MAP_FIRST_DIGEST // empty" "$dir/shards.json")"
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

# Where this publication's set lives on the worker.
#
# A directory per published set, named for the set it came from, rather than one
# directory rewritten in place. That is what makes a rollback coherent: the unit
# names its own set, so restoring the unit restores the pairing. Replacing the
# set in place meant a restored binary met a set of a schema it could not read,
# which is an outage rather than a rollback.
# Pure: it is used in command substitution, and `fail` inside `$(...)` exits
# only the subshell -- the caller would carry on with an empty string. The name
# is checked in validate_inputs, where an exit actually stops the deploy.
shard_set_path() {
  printf '%s/%s\n' "$TRANSPARENT_SHARD_DIR" "$(basename "$TRANSPARENT_SHARD_SOURCE")"
}

# Renders the unit for this publication and checks it, printing the path.
#
# Local, so `validate` runs it without a host. Nothing validated this file
# before `systemctl enable --now` executed it, which is the gap the Caddyfile
# has never had: a malformed ExecStart is a failed start, and a failed start is
# a rollback. `systemd-analyze verify` is deliberately not the gate -- at stage
# time the binary is not yet at the path the unit names, so it would fail every
# first deploy for a reason that is not a fault.
# Sets RENDERED_UNIT rather than printing it, because every check below calls
# `fail`, and a `fail` inside `$(render_unit)` would exit the subshell while the
# caller carried on with an empty path -- which is how a broken unit reached the
# end of a validate run reporting success.
RENDERED_UNIT=""
render_unit() {
  local set_path unit execstarts exec_line
  set_path="$(shard_set_path)"
  unit="$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service.rendered"
  sed "s|TRANSPARENT_SHARD_SET_PATH|$set_path|" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service" >"$unit"
  grep -q "TRANSPARENT_SHARD_SET_PATH" "$unit" \
    && fail "unit still carries an unsubstituted shard-set token"
  execstarts="$(grep -c '^ExecStart=' "$unit" || true)"
  [[ "$execstarts" -eq 1 ]] \
    || fail "rendered unit has $execstarts ExecStart lines, expected exactly 1"
  exec_line="$(sed -n 's/^ExecStart=//p' "$unit")"
  [[ "$exec_line" == /* ]] \
    || fail "unit ExecStart does not begin with an absolute path: $exec_line"
  grep -q -- "--shard-dir $set_path" "$unit" \
    || fail "unit does not name the shard set it is being deployed with"
  # A continuation that did not join leaves an argument stranded on its own
  # line, which systemd reads as a second directive rather than as an error.
  grep -qE '\\$' "$unit" \
    && fail "unit uses line continuations; keep ExecStart on one line"
  RENDERED_UNIT="$unit"
}

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

  render_unit
  local unit="$RENDERED_UNIT"

  worker_ssh "mkdir -p $(printf %q "$staged")"
  scp "${opts[@]}" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server" \
    "$TRANSPARENT_ARTIFACT_DIR/SHA256SUMS" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$staged/"
  scp "${opts[@]}" "$rendered" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$staged/Caddyfile"
  scp "${opts[@]}" "$unit" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$staged/unit.rendered"

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

# The set is immutable once published, so this is a plain mirror into a
# directory of its own.
#
# It used to mirror into one fixed directory with --delete, which deleted the
# running set before the new binary had proved it could serve the new one. That
# was survivable while every set shared a schema. It stopped being survivable
# when a set could be refused at load: the rollback restores the binary and the
# unit, so it would have restored a binary onto a set of a schema it cannot
# read, and left the service down rather than back.
#
# --delete still applies *within* this set's directory, so a re-run repairs a
# partial copy rather than accumulating strays. The previous set stays where it
# is, which is what the previous unit points at, which is what makes the
# rollback a rollback.
ship_shards() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  local dest
  dest="$(shard_set_path)"
  echo "== ship shard set ($TRANSPARENT_SHARD_SOURCE -> $dest)"
  worker_ssh "sudo mkdir -p $(printf %q "$dest") && sudo chown $(printf %q "$TRANSPARENT_DEPLOY_USER") $(printf %q "$dest")"
  rsync -a --delete --info=stats1 \
    -e "ssh $(ssh_opts | tr '\n' ' ')" \
    "$TRANSPARENT_SHARD_SOURCE/" \
    "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_WORKER_HOST:$dest/"

  # Sets are kept, so say what is accumulating. Nothing prunes them: deciding
  # which set is still someone's rollback target is not a decision this script
  # can make while it is the thing doing the rolling.
  worker_ssh "du -sh $(printf %q "$TRANSPARENT_SHARD_DIR")/* 2>/dev/null; df -h $(printf %q "$TRANSPARENT_SHARD_DIR") | tail -1"
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
sudo install -m 0644 "$staged/unit.rendered" "$release/transparent-shard-server.service"

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
  echo "$health" | jq -e "$JQ_HEALTH_SERVING" >/dev/null \
    || fail "worker is not serving: $health"

  local served expected
  served="$(echo "$health" | jq -er "$JQ_HEALTH_SHARDS")"
  expected="$(jq -er "$JQ_MAP_SHARD_COUNT" "$TRANSPARENT_SHARD_SOURCE/shards.json")"
  [[ "$served" -eq "$expected" ]] \
    || fail "worker serves $served shards, the published map names $expected"
  echo "serving $served shards"

  # init proves the parameters a wallet would validate are derivable, which the
  # health probe does not: health answers before any runtime has been built.
  local init
  init="$(curl --fail --silent --max-time 30 "$url/v1/shards/init")" \
    || fail "init did not answer"
  echo "$init" | jq -e "$JQ_INIT_COMPLETE" >/dev/null \
    || fail "init is missing geometries, coverage or map digest: $init"

  # The worker prepares parameters only for the geometries its set actually
  # names, so this is what catches a worker serving a *different* set than the
  # one just shipped: the shard count could match while the shapes did not.
  local served_geometries expected_geometries
  served_geometries="$(echo "$init" | jq -er "$JQ_INIT_GEOMETRY_NAMES")"
  expected_geometries="$(jq -er "$JQ_MAP_GEOMETRIES" \
    "$TRANSPARENT_SHARD_SOURCE/shards.json")"
  [[ "$served_geometries" == "$expected_geometries" ]] \
    || fail "worker serves geometries [$served_geometries], the published map names [$expected_geometries]"

  echo "$init" \
    | jq -r "$JQ_INIT_SUMMARY"

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
  echo "$public" | jq -e "$JQ_INIT_HAS_GEOMETRIES" >/dev/null \
    || fail "public init is not the shard service: $public"
  # Every geometry the worker serves must publish both tables' parameters. A
  # half-populated entry would be a client deriving one table against the other.
  echo "$public" \
    | jq -e "$JQ_INIT_GEOMETRY_COMPLETE" >/dev/null \
    || fail "public init declares an incomplete geometry: $public"

  # The edge must serve the same set as the worker, not a stale or different one.
  local public_shards
  local public_shards published_shards
  public_shards="$(echo "$public" | jq -er "$JQ_INIT_SHARDS")"
  # Read before the comparison rather than inside it. `set -e` does not apply
  # within `[[ ]]`, so a jq that failed there produced an empty string, which
  # `-eq` evaluates as 0 -- and a zero-shard map would have compared equal to a
  # zero-shard edge.
  published_shards="$(jq -er "$JQ_MAP_SHARD_COUNT" "$TRANSPARENT_SHARD_SOURCE/shards.json")" \
    || fail "cannot read the published shard count"
  [[ "$public_shards" -eq "$published_shards" ]] \
    || fail "public edge serves $public_shards shards, the published map names $published_shards"

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
  local operator_status operator_path
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
  local first_digest
  first_digest="$(jq -er "$JQ_MAP_FIRST_DIGEST" "$TRANSPARENT_SHARD_SOURCE/shards.json")" \
    || fail "cannot read the first shard's manifest digest"
  published_bytes="$(wc -c <"$TRANSPARENT_SHARD_SOURCE/$first_digest/filter.bin" | tr -d ' ')"
  [[ "$filter_bytes" -eq "$published_bytes" ]] \
    || fail "shard 0 filter is $filter_bytes bytes at the edge, $published_bytes as published"

  echo "public edge serving $public_shards shards; shard 0 filter $filter_bytes B; \
/v1/health, /metrics and /v1/ready correctly 404"
}

# ----------------------------------------------------------------------- main

case "$MODE" in
  jq-programs)
    # For ops/scripts/check-jq-contracts.sh. Prints and exits without reading
    # any input, so it is safe to call with no environment set.
    jq_programs
    ;;
  validate)
    # Offline, for CI. Exercises the input checks without touching a host.
    validate_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    # The unit is rendered and checked here too, so CI catches a malformed one
    # rather than the worker catching it after the running service has stopped.
    if [[ -r "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service" ]]; then
      render_unit
      echo "unit renders to $RENDERED_UNIT, naming $(shard_set_path)"
    else
      echo "no unit in $TRANSPARENT_ARTIFACT_DIR; skipping the unit check"
    fi
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
    # -E, without which the ERR trap below is not inherited by shell functions
    # and so never fires -- and every failure that matters happens inside
    # activate() or verify(). This is only safe now that ship_shards leaves the
    # previous set in place: arming a rollback that restores a binary onto a set
    # it cannot read would have been worse than not arming it.
    set -E
    trap 'rollback' ERR
    activate
    verify
    trap - ERR
    echo "deployed $TRANSPARENT_RELEASE_SHA"
    ;;
  *)
    fail "unknown mode $MODE (jq-programs, validate, preflight, deploy)"
    ;;
esac
