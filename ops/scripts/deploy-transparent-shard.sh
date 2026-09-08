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
ACTIVATED_WORKERS=()
ROUTER_ACTIVATED=0
DEPLOY_PLAN=""
TRANSACTION_ID=""
TRANSACTION_FILE=""
EXPECTED_BINARY_SHA256=""
# shellcheck source=ops/scripts/transparent-fleet-state.sh
source "$(dirname "${BASH_SOURCE[0]}")/transparent-fleet-state.sh"

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

readonly JQ_HEALTH_ASSIGNED='.assigned_shards'

# /v1/ready on the VPC. What the fleet deploy asserts before a worker is put
# back in rotation: which map and assignment it runs under, and whether every
# assigned runtime is warm. Nullable fields are defaulted so the contract
# check, which requires a truthy result, passes against the whole-set fixture.
readonly JQ_READY_OK='.ready == true'
readonly JQ_READY_MAP='.map_sha256'
readonly JQ_READY_ASSIGNMENT='.assignment_sha256 // ""'
readonly JQ_READY_WARM='"\(.warm_runtimes)/\(.target_runtimes) runtimes warm, mode \(.mode)"'
readonly JQ_READY_BINARY='.binary_sha256'
readonly JQ_READY_WORKER_ASSIGNMENT='.worker_assignment_sha256 // ""'
readonly JQ_READY_REASON='.reason // "ready"'

# /v1/shards/init. InitResponse and GeometryInit in the same file.
readonly JQ_INIT_COMPLETE='(.geometries | length > 0) and .covered_through and .map_sha256'
readonly JQ_INIT_MAP='.map_sha256'
readonly JQ_INIT_ASSIGNED='.assigned_shards'
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
    JQ_HEALTH_SERVING JQ_HEALTH_SHARDS JQ_HEALTH_ASSIGNED \
    JQ_READY_OK JQ_READY_MAP JQ_READY_ASSIGNMENT JQ_READY_WARM JQ_READY_REASON JQ_READY_BINARY JQ_READY_WORKER_ASSIGNMENT \
    JQ_INIT_COMPLETE JQ_INIT_GEOMETRY_NAMES JQ_INIT_SHARDS JQ_INIT_HAS_GEOMETRIES \
    JQ_INIT_MAP JQ_INIT_ASSIGNED \
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
[[ -z "$(systemctl show transparent-shard-server.service -p DropInPaths --value)" ]] || { echo "unmanaged service drop-ins require reconciliation" >&2; exit 1; }
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
# Single-host pilot budgets. A fleet worker gets its roster's values instead.
PILOT_CACHE_BYTES=8589934592
PILOT_MEMORY_MAX=12G

RENDERED_UNIT=""
render_unit() {
  # Optional overrides, for a fleet worker: cache bytes, MemoryMax, extra
  # arguments, and the file name to render to.
  local cache_bytes="${1:-$PILOT_CACHE_BYTES}"
  local memory_max="${2:-$PILOT_MEMORY_MAX}"
  local extra_args="${3:-}"
  local suffix="${4:-}"
  local build_slots="${5:-1}"
  local set_path unit execstarts exec_line
  set_path="$(shard_set_path)"
  unit="$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service${suffix}.rendered"
  [[ "$cache_bytes" =~ ^[0-9]+$ ]] || fail "cache bytes must be a number: $cache_bytes"
  [[ "$memory_max" =~ ^[0-9]+[KMGT]?$ ]] || fail "MemoryMax must be a systemd size: $memory_max"
  [[ "$build_slots" =~ ^[1-9][0-9]?$ ]] || fail "build slots must be a small positive number: $build_slots"
  # Quoted regex: an unquoted `[... ]` with a space inside is parsed as two
  # words. Letters, digits, space, dot, underscore, slash, equals, dash.
  local plain='^[-A-Za-z0-9 ._/=]*$'
  [[ "$extra_args" =~ $plain ]] || fail "extra arguments contain characters the unit must not carry"
  sed -e "s|TRANSPARENT_SHARD_SET_PATH|$set_path|" \
    -e "s|TRANSPARENT_CACHE_BYTES|$cache_bytes|" \
    -e "s|TRANSPARENT_MEMORY_MAX|$memory_max|" \
    -e "s|TRANSPARENT_BUILD_SLOTS|$build_slots|" \
    -e "s|TRANSPARENT_EXTRA_ARGS|$extra_args|" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service" \
    | sed -e 's/[[:space:]]*$//' >"$unit"
  grep -q "TRANSPARENT_SHARD_SET_PATH\|TRANSPARENT_CACHE_BYTES\|TRANSPARENT_MEMORY_MAX\|TRANSPARENT_BUILD_SLOTS\|TRANSPARENT_EXTRA_ARGS" "$unit" \
    && fail "unit still carries an unsubstituted token"
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
  [[ "$action" == restart ]] || return 0
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

# ---------------------------------------------------------------------- fleet
#
# Many workers and a router, from one assignment. The assignment is generated
# by `shard-assign plan` in the workflow from the published set and the
# roster, and every worker reports its digest in /v1/ready, so the deploy can
# assert that each host runs the assignment it shipped. Order matters and is
# fixed: every host is prepared and its subset verified with the staged binary
# before any running service is stopped; archive owners activate before recent
# replicas; each worker is polled until it is warm; the router switches last;
# the public edge is verified through the router; pruning runs after that.
#
# The single-host modes above are unchanged: this is what the pilot grows into,
# not a replacement for it.

fleet_json() { printf '%s' "$TRANSPARENT_FLEET_JSON"; }

validate_fleet_inputs() {
  [[ "${TRANSPARENT_REPLICA_ACTIVATION:-paired}" =~ ^(paired|serial)$ ]] || fail "replica activation must be paired or serial"
  [[ "${TRANSPARENT_OWNER_ACTIVATION:-serial}" =~ ^(parallel|serial)$ ]] || fail "owner activation must be parallel or serial"
  [[ "${TRANSPARENT_FORCE_REDEPLOY:-false}" =~ ^(true|false)$ ]] || fail "force redeploy must be true or false"

  require_env TRANSPARENT_FLEET_JSON TRANSPARENT_ASSIGNMENT TRANSPARENT_DEPLOY_USER \
    TRANSPARENT_RELEASE_SHA TRANSPARENT_ARTIFACT_DIR TRANSPARENT_SHARD_DIR \
    TRANSPARENT_PUBLIC_URL TRANSPARENT_SHARD_SOURCE
  [[ "$TRANSPARENT_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] \
    || fail "TRANSPARENT_RELEASE_SHA must be a full commit SHA"
  [[ "$TRANSPARENT_PUBLIC_URL" =~ ^https://[A-Za-z0-9.-]+$ ]] \
    || fail "TRANSPARENT_PUBLIC_URL must be https:// and a bare host"
  [[ "$TRANSPARENT_SHARD_DIR" = /* ]] || fail "TRANSPARENT_SHARD_DIR must be absolute"
  [[ -f "$TRANSPARENT_ASSIGNMENT" ]] || fail "assignment $TRANSPARENT_ASSIGNMENT does not exist"
  jq -e 'type == "array" and length > 0' <<<"$(fleet_json)" >/dev/null \
    || fail "TRANSPARENT_FLEET_JSON must be a nonempty array"
  if [[ -n "${TRANSPARENT_CANARY_WORKER_IDS:-}" ]]; then
    [[ "$TRANSPARENT_CANARY_WORKER_IDS" =~ ^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$ ]] || fail "canary IDs must be comma-separated worker IDs"
    local canary
    for canary in ${TRANSPARENT_CANARY_WORKER_IDS//,/ }; do
      jq -e --arg id "$canary" 'any(.[]; .id == $id)' <<<"$(fleet_json)" >/dev/null || fail "unknown canary worker $canary"
    done
  fi
  local id host
  while IFS=$'\t' read -r id host; do
    [[ "$id" =~ ^[A-Za-z0-9_-]+$ ]] || fail "worker id $id is not a plain name"
    [[ "$host" =~ ^[A-Za-z0-9._-]+$ ]] || fail "worker $id ssh_host is not a plain host"
    jq -e --arg id "$id" '.workers[] | select(.id == $id)' "$TRANSPARENT_ASSIGNMENT" >/dev/null \
      || fail "roster names worker $id but the assignment does not"
  done < <(jq -r '.[] | "\(.id)\t\(.ssh_host)"' <<<"$(fleet_json)")
  if [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]]; then
    [[ "$TRANSPARENT_ROUTER_HOST" =~ ^[A-Za-z0-9._-]+$ ]] || fail "TRANSPARENT_ROUTER_HOST is not a plain host"
  fi
  if command -v "$TRANSPARENT_ARTIFACT_DIR/shard-assign" >/dev/null; then
    "$TRANSPARENT_ARTIFACT_DIR/shard-assign" check --shard-dir "$TRANSPARENT_SHARD_SOURCE" \
      --assignment "$TRANSPARENT_ASSIGNMENT" || fail "the assignment does not describe $TRANSPARENT_SHARD_SOURCE"
  fi
  ASSIGNMENT_SHA256="$(sha256sum "$TRANSPARENT_ASSIGNMENT" | cut -d' ' -f1)"
  EXPECTED_MAP_SHA256="$(jq -er '.set.map_sha256' "$TRANSPARENT_ASSIGNMENT")" \
    || fail "the assignment carries no map digest"
  echo "assignment file $ASSIGNMENT_SHA256 for map $EXPECTED_MAP_SHA256"
}
ASSIGNMENT_SHA256=""
EXPECTED_MAP_SHA256=""

# The assignment's own digest, as the worker computes it over the canonical
# (compact) serialization, is what /v1/ready reports; the file digest above
# only names the artifact. Both are recorded.
assignment_digest() {
  "$TRANSPARENT_ARTIFACT_DIR/shard-assign" check --shard-dir "$TRANSPARENT_SHARD_SOURCE" \
    --assignment "$TRANSPARENT_ASSIGNMENT" | awk '{print $1}'
}

host_ssh() {
  local host="$1"; shift
  local -a opts
  mapfile -t opts < <(ssh_opts)
  # shellcheck disable=SC2029
  ssh "${opts[@]}" "$TRANSPARENT_DEPLOY_USER@$host" "$@"
}

worker_field() { jq -r --arg id "$1" ".[] | select(.id == \$id) | .$2" <<<"$(fleet_json)"; }
worker_ids() { jq -r '.[].id' <<<"$(fleet_json)"; }
# Archive owners first, then replicas, which is the activation order.
worker_ids_in_activation_order() {
  jq -r '(map(select(.role == "archive-owner")) + map(select(.role != "archive-owner"))) | .[].id' <<<"$(fleet_json)"
}

# MiB of the files one worker receives, summed from the set's own directory.
subset_size_mib() {
  local id="$1" total=0 size entry
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    size="$(du -sm "$TRANSPARENT_SHARD_SOURCE/${entry%/}" 2>/dev/null | cut -f1)"
    total=$((total + ${size:-0}))
  done < <("$TRANSPARENT_ARTIFACT_DIR/shard-assign" files --shard-dir "$TRANSPARENT_SHARD_SOURCE" \
    --assignment "$TRANSPARENT_ASSIGNMENT" --worker-id "$id")
  echo "$total"
}

fleet_preflight() {
  local id host
  for id in $(worker_ids); do
    host="$(worker_field "$id" ssh_host)"
    [[ "$(worker_action "$id")" == restart ]] || continue
    echo "== preflight $id ($host)"
    host_ssh "$host" bash -s <<'REMOTE'
set -euo pipefail
[[ "$(uname -m)" == "x86_64" ]] || { echo "worker is not x86_64" >&2; exit 1; }
for tool in curl jq sha256sum systemctl rsync; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 1; }
done
sudo -n true || { echo "passwordless sudo is required" >&2; exit 1; }
[[ -z "$(systemctl show transparent-shard-server.service -p DropInPaths --value)" ]] || { echo "unmanaged service drop-ins require reconciliation" >&2; exit 1; }
free -g | awk '/^Mem:/ {print "memory: " $2 " GiB total, " $7 " GiB available"}'
REMOTE
    # Capacity against this worker's own file list, not the whole set.
    local need have
    need="$(subset_size_mib "$id")"
    have="$(host_ssh "$host" "df -Pm $(printf %q "$(dirname "$TRANSPARENT_SHARD_DIR")") | awk 'NR==2 {print \$4}'")"
    echo "$id needs ${need} MiB; has ${have} MiB free"
    (( have > need * 2 + 1024 )) || fail "$id has too little space for its subset"
  done
  if [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]]; then
    echo "== preflight router ($TRANSPARENT_ROUTER_HOST)"
    host_ssh "$TRANSPARENT_ROUTER_HOST" bash -s <<'REMOTE'
set -euo pipefail
for tool in caddy curl systemctl; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 1; }
done
systemctl is-enabled caddy >/dev/null 2>&1 || { echo "caddy is not enabled" >&2; exit 1; }
sudo -n true || { echo "passwordless sudo is required" >&2; exit 1; }
REMOTE
  fi
}

# Stages binary, unit, assignment and this worker's subset on every host, and
# the router's Caddyfile on the router. Nothing is activated.
fleet_prepare() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA"
  local assignment_sha
  assignment_sha="$(assignment_digest)"
  [[ "$assignment_sha" =~ ^[0-9a-f]{64}$ ]] || fail "could not compute the assignment digest"
  # One worker's staging and copy. Every worker's runs at once below: the
  # copies are independent and the coordinator's disk and NIC, not any one
  # worker, bound the total.
  prepare_worker() {
    local id="$1" host dest
  host="$(worker_field "$id" ssh_host)"
  local action
  action="$(worker_action "$id")"
  [[ "$action" == restart || "$action" == tools ]] || return 0
  local unit="$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server.service.$id.rendered"
  echo "== stage $id ($host): binary, tools, unit, assignment"
  host_ssh "$host" "mkdir -p $(printf %q "$staged")"
  scp "${opts[@]}" \
    "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server" \
    "$TRANSPARENT_ARTIFACT_DIR/shard-prune" \
    "$TRANSPARENT_ARTIFACT_DIR/SHA256SUMS" \
    "$TRANSPARENT_ASSIGNMENT" \
    "$TRANSPARENT_DEPLOY_USER@$host:$staged/"
  scp "${opts[@]}" "$unit" "$TRANSPARENT_DEPLOY_USER@$host:$staged/unit.rendered"
  # The unit names the assignment by its digest; the staged copy carries
  # that name too, so the verify-only run below and the install find it.
  host_ssh "$host" bash -s -- "$staged" "$assignment_sha" <<'REMOTE'
set -euo pipefail
cd "$1"
sha256sum -c SHA256SUMS --ignore-missing
chmod 0755 transparent-shard-server shard-prune
cp -f assignment.json "$2.json"
REMOTE
  dest="$(shard_set_path)"
  echo "== ship $id subset ($TRANSPARENT_SHARD_SOURCE -> $host:$dest)"
  host_ssh "$host" "sudo mkdir -p $(printf %q "$dest") && sudo chown $(printf %q "$TRANSPARENT_DEPLOY_USER") $(printf %q "$dest")"
  local list
  list="$(mktemp)"
  "$TRANSPARENT_ARTIFACT_DIR/shard-assign" files --shard-dir "$TRANSPARENT_SHARD_SOURCE" \
    --assignment "$TRANSPARENT_ASSIGNMENT" --worker-id "$id" >"$list"
  # -r explicitly: --files-from does not imply recursion into the listed
  # directories. No --delete: the previous set stays where its unit points.
  rsync -a -r --files-from="$list" --info=stats1 \
    -e "ssh $(ssh_opts | tr '\n' ' ')" \
    "$TRANSPARENT_SHARD_SOURCE/" \
    "$TRANSPARENT_DEPLOY_USER@$host:$dest/"
  rm -f "$list"
  }
  local -a pids=() ids=()
  local id log
  for id in $(worker_ids); do
    log="$(mktemp)"
    prepare_worker "$id" >"$log" 2>&1 &
    pids+=($!); ids+=("$id:$log")
  done
  local i failed=0
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then failed=1; fi
    cat "${ids[$i]#*:}"; rm -f "${ids[$i]#*:}"
  done
  [[ "$failed" -eq 0 ]] || fail "staging or copying failed on at least one worker; see above"
  fleet_stage_router
}

# Renders the router's Caddyfile from the assignment, stages it on the router
# and validates it there; nothing is installed.
fleet_stage_router() {
  local -a opts
  mapfile -t opts < <(ssh_opts)
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA"
  if [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]]; then
    local host_name="${TRANSPARENT_PUBLIC_URL#https://}"
    local caddyfile="$TRANSPARENT_ARTIFACT_DIR/Caddyfile.router.rendered"
    # The internal listener carries the same routes over plain HTTP on the
    # router's VPC address, for the load harness and verification from the
    # coordinator before the public name points at the router.
    local -a internal=()
    [[ -n "${TRANSPARENT_ROUTER_INTERNAL_PORT:-}" ]] \
      && internal=(--internal-listen "$TRANSPARENT_ROUTER_HOST:$TRANSPARENT_ROUTER_INTERNAL_PORT")
    "$TRANSPARENT_ARTIFACT_DIR/shard-assign" caddyfile --assignment "$TRANSPARENT_ASSIGNMENT" \
      --public-host "$host_name" "${internal[@]}" --out "$caddyfile"
    grep -q "^$host_name {" "$caddyfile" || fail "rendered router config does not name $host_name"
    echo "== stage router ($TRANSPARENT_ROUTER_HOST): Caddyfile"
    host_ssh "$TRANSPARENT_ROUTER_HOST" "mkdir -p $(printf %q "$staged")"
    scp "${opts[@]}" "$caddyfile" "$TRANSPARENT_DEPLOY_USER@$TRANSPARENT_ROUTER_HOST:$staged/Caddyfile"
    host_ssh "$TRANSPARENT_ROUTER_HOST" bash -s -- "$staged" "${TRANSPARENT_CANARY_WORKER_IDS:-}" <<'REMOTE'
set -euo pipefail
if [[ -n "$2" ]]; then
  diff -q <(sudo sed '/^[[:space:]]*#/d' /etc/caddy/Caddyfile) <(sed '/^[[:space:]]*#/d' "$1/Caddyfile") >/dev/null \
    || { echo "canary rollout requires unchanged routing" >&2; exit 1; }
fi
sudo caddy validate --config "$1/Caddyfile" --adapter caddyfile
if sudo test -r /etc/caddy/Caddyfile; then
  echo "router config changes (live -> staged):"
  sudo diff -u /etc/caddy/Caddyfile "$1/Caddyfile" || true
fi
REMOTE
  fi
}

# The staged binary loads and verifies each worker's subset under its own
# unit's arguments while the running service is untouched. This is the gate
# between "copied" and "activated": a partial copy, a wrong assignment or a
# budget the assignment does not fit all stop here.
fleet_verify_prepared() {
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA"
  verify_worker() {
    local id="$1" host
    host="$(worker_field "$id" ssh_host)"
    [[ "$(worker_action "$id")" == restart ]] || return 0
    echo "== verify prepared $id ($host)"
    host_ssh "$host" bash -s -- "$staged" <<'REMOTE'
set -euo pipefail
staged="$1"
exec_line="$(sed -n 's/^ExecStart=//p' "$staged/unit.rendered")"
# The unit's own arguments, with the binary and the assignment path pointed at
# the staged copies, so what is verified is what will run.
args="${exec_line#* }"
args="${args//\/opt\/transparent-pir\/assignments\//$staged/}"
# shellcheck disable=SC2086
report="$("$staged/transparent-shard-server" $args --verify-only)"
echo "$report"
if jq -e '.runtime_cache != null' <<<"$report" >/dev/null; then
  need="$(jq -r '.runtime_cache | .missing_bytes + .temporary_bytes' <<<"$report")"
  read -r total available < <(df -PB1 /srv/transparent-pir/runtime-cache | awk 'NR==2 {print $2, $4}')
  (( available - need >= total / 5 )) || { echo "runtime cache would violate 20% filesystem headroom" >&2; exit 1; }
fi
REMOTE
  }
  # Every worker verifies at once; each reads its own disk.
  local -a pids=() logs=()
  local id log
  for id in $(worker_ids); do
    log="$(mktemp)"
    verify_worker "$id" >"$log" 2>&1 &
    pids+=($!); logs+=("$log")
  done
  local i failed=0
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then failed=1; fi
    cat "${logs[$i]}"; rm -f "${logs[$i]}"
  done
  [[ "$failed" -eq 0 ]] || fail "verification failed on at least one worker; see above"
}

# Polls a worker's readiness over the VPC until it reports ready under the
# expected map and assignment, or gives up.
# Polls a worker's readiness over the VPC until it reports ready under the
# expected map and assignment. Gives up when the warm-up stops making
# progress for `stall` polls, or after `attempts` polls in all: an archive
# owner's warm-up is minutes per host and varies between hosts, and a fixed
# cap gave up on one at 135 of 160 runtimes.
wait_ready() {
  local id="$1" upstream="$2" expect_assignment="$3" attempts="${4:-720}" stall="${5:-30}"
  local attempt ready warm last_warm="" stalled=0
  for attempt in $(seq 1 "$attempts"); do
    if ready="$(curl --silent --max-time 10 "http://$upstream/v1/ready" 2>/dev/null)" && [[ -n "$ready" ]]; then
      if echo "$ready" | jq -e "$JQ_READY_OK" >/dev/null 2>&1; then
        local map assignment
        map="$(echo "$ready" | jq -er "$JQ_READY_MAP")"
        assignment="$(echo "$ready" | jq -er "$JQ_READY_ASSIGNMENT")"
        [[ "$map" == "$EXPECTED_MAP_SHA256" ]] \
          || fail "$id is ready under map $map, expected $EXPECTED_MAP_SHA256"
        local effective binary
        effective="$(echo "$ready" | jq -er "$JQ_READY_WORKER_ASSIGNMENT")"
        binary="$(echo "$ready" | jq -er "$JQ_READY_BINARY")"
        [[ "$effective" == "$(worker_digest "$id")" && "$binary" == "$EXPECTED_BINARY_SHA256" ]] \
          || fail "$id is ready under an unexpected worker assignment or binary"
        echo "$id assignment document $assignment (candidate $expect_assignment)"
        echo "$id ready: $(echo "$ready" | jq -r "$JQ_READY_WARM")"
        return 0
      fi
      warm="$(echo "$ready" | jq -r "$JQ_READY_WARM")"
      if [[ "$warm" == "$last_warm" ]]; then stalled=$((stalled + 1)); else stalled=0; last_warm="$warm"; fi
      [[ $((attempt % 6)) -eq 0 ]] && echo "$id: $(echo "$ready" | jq -r "$JQ_READY_REASON"), $warm"
      [[ "$stalled" -lt "$stall" ]] || fail "$id stopped warming at $warm for $((stall * 10)) s"
    fi
    sleep 10
  done
  fail "$id never became ready after $((attempts * 10)) s"
}

fleet_activate_workers() {
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA" assignment_sha
  assignment_sha="$(assignment_digest)"
  activate_worker() {
    local id="$1" host upstream action unit_sha started=$SECONDS
    host="$(worker_field "$id" ssh_host)"
    upstream="$(worker_field "$id" upstream)"
    action="$(worker_action "$id")"
    unit_sha="$(jq -r --arg id "$id" '.workers[$id].desired.unit' "$DEPLOY_PLAN")"
    echo "== activate $id ($host): $action"
    host_ssh "$host" bash -s -- "$staged" "$TRANSPARENT_RELEASE_SHA" "$assignment_sha" "$TRANSACTION_ID" "$action" "$unit_sha" <<'REMOTE'
set -euo pipefail
staged="$1"; sha="$2"; assignment_sha="$3"; transaction="$4"; action="$5"; unit_sha="$6"
release="/opt/transparent-pir/releases/$sha"
backup="/opt/transparent-pir/transactions/$transaction"
sudo mkdir -p "$release" "$backup" /opt/transparent-pir/assignments
# Complete the snapshot before the first mutation; a failed snapshot is never
# interpreted as an empty previous installation during rollback.
for path in /usr/local/bin/transparent-shard-server /usr/local/bin/shard-prune \
  /etc/systemd/system/transparent-shard-server.service /opt/transparent-pir/current-release /opt/transparent-pir/current-unit-digest; do
  if sudo test -f "$path"; then sudo cp -p "$path" "$backup/$(basename "$path")"; fi
done
systemctl is-active transparent-shard-server.service >"$staged/previous-active" || true
systemctl is-enabled transparent-shard-server.service >"$staged/previous-enabled" || true
sudo cp "$staged/previous-active" "$staged/previous-enabled" "$backup/"
printf '%s\n' "$action" | sudo tee "$backup/action" >/dev/null
sudo touch "$backup/touched"
sudo install -m 0755 "$staged/shard-prune" "$release/"
sudo install -m 0755 "$release/shard-prune" /usr/local/bin/shard-prune
if [[ "$action" == tools ]]; then exit 0; fi
sudo install -m 0755 "$staged/transparent-shard-server" "$release/"
sudo install -m 0644 "$staged/unit.rendered" "$release/transparent-shard-server.service"
sudo install -m 0644 "$staged/$assignment_sha.json" "/opt/transparent-pir/assignments/$assignment_sha.json"
sudo systemctl stop transparent-shard-server.service 2>/dev/null || true
sudo install -m 0755 "$release/transparent-shard-server" /usr/local/bin/transparent-shard-server.next
sudo mv -f /usr/local/bin/transparent-shard-server.next /usr/local/bin/transparent-shard-server
sudo install -m 0644 "$release/transparent-shard-server.service" /etc/systemd/system/transparent-shard-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now transparent-shard-server.service
printf '%s\n' "$sha" | sudo tee /opt/transparent-pir/current-release >/dev/null
printf '%s\n' "$unit_sha" | sudo tee /opt/transparent-pir/current-unit-digest >/dev/null
REMOTE
    wait_ready "$id" "$upstream" "$assignment_sha"
    host_ssh "$host" "rm -rf $(printf %q "$staged")"
    printf 'worker:%s\t%s\n' "$id" "$((SECONDS - started))" >>"$TRANSPARENT_ARTIFACT_DIR/deploy-timings.tsv"
  }
  run_batch() {
    local id host log i failed=0
    local -a pids=() logs=()
    for id in "$@"; do
      host="$(worker_field "$id" ssh_host)"
      transaction_worker "$host" "$id" "$(worker_action "$id")"
      log="$(mktemp)"
      (trap - EXIT; activate_worker "$id") >"$log" 2>&1 &
      pids+=($!); logs+=("$log")
    done
    for i in "${!pids[@]}"; do
      if ! wait "${pids[$i]}"; then failed=1; fi
      cat "${logs[$i]}"; rm -f "${logs[$i]}"
    done
    [[ "$failed" -eq 0 ]] || fail "activation batch failed"
  }
  local id group ready healthy total limit fresh action
  local -a owners=() pending=() batch=()
  for id in $(worker_ids_in_activation_order); do
    action="$(worker_action "$id")"
    [[ "$action" == restart || "$action" == tools ]] || continue
    if [[ "$action" == tools ]]; then run_batch "$id"
    elif [[ "$(worker_field "$id" role)" == archive-owner ]]; then owners+=("$id")
    fi
  done
  if [[ "${TRANSPARENT_OWNER_ACTIVATION:-serial}" == parallel ]]; then
    run_batch "${owners[@]}"
  else
    for id in "${owners[@]}"; do run_batch "$id"; done
  fi
  for group in $(jq -r '[.workers[] | select(.role == "recent-replica") | .replica_group] | unique[]' "$TRANSPARENT_ASSIGNMENT"); do
    pending=(); total=0; fresh=true
    for id in $(jq -r --arg group "$group" '.workers[] | select(.replica_group == $group) | .id' "$TRANSPARENT_ASSIGNMENT"); do
      total=$((total + 1))
      [[ "$(worker_action "$id")" != restart ]] || pending+=("$id")
      # First activation is established by the absence of an installed binary,
      # never by an unhealthy fleet being mistaken for a fresh one.
      if host_ssh "$(worker_field "$id" ssh_host)" test -x /usr/local/bin/transparent-shard-server; then fresh=false; fi
    done
    while [[ "${#pending[@]}" -gt 0 ]]; do
      healthy=0
      for id in $(jq -r --arg group "$group" '.workers[] | select(.replica_group == $group) | .id' "$TRANSPARENT_ASSIGNMENT"); do
        ready="$(curl --silent --max-time 10 "http://$(worker_field "$id" upstream)/v1/ready" || true)"
        if jq -e '.ready == true and .mode == "warm" and .warm_runtimes == .target_runtimes' <<<"$ready" >/dev/null 2>&1; then healthy=$((healthy + 1)); fi
      done
      limit="$(replica_batch_limit "$total" "$healthy" "$fresh")"
      [[ "$limit" -gt 0 ]] || fail "replica group $group has $healthy/$total healthy; cannot preserve its serving floor"
      batch=("${pending[@]:0:limit}")
      run_batch "${batch[@]}"
      pending=("${pending[@]:limit}")
      # Caddy probes every 5 s and remembers passive failures for 30 s. Allow
      # both to settle before retiring another pair; also probe its public route.
      sleep "${TRANSPARENT_ROUTER_SETTLE_SECONDS:-35}"
      if [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]]; then
        curl --fail --silent --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/shards/init" >/dev/null
      fi
    done
  done
}

fleet_activate_router() {
  [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]] || { echo "no router host; skipping"; return 0; }
  local staged="/tmp/transparent-pir-$TRANSPARENT_RELEASE_SHA"
  echo "== activate router ($TRANSPARENT_ROUTER_HOST)"
  if host_ssh "$TRANSPARENT_ROUTER_HOST" bash -s -- "$staged" <<'REMOTE'
set -euo pipefail
# The renderer emits only whole-line audit comments; directives stay exact.
diff -q <(sudo sed '/^[[:space:]]*#/d' /etc/caddy/Caddyfile) <(sed '/^[[:space:]]*#/d' "$1/Caddyfile") >/dev/null
REMOTE
  then
    echo "router unchanged; skipping reload"
    return 0
  fi
  [[ -n "$TRANSACTION_ID" ]] || transaction_begin
  jq --arg host "$TRANSPARENT_ROUTER_HOST" '.router = $host' "$TRANSACTION_FILE" >"$TRANSACTION_FILE.next"
  mv "$TRANSACTION_FILE.next" "$TRANSACTION_FILE"
  transaction_publish
  ROUTER_ACTIVATED=1
  host_ssh "$TRANSPARENT_ROUTER_HOST" bash -s -- "$staged" "$TRANSACTION_ID" <<'REMOTE'
set -euo pipefail
staged="$1"
backup="/opt/transparent-pir/transactions/$2"
sudo mkdir -p "$backup"
if sudo test -f /etc/caddy/Caddyfile; then sudo cp -p /etc/caddy/Caddyfile "$backup/Caddyfile"; fi
sudo touch "$backup/router-touched"
sudo caddy validate --config "$staged/Caddyfile" --adapter caddyfile
sudo install -m 0644 "$staged/Caddyfile" /etc/caddy/Caddyfile
sudo systemctl reload caddy || sudo systemctl restart caddy
rm -rf "$staged"
REMOTE
}

# Through the router, over TLS: the set identity, one private setup per
# worker's first assigned shard, a manifest that digests to its path, and the
# operator routes refused. Over the VPC: a request to a worker for a shard it
# does not own is a 421, which is what proves the router is doing the routing.
# The internal listener answers the same as the public edge; checked from the
# coordinator, which the router's firewall admits.
fleet_verify_internal() {
  [[ -n "${TRANSPARENT_ROUTER_HOST:-}" && -n "${TRANSPARENT_ROUTER_INTERNAL_PORT:-}" ]] || return 0
  local base="http://$TRANSPARENT_ROUTER_HOST:$TRANSPARENT_ROUTER_INTERNAL_PORT"
  echo "== verify internal listener at $base"
  local map
  map="$(curl --silent --fail --max-time 20 "$base/v1/shards" | sha256sum | cut -d' ' -f1)" \
    || fail "the internal listener did not serve the map"
  [[ "$map" == "$EXPECTED_MAP_SHA256" ]] || fail "internal listener serves map $map, expected $EXPECTED_MAP_SHA256"
  curl --silent --fail --max-time 20 "$base/v1/shards/init" | jq -e '.schema' >/dev/null \
    || fail "the internal listener did not serve init"
  echo "internal listener serving map $map"
}

fleet_verify_public() {
  echo "== verify public edge at $TRANSPARENT_PUBLIC_URL"
  local attempt public
  for attempt in $(seq 1 30); do
    if public="$(curl --fail --silent --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/shards/init" 2>/dev/null)"; then
      break
    fi
    [[ "$attempt" -lt 30 ]] || fail "public endpoint never answered over TLS"
    sleep 10
  done
  local public_map
  public_map="$(echo "$public" | jq -er "$JQ_INIT_MAP")" || fail "public init carries no map digest"
  [[ "$public_map" == "$EXPECTED_MAP_SHA256" ]] \
    || fail "the edge serves map $public_map, the assignment was made for $EXPECTED_MAP_SHA256"
  local operator_path operator_status
  for operator_path in /v1/health /metrics /v1/ready; do
    operator_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      --max-time 15 "$TRANSPARENT_PUBLIC_URL$operator_path")"
    [[ "$operator_status" == "404" ]] \
      || fail "$operator_path is reachable publicly (HTTP $operator_status); the route is too wide"
  done
  local id first digest status upstream other other_digest
  for id in $(worker_ids); do
    first="$(jq -er --arg id "$id" '.workers[] | select(.id == $id) | .shards[0] // empty' "$TRANSPARENT_ASSIGNMENT")"
    [[ -n "$first" ]] || continue
    digest="$(jq -er --argjson id "$first" '.shards[] | select(.shard_id == $id) | .manifest_digest' "$TRANSPARENT_SHARD_SOURCE/shards.json")"
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 60 \
      "$TRANSPARENT_PUBLIC_URL/v1/shards/$first/revisions/$digest/setup/directory/0")"
    [[ "$status" == "200" ]] || fail "setup for shard $first (owned by $id) is HTTP $status through the edge"
    local served
    served="$(curl --fail --silent --max-time 15 "$TRANSPARENT_PUBLIC_URL/v1/shards/$first/revisions/$digest/manifest" | sha256sum | cut -d' ' -f1)"
    [[ "$served" == "$digest" ]] || fail "manifest for shard $first digests to $served, not $digest"
    # The worker's own view over the VPC: it holds exactly the shards the
    # assignment gives it.
    upstream="$(jq -er --arg id "$id" '.workers[] | select(.id == $id) | .upstream' "$TRANSPARENT_ASSIGNMENT")"
    local assigned expected_assigned
    assigned="$(curl --fail --silent --max-time 15 "http://$upstream/v1/health" | jq -er "$JQ_HEALTH_ASSIGNED")" \
      || fail "$id health does not report an assigned shard count"
    expected_assigned="$(jq -er --arg id "$id" '.workers[] | select(.id == $id) | .shards | length' "$TRANSPARENT_ASSIGNMENT")"
    [[ "$assigned" -eq "$expected_assigned" ]] \
      || fail "$id holds $assigned shards, the assignment gives it $expected_assigned"
    local init_assigned
    init_assigned="$(curl --fail --silent --max-time 15 "http://$upstream/v1/shards/init" | jq -er "$JQ_INIT_ASSIGNED")" \
      || fail "$id init does not report an assigned shard count"
    [[ "$init_assigned" -eq "$expected_assigned" ]] \
      || fail "$id init reports $init_assigned assigned shards, health reports $assigned"
    # A shard this worker does not own, asked of it directly.
    other="$(jq -er --arg id "$id" '[.workers[] | select(.id != $id) | .shards[]] | map(select(. as $s | ($ARGS.named.mine | index($s)) == null)) | .[0] // empty' \
      --argjson mine "$(jq -c --arg id "$id" '.workers[] | select(.id == $id) | .shards' "$TRANSPARENT_ASSIGNMENT")" "$TRANSPARENT_ASSIGNMENT")"
    if [[ -n "$other" ]]; then
      other_digest="$(jq -er --argjson id "$other" '.shards[] | select(.shard_id == $id) | .manifest_digest' "$TRANSPARENT_SHARD_SOURCE/shards.json")"
      status="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 15 \
        "http://$upstream/v1/shards/$other/revisions/$other_digest/setup/directory/0")"
      [[ "$status" == "421" ]] || fail "$id answered HTTP $status for unassigned shard $other; expected 421"
    fi
    echo "$id: shard $first served through the edge; unassigned shard refused directly"
  done
  echo "public edge serves map $public_map; operator routes 404"
}

fleet_prune() {
  local id host
  for id in $(worker_ids); do
    host="$(worker_field "$id" ssh_host)"
    [[ "$(worker_action "$id")" == restart ]] || continue
    echo "== prune $id ($host)"
    host_ssh "$host" bash -s -- "$(shard_set_path)" "$id" "$TRANSACTION_ID" <<'REMOTE'
set -euo pipefail
set_path="$1"; id="$2"
# From the ExecStart line only: the unit's comments mention the flag too.
assignment="$(sed -n '/^ExecStart=/s/.*--assignment \([^ ]*\).*/\1/p' /etc/systemd/system/transparent-shard-server.service)"
[[ -n "$assignment" && "$assignment" != *$'\n'* ]] || { echo "could not read the assignment path from the unit" >&2; exit 1; }
# Keep every retained revision in the current and rollback publication.
old_unit="/opt/transparent-pir/transactions/$3/transparent-shard-server.service"
keep=(--runtime-cache-prune-set "$set_path")
if sudo test -f "$old_unit"; then
  old_set="$(sudo sed -n '/^ExecStart=/s/.*--shard-dir \([^ ]*\).*/\1/p' "$old_unit")"
  [[ -n "$old_set" ]] || { echo "rollback unit has no shard set" >&2; exit 1; }
  keep+=(--runtime-cache-prune-set "$old_set")
fi
# A shared physical set may still be needed under the previous assignment.
# Defer plaintext pruning in that case rather than weakening rollback retention.
if [[ "${old_set:-}" != "$set_path" ]]; then
sudo /usr/local/bin/shard-prune --shard-dir "$set_path" --assignment "$assignment" --worker-id "$id" --apply \
  | jq -r '"\(.deleted | length) removed, \(.bytes_freed) bytes freed"'
fi
sudo /usr/local/bin/transparent-shard-server --runtime-cache-dir /srv/transparent-pir/runtime-cache "${keep[@]}"
REMOTE
  done
}

rollback_fleet() {
  echo "== rollback transaction $TRANSACTION_ID" >&2
  local host failed=0
  if [[ "$ROUTER_ACTIVATED" -eq 1 && -n "${TRANSPARENT_ROUTER_HOST:-}" ]]; then
    host_ssh "$TRANSPARENT_ROUTER_HOST" bash -s -- "$TRANSACTION_ID" <<'REMOTE' || failed=1
set -euo pipefail
backup="/opt/transparent-pir/transactions/$1"
sudo test -f "$backup/router-touched" || exit 0
if sudo test -f "$backup/Caddyfile"; then
  sudo install -m 0644 "$backup/Caddyfile" /etc/caddy/Caddyfile
  sudo systemctl reload caddy
else
  sudo rm -f /etc/caddy/Caddyfile
fi
REMOTE
  fi
  for host in "${ACTIVATED_WORKERS[@]}"; do
    host_ssh "$host" bash -s -- "$TRANSACTION_ID" <<'REMOTE' || failed=1
set -euo pipefail
backup="/opt/transparent-pir/transactions/$1"
sudo test -f "$backup/touched" || exit 0
action="$(sudo cat "$backup/action")"
if [[ "$action" != tools ]]; then sudo systemctl stop transparent-shard-server.service || true; fi
for path in /usr/local/bin/transparent-shard-server /usr/local/bin/shard-prune \
  /etc/systemd/system/transparent-shard-server.service /opt/transparent-pir/current-release /opt/transparent-pir/current-unit-digest; do
  [[ "$action" != tools || "$path" == /usr/local/bin/shard-prune ]] || continue
  name="$(basename "$path")"
  if sudo test -f "$backup/$name"; then sudo cp -p "$backup/$name" "$path"; else sudo rm -f "$path"; fi
done
if [[ "$action" != tools ]]; then
  sudo systemctl daemon-reload
  if [[ "$(sudo cat "$backup/previous-enabled")" == enabled ]]; then
    sudo systemctl enable transparent-shard-server.service
  else
    sudo systemctl disable transparent-shard-server.service || true
  fi
  if [[ "$(sudo cat "$backup/previous-active")" == active ]]; then
    sudo systemctl start transparent-shard-server.service
  fi
fi
REMOTE
  done
  # A rollback is not complete merely because systemctl returned successfully.
  local id upstream expected ready attempt restored
  for host in "${ACTIVATED_WORKERS[@]}"; do
    id="$(jq -r --arg host "$host" '.workers[] | select(.host == $host) | .id' "$TRANSACTION_FILE")"
    expected="$(jq -c --arg id "$id" '.before[$id] // {}' "$TRANSACTION_FILE")"
    if ! jq -e '.ready == true' <<<"$expected" >/dev/null; then continue; fi
    upstream="$(jq -r --arg id "$id" '.upstreams[$id]' "$TRANSACTION_FILE")"
    restored=0
    for attempt in $(seq 1 720); do
      ready="$(curl --silent --max-time 10 "http://$upstream/v1/ready" || true)"
      if jq -e --argjson old "$expected" '.ready == true and .map_sha256 == $old.map_sha256 and .assignment_sha256 == $old.assignment_sha256 and (.binary_sha256 == $old.binary_sha256)' <<<"$ready" >/dev/null 2>&1; then restored=1; break; fi
      sleep 10
    done
    [[ "$restored" -eq 1 ]] || { echo "rollback readiness failed for $id" >&2; failed=1; }
  done
  if [[ -n "$TRANSACTION_FILE" ]]; then
    jq --arg status "$([[ "$failed" == 0 ]] && echo rolled-back || echo rollback-failed)" '.status = $status' "$TRANSACTION_FILE" >"$TRANSACTION_FILE.next"
    mv "$TRANSACTION_FILE.next" "$TRANSACTION_FILE"
  fi
  [[ "$failed" -eq 0 ]]
}

fleet_rollback_all() {
  local root="${TRANSPARENT_TRANSACTION_DIR:-$HOME/.local/state/transparent-pir-deploy}"
  TRANSACTION_FILE="$(cat "$root/latest")"
  TRANSACTION_ID="$(jq -er '.id' "$TRANSACTION_FILE")"
  [[ "$(jq -r '.status' "$TRANSACTION_FILE")" != rolled-back ]] || fail "latest deployment is already rolled back"
  TRANSPARENT_ROUTER_HOST="$(jq -r '.router // ""' "$TRANSACTION_FILE")"
  [[ -z "$TRANSPARENT_ROUTER_HOST" ]] || ROUTER_ACTIVATED=1
  mapfile -t ACTIVATED_WORKERS < <(jq -r '.workers[].host' "$TRANSACTION_FILE")
  rollback_fleet
}

# ----------------------------------------------------------------------- main

case "$MODE" in
  library) ;;
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
  fleet-validate)
    validate_fleet_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    echo "fleet inputs, assignment and shard set are valid"
    ;;
  fleet-preflight)
    validate_fleet_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    timed_phase fleet_diff fleet_diff
    timed_phase fleet_preflight fleet_preflight
    timed_phase fleet_prepare fleet_prepare
    timed_phase fleet_verify_prepared fleet_verify_prepared
    echo "fleet preflight complete; every subset verified with the staged binary; nothing was activated"
    ;;
  fleet-deploy)
    validate_fleet_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    timed_phase fleet_diff fleet_diff
    timed_phase fleet_preflight fleet_preflight
    timed_phase fleet_prepare fleet_prepare
    timed_phase fleet_verify_prepared fleet_verify_prepared
    set -E
    transaction_begin
    trap 'rc=$?; if (( rc != 0 )); then trap - EXIT; rollback_fleet; fi' EXIT
    timed_phase fleet_activate_workers fleet_activate_workers
    timed_phase fleet_activate_router fleet_activate_router
    timed_phase fleet_verify_internal fleet_verify_internal
    timed_phase fleet_verify_public fleet_verify_public
    timed_phase fleet_verify_workers fleet_verify_workers
    jq '.status = "complete"' "$TRANSACTION_FILE" >"$TRANSACTION_FILE.next"
    mv "$TRANSACTION_FILE.next" "$TRANSACTION_FILE"
    trap - EXIT
    timed_phase fleet_prune fleet_prune
    echo "deployed $TRANSPARENT_RELEASE_SHA to the fleet under assignment $(assignment_digest)"
    ;;
  fleet-router)
    # The router alone: a changed Caddyfile (a new listener, a changed
    # assignment that moved no shard) without touching the warm workers.
    validate_fleet_inputs
    validate_shard_set "$TRANSPARENT_SHARD_SOURCE"
    [[ -n "${TRANSPARENT_ROUTER_HOST:-}" ]] || fail "fleet-router needs TRANSPARENT_ROUTER_HOST"
    fleet_stage_router
    set -E
    trap 'rc=$?; if (( rc != 0 )); then trap - EXIT; rollback_fleet; fi' EXIT
    timed_phase fleet_activate_router fleet_activate_router
    timed_phase fleet_verify_internal fleet_verify_internal
    trap - EXIT
    echo "router configured for assignment $(assignment_digest)"
    ;;
  fleet-rollback)
    validate_fleet_inputs
    fleet_rollback_all
    echo "fleet rolled back to the previously activated release"
    ;;
  *)
    fail "unknown mode $MODE (jq-programs, validate, preflight, deploy, fleet-validate, fleet-preflight, fleet-deploy, fleet-router, fleet-rollback)"
    ;;
esac
