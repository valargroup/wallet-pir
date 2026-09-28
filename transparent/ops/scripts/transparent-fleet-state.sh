#!/usr/bin/env bash
# Sourced by deploy-transparent-shard.sh. Stable comparisons and transaction
# bookkeeping are shared with the offline rollout tests.

unit_digest() {
  sed -E 's@--assignment /opt/transparent-pir/assignments/[0-9a-f]+\.json@--assignment ASSIGNMENT@g' "$1" | sha256sum | cut -d' ' -f1
}

worker_digest() {
  "$TRANSPARENT_ARTIFACT_DIR/shard-assign" worker-digest --assignment "$TRANSPARENT_ASSIGNMENT" --worker-id "$1"
}

worker_action() { jq -r --arg id "$1" '.workers[$id].action' "$DEPLOY_PLAN"; }

# Only preserve the control paths installed by the publisher workflow. Unknown
# control configuration requires reconciliation, not a silent downgrade.
publisher_control_enabled() {
  local args=" $1 "
  if [[ "$args" != *--control-socket* && "$args" != *--active-record* ]]; then
    echo false; return
  fi
  [[ "$args" == *' --control-socket /run/transparent-pir/control.sock '* &&
     "$args" == *' --active-record /opt/transparent-publisher/active.json '* ]] \
    || fail "unrecognized publisher control arguments; reconcile before a fleet rollout"
  echo true
}

fleet_has_publisher_control() {
  [[ -n "${DEPLOY_PLAN:-}" && -r "$DEPLOY_PLAN" ]] &&
    jq -e '.publisher_control == true' "$DEPLOY_PLAN" >/dev/null
}

fleet_check_publisher_shadow() {
  # The cutover stops the old publisher before activation instead.
  [[ "${TRANSPARENT_SCHEMA_CUTOVER:-false}" == true ]] && return 0
  local config="${TRANSPARENT_PUBLISHER_CONFIG:-/opt/transparent-publisher/controller.json}"
  if fleet_has_publisher_control || [[ -e "$config" ]]; then
    if [[ ! -r "$config" ]] || ! jq -e '.shadow == true' "$config" >/dev/null; then
      fail "fixed-publication fleet rollout requires the continuous publisher to remain in shadow"
    fi
  fi
}

# Unknown or unhealthy state never qualifies for a skip.
classify_worker() {
  local ready="$1" installed="$2" wanted="$3" tools_match="$4"
  if [[ "${TRANSPARENT_FORCE_REDEPLOY:-false}" == true ]] || \
    ! jq -e --argjson installed "$installed" --argjson wanted "$wanted" '
      .ready == true and .mode == "warm" and .warm_runtimes == .target_runtimes and
      .binary_sha256 == $wanted.binary and .worker_assignment_sha256 == $wanted.assignment and
      .map_sha256 == $wanted.map and $installed.binary == $wanted.binary and
      $installed.unit == $wanted.unit and $installed.recorded_unit == $wanted.unit and
      ($installed.drop_ins // "") == ""
    ' <<<"$ready" >/dev/null 2>&1; then
    echo restart
  elif [[ "$tools_match" != true ]]; then echo tools
  else echo skip
  fi
}

fleet_diff() {
  DEPLOY_PLAN="$TRANSPARENT_ARTIFACT_DIR/deploy-plan.json"
  printf '{"workers":{},"publisher_control":false}\n' >"$DEPLOY_PLAN"
  local id host cache memory slots extra assignment_sha unit ready installed wanted action tools_match binary helper control
  assignment_sha="$(assignment_digest)"
  binary="$(sha256sum "$TRANSPARENT_ARTIFACT_DIR/transparent-shard-server" | cut -d' ' -f1)"
  helper="$(sha256sum "$TRANSPARENT_ARTIFACT_DIR/shard-prune" | cut -d' ' -f1)"
  # shellcheck disable=SC2034 # consumed by wait_ready in the calling script
  EXPECTED_BINARY_SHA256="$binary"
  for id in $(worker_ids); do
    host="$(worker_field "$id" ssh_host)"
    cache="$(jq -er --arg id "$id" '.workers[] | select(.id == $id) | .cache_bytes' "$TRANSPARENT_ASSIGNMENT")"
    memory="$(worker_field "$id" memory_max)"
    slots="$(worker_field "$id" build_slots)"; [[ "$slots" != null && -n "$slots" ]] || slots=1
    installed="$(host_ssh "$host" bash -s <<'REMOTE'
set -euo pipefail
binary="$(sha256sum /usr/local/bin/transparent-shard-server 2>/dev/null | cut -d' ' -f1 || true)"
helper="$(sha256sum /usr/local/bin/shard-prune 2>/dev/null | cut -d' ' -f1 || true)"
unit="$(sed -E 's@--assignment /opt/transparent-pir/assignments/[0-9a-f]+\.json@--assignment ASSIGNMENT@g' /etc/systemd/system/transparent-shard-server.service 2>/dev/null | sha256sum | cut -d' ' -f1 || true)"
recorded="$(cat /opt/transparent-pir/current-unit-digest 2>/dev/null || true)"
drop_ins="$(systemctl show transparent-shard-server.service -p DropInPaths --value)"
exec_start="$(sed -n 's/^ExecStart=//p' /etc/systemd/system/transparent-shard-server.service 2>/dev/null || true)"
jq -cn --arg binary "$binary" --arg helper "$helper" --arg unit "$unit" --arg recorded "$recorded" --arg drop_ins "$drop_ins" --arg exec_start "$exec_start" '{binary:$binary,helper:$helper,unit:$unit,recorded_unit:$recorded,drop_ins:$drop_ins,exec_start:$exec_start}'
REMOTE
)"
    control="$(publisher_control_enabled "$(jq -r '.exec_start' <<<"$installed")")"
    # A schema cutover moves every worker off the publisher's current
    # publication, which that publisher's binaries cannot serve. Workers get
    # plain units on the new set; the new publisher's deployment restores
    # publication control afterwards.
    if [[ "${TRANSPARENT_SCHEMA_CUTOVER:-false}" == true ]]; then control=false; fi
    extra="--assignment /opt/transparent-pir/assignments/$assignment_sha.json --worker-id $id --prune-excess --runtime-cache-dir /srv/transparent-pir/runtime-cache --runtime-cache-max-bytes $((cache * 2))"
    if [[ "$control" == true ]]; then
      extra+=" --control-socket /run/transparent-pir/control.sock --active-record /opt/transparent-publisher/active.json"
    fi
    render_unit "$cache" "$memory" "$extra" ".$id" "$slots"
    if [[ "$control" == true ]]; then
      awk '{print; if ($0 == "[Service]") print "RuntimeDirectory=transparent-pir"}' "$RENDERED_UNIT" >"$RENDERED_UNIT.next"
      mv "$RENDERED_UNIT.next" "$RENDERED_UNIT"
      if [[ "$(worker_field "$id" role)" == recent-replica ]]; then
        # Match continuous-publication upgrades: reclaim file cache while
        # leaving space for a loaded tail build and concurrent private work.
        awk '{print; if ($0 == "[Service]") print "MemoryHigh=5905580032"}' "$RENDERED_UNIT" >"$RENDERED_UNIT.next"
        mv "$RENDERED_UNIT.next" "$RENDERED_UNIT"
      elif [[ "$(worker_field "$id" role)" == archive-owner ]]; then
        awk '{print; if ($0 == "[Service]") print "MemoryHigh=51539607552"}' "$RENDERED_UNIT" >"$RENDERED_UNIT.next"
        mv "$RENDERED_UNIT.next" "$RENDERED_UNIT"
      fi
    fi
    unit="$(unit_digest "$RENDERED_UNIT")"
    wanted="$(jq -cn --arg binary "$binary" --arg unit "$unit" --arg assignment "$(worker_digest "$id")" --arg map "$EXPECTED_MAP_SHA256" '{binary:$binary,unit:$unit,assignment:$assignment,map:$map}')"
    ready="$(curl --silent --max-time 10 "http://$(worker_field "$id" upstream)/v1/ready" || true)"
    jq -e 'type == "object"' <<<"$ready" >/dev/null 2>&1 || ready='{}'
    if [[ "$control" == true ]]; then
      jq -e --arg map "$EXPECTED_MAP_SHA256" --arg assignment "$(worker_digest "$id")" \
        '.ready == true and .map_sha256 == $map and .worker_assignment_sha256 == $assignment' <<<"$ready" >/dev/null \
        || fail "publisher-controlled worker $id must be healthy on the planned publication and assignment"
    fi
    tools_match="$(jq -r --arg helper "$helper" '.helper == $helper' <<<"$installed")"
    action="$(classify_worker "$ready" "$installed" "$wanted" "$tools_match")"
    if [[ -n "${TRANSPARENT_CANARY_WORKER_IDS:-}" ]]; then
      jq -e --arg map "$EXPECTED_MAP_SHA256" '.ready == true and .map_sha256 == $map' <<<"$ready" >/dev/null \
        || fail "canary rollout requires an already healthy fleet on the same publication"
      if [[ ",$TRANSPARENT_CANARY_WORKER_IDS," != *",$id,"* ]]; then action=defer; fi
    fi
    echo "== diff $id: $action (binary, effective assignment, unit and warm readiness compared)"
    jq --arg id "$id" --arg action "$action" --argjson desired "$wanted" --argjson before "$ready" --argjson control "$control" \
      '.publisher_control = (.publisher_control or $control) | .workers[$id] = {action:$action,desired:$desired,before:$before,publisher_control:$control}' "$DEPLOY_PLAN" >"$DEPLOY_PLAN.next"
    mv "$DEPLOY_PLAN.next" "$DEPLOY_PLAN"
  done
  fleet_check_publisher_shadow
}

transaction_begin() {
  local root="${TRANSPARENT_TRANSACTION_DIR:-$HOME/.local/state/transparent-pir-deploy}"
  mkdir -p "$root"
  TRANSACTION_ID="${TRANSPARENT_RELEASE_SHA}-$(date +%s)-$$"
  TRANSACTION_FILE="$root/$TRANSACTION_ID.json"
  local before='{}' upstreams
  if [[ -n "$DEPLOY_PLAN" ]]; then before="$(jq '.workers | map_values(.before)' "$DEPLOY_PLAN")"; fi
  upstreams="$(jq '[.workers[] | {key:.id,value:.upstream}] | from_entries' "$TRANSPARENT_ASSIGNMENT")"
  jq -n --arg id "$TRANSACTION_ID" --argjson before "$before" --argjson upstreams "$upstreams" \
    '{id:$id,workers:[],router:null,status:"activating",before:$before,upstreams:$upstreams}' >"$TRANSACTION_FILE"
}

transaction_publish() {
  local root
  root="$(dirname "$TRANSACTION_FILE")"
  printf '%s\n' "$TRANSACTION_FILE" >"$root/latest.next"
  mv "$root/latest.next" "$root/latest"
}

transaction_worker() {
  local host="$1" id="$2" action="$3"
  jq --arg host "$host" --arg id "$id" --arg action "$action" \
    '.workers += [{host:$host,id:$id,action:$action}]' "$TRANSACTION_FILE" >"$TRANSACTION_FILE.next"
  mv "$TRANSACTION_FILE.next" "$TRANSACTION_FILE"
  transaction_publish
  ACTIVATED_WORKERS+=("$host")
}

# Safe concurrency is group-specific. Existing unhealthy members consume the
# unavailable allowance even if they are not part of this rollout.
replica_batch_limit() {
  local total="$1" healthy="$2" fresh="$3"
  if [[ "$fresh" == true ]]; then echo 2; return; fi
  local limit=$((healthy - (total + 1) / 2))
  (( limit <= 2 )) || limit=2
  if [[ "${TRANSPARENT_REPLICA_ACTIVATION:-paired}" == serial && "$limit" -gt 1 ]]; then limit=1; fi
  echo "$limit"
}

# Commands remain simple calls so errexit is not accidentally disabled by an
# enclosing conditional. Failures are recorded by the transaction EXIT trap.
timed_phase() {
  local phase="$1"; shift
  local started=$SECONDS
  "$@"
  local elapsed=$((SECONDS - started))
  printf '%s\t%s\n' "$phase" "$elapsed" >>"$TRANSPARENT_ARTIFACT_DIR/deploy-timings.tsv"
  echo "== timing $phase: ${elapsed}s"
}

fleet_verify_workers() {
  local id ready before
  for id in $(worker_ids); do
    if [[ "$(worker_action "$id")" == defer ]]; then
      ready="$(curl --fail --silent --max-time 10 "http://$(worker_field "$id" upstream)/v1/ready")"
      before="$(jq -c --arg id "$id" '.workers[$id].before' "$DEPLOY_PLAN")"
      jq -e --argjson before "$before" '.ready == true and .map_sha256 == $before.map_sha256 and
        .assignment_sha256 == $before.assignment_sha256 and .binary_sha256 == $before.binary_sha256' <<<"$ready" >/dev/null \
        || fail "deferred worker $id changed during canary rollout"
    else
      wait_ready "$id" "$(worker_field "$id" upstream)" "$(assignment_digest)"
    fi
  done
}
