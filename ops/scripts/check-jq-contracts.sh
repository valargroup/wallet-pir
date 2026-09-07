#!/usr/bin/env bash
# Compiles every jq program the deploy scripts run, and evaluates the ones whose
# payload we can serialize offline.
#
# Two bugs shipped in one evening that this exists to catch:
#
#   * A field moved. `.directory_scheme` became `.geometries[].directory_scheme`
#     when a set became able to mix geometries. `verify_public` was migrated,
#     `verify` was not, and it would have failed *after* activate() with the
#     rollback trap armed.
#   * A program stopped compiling. `join(\" \")` inside a jq interpolation,
#     inside single-quoted bash: the backslashes reached jq literally, jq exited
#     3, and a deploy whose every substantive check had passed was reported as a
#     failure.
#
# Neither was reachable before. `shellcheck` sees a single-quoted jq program as
# one opaque word, and `validate` mode runs only the three programs that read
# shards.json -- none of the nine that read a served document.
#
# The programs come from the scripts themselves, via their `jq-programs` mode,
# and the payloads come from `ops/fixtures/`, written by
# server/transparent-shard-server/tests/operator_payloads.rs from the server's
# own types. Neither side transcribes the other, so a rename cannot leave one of
# them agreeing with a stale copy of the other.
#
# One check covers both bugs: `jq -e PROGRAM FIXTURE` exits 3 when the program
# will not compile and 1 when the result is null or false. Every one of these
# programs is truthy against a correct payload -- a renamed field makes
# `... and .map_sha256` null, and `all(.geometries[]; .name and ...)` false --
# so the invariant is simply that it exits 0.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

FIXTURES="ops/fixtures/transparent-shard"
failures=0

note() { printf '  %-34s %s\n' "$1" "$2"; }
bad() { note "$1" "$2"; failures=$((failures + 1)); }

for tool in jq shellcheck; do
  command -v "$tool" >/dev/null \
    || { echo "error: $tool is required but not installed" >&2; exit 2; }
done

echo "== shell syntax and lint"
for script in ops/scripts/*.sh; do
  if ! bash -n "$script"; then
    bad "$script" "does not parse"
    continue
  fi
  if ! shellcheck "$script"; then
    bad "$script" "shellcheck"
    continue
  fi
  note "$script" "ok"
done

# The fixture a program is run against is decided by its name. That is what the
# prefixes in the deploy script are for.
fixture_for() {
  case "$1" in
    JQ_MAP_*) echo "$FIXTURES/shards.json" ;;
    JQ_HEALTH_*) echo "$FIXTURES/health.json" ;;
    JQ_INIT_*) echo "$FIXTURES/init.json" ;;
    *) return 1 ;;
  esac
}

echo "== ops/scripts/deploy-transparent-shard.sh: compile and evaluate"
while IFS=$'\t' read -r -d '' name program; do
  if ! fixture="$(fixture_for "$name")"; then
    bad "$name" "no fixture: name it JQ_MAP_*, JQ_HEALTH_* or JQ_INIT_*"
    continue
  fi
  [[ -f "$fixture" ]] || {
    bad "$name" "missing $fixture -- run the operator_payloads test"
    continue
  }
  if output="$(jq -e "$program" "$fixture" 2>&1 >/dev/null)"; then
    note "$name" "ok against $(basename "$fixture")"
  else
    status=$?
    case "$status" in
      3) bad "$name" "does not compile: ${output//$'\n'/ }" ;;
      1) bad "$name" "evaluates to null or false against $(basename "$fixture") -- the payload no longer carries what it reads" ;;
      *) bad "$name" "jq exited $status: ${output//$'\n'/ }" ;;
    esac
  fi
done < <(ops/scripts/deploy-transparent-shard.sh jq-programs)

# The enhance programs read the coordinator's payloads, and a fixture for those
# means standing up enhance-pir-server. So they are compiled and not evaluated,
# which still covers the quoting class -- two of them carry inner quotes that had
# to be backslashed inside an ssh command string. Field-path drift there remains
# uncovered, deliberately and visibly.
echo "== ops/scripts/deploy-enhance-pir.sh: compile only (no fixtures yet)"
while IFS=$'\t' read -r -d '' name program; do
  # A superset of the jq variables these programs take. An undefined $var is a
  # compile error, and an unused one is harmless, so one set serves all of them.
  if output="$(jq -n \
      --arg coordinator x \
      --argjson enhance '[]' \
      --argjson old '{}' \
      --argjson new '{}' \
      --argjson expected 0 \
      "$program" 2>&1 >/dev/null)"; then
    note "$name" "compiles"
  else
    status=$?
    if [[ "$status" -eq 3 ]]; then
      bad "$name" "does not compile: ${output//$'\n'/ }"
    else
      # Anything else is the program objecting to the dummy input, which says
      # nothing about whether it compiled.
      note "$name" "compiles"
    fi
  fi
done < <(ops/scripts/deploy-enhance-pir.sh jq-programs)

# The published fixture is a real ShardMap, so the script's own offline gate
# should accept it -- CI's hand-written one carries no `geometry` key and only
# happened to pass. `validate_shard_set` also wants a directory per revision the
# map names, so those are staged here rather than committed: their names are
# manifest digests, which change with the payload, and a tree full of churning
# empty directories would be worse than the check is good.
echo "== ops/scripts/deploy-transparent-shard.sh validate, against the real fixture"
staged="$(mktemp -d)"
trap 'rm -rf "$staged"' EXIT
cp "$FIXTURES/shards.json" "$staged/"
while read -r digest; do
  mkdir -p "$staged/$digest"
done < <(jq -er "$(ops/scripts/deploy-transparent-shard.sh jq-programs \
  | tr '\0' '\n' | awk -F'\t' '$1 == "JQ_MAP_DIGESTS" { print $2 }')" "$FIXTURES/shards.json")

if TRANSPARENT_WORKER_HOST=worker.example.net \
  TRANSPARENT_DEPLOY_USER=deploy \
  TRANSPARENT_RELEASE_SHA=0000000000000000000000000000000000000000 \
  TRANSPARENT_ARTIFACT_DIR=/tmp/artifact \
  TRANSPARENT_SHARD_DIR=/srv/transparent-pir/shards \
  TRANSPARENT_PUBLIC_URL=https://transparent.example.net \
  TRANSPARENT_SHARD_SOURCE="$staged" \
  ops/scripts/deploy-transparent-shard.sh validate >/dev/null; then
  note "validate" "accepts the published fixture"
else
  bad "validate" "rejected the fixture the server itself published"
fi

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "error: $failures ops contract check(s) failed" >&2
  exit 1
fi
echo
echo "all ops contract checks passed"
