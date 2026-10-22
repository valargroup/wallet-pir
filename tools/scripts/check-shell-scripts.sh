#!/usr/bin/env bash
# Parses and lints every operations shell script with bash and shellcheck.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

failures=0

note() { printf '  %-34s %s\n' "$1" "$2"; }
bad() { note "$1" "$2"; failures=$((failures + 1)); }

command -v shellcheck >/dev/null \
  || { echo "error: shellcheck is required but not installed" >&2; exit 2; }

echo "== shell syntax and lint"
for script in tools/scripts/*.sh tools/transparent/scripts/*.sh; do
  if ! bash -n "$script"; then
    bad "$script" "does not parse"
    continue
  fi
  if ! shellcheck -x "$script"; then
    bad "$script" "shellcheck"
    continue
  fi
  note "$script" "ok"
done

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "error: $failures shell script check(s) failed" >&2
  exit 1
fi
echo
echo "all shell script checks passed"
