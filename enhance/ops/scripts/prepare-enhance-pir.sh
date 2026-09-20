#!/usr/bin/env bash
# Build a separate journal; never stop or reconfigure a serving process.
#
# Schema 8 widens the PIR row to 29 records. Its journal manifest, shard digests
# and worker artifacts are all incompatible with the schema-7 nine-record data
# the fleet is serving, so this prepares into its own directory and leaves the
# live -v7 directory untouched: that directory is the rollback data.
set -euo pipefail
for name in WALLET_PIR_COORDINATOR_HOST WALLET_PIR_DEPLOY_USER ENHANCE_RELEASE_SHA ENHANCE_ARTIFACT_DIR WALLET_PIR_SSH_KEY_PATH WALLET_PIR_KNOWN_HOSTS_PATH; do
  [[ -n "${!name:-}" ]] || { echo "$name is required" >&2; exit 2; }
done
[[ "$ENHANCE_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$WALLET_PIR_COORDINATOR_HOST" =~ ^[A-Za-z0-9.-]+$ ]]
[[ "$WALLET_PIR_DEPLOY_USER" =~ ^[a-z_][a-z0-9_-]*$ ]]

# Must match deploy-enhance-pir.sh and enhance_pir::types.
ENHANCE_DATA_DIR="${ENHANCE_DATA_DIR:-/srv/zakura/enhance-data-r29}"
ENHANCE_TARGET_RECORDS_PER_ROW="${ENHANCE_TARGET_RECORDS_PER_ROW:-29}"
[[ "$ENHANCE_DATA_DIR" =~ ^/[A-Za-z0-9._/-]+$ ]]
[[ "$ENHANCE_TARGET_RECORDS_PER_ROW" =~ ^[0-9]+$ ]]

ssh_options=(-i "$WALLET_PIR_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$WALLET_PIR_KNOWN_HOSTS_PATH" -o ConnectTimeout=10)
target="$WALLET_PIR_DEPLOY_USER@$WALLET_PIR_COORDINATOR_HOST"
stage="/tmp/enhance-prepare-$ENHANCE_RELEASE_SHA"
ssh "${ssh_options[@]}" "$target" mkdir -p "$stage"
(cd "$ENHANCE_ARTIFACT_DIR" && sha256sum --check --status SHA256SUMS)
scp -q "${ssh_options[@]}" "$ENHANCE_ARTIFACT_DIR/enhance-pir-server" "$target:$stage/enhance-pir-server"
digest=$(sha256sum "$ENHANCE_ARTIFACT_DIR/enhance-pir-server" | awk '{print $1}')
ssh "${ssh_options[@]}" "$target" bash -s -- "$stage" "$digest" "$ENHANCE_RELEASE_SHA" \
  "$ENHANCE_DATA_DIR" "$ENHANCE_TARGET_RECORDS_PER_ROW" <<'REMOTE'
set -euo pipefail
stage="$1"; digest="$2"; release="$3"; data_dir="$4"; records_per_row="$5"
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
printf '%s  %s\n' "$digest" "$stage/enhance-pir-server" | sha256sum --check --status
chmod 0755 "$stage/enhance-pir-server"
# Refuse to ingest into whatever directory a service is currently serving from,
# whichever directory that happens to be. The former version of this check
# compared against one hardcoded path, which silently stopped protecting
# anything the moment the serving directory changed.
if as_root systemctl is-active --quiet enhance-pir-server; then
  serving="$(as_root systemctl show -p ExecStart --value enhance-pir-server)"
  if grep -Fq -- "--data-dir $data_dir " <<<"$serving " ; then
    echo "enhance-pir-server is already serving from $data_dir; refusing concurrent ingestion" >&2
    exit 1
  fi
fi
as_root install -d -m 0700 "$data_dir"
# Clear any stale receipt first: a receipt must never outlive the ingestion that
# earned it, or an interrupted prepare would look complete.
as_root rm -f "$data_dir/prepared-release"
as_root flock -n /run/lock/enhance-layout-prepare.lock "$stage/enhance-pir-server" \
  --prepare-only --zakura-rpc-url http://127.0.0.1:8232 \
  --zakura-cookie /root/.cache/zakura/.cookie --data-dir "$data_dir"
# The prepared journal must carry the layout this release serves before the
# receipt is written. `--prepare-only` refuses an incompatible journal outright,
# so this mainly catches a data_dir that was prepared by some other release.
prepared_rpr="$(as_root jq -r '.records_per_row // 0' "$data_dir/enhance/manifest.json")"
[[ "$prepared_rpr" == "$records_per_row" ]] || {
  echo "prepared journal has $prepared_rpr records per row, expected $records_per_row" >&2
  exit 1
}
# The receipt binds release AND layout; deploy-enhance-pir.sh requires both.
printf '%s records-per-row=%s\n' "$release" "$records_per_row" |
  as_root tee "$data_dir/prepared-release" >/dev/null
REMOTE
