#!/usr/bin/env bash
# Build a separate journal; never stop or reconfigure a serving process.
set -euo pipefail
for name in WALLET_PIR_COORDINATOR_HOST WALLET_PIR_DEPLOY_USER ENHANCE_RELEASE_SHA ENHANCE_ARTIFACT_DIR WALLET_PIR_SSH_KEY_PATH WALLET_PIR_KNOWN_HOSTS_PATH; do
  [[ -n "${!name:-}" ]] || { echo "$name is required" >&2; exit 2; }
done
[[ "$ENHANCE_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$WALLET_PIR_COORDINATOR_HOST" =~ ^[A-Za-z0-9.-]+$ ]]
[[ "$WALLET_PIR_DEPLOY_USER" =~ ^[a-z_][a-z0-9_-]*$ ]]
ssh_options=(-i "$WALLET_PIR_SSH_KEY_PATH" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$WALLET_PIR_KNOWN_HOSTS_PATH" -o ConnectTimeout=10)
target="$WALLET_PIR_DEPLOY_USER@$WALLET_PIR_COORDINATOR_HOST"
stage="/tmp/enhance-prepare-$ENHANCE_RELEASE_SHA"
ssh "${ssh_options[@]}" "$target" mkdir -p "$stage"
(cd "$ENHANCE_ARTIFACT_DIR" && sha256sum --check --status SHA256SUMS)
scp -q "${ssh_options[@]}" "$ENHANCE_ARTIFACT_DIR/enhance-pir-server" "$target:$stage/enhance-pir-server"
digest=$(sha256sum "$ENHANCE_ARTIFACT_DIR/enhance-pir-server" | awk '{print $1}')
ssh "${ssh_options[@]}" "$target" bash -s -- "$stage" "$digest" "$ENHANCE_RELEASE_SHA" <<'REMOTE'
set -euo pipefail
as_root() { if [[ "$(id -u)" -eq 0 ]]; then "$@"; else sudo -n "$@"; fi; }
printf '%s  %s\n' "$2" "$1/enhance-pir-server" | sha256sum --check --status
chmod 0755 "$1/enhance-pir-server"
if as_root systemctl is-active --quiet enhance-pir-server && as_root systemctl show -p ExecStart --value enhance-pir-server | grep -Fq '/srv/zakura/enhance-data-v7'; then
  echo "schema 7 is already serving from the preparation directory; refusing concurrent ingestion" >&2
  exit 1
fi
as_root install -d -m 0700 /srv/zakura/enhance-data-v7
as_root rm -f /srv/zakura/enhance-data-v7/prepared-release
as_root flock -n /run/lock/enhance-schema7-prepare.lock "$1/enhance-pir-server" \
  --prepare-only --zakura-rpc-url http://127.0.0.1:8232 \
  --zakura-cookie /root/.cache/zakura/.cookie --data-dir /srv/zakura/enhance-data-v7
printf '%s\n' "$3" | as_root tee /srv/zakura/enhance-data-v7/prepared-release >/dev/null
REMOTE
