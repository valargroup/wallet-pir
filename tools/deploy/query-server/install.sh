#!/usr/bin/env bash
# Install or upgrade the unified Wallet PIR query node on this Linux host:
#
#   curl -fsSL https://wallet-pir.valargroup.dev/install | sudo bash
#   curl -fsSL https://wallet-pir.valargroup.dev/install | sudo bash -s -- --domain pir.example.org
#
# One configuration: Enhance, Status, Transparent history, transaction details
# and Receiver in one pir-query-server process, synchronizing publisher-prepared
# material over HTTPS. Nothing is prompted. Every run ends with one
# `wallet-pir-install:` line on stdout; see docs/query-server.md.
set -euo pipefail

# Bumped after each `query-server-v*` release (.github/workflows/release-query-server.yml).
RELEASE_TAG="query-server-v0.1.5"
ARCHIVE_SHA256="c007ed06b1945ebbbd674210b4ded2a789df8a903f9226c46e661bb88a27de11"

REPOSITORY="valargroup/wallet-pir"
PUBLISHER="https://enhance-pir.valargroup.dev"
RECEIVER_PUBLISHER="https://receiver-pir.valargroup.dev"
ENHANCE_INIT="https://wallet-pir.valargroup.dev/enhance/v1/init"
MIN_CPUS=16
MIN_MEMORY_BYTES=$((92 * 1024 * 1024 * 1024))
MIN_DISK_BYTES=500000000000
# Room left outside the service's hard memory limit for the OS and Caddy.
MEMORY_RESERVE_GIB=12

INSTALL_ROOT=/opt/wallet-pir-query
STATE_ROOT=/srv/wallet-pir-query
CONFIG_DIR=/etc/wallet-pir-query
CONFIG="$CONFIG_DIR/query-server.json"
UNIT=wallet-pir-query.service
UNIT_PATH="/etc/systemd/system/$UNIT"
# A fleet-provisioned node mounts its volume with this unit; its deployment
# tooling owns it, so this installer refuses to touch it.
FLEET_VOLUME_UNIT=/etc/systemd/system/wallet-pir-query-data.service
MANAGEMENT=127.0.0.1:18081
PUBLIC=127.0.0.1:8090
CADDY_MARKER="# Managed by the Wallet PIR query node installer."

# Tests point the hardware probes at a fake host.
PROC_ROOT="${WALLET_PIR_INSTALL_PROC:-/proc}"
SYS_ROOT="${WALLET_PIR_INSTALL_SYS:-/sys}"

CHECK_ONLY=0
DRY_RUN=0
WAIT_READY=0
DOMAIN=""
VERSION=""
FAILURES=()

usage() {
  cat <<'EOF'
Usage: install.sh [--check-only] [--dry-run] [--domain NAME] [--version TAG] [--wait-ready]

  --check-only   check platform and hardware, change nothing (no root needed)
  --dry-run      run every check and print the changes instead of making them
  --domain NAME  also install Caddy and serve wallets over HTTPS at NAME
                 (its DNS must already point here, ports 80 and 443 open)
  --version TAG  install another query-server-v* release than the pinned one
  --wait-ready   wait (up to 3 hours) until the node serves every product

Requires Linux x86-64-v3, systemd, root, 16 vCPUs, 92 GiB RAM and 500 GB disk
for /srv/wallet-pir-query. Re-running upgrades the binary and configuration.
Exit codes: 0 ok, 2 usage, 3 hardware, 4 platform or prerequisite,
5 download or verification, 6 configuration check or start.
EOF
}

say() { printf 'wallet-pir-install: %s\n' "$*" >&2; }

die() {
  local class="$1" code="$2"
  shift 2
  printf 'wallet-pir-install: error %s: %s\n' "$class" "$*"
  exit "$code"
}

# Runs a change, or prints it under --dry-run.
run() {
  if ((DRY_RUN)); then
    printf '+ %s\n' "$*" >&2
  else
    "$@"
  fi
}

# Writes stdin to a file atomically, or prints its path under --dry-run.
write_file() {
  local path="$1" mode="$2"
  if ((DRY_RUN)); then
    printf '+ write %s\n' "$path" >&2
    cat >/dev/null
    return
  fi
  install -d -m 0755 "$(dirname "$path")"
  cat >"$path.new"
  chmod "$mode" "$path.new"
  mv -f "$path.new" "$path"
}

parse_args() {
  while (($#)); do
    case "$1" in
      --check-only) CHECK_ONLY=1 ;;
      --dry-run) DRY_RUN=1 ;;
      --wait-ready) WAIT_READY=1 ;;
      --domain)
        (($# > 1)) || die usage 2 "--domain needs a host name"
        DOMAIN="$2"
        shift
        ;;
      --version)
        (($# > 1)) || die usage 2 "--version needs a release tag"
        VERSION="$2"
        shift
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *) die usage 2 "unknown option $1 (see --help)" ;;
    esac
    shift
  done
  if [[ -n "$DOMAIN" && ! "$DOMAIN" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]]; then
    die usage 2 "--domain must be a plain host name, got $DOMAIN"
  fi
  if [[ -n "$VERSION" && ! "$VERSION" =~ ^query-server-v[0-9A-Za-z.+-]+$ ]]; then
    die usage 2 "--version must be a query-server-v* tag, got $VERSION"
  fi
}

check_platform() {
  local system machine
  system="$(uname -s)"
  machine="$(uname -m)"
  [[ "$system" == Linux ]] || die platform 4 "this installer supports Linux only, found $system"
  [[ "$machine" == x86_64 || "$machine" == amd64 ]] || die platform 4 "the release is built for x86_64, found $machine"
  # The release targets x86-64-v3 with carry-less multiplication.
  local flags missing=() flag
  flags=" $(awk -F: '/^flags/ { print $2; exit }' "$PROC_ROOT/cpuinfo") "
  for flag in avx avx2 bmi1 bmi2 f16c fma abm movbe xsave pclmulqdq; do
    [[ "$flags" == *" $flag "* ]] || missing+=("$flag")
  done
  ((${#missing[@]} == 0)) || die platform 4 "CPU lacks x86-64-v3 features the release needs: ${missing[*]}"
  if ((!CHECK_ONLY)); then
    command -v systemctl >/dev/null || die platform 4 "systemd is required"
    (($(id -u) == 0)) || die platform 4 "run as root (curl ... | sudo bash)"
  fi
}

# The cgroup files that apply to this process, one path per line.
cgroup_files() {
  local name="$1" _ controllers relpath
  while IFS=: read -r _ controllers relpath; do
    if [[ -z "$controllers" ]]; then
      printf '%s\n' "$SYS_ROOT/fs/cgroup/${relpath#/}/$name" "$SYS_ROOT/fs/cgroup/$name"
    elif [[ "$name" == memory.max && ",$controllers," == *,memory,* ]]; then
      printf '%s\n' "$SYS_ROOT/fs/cgroup/memory/${relpath#/}/memory.limit_in_bytes"
    elif [[ "$name" == cpu.max && ",$controllers," == *,cpu,* ]]; then
      printf '%s\n' "$SYS_ROOT/fs/cgroup/cpu/${relpath#/}/cpu.cfs_quota_us"
    fi
  done <"$PROC_ROOT/self/cgroup"
}

effective_cpus() {
  local cpus file quota period limit
  cpus="$(nproc)"
  while read -r file; do
    [[ -f "$file" ]] || continue
    read -r quota period <"$file" || true
    if [[ "$file" == *cfs_quota_us ]]; then
      period="$(cat "${file%cfs_quota_us}cfs_period_us" 2>/dev/null || echo 100000)"
    fi
    [[ "$quota" =~ ^[0-9]+$ && "$period" =~ ^[0-9]+$ && "$period" -gt 0 ]] || continue
    limit=$((quota / period))
    ((limit < cpus)) && cpus="$limit"
  done < <(cgroup_files cpu.max)
  printf '%s\n' "$cpus"
}

effective_memory_bytes() {
  local memory file limit
  memory="$(awk '/^MemTotal:/ { print $2 * 1024; exit }' "$PROC_ROOT/meminfo")"
  while read -r file; do
    [[ -f "$file" ]] || continue
    limit="$(tr -d '\n' <"$file")"
    [[ "$limit" =~ ^[0-9]+$ ]] || continue
    ((limit < memory)) && memory="$limit"
  done < <(cgroup_files memory.max)
  printf '%s\n' "$memory"
}

disk_bytes() {
  local path="$STATE_ROOT"
  while [[ ! -e "$path" ]]; do
    path="$(dirname "$path")"
  done
  df -PB1 "$path" | awk 'NR == 2 { gsub(/B$/, "", $2); print $2 }'
}

gib() { awk -v b="$1" 'BEGIN { printf "%.1f GiB", b / 1073741824 }'; }

check_hardware() {
  local cpus memory disk
  cpus="$(effective_cpus)"
  memory="$(effective_memory_bytes)"
  disk="$(disk_bytes)"
  [[ "$cpus" =~ ^[0-9]+$ && "$memory" =~ ^[0-9]+$ && "$disk" =~ ^[0-9]+$ ]] ||
    die platform 4 "cannot read this host's CPU count, memory or disk size"
  ((cpus >= MIN_CPUS)) || FAILURES+=("found $cpus vCPUs, need at least $MIN_CPUS")
  ((memory >= MIN_MEMORY_BYTES)) || FAILURES+=("found $(gib "$memory") of RAM available to this host, need at least $(gib "$MIN_MEMORY_BYTES")")
  ((disk >= MIN_DISK_BYTES)) || FAILURES+=("the filesystem for $STATE_ROOT holds $((disk / 1000000000)) GB, need at least $((MIN_DISK_BYTES / 1000000000)) GB")
  if ((${#FAILURES[@]})); then
    local failure
    for failure in "${FAILURES[@]}"; do
      say "requirement not met: $failure"
    done
    die hardware 3 "${#FAILURES[@]} hardware requirement(s) not met; this host cannot run the query node"
  fi
  MEMORY_BYTES="$memory"
  say "hardware ok: $cpus vCPUs, $(gib "$memory") RAM, $((disk / 1000000000)) GB for $STATE_ROOT"
}

ensure_tools() {
  local missing=() tool
  for tool in curl tar sha256sum jq; do
    command -v "$tool" >/dev/null || missing+=("$tool")
  done
  [[ -z "$DOMAIN" ]] || command -v caddy >/dev/null || missing+=(caddy)
  ((${#missing[@]})) || return 0
  command -v apt-get >/dev/null || die prerequisite 4 "install ${missing[*]} and re-run (no apt-get here)"
  say "installing ${missing[*]}"
  run env DEBIAN_FRONTEND=noninteractive apt-get update -qq || die prerequisite 4 "apt-get update failed"
  run env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${missing[@]}" ||
    die prerequisite 4 "apt-get install ${missing[*]} failed"
}

fetch() { curl --fail --silent --show-error --location --proto '=https' --max-time "$1" "${@:2}"; }

check_sources() {
  local discovery init
  discovery="$(fetch 15 "$PUBLISHER/.well-known/pir-service-discovery")" ||
    die prerequisite 4 "publisher $PUBLISHER is unreachable"
  jq -e '.service == "pir-publisher"' >/dev/null <<<"$discovery" ||
    die prerequisite 4 "$PUBLISHER did not answer as a PIR publisher"
  # A combined Enhance role must hold every shard of the current publication.
  init="$(fetch 30 "$ENHANCE_INIT")" || die prerequisite 4 "cannot read the Enhance shard list from $ENHANCE_INIT"
  SHARDS="$(jq -ce '[.coverage.shards[].id] | select(length > 0)' <<<"$init")" ||
    die prerequisite 4 "$ENHANCE_INIT lists no Enhance shards"
  fetch 15 "$RECEIVER_PUBLISHER/.well-known/pir-service-discovery" >/dev/null ||
    die prerequisite 4 "Receiver publisher $RECEIVER_PUBLISHER is unreachable"
  say "publishers ok; Enhance shards $SHARDS"
}

resolve_release() {
  TAG="${VERSION:-$RELEASE_TAG}"
  [[ -n "$TAG" ]] || die download 5 "no release is pinned yet; pass --version query-server-v<version>"
  ARCHIVE="pir-query-server-${TAG#query-server-}-linux-x86_64.tar.gz"
  BASE_URL="https://github.com/$REPOSITORY/releases/download/$TAG"
  if [[ "$TAG" == "$RELEASE_TAG" ]]; then
    EXPECTED_SHA256="$ARCHIVE_SHA256"
  else
    # Only HTTPS to GitHub vouches for an unpinned release's checksum.
    say "warning: $TAG is not the pinned release; trusting its published SHA256SUMS"
    EXPECTED_SHA256="$(fetch 30 "$BASE_URL/SHA256SUMS" | awk -v f="$ARCHIVE" '$2 == f || $2 == "*" f { print $1 }')" ||
      die download 5 "cannot read SHA256SUMS of $TAG"
  fi
  [[ "$EXPECTED_SHA256" =~ ^[0-9a-f]{64}$ ]] || die download 5 "no checksum for $ARCHIVE"
}

install_release() {
  RELEASE_DIR="$INSTALL_ROOT/releases/$TAG"
  if [[ -x "$RELEASE_DIR/pir-query-server" ]]; then
    say "release $TAG already unpacked"
    return
  fi
  if ((DRY_RUN)); then
    printf '+ download %s/%s, verify sha256 %s, unpack to %s\n' "$BASE_URL" "$ARCHIVE" "$EXPECTED_SHA256" "$RELEASE_DIR" >&2
    return
  fi
  SCRATCH="$(mktemp -d)"
  trap 'rm -rf "$SCRATCH"' EXIT
  local scratch="$SCRATCH"
  say "downloading $ARCHIVE"
  fetch 1800 --output "$scratch/$ARCHIVE" "$BASE_URL/$ARCHIVE" || die download 5 "download of $BASE_URL/$ARCHIVE failed"
  printf '%s  %s\n' "$EXPECTED_SHA256" "$scratch/$ARCHIVE" | sha256sum --check --status ||
    die download 5 "$ARCHIVE does not match sha256 $EXPECTED_SHA256"
  mkdir -p "$scratch/release"
  tar -xzf "$scratch/$ARCHIVE" -C "$scratch/release" || die download 5 "cannot unpack $ARCHIVE"
  local file
  for file in pir-query-server query-server.json "$UNIT" revision; do
    [[ -f "$scratch/release/$file" ]] || die download 5 "$ARCHIVE has no $file"
  done
  install -d -m 0755 "$INSTALL_ROOT/releases"
  rm -rf "$RELEASE_DIR.partial"
  mv "$scratch/release" "$RELEASE_DIR.partial"
  mv "$RELEASE_DIR.partial" "$RELEASE_DIR"
}

install_config() {
  local rendered="$RELEASE_DIR/query-server.json" binary="$RELEASE_DIR/pir-query-server"
  if ((DRY_RUN)); then
    printf '+ write %s with Enhance shards %s, then %s --config ... --check\n' "$CONFIG" "$SHARDS" "$binary" >&2
    return
  fi
  install -d -m 0755 "$CONFIG_DIR"
  jq --argjson shards "$SHARDS" '.enhance.settings.synchronization.shards = $shards' "$rendered" >"$CONFIG.new"
  chmod 0644 "$CONFIG.new"
  "$binary" --config "$CONFIG.new" --check >&2 || die check 6 "pir-query-server refused the configuration"
  mv -f "$CONFIG.new" "$CONFIG"
  local dir
  for dir in $(jq -r '.. | objects | (.data_dir, .state_dir, .shard_dir) // empty' "$CONFIG"); do
    install -d -m 0700 "$dir"
  done
}

# The release ships the fleet unit; a standalone host has no data-volume unit
# and sizes the memory limits from its own RAM.
install_unit() {
  local memory_gib max high
  memory_gib=$((MEMORY_BYTES / 1073741824))
  max=$((memory_gib - MEMORY_RESERVE_GIB))
  high=$((max - 8))
  local source="$RELEASE_DIR/$UNIT"
  if ((DRY_RUN)) && [[ ! -f "$source" ]]; then
    printf '+ install %s (MemoryHigh=%sG MemoryMax=%sG) and restart it\n' "$UNIT_PATH" "$high" "$max" >&2
    return
  fi
  sed -e '/^Requires=wallet-pir-query-data.service$/d' \
    -e '/^ConditionPathIsMountPoint=/d' \
    -e 's/^After=.*/After=network-online.target\nWants=network-online.target/' \
    -e "s/^MemoryHigh=.*/MemoryHigh=${high}G/" \
    -e "s/^MemoryMax=.*/MemoryMax=${max}G/" \
    "$source" | write_file "$UNIT_PATH" 0644
  run ln -sfn "$RELEASE_DIR" "$INSTALL_ROOT/current"
  run systemctl daemon-reload
  run systemctl enable --quiet "$UNIT"
  run systemctl restart "$UNIT"
}

install_caddy() {
  [[ -n "$DOMAIN" ]] || return 0
  local caddyfile=/etc/caddy/Caddyfile
  if [[ -s "$caddyfile" ]] && ! grep -qF "$CADDY_MARKER" "$caddyfile" &&
    ! grep -q '^# The Caddyfile is an easy way' "$caddyfile"; then
    die prerequisite 4 "$caddyfile was not written by this installer; add the wallet routes yourself"
  fi
  # Wallet routes as on fleet query nodes (tools/infra/digitalocean/query-serving/cloud-init.yaml.tftpl),
  # plus Receiver, which fleet nodes serve through a separate edge.
  write_file "$caddyfile" 0644 <<EOF
$CADDY_MARKER
$DOMAIN {
	@wallet path /enhance/v1/* /status/v1/* /transparent/v1/* /receiver/v1/* /v1/enhance/* /v1/status/* /v1/receiver/* /v2/*
	handle @wallet {
		reverse_proxy $PUBLIC
	}
	handle {
		respond 404
	}
}
EOF
  ((DRY_RUN)) || caddy validate --config "$caddyfile" --adapter caddyfile >&2 || die check 6 "caddy refused $caddyfile"
  run systemctl enable --quiet --now caddy
  run systemctl reload caddy
}

wait_health() {
  ((DRY_RUN)) && return 0
  local _ state
  for _ in $(seq 1 60); do
    state="$(systemctl show -p ActiveState --value "$UNIT")"
    [[ "$state" == active || "$state" == activating ]] ||
      die start 6 "$UNIT is $state; see journalctl -u $UNIT -n 50"
    curl --fail --silent --max-time 2 "http://$MANAGEMENT/health" >/dev/null && return 0
    sleep 2
  done
  die start 6 "$UNIT did not answer http://$MANAGEMENT/health within 120 s; see journalctl -u $UNIT -n 50"
}

ready() { curl --fail --silent --max-time 5 "http://$MANAGEMENT/ready" | jq -e '.ready == true' >/dev/null 2>&1; }

wait_ready() {
  ((WAIT_READY && !DRY_RUN)) || return 0
  say "waiting for readiness; a cold start downloads tens of GiB"
  local started=$SECONDS
  while ((SECONDS - started < 10800)); do
    ready && return 0
    systemctl is-active --quiet "$UNIT" || die start 6 "$UNIT stopped while synchronizing; see journalctl -u $UNIT -n 50"
    sleep 30
  done
  die start 6 "not ready after 3 hours; see curl -s http://$MANAGEMENT/ready"
}

main() {
  parse_args "$@"
  check_platform
  check_hardware
  if ((CHECK_ONLY)); then
    printf 'wallet-pir-install: ok check-only cpus_ram_disk=met\n'
    return
  fi
  [[ ! -e "$FLEET_VOLUME_UNIT" ]] ||
    die platform 4 "this is a fleet-provisioned query node; deploy it with wallet-pir-deploy.py instead"
  ensure_tools
  check_sources
  resolve_release
  install_release
  install_config
  install_unit
  wait_health
  install_caddy
  wait_ready
  local is_ready=false endpoint="http://$PUBLIC"
  ((DRY_RUN)) || ! ready || is_ready=true
  [[ -z "$DOMAIN" ]] || endpoint="https://$DOMAIN"
  printf 'wallet-pir-install: ok version=%s dry_run=%s ready=%s endpoint=%s watch="curl -s http://%s/ready"\n' \
    "$TAG" "$((DRY_RUN))" "$is_ready" "$endpoint" "$MANAGEMENT"
}

main "$@"
