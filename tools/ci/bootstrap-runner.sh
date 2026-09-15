#!/usr/bin/env bash
# Bootstrap the shared Wallet PIR CI host as root. Registration is separate so
# this script and its logs contain no GitHub credentials.
set -euo pipefail
[[ $(id -u) == 0 ]]
# shellcheck disable=SC1091
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 && $(uname -m) == x86_64 ]]
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential ca-certificates curl git gh jq shellcheck \
  clang libclang-dev protobuf-compiler pkg-config libssl-dev python3 nodejs \
  rsync unzip libicu74 ufw

# Different Unix identities and 0700 homes separate fast/PR state from trusted
# main/release state on the same physical machine. Neither user gets sudo.
for lane in fast build; do
  user="wallet-pir-$lane"
  id "$user" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$user"
  chmod 0700 "/home/$user"
  install -d -o "$user" -g "$user" -m 0700 "/home/$user/actions-runner"
  if [[ ! -x "/home/$user/.cargo/bin/rustup" ]]; then
    curl --proto '=https' --tlsv1.2 -fsS https://sh.rustup.rs -o /tmp/wallet-pir-rustup.sh
    runuser -u "$user" -- sh /tmp/wallet-pir-rustup.sh -y --profile minimal --default-toolchain 1.91.0
  fi
  runuser -u "$user" -- "/home/$user/.cargo/bin/rustup" component add --toolchain 1.91.0 rustfmt clippy
  if [[ ! -x "/home/$user/actions-runner/config.sh" ]]; then
    archive=/tmp/wallet-pir-actions-runner.tar.gz
    curl -fLsS https://github.com/actions/runner/releases/download/v2.337.0/actions-runner-linux-x64-2.337.0.tar.gz -o "$archive"
    printf '%s  %s\n' 70920811a4f8ad4328818682bca5c6469c1c942fab52448868071d0063816613 "$archive" | sha256sum -c -
    tar -xzf "$archive" -C "/home/$user/actions-runner"
    chown -R "$user:$user" "/home/$user/actions-runner"
  fi
done

# Restrict login to public-key SSH. CI needs outbound access only.
cat >/etc/ssh/sshd_config.d/10-wallet-pir-ci.conf <<'SSH'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
SSH
sshd -t
systemctl reload ssh
ufw allow 22/tcp
ufw default deny incoming
ufw default allow outgoing
ufw --force enable

# Package upgrades must not restart runners in the middle of a job.
mkdir -p /etc/needrestart/conf.d
cat >/etc/needrestart/conf.d/actions_runner_services.conf <<'CONF'
$nrconf{override_rc}{qr(^actions\.runner\..+\.service$)} = 0;
CONF
printf 'Bootstrap complete: %s CPUs, %s MiB RAM\n' "$(nproc)" "$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)"
df -h /
