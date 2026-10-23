#!/bin/bash
# First-boot install of the per-host APM sidecar on a newly provisioned droplet.
#
# Terraform appends this script to a managed droplet's user_data when
# `apm_sidecar_boot` is set (tools/infra/digitalocean/apm-sidecar), exporting:
#   PIR_APM_SIDECAR_URL            where the pinned pir-apm-sidecar binary is served
#   PIR_APM_SIDECAR_SHA256         that binary's sha256, which names its release directory
#   PIR_APM_SIDECAR_UNIT_TEMPLATE  base64 of tools/deploy/monitoring/pir-apm-sidecar.service.in
# It installs the binary as the deploy engine stages it, the unit exactly as the
# `apm-sidecar.observer` renderer renders it for that release, and a provisional
# configuration naming the droplet and its private address, then enables the
# unit. Nothing here is secret and no key material reaches the host: the unit
# starts once `wallet-pir-monitoring.py install-credentials --role sidecar`
# installs the certificate issued from Infisical, and the next `apm-sidecar`
# deploy replaces the configuration with the inventory's.
#
# PIR_APM_SIDECAR_ROOT (tests only) prefixes every path; PIR_APM_SIDECAR_METADATA
# overrides the DigitalOcean metadata origin.
set -euo pipefail

root="${PIR_APM_SIDECAR_ROOT:-}"
metadata="${PIR_APM_SIDECAR_METADATA:-http://169.254.169.254/metadata/v1}"
sha="$PIR_APM_SIDECAR_SHA256"
[[ "$sha" =~ ^[0-9a-f]{64}$ ]] || { echo "PIR_APM_SIDECAR_SHA256 is not a sha256" >&2; exit 1; }
[[ "$PIR_APM_SIDECAR_URL" =~ ^https?://[A-Za-z0-9./_~:-]+$ ]] || { echo "PIR_APM_SIDECAR_URL is not a plain URL" >&2; exit 1; }

release="/opt/pir-apm-sidecar/releases/$sha"
config=/etc/pir-apm-sidecar/sidecar.json
install -d -m 755 "$root$release" "$root/etc/pir-apm-sidecar" "$root/etc/systemd/system"
install -d -m 700 "$root/etc/pir-apm-sidecar/credentials"
curl -fsSL --retry 10 --retry-all-errors --max-time 120 -o "$root$release/pir-apm-sidecar.next" "$PIR_APM_SIDECAR_URL"
echo "$sha  $root$release/pir-apm-sidecar.next" | sha256sum -c --quiet
chmod 755 "$root$release/pir-apm-sidecar.next"
mv "$root$release/pir-apm-sidecar.next" "$root$release/pir-apm-sidecar"

host_id="$(curl -fsS --retry 10 --retry-all-errors --max-time 10 "$metadata/hostname")"
address="$(curl -fsS --retry 10 --retry-all-errors --max-time 10 "$metadata/interfaces/private/0/ipv4/address")"
[[ "$host_id" =~ ^[a-z0-9@._:/-]{1,80}$ ]] || { echo "droplet name is not a monitoring host id" >&2; exit 1; }
[[ "$address" =~ ^(10|172|192)\.[0-9.]+$ ]] || { echo "no private IPv4 address" >&2; exit 1; }

# The renderer's encoding: sorted keys, two-space indent, trailing newline.
cat >"$root$config.next" <<EOF
{
  "hooks": [],
  "host_id": "$host_id",
  "listen": "$address:9443",
  "required_volumes": [],
  "state_path": "/var/lib/pir-apm-sidecar/state.json",
  "tls": {
    "cert": "server.pem",
    "client_ca": "monitoring-ca.pem",
    "collector_identity": "pir-apm-collector",
    "key": "server.key"
  },
  "units": []
}
EOF
chmod 644 "$root$config.next"
mv "$root$config.next" "$root$config"

# role_config.render: ExecStart, ReadWritePaths and ReadOnlyPaths lead the Service section.
echo "$PIR_APM_SIDECAR_UNIT_TEMPLATE" | base64 -d | awk -v exec="$release/pir-apm-sidecar --config $config" '
  /^\[/ { section = $0 }
  section == "[Service]" && /^(ExecStart|ReadWritePaths|ReadOnlyPaths)=/ { next }
  { print }
  $0 == "[Service]" { print "ExecStart=" exec; print "ReadWritePaths="; print "ReadOnlyPaths=" }
' >"$root/etc/systemd/system/pir-apm-sidecar.service.next"
chmod 644 "$root/etc/systemd/system/pir-apm-sidecar.service.next"
mv "$root/etc/systemd/system/pir-apm-sidecar.service.next" "$root/etc/systemd/system/pir-apm-sidecar.service"

# Enabled, not started: its credential files do not exist until installed.
systemctl daemon-reload
systemctl enable pir-apm-sidecar.service
