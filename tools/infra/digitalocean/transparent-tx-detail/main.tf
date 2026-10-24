

resource "digitalocean_droplet" "transparent_txid_display_archive" {
  count      = var.transparent_txid_display_archive_count
  name       = format("transparent-pir-txid-display-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_txid_display_archive_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [var.transparent_worker_tag]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/../apm-sidecar/user-data.tftpl", {
    script = var.apm_sidecar_boot_script
    cloud_config = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
      packages          = jsonencode(concat(local.common_packages, ["caddy"]))
      deploy_public_key = var.transparent_worker_deploy_public_key
    })
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


locals {
  transparent_txid_display_site_hostname = "transparent-pir-txid.valargroup.dev"
}


resource "digitalocean_droplet" "transparent_txid_display_site" {
  count      = var.transparent_txid_display_site_enabled ? 1 : 0
  name       = "transparent-pir-txid-k8-01"
  image      = var.image
  region     = var.region
  size       = var.transparent_txid_display_site_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [var.transparent_worker_tag, digitalocean_tag.transparent_txid_display_site[0].name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/../apm-sidecar/user-data.tftpl", {
    script = var.apm_sidecar_boot_script
    cloud_config = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
      packages          = jsonencode(concat(local.common_packages, ["caddy"]))
      deploy_public_key = var.transparent_worker_deploy_public_key
    })
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


resource "digitalocean_tag" "transparent_txid_display_site" {
  count = var.transparent_txid_display_site_enabled ? 1 : 0
  name  = "transparent-pir-txid-site"
}


# Public TLS for the site, and the worker port for the coordinator's display
# controller, which ships candidates and checks readiness.
resource "cloudflare_dns_record" "transparent_txid_display_site" {
  count   = var.transparent_txid_display_site_enabled ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_txid_display_site_hostname
  type    = "A"
  content = digitalocean_droplet.transparent_txid_display_site[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = "Transparent PIR one-query txid display site; managed by Terraform"
}
# Volume for the txid display genesis ingest, mounted on the coordinator at
# /srv/txid-display-genesis. It began as scratch for layout experiments and
# census runs, but it now holds production state: the v2x genesis journal
# (journal-v2x) and the publication roots the live txid display controller
# builds from (v4/root for txid-4k8, and the txid-2k8 root kept for rollback).
# See docs/transparent/status.md. Do not destroy or detach it while the txid
# display serves from it. It is kept off the node volume so the ingest cannot
# consume /srv/zakura's 20% headroom.
resource "digitalocean_volume" "txid_display_genesis" {
  region                  = var.region
  name                    = "wallet-pir-txid-display-genesis"
  size                    = 150
  initial_filesystem_type = "xfs"
  description             = "Txid display genesis journal and census scratch; layout experiments, not served"
}


resource "digitalocean_volume_attachment" "txid_display_genesis" {
  droplet_id = var.coordinator_id
  volume_id  = digitalocean_volume.txid_display_genesis.id
}

locals {
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}
