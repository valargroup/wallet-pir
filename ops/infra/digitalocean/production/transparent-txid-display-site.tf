# The one-query txid display site: a second display lineage (`txid-2k8`) on its
# own host and public hostname, next to the `txid-2k` lineage that
# transparent-pir.valargroup.dev routes to display-01. Wallet builds pinned to
# the `txid-2k` client keep using the router; newer builds use this hostname.
#
# The host runs one combined display worker and fronts the hostname with Caddy,
# rendered by `wallet-pir-deploy.py txid-display-deploy --phase route` from a
# site request. It shares the worker tag for the common worker firewall and has
# its own firewall for public TLS and the coordinator's worker port, so it does
# not depend on the router lineage's `transparent_txid_display_port_enabled`.
#
# Sizing (status.md, 2026-10-11): 191 runtimes of 80.04 MiB at the 3.51M-height
# journal end, 15.2 GiB, plus about 2.4 GiB a year; the same size and unit
# limits as display-01.

variable "transparent_txid_display_site_enabled" {
  description = "Whether the one-query txid display site host and its DNS record exist."
  type        = bool
  default     = false
}

variable "transparent_txid_display_site_size" {
  description = "One-query txid display site host size."
  type        = string
  default     = "m-8vcpu-64gb"
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
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_worker.name, digitalocean_tag.transparent_txid_display_site[0].name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/transparent/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
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
resource "digitalocean_firewall" "transparent_txid_display_site" {
  count = var.transparent_txid_display_site_enabled ? 1 : 0
  name  = "transparent-pir-txid-site"
  tags  = [digitalocean_tag.transparent_txid_display_site[0].name]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol    = "tcp"
    port_range  = "8095"
    source_tags = [digitalocean_tag.coordinator.name]
  }
}

resource "digitalocean_project_resources" "transparent_txid_display_site" {
  count     = var.transparent_txid_display_site_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.transparent_txid_display_site[0].urn]
}

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

output "transparent_txid_display_site" {
  description = "The one-query txid display site host's name and addresses, or null."
  value = var.transparent_txid_display_site_enabled ? {
    name         = digitalocean_droplet.transparent_txid_display_site[0].name
    private_ipv4 = digitalocean_droplet.transparent_txid_display_site[0].ipv4_address_private
    public_ipv4  = digitalocean_droplet.transparent_txid_display_site[0].ipv4_address
  } : null
}
