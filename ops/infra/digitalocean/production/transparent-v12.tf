# The transparent v12 (decoupled pages) worker: one combined host serving both
# v12 tiers, archive and recent, next to the v11 fleet until the switch. It is
# absent from the v11 worker roster and shares the worker tag, so it gets the
# worker firewall. The staging hostname points at the existing router, which
# serves it as a second site rendered from the v12 assignment.
#
# Sizing: the decoupled-pages census estimates about 28 GiB of prepared tables
# for the whole v12 set; 64 GiB leaves room for growth and the disk cache.

variable "transparent_v12_enabled" {
  description = "Whether the v12 worker and its staging DNS record exist."
  type        = bool
  default     = false
}

variable "transparent_v12_size" {
  description = "Transparent v12 worker size."
  type        = string
  default     = "m-8vcpu-64gb"
}

locals {
  transparent_v12_staging_hostname = "transparent-pir-v12.valargroup.dev"
}

resource "digitalocean_droplet" "transparent_v12" {
  count      = var.transparent_v12_enabled ? 1 : 0
  name       = "transparent-pir-v12-01"
  image      = var.image
  region     = var.region
  size       = var.transparent_v12_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_worker.name]
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

resource "digitalocean_project_resources" "transparent_v12" {
  count     = var.transparent_v12_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.transparent_v12[0].urn]
}

resource "cloudflare_dns_record" "transparent_v12_staging" {
  count   = var.transparent_v12_enabled && var.transparent_router_count > 0 ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_v12_staging_hostname
  type    = "A"
  content = digitalocean_droplet.transparent_router[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = "Transparent PIR v12 staging site on the router; managed by Terraform"
}

output "transparent_v12" {
  description = "The v12 worker's name and addresses, or null."
  value = var.transparent_v12_enabled ? {
    name         = digitalocean_droplet.transparent_v12[0].name
    private_ipv4 = digitalocean_droplet.transparent_v12[0].ipv4_address_private
    public_ipv4  = digitalocean_droplet.transparent_v12[0].ipv4_address
  } : null
}
