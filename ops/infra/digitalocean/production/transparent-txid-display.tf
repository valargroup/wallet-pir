# The txid display archive host: the sealed txid display v2 archives from
# height 1, one table per 40,000-entry archive. It is not a history archive
# owner and is absent from the transparent worker roster. It shares the worker
# tag, so it gets the worker firewall, including the display port the
# `firewall` phase opens.
#
# Sizing (status.md, 2026-10-08): about 426 archive tables at the 3.51M-height
# journal end, 16.7 GiB of runtimes at 40.05 MiB each, plus about 2.5 GiB a
# year. 64 GiB leaves 49.7G usable: about nine years of growth.

variable "transparent_txid_display_archive_count" {
  description = "Txid display archive hosts: zero or one."
  type        = number
  default     = 0

  validation {
    condition     = var.transparent_txid_display_archive_count >= 0 && var.transparent_txid_display_archive_count <= 1
    error_message = "The txid display archive is a single host."
  }
}

variable "transparent_txid_display_archive_size" {
  description = "Txid display archive host size."
  type        = string
  default     = "m-8vcpu-64gb"
}

resource "digitalocean_droplet" "transparent_txid_display_archive" {
  count      = var.transparent_txid_display_archive_count
  name       = format("transparent-pir-txid-display-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_txid_display_archive_size
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

resource "digitalocean_project_resources" "transparent_txid_display_archive" {
  count     = var.transparent_txid_display_archive_count
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.transparent_txid_display_archive[0].urn]
}

output "transparent_txid_display_archive" {
  description = "The txid display archive host's name and addresses, or null."
  value = var.transparent_txid_display_archive_count > 0 ? {
    name         = digitalocean_droplet.transparent_txid_display_archive[0].name
    private_ipv4 = digitalocean_droplet.transparent_txid_display_archive[0].ipv4_address_private
    public_ipv4  = digitalocean_droplet.transparent_txid_display_archive[0].ipv4_address
  } : null
}
