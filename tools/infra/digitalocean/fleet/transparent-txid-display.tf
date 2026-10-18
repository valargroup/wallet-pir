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


resource "digitalocean_project_resources" "transparent_txid_display_archive" {
  count     = var.transparent_txid_display_archive_count
  project   = var.wallet_pir_project_id
  resources = [module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_archive[0].urn]
}


output "transparent_txid_display_archive" {
  description = "The txid display archive host's name and addresses, or null."
  value = var.transparent_txid_display_archive_count > 0 ? {
    name         = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_archive[0].name
    private_ipv4 = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_archive[0].ipv4_address_private
    public_ipv4  = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_archive[0].ipv4_address
  } : null
}
