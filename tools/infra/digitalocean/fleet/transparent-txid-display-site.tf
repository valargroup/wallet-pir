# The one-query txid display site: a second display lineage (`txid-2k8`) on its
# own host and public hostname, next to the `txid-2k` lineage that
# transparent-pir.valargroup.dev routes to display-01. Wallet builds pinned to
# the `txid-2k` client keep using the router; newer builds use this hostname.
#
# The host runs one combined display worker and fronts the hostname with Caddy,
# rendered by `wallet-pir-deploy.py txid-display-deploy --phase route` from a
# site request. It shares the worker tag with the other display hosts.
#
# Sizing (status.md, 2026-10-11): 191 runtimes of 80.04 MiB at the 3.51M-height
# journal end, 15.2 GiB, plus about 2.4 GiB a year; display-01's memory and
# unit limits.

variable "transparent_txid_display_site_enabled" {
  description = "Whether the one-query txid display site host and its DNS record exist."
  type        = bool
  default     = false
}


variable "transparent_txid_display_site_size" {
  description = "One-query txid display site host size."
  type        = string
  # The AMD m-8vcpu-64gb was not offered in ams3 on 2026-10-12.
  default = "m-8vcpu-64gb-intel"
}


resource "digitalocean_project_resources" "transparent_txid_display_site" {
  count     = var.transparent_txid_display_site_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_site[0].urn]
}


output "transparent_txid_display_site" {
  description = "The one-query txid display site host's name and addresses, or null."
  value = var.transparent_txid_display_site_enabled ? {
    name         = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_site[0].name
    private_ipv4 = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_site[0].ipv4_address_private
    public_ipv4  = module.transparent_tx_detail.digitalocean_droplet_transparent_txid_display_site[0].ipv4_address
  } : null
}
