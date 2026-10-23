# The transparent v12 (decoupled pages) worker: one combined host serving both
# v12 tiers, archive and recent, next to the v11 fleet until the switch. It is
# absent from the v11 worker roster and shares the worker tag.
# It also fronts its own staging hostname with Caddy, rendered
# by the v12 controller exactly as the router's site is: the v11 controller
# rewrites the router's whole Caddyfile, so a second site there would be lost
# on its next reconcile. At the switch the router serves the v12 rendering.
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


resource "digitalocean_project_resources" "transparent_v12" {
  count     = var.transparent_v12_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [module.transparent.digitalocean_droplet_transparent_v12[0].urn]
}


output "transparent_v12" {
  description = "The v12 worker's name and addresses, or null."
  value = var.transparent_v12_enabled ? {
    name         = module.transparent.digitalocean_droplet_transparent_v12[0].name
    private_ipv4 = module.transparent.digitalocean_droplet_transparent_v12[0].ipv4_address_private
    public_ipv4  = module.transparent.digitalocean_droplet_transparent_v12[0].ipv4_address
  } : null
}
