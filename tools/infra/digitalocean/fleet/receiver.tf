# The receiver directory's single indexing and serving Droplet
# (ops/receiver/digitalocean). Opt in to manage it; the existing Droplet and DNS
# record are imported as its README describes, never recreated.
variable "receiver_pir_enabled" {
  type    = bool
  default = false
}

resource "digitalocean_droplet" "receiver_pir" {
  count      = var.receiver_pir_enabled ? 1 : 0
  name       = "receiver-pir-poc-01"
  region     = "nyc3"
  size       = "s-4vcpu-8gb-amd"
  image      = "ubuntu-24-04-x64"
  ssh_keys   = var.ssh_key_ids
  monitoring = true
  ipv6       = true
  user_data  = file("${path.module}/../../../receiver/digitalocean/cloud-init.yaml")
  lifecycle {
    prevent_destroy = true
    # An imported Droplet has no keys in state, and a key change forces replacement.
    ignore_changes = [user_data, ssh_keys]
  }
}

resource "digitalocean_project_resources" "receiver_pir" {
  count     = var.receiver_pir_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.receiver_pir[0].urn]
}

resource "cloudflare_dns_record" "receiver_pir" {
  count   = var.receiver_pir_enabled ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "receiver-pir.valargroup.dev"
  type    = "A"
  content = digitalocean_droplet.receiver_pir[0].ipv4_address
  ttl     = 300
  proxied = false
}

output "receiver_pir_ipv4" {
  value = try(digitalocean_droplet.receiver_pir[0].ipv4_address, null)
}

output "receiver_pir_private_ipv4" {
  value = try(digitalocean_droplet.receiver_pir[0].ipv4_address_private, null)
}
