# Receiver's public DNS record. Ingestion runs in the shared pir-publisher and the
# coordinator's Caddy serves the wallet hostname, so the record names the
# coordinator. The earlier receiver-pir-poc-01 Droplet was retired on 2026-10-27.
variable "receiver_pir_enabled" {
  type    = bool
  default = false
}

resource "cloudflare_dns_record" "receiver_pir" {
  count   = var.receiver_pir_enabled ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "receiver-pir.valargroup.dev"
  type    = "A"
  content = digitalocean_droplet.coordinator.ipv4_address
  ttl     = 300
  proxied = false
}
