# The receiver directory's single indexing and serving Droplet
# (receiver/ops/digitalocean). Opt in to manage it; the existing Droplet and DNS
# record are imported as its README describes, never recreated, and the firewall,
# which does not exist yet, is created.
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
  user_data  = file("${path.module}/../../../../receiver/ops/digitalocean/cloud-init.yaml")
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

# Public TLS for the wallet routes; the service port only from the PIR monitor,
# which reads health and metrics over the private network.
resource "digitalocean_firewall" "receiver_pir" {
  count       = var.receiver_pir_enabled ? 1 : 0
  name        = "receiver-pir"
  droplet_ids = [digitalocean_droplet.receiver_pir[0].id]
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.allowed_ssh_cidrs
  }
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
  dynamic "inbound_rule" {
    for_each = var.pir_monitor_enabled ? [18380] : []
    content {
      protocol           = "tcp"
      port_range         = tostring(inbound_rule.value)
      source_droplet_ids = [digitalocean_droplet.pir_monitor[0].id]
    }
  }
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
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
