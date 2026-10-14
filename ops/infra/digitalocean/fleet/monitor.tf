# Independent public-path monitoring; opt in without changing serving resources.
variable "pir_monitor_enabled" {
  type    = bool
  default = false
}

resource "digitalocean_droplet" "pir_monitor" {
  count      = var.pir_monitor_enabled ? 1 : 0
  name       = "wallet-pir-monitor-01"
  region     = "nyc3"
  size       = "s-2vcpu-4gb"
  image      = "ubuntu-24-04-x64"
  ssh_keys   = var.ssh_key_ids
  monitoring = true
  ipv6       = true
  user_data  = <<-YAML
    #cloud-config
    package_update: true
    packages: [ca-certificates, curl, caddy]
    runcmd:
      - [useradd, --system, --home-dir, /var/lib/pir-monitor, --shell, /usr/sbin/nologin, pir-monitor]
      - [install, -d, -m, '0755', /opt/pir-monitor/releases, /etc/pir-monitor]
  YAML
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}

resource "digitalocean_project_resources" "pir_monitor" {
  count     = var.pir_monitor_enabled ? 1 : 0
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.pir_monitor[0].urn]
}

resource "digitalocean_firewall" "pir_monitor" {
  count       = var.pir_monitor_enabled ? 1 : 0
  name        = "wallet-pir-monitor"
  droplet_ids = [digitalocean_droplet.pir_monitor[0].id]
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = concat(var.allowed_ssh_cidrs, ["${var.wallet_pir_coordinator_dns_ipv4}/32"])
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

resource "cloudflare_dns_record" "pir_monitor" {
  count   = var.pir_monitor_enabled ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = "monitor-pir.valargroup.dev"
  type    = "A"
  content = digitalocean_droplet.pir_monitor[0].ipv4_address
  ttl     = 300
  proxied = false
}

output "pir_monitor_ipv4" {
  value = try(digitalocean_droplet.pir_monitor[0].ipv4_address, null)
}
