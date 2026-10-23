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
  user_data = templatefile("${path.module}/../apm-sidecar/user-data.tftpl", {
    script       = local.apm_sidecar_boot_script
    cloud_config = <<-YAML
      #cloud-config
      package_update: true
      packages: [ca-certificates, curl, caddy]
      runcmd:
        - [useradd, --system, --home-dir, /var/lib/pir-monitor, --shell, /usr/sbin/nologin, pir-monitor]
        - [install, -d, -m, '0755', /opt/pir-monitor/releases, /etc/pir-monitor]
    YAML
  })
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
