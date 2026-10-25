locals {
  name = split(".", var.hostname)[0]
}

resource "digitalocean_tag" "query" {
  name = local.name
}

resource "digitalocean_droplet" "query" {
  name       = local.name
  image      = var.image
  region     = var.region
  size       = var.size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.query.name]
  monitoring = true
  ipv6       = true
  user_data = templatefile("${path.module}/../apm-sidecar/user-data.tftpl", {
    script = var.apm_sidecar_boot_script
    cloud_config = templatefile("${path.module}/cloud-init.yaml.tftpl", {
      hostname    = var.hostname
      volume_name = "${local.name}-data"
    })
  })
  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_volume" "data" {
  region                  = var.region
  name                    = "${local.name}-data"
  size                    = var.data_volume_gib
  initial_filesystem_type = "xfs"
  description             = "Downloaded Wallet PIR publications and prepared runtime caches"
  lifecycle {
    prevent_destroy = true
  }
}

resource "digitalocean_volume_attachment" "data" {
  droplet_id = digitalocean_droplet.query.id
  volume_id  = digitalocean_volume.data.id
}

resource "digitalocean_project_resources" "query" {
  project   = var.project_id
  resources = [digitalocean_droplet.query.urn, digitalocean_volume.data.urn]
}

resource "cloudflare_dns_record" "query" {
  zone_id = var.cloudflare_zone_id
  name    = var.hostname
  type    = "A"
  content = digitalocean_droplet.query.ipv4_address
  proxied = false
  ttl     = 300
  comment = "Unified query candidate; existing public routing is unchanged"
}
