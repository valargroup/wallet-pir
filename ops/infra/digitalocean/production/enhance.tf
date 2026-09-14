resource "digitalocean_tag" "enhance_worker" {
  name = "enhance-pir-worker"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_droplet" "enhance_worker" {
  count      = length(local.enhance_worker_names)
  name       = local.enhance_worker_names[count.index]
  image      = var.image
  region     = var.region
  size       = var.enhance_worker_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/enhance/cloud-init-worker.yaml.tftpl", {
    packages          = jsonencode(local.common_packages)
    deploy_public_key = var.enhance_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

# One-time migration holding addresses. Move worker[0/1] here in state BEFORE
# planning c-4 creation; this preserves the old pair for the 24-hour rollback
# window instead of attempting an in-place disk downsize or destroying them.
resource "digitalocean_droplet" "enhance_legacy_worker" {
  count      = var.enhance_legacy_worker_count
  name       = format("enhance-pir-worker-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = "s-4vcpu-8gb"
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true
  lifecycle {
    ignore_changes = [user_data, image, ssh_keys, size]
  }
}

resource "digitalocean_firewall" "enhance_worker" {
  name = "enhance-pir-workers"
  tags = [digitalocean_tag.enhance_worker.name]

  inbound_rule {
    protocol    = "tcp"
    port_range  = "22"
    source_tags = [digitalocean_tag.coordinator.name]
  }

  dynamic "inbound_rule" {
    for_each = var.allowed_ssh_cidrs
    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }

  inbound_rule {
    protocol    = "tcp"
    port_range  = "8091"
    source_tags = [digitalocean_tag.coordinator.name]
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

# Additive membership avoids rewriting the shared project resource set on expansion.
resource "digitalocean_project_resources" "enhance_added_workers" {
  count     = length(local.enhance_worker_names) - 2
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.enhance_worker[count.index + 2].urn]
}

resource "cloudflare_dns_record" "enhance" {
  zone_id = var.cloudflare_zone_id
  name    = local.enhance_public_hostname
  type    = "A"
  content = var.wallet_pir_coordinator_dns_ipv4
  ttl     = 300
  proxied = false
  comment = "Enhance PIR production coordinator; managed by Terraform"
}

