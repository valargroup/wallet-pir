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
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-worker.yaml.tftpl", {
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
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true
  lifecycle {
    ignore_changes = [user_data, image, ssh_keys, size]
  }
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


resource "digitalocean_tag" "enhance_packing_router" {
  name = "enhance-pir-packing-router"
}


resource "digitalocean_droplet" "enhance_packing_router" {
  count      = var.enhance_packing_router_count
  name       = format("enhance-pir-packing-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = "c-4"
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.enhance_packing_router.name]
  monitoring = true
  ipv6       = true
  user_data  = <<-YAML
    #cloud-config
    package_update: true
    packages: [ca-certificates, curl, jq]
    runcmd:
      - [useradd, --system, --home-dir, /srv/enhance-pir-router, --shell, /usr/sbin/nologin, enhance-pir]
      - [install, -d, -o, enhance-pir, -g, enhance-pir, /srv/enhance-pir-router]
  YAML
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}


resource "digitalocean_droplet" "enhance_pool_worker" {
  count      = var.enhance_pool_worker_count
  name       = format("enhance-pir-pool-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = "c-4"
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  ipv6       = true
  user_data = templatefile("${path.module}/cloud-init-worker.yaml.tftpl", {
    packages          = jsonencode(local.common_packages)
    deploy_public_key = var.enhance_worker_deploy_public_key
  })
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}

locals {
  common_packages         = ["ca-certificates", "curl", "jq", "htop"]
  enhance_public_hostname = "enhance-pir.valargroup.dev"
  enhance_worker_groups = [for group in range(var.enhance_group_count) : {
    name     = format("shard-group-%02d", group + 1)
    replicas = [for replica in range(2) : format("enhance-pir-worker-%02d", group * 2 + replica + 1)]
  }]
  enhance_worker_names = flatten([for group in local.enhance_worker_groups : group.replicas])
}
