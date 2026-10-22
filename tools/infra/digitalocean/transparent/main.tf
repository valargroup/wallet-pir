resource "digitalocean_tag" "transparent_worker" {
  name = "transparent-pir-worker"

  lifecycle {
    create_before_destroy = true
  }
}


resource "digitalocean_tag" "transparent_loadgen" {
  count = var.transparent_loadgen_count > 0 ? 1 : 0
  name  = "transparent-pir-loadgen"

  lifecycle {
    create_before_destroy = true
  }
}


resource "digitalocean_tag" "transparent_router" {
  name = "transparent-pir-router"

  lifecycle {
    create_before_destroy = true
  }
}


# The transparent two-tier fleet. Both tiers share the worker tag;
# roles differ in size and in the roster the outputs render. Names carry the
# role so an assignment and a `/v1/ready` body read the same as the droplet list.
resource "digitalocean_droplet" "transparent_recent" {
  count      = var.transparent_recent_count
  name       = format("transparent-pir-recent-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_recent_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


# Archive owners are named, not counted, so the operator can add a new owner and
# later remove old ones without Terraform renumbering the survivors. Which
# shards each one holds is the inventory's partition, not this list.
resource "digitalocean_droplet" "transparent_archive" {
  for_each   = toset(var.transparent_archive_names)
  name       = each.key
  image      = var.image
  region     = var.region
  size       = var.transparent_archive_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


# Load generators: measurement clients inside the VPC with dedicated CPUs,
# reaching the workers and the router's internal listener. Not part of the
# serving fleet; provisioned for a measurement and destroyed after.
resource "digitalocean_droplet" "transparent_loadgen" {
  count      = var.transparent_loadgen_count
  name       = format("transparent-pir-loadgen-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_loadgen_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.transparent_loadgen[0].name]
  monitoring = true
  backups    = false
  ipv6       = false

  user_data = templatefile("${path.module}/cloud-init-transparent-router.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["build-essential", "pkg-config"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


# The router: the only public host of the fleet. It terminates TLS and routes
# each shard id to its owner or the replica pool from a Caddyfile the deploy
# renders out of the assignment.
resource "digitalocean_droplet" "transparent_router" {
  count      = var.transparent_router_count
  name       = format("transparent-pir-router-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_router_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.transparent_router.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-router.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


# Taken from the router attribute rather than a hand-maintained variable, so a
# rebuilt router cannot leave the record pointing at an address nothing answers
# on. The Enhance record predates this and still carries a hardcoded IP.
resource "cloudflare_dns_record" "transparent" {
  count   = var.transparent_router_count
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_public_hostname
  type    = "A"
  content = digitalocean_droplet.transparent_router[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = "Transparent PIR router; managed by Terraform"
}


locals {
  transparent_v12_staging_hostname = "transparent-pir-v12.valargroup.dev"
}


resource "digitalocean_droplet" "transparent_v12" {
  count      = var.transparent_v12_enabled ? 1 : 0
  name       = "transparent-pir-v12-01"
  image      = var.image
  region     = var.region
  size       = var.transparent_v12_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = var.vpc_id
  tags       = [digitalocean_tag.transparent_worker.name, digitalocean_tag.transparent_v12[0].name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}


resource "digitalocean_tag" "transparent_v12" {
  count = var.transparent_v12_enabled ? 1 : 0
  name  = "transparent-pir-v12"
}


# Public TLS for the staging site, and the plain-HTTP internal listener for
# load and deploy checks from the coordinator.
resource "cloudflare_dns_record" "transparent_v12_staging" {
  count   = var.transparent_v12_enabled ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_v12_staging_hostname
  type    = "A"
  content = digitalocean_droplet.transparent_v12[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = "Transparent PIR v12 staging site on the v12 worker; managed by Terraform"
}
# Fresh activity journal/publication lineage. Never reuse or resize the node volume.
resource "digitalocean_volume" "transparent_activity" {
  region                  = var.region
  name                    = "wallet-pir-transparent-activity"
  size                    = 250
  initial_filesystem_type = "xfs"
  description             = "Transparent v3 journal and v11 publication; retained rollback lineage"

  lifecycle {
    prevent_destroy = true
  }
}


resource "digitalocean_volume_attachment" "transparent_activity" {
  droplet_id = var.coordinator_id
  volume_id  = digitalocean_volume.transparent_activity.id
}

locals {
  common_packages             = ["ca-certificates", "curl", "jq", "htop"]
  transparent_public_hostname = "transparent-pir.valargroup.dev"
}
