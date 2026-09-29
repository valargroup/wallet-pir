locals {
  wallet_pir_coordinator_name = "wallet-pir-coordinator-01"
  enhance_public_hostname     = "enhance-pir.valargroup.dev"
  # The transparent fleet terminates TLS on the worker rather than behind the
  # Enhance coordinator's Caddy. That Caddyfile is rendered and installed by
  # deploy-enhance-pir.sh, and a token it does not substitute would fail
  # `caddy validate` on the next Enhance deploy -- a live service broken by a
  # change with nothing to do with it. When the coordinator daemon lands and
  # there is more than one worker, fronting moves there and this record follows.
  transparent_public_hostname = "transparent-pir.valargroup.dev"
  # Append-only three-shard ranges. Preserve existing worker addresses and names.
  enhance_worker_groups = [for group in range(var.enhance_group_count) : {
    name     = format("shard-group-%02d", group + 1)
    replicas = [for replica in range(2) : format("enhance-pir-worker-%02d", group * 2 + replica + 1)]
  }]
  enhance_worker_names = flatten([for group in local.enhance_worker_groups : group.replicas])
  common_packages      = ["ca-certificates", "curl", "jq", "htop"]
}

resource "digitalocean_vpc" "wallet" {
  name     = "wallet-pir-production"
  region   = var.region
  ip_range = "10.142.0.0/24"
}

resource "digitalocean_tag" "coordinator" {
  name = "wallet-pir-coordinator"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_droplet" "coordinator" {
  name       = local.wallet_pir_coordinator_name
  image      = var.image
  region     = var.region
  size       = var.wallet_pir_coordinator_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.coordinator.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-coordinator.yaml.tftpl", {
    packages = jsonencode(concat(local.common_packages, ["xfsprogs"]))
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_volume" "zakura" {
  region                  = var.region
  name                    = "spendability-memo-pir-zakura"
  size                    = 1024
  initial_filesystem_type = "xfs"
  description             = "Zakura archive and canonical Ironwood Enhance records"

  # DigitalOcean cannot rename a volume in place. Keep the historical provider
  # name so this production data volume is never replaced for branding alone.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [name, description]
  }
}

resource "digitalocean_volume_attachment" "zakura" {
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.zakura.id
}

# The coordinator intentionally has no cloud firewall. SSH must remain reachable
# when operator/VPN source addresses change. Keep private services bound to loopback.

resource "digitalocean_project_resources" "wallet" {
  project = var.wallet_pir_project_id
  resources = concat(
    [digitalocean_droplet.coordinator.urn, digitalocean_volume.zakura.urn],
    [for worker in slice(digitalocean_droplet.enhance_worker, 0, 2) : worker.urn],
    [for worker in digitalocean_droplet.enhance_legacy_worker : worker.urn],
    [for worker in digitalocean_droplet.transparent_recent : worker.urn],
    # values() of a for_each resource is in name order: archive-01, archive-02, ...
    [for worker in values(digitalocean_droplet.transparent_archive) : worker.urn],
    [for router in digitalocean_droplet.transparent_router : router.urn],
    [for host in digitalocean_droplet.transparent_loadgen : host.urn],
  )
}

# Keep the existing production resources while renaming Terraform addresses.
moved {
  from = digitalocean_vpc.memo
  to   = digitalocean_vpc.enhance
}

moved {
  from = digitalocean_project_resources.memo
  to   = digitalocean_project_resources.enhance
}

moved {
  from = digitalocean_vpc.enhance
  to   = digitalocean_vpc.wallet
}

moved {
  from = digitalocean_tag.worker
  to   = digitalocean_tag.enhance_worker
}

moved {
  from = digitalocean_droplet.worker
  to   = digitalocean_droplet.enhance_worker
}

moved {
  from = digitalocean_firewall.worker
  to   = digitalocean_firewall.enhance_worker
}

moved {
  from = digitalocean_project_resources.enhance
  to   = digitalocean_project_resources.wallet
}

# The archive owners were created with `count`; keep them under their names.
moved {
  from = digitalocean_droplet.transparent_archive[0]
  to   = digitalocean_droplet.transparent_archive["transparent-pir-archive-01"]
}

moved {
  from = digitalocean_droplet.transparent_archive[1]
  to   = digitalocean_droplet.transparent_archive["transparent-pir-archive-02"]
}
