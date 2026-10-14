locals {
  # The production root's transparent worker packages: common_packages plus caddy.
  packages = ["ca-certificates", "curl", "jq", "htop", "caddy"]
}

# Elastic recent replicas only. Archive owners, static recent replicas, the
# router, the tag and the firewall all belong to the production root.
resource "digitalocean_droplet" "recent" {
  for_each   = var.members
  name       = each.key
  image      = each.value.image
  region     = var.region
  size       = each.value.size
  vpc_uuid   = var.vpc_uuid
  ssh_keys   = var.ssh_key_ids
  tags       = [var.worker_tag]
  monitoring = true
  ipv6       = true
  backups    = false

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(local.packages)
    deploy_public_key = var.deploy_public_key
    host_key          = lookup(var.host_keys, each.key, null)
  })

  # A member's host facts never change during its life: a new image or cloud
  # config applies to members created afterwards, never by rebuilding one.
  lifecycle {
    ignore_changes = [image, user_data]
  }
}

# One entry per member rather than one shared list, so a scale step creates or
# destroys only its own members' entries.
resource "digitalocean_project_resources" "recent" {
  for_each  = var.members
  project   = var.project_id
  resources = [digitalocean_droplet.recent[each.key].urn]
}
