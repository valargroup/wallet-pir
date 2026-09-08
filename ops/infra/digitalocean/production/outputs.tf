output "coordinator_public_ipv4" {
  value = digitalocean_droplet.coordinator.ipv4_address
}

output "public_url" {
  value = "https://${local.public_hostname}"
}

output "coordinator_private_ipv4" {
  value = digitalocean_droplet.coordinator.ipv4_address_private
}

output "worker_public_ipv4" {
  value = [for worker in digitalocean_droplet.worker : worker.ipv4_address]
}

output "worker_private_ipv4" {
  value = [for worker in digitalocean_droplet.worker : worker.ipv4_address_private]
}

output "worker_groups" {
  description = "Stable shard groups and the two replica Droplets in each group."
  value = [for group in local.worker_groups : {
    name = group.name
    replicas = [for replica_name in group.replicas : {
      name         = replica_name
      public_ipv4  = digitalocean_droplet.worker[index(local.worker_names, replica_name)].ipv4_address
      private_ipv4 = digitalocean_droplet.worker[index(local.worker_names, replica_name)].ipv4_address_private
    }]
  }]
}

output "zakura_volume_id" {
  value = digitalocean_volume.zakura.id
}

output "transparent_worker_private_ipv4" {
  value = [for worker in digitalocean_droplet.transparent_worker : worker.ipv4_address_private]
}

# The fleet roster in the shape `TRANSPARENT_FLEET_JSON` takes: one entry per
# worker with the budgets its unit is rendered with and the upstream the router
# proxies to. Set the repository variable from this output rather than by hand,
# so an address or a budget cannot drift between the hosts and the deploy.
output "transparent_fleet_json" {
  description = "Roster for TRANSPARENT_FLEET_JSON; ssh_host and upstream are VPC addresses."
  value = jsonencode(concat(
    [for worker in digitalocean_droplet.transparent_recent : {
      id            = worker.name
      role          = "recent-replica"
      replica_group = "recent"
      ssh_host      = worker.ipv4_address_private
      upstream      = "${worker.ipv4_address_private}:8093"
      cache_bytes   = var.transparent_recent_cache_bytes
      memory_max    = var.transparent_recent_memory_max
    }],
    [for worker in digitalocean_droplet.transparent_archive : {
      id            = worker.name
      role          = "archive-owner"
      replica_group = null
      ssh_host      = worker.ipv4_address_private
      upstream      = "${worker.ipv4_address_private}:8093"
      cache_bytes   = var.transparent_archive_cache_bytes
      memory_max    = var.transparent_archive_memory_max
    }],
  ))
}

output "transparent_router_host" {
  description = "For TRANSPARENT_ROUTER_HOST: the router's VPC address, or empty without a router."
  value       = var.transparent_router_count > 0 ? digitalocean_droplet.transparent_router[0].ipv4_address_private : ""
}

output "transparent_router_public_ipv4" {
  value = [for router in digitalocean_droplet.transparent_router : router.ipv4_address]
}
