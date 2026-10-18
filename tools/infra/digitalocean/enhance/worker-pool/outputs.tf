output "worker_groups" {
  description = "Durable resource identities and private origins for bootstrap and qualified registration."
  value = [for group in range(var.group_count) : {
    name = format("enhance-v4-group-%02d", group + 1)
    replicas = [for replica in range(2) : {
      name         = digitalocean_droplet.worker[group * 2 + replica].name
      resource_id  = tostring(digitalocean_droplet.worker[group * 2 + replica].id)
      public_ipv4  = digitalocean_droplet.worker[group * 2 + replica].ipv4_address
      private_ipv4 = digitalocean_droplet.worker[group * 2 + replica].ipv4_address_private
      url          = "http://${digitalocean_droplet.worker[group * 2 + replica].ipv4_address_private}:8291"
    }]
  }]
}
