output "members" {
  description = "Durable identities of the elastic recent replicas, keyed by member name."
  value = { for name, droplet in digitalocean_droplet.recent : name => {
    id           = droplet.id
    ipv4_private = droplet.ipv4_address_private
    urn          = droplet.urn
    size         = droplet.size
    image        = droplet.image
  } }
}
