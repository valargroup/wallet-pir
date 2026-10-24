output "node" {
  value = {
    id            = digitalocean_droplet.query.id
    name          = digitalocean_droplet.query.name
    public_ipv4   = digitalocean_droplet.query.ipv4_address
    private_ipv4  = digitalocean_droplet.query.ipv4_address_private
    hostname      = var.hostname
    public_url    = "https://${var.hostname}"
    volume_id     = digitalocean_volume.data.id
    volume_gib    = var.data_volume_gib
    state_root    = "/srv/wallet-pir-query"
    management    = "127.0.0.1:18081"
    public_listen = "127.0.0.1:8090"
  }
}
