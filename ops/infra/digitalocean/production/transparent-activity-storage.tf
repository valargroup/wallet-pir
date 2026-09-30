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
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.transparent_activity.id
}

resource "digitalocean_project_resources" "transparent_activity_storage" {
  project   = var.wallet_pir_project_id
  resources = [digitalocean_volume.transparent_activity.urn]
}
