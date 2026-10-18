resource "digitalocean_project_resources" "transparent_activity_storage" {
  project   = var.wallet_pir_project_id
  resources = [module.transparent.digitalocean_volume_transparent_activity.urn]
}
