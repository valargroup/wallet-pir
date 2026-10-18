# Additive membership avoids rewriting the shared project resource set on expansion.
resource "digitalocean_project_resources" "enhance_added_workers" {
  count     = length(local.enhance_worker_names) - 2
  project   = var.wallet_pir_project_id
  resources = [module.enhance.digitalocean_droplet_enhance_worker[count.index + 2].urn]
}
