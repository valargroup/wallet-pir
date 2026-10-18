resource "digitalocean_project_resources" "txid_display_genesis_storage" {
  project   = var.wallet_pir_project_id
  resources = [module.transparent_tx_detail.digitalocean_volume_txid_display_genesis.urn]
}
