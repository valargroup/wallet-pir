# Dedicated scratch volume for the txid display genesis ingest and its layout
# experiments (journal from height 0, census scratch, experimental publication
# roots). Kept off the node volume so the ingest cannot consume /srv/zakura's
# 20% headroom. Nothing on it is served; destroy it once the backfill lineage
# has its own host.
resource "digitalocean_volume" "txid_display_genesis" {
  region                  = var.region
  name                    = "wallet-pir-txid-display-genesis"
  size                    = 150
  initial_filesystem_type = "xfs"
  description             = "Txid display genesis journal and census scratch; layout experiments, not served"
}

resource "digitalocean_volume_attachment" "txid_display_genesis" {
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.txid_display_genesis.id
}

resource "digitalocean_project_resources" "txid_display_genesis_storage" {
  project   = var.wallet_pir_project_id
  resources = [digitalocean_volume.txid_display_genesis.urn]
}
