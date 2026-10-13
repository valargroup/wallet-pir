# Volume for the txid display genesis ingest, mounted on the coordinator at
# /srv/txid-display-genesis. It began as scratch for layout experiments and
# census runs, but it now holds production state: the v2x genesis journal
# (journal-v2x) and the publication roots the live txid display controller
# builds from (v4/root for txid-4k8, and the txid-2k8 root kept for rollback).
# See transparent/docs/status.md. Do not destroy or detach it while the txid
# display serves from it. It is kept off the node volume so the ingest cannot
# consume /srv/zakura's 20% headroom.
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
