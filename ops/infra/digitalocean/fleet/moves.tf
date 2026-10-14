moved {
  from = digitalocean_tag.enhance_worker
  to   = module.enhance.digitalocean_tag.enhance_worker
}

moved {
  from = digitalocean_droplet.enhance_worker
  to   = module.enhance.digitalocean_droplet.enhance_worker
}

moved {
  from = digitalocean_droplet.enhance_legacy_worker
  to   = module.enhance.digitalocean_droplet.enhance_legacy_worker
}

moved {
  from = digitalocean_firewall.enhance_worker
  to   = module.enhance.digitalocean_firewall.enhance_worker
}

moved {
  from = cloudflare_dns_record.enhance
  to   = module.enhance.cloudflare_dns_record.enhance
}

moved {
  from = digitalocean_tag.enhance_packing_router
  to   = module.enhance.digitalocean_tag.enhance_packing_router
}

moved {
  from = digitalocean_droplet.enhance_packing_router
  to   = module.enhance.digitalocean_droplet.enhance_packing_router
}

moved {
  from = digitalocean_firewall.enhance_packing_router
  to   = module.enhance.digitalocean_firewall.enhance_packing_router
}

moved {
  from = digitalocean_droplet.enhance_pool_worker
  to   = module.enhance.digitalocean_droplet.enhance_pool_worker
}

moved {
  from = digitalocean_tag.status_host
  to   = module.status.digitalocean_tag.status_host
}

moved {
  from = digitalocean_droplet.status_host
  to   = module.status.digitalocean_droplet.status_host
}

moved {
  from = digitalocean_firewall.status_host
  to   = module.status.digitalocean_firewall.status_host
}

moved {
  from = digitalocean_tag.transparent_worker
  to   = module.transparent.digitalocean_tag.transparent_worker
}

moved {
  from = digitalocean_tag.transparent_loadgen
  to   = module.transparent.digitalocean_tag.transparent_loadgen
}

moved {
  from = digitalocean_tag.transparent_router
  to   = module.transparent.digitalocean_tag.transparent_router
}

moved {
  from = digitalocean_droplet.transparent_recent
  to   = module.transparent.digitalocean_droplet.transparent_recent
}

moved {
  from = digitalocean_droplet.transparent_archive
  to   = module.transparent.digitalocean_droplet.transparent_archive
}

moved {
  from = digitalocean_droplet.transparent_loadgen
  to   = module.transparent.digitalocean_droplet.transparent_loadgen
}

moved {
  from = digitalocean_droplet.transparent_router
  to   = module.transparent.digitalocean_droplet.transparent_router
}

moved {
  from = digitalocean_firewall.transparent_worker
  to   = module.transparent.digitalocean_firewall.transparent_worker
}

moved {
  from = digitalocean_firewall.transparent_router
  to   = module.transparent.digitalocean_firewall.transparent_router
}

moved {
  from = digitalocean_firewall.transparent_loadgen
  to   = module.transparent.digitalocean_firewall.transparent_loadgen
}

moved {
  from = cloudflare_dns_record.transparent
  to   = module.transparent.cloudflare_dns_record.transparent
}

moved {
  from = digitalocean_droplet.transparent_v12
  to   = module.transparent.digitalocean_droplet.transparent_v12
}

moved {
  from = digitalocean_tag.transparent_v12
  to   = module.transparent.digitalocean_tag.transparent_v12
}

moved {
  from = digitalocean_firewall.transparent_v12
  to   = module.transparent.digitalocean_firewall.transparent_v12
}

moved {
  from = cloudflare_dns_record.transparent_v12_staging
  to   = module.transparent.cloudflare_dns_record.transparent_v12_staging
}

moved {
  from = digitalocean_volume.transparent_activity
  to   = module.transparent.digitalocean_volume.transparent_activity
}

moved {
  from = digitalocean_volume_attachment.transparent_activity
  to   = module.transparent.digitalocean_volume_attachment.transparent_activity
}

moved {
  from = digitalocean_droplet.transparent_txid_display_archive
  to   = module.transparent_tx_detail.digitalocean_droplet.transparent_txid_display_archive
}

moved {
  from = digitalocean_droplet.transparent_txid_display_site
  to   = module.transparent_tx_detail.digitalocean_droplet.transparent_txid_display_site
}

moved {
  from = digitalocean_tag.transparent_txid_display_site
  to   = module.transparent_tx_detail.digitalocean_tag.transparent_txid_display_site
}

moved {
  from = digitalocean_firewall.transparent_txid_display_site
  to   = module.transparent_tx_detail.digitalocean_firewall.transparent_txid_display_site
}

moved {
  from = cloudflare_dns_record.transparent_txid_display_site
  to   = module.transparent_tx_detail.cloudflare_dns_record.transparent_txid_display_site
}

moved {
  from = digitalocean_volume.txid_display_genesis
  to   = module.transparent_tx_detail.digitalocean_volume.txid_display_genesis
}

moved {
  from = digitalocean_volume_attachment.txid_display_genesis
  to   = module.transparent_tx_detail.digitalocean_volume_attachment.txid_display_genesis
}

