module "enhance" {
  source                           = "../enhance"
  allowed_ssh_cidrs                = var.allowed_ssh_cidrs
  cloudflare_zone_id               = var.cloudflare_zone_id
  coordinator_tag                  = digitalocean_tag.coordinator.name
  enable_backups                   = var.enable_backups
  enhance_group_count              = var.enhance_group_count
  enhance_legacy_worker_count      = var.enhance_legacy_worker_count
  enhance_packing_router_count     = var.enhance_packing_router_count
  enhance_pool_worker_count        = var.enhance_pool_worker_count
  enhance_worker_deploy_public_key = var.enhance_worker_deploy_public_key
  enhance_worker_size              = var.enhance_worker_size
  image                            = var.image
  region                           = var.region
  ssh_key_ids                      = var.ssh_key_ids
  vpc_id                           = digitalocean_vpc.wallet.id
  wallet_pir_coordinator_dns_ipv4  = var.wallet_pir_coordinator_dns_ipv4
}

module "status" {
  source                           = "../status"
  allowed_ssh_cidrs                = var.allowed_ssh_cidrs
  coordinator_tag                  = digitalocean_tag.coordinator.name
  enhance_worker_deploy_public_key = var.enhance_worker_deploy_public_key
  image                            = var.image
  region                           = var.region
  ssh_key_ids                      = var.ssh_key_ids
  status_control_public_key        = var.status_control_public_key
  status_host_count                = var.status_host_count
  status_host_size                 = var.status_host_size
  vpc_id                           = digitalocean_vpc.wallet.id
}

module "transparent" {
  source                                = "../transparent"
  allowed_ssh_cidrs                     = var.allowed_ssh_cidrs
  cloudflare_zone_id                    = var.cloudflare_zone_id
  coordinator_id                        = digitalocean_droplet.coordinator.id
  coordinator_tag                       = digitalocean_tag.coordinator.name
  enable_backups                        = var.enable_backups
  image                                 = var.image
  region                                = var.region
  ssh_key_ids                           = var.ssh_key_ids
  transparent_archive_names             = var.transparent_archive_names
  transparent_archive_size              = var.transparent_archive_size
  transparent_loadgen_count             = var.transparent_loadgen_count
  transparent_loadgen_extra_ssh_cidrs   = var.transparent_loadgen_extra_ssh_cidrs
  transparent_loadgen_size              = var.transparent_loadgen_size
  transparent_recent_count              = var.transparent_recent_count
  transparent_recent_size               = var.transparent_recent_size
  transparent_router_count              = var.transparent_router_count
  transparent_router_size               = var.transparent_router_size
  transparent_txid_display_port_enabled = var.transparent_txid_display_port_enabled
  transparent_v12_enabled               = var.transparent_v12_enabled
  transparent_v12_size                  = var.transparent_v12_size
  transparent_worker_deploy_public_key  = var.transparent_worker_deploy_public_key
  vpc_id                                = digitalocean_vpc.wallet.id
}

module "transparent_tx_detail" {
  source                                 = "../transparent-tx-detail"
  cloudflare_zone_id                     = var.cloudflare_zone_id
  coordinator_id                         = digitalocean_droplet.coordinator.id
  coordinator_tag                        = digitalocean_tag.coordinator.name
  enable_backups                         = var.enable_backups
  image                                  = var.image
  region                                 = var.region
  ssh_key_ids                            = var.ssh_key_ids
  transparent_txid_display_archive_count = var.transparent_txid_display_archive_count
  transparent_txid_display_archive_size  = var.transparent_txid_display_archive_size
  transparent_txid_display_site_enabled  = var.transparent_txid_display_site_enabled
  transparent_txid_display_site_size     = var.transparent_txid_display_site_size
  transparent_worker_deploy_public_key   = var.transparent_worker_deploy_public_key
  transparent_worker_tag                 = module.transparent.digitalocean_tag_transparent_worker.name
  vpc_id                                 = digitalocean_vpc.wallet.id
}

