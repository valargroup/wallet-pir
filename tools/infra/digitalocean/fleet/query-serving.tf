# The shared query node. The retired Query 01 provisioning path is removed.
# Keep this module address and its installed inputs unchanged.
variable "query_serving_02_enabled" {
  description = "Opt in to the 128 GB replacement query node; preserves the existing fleet."
  type        = bool
  default     = false
}

module "query_serving_02" {
  source             = "../query-serving"
  count              = var.query_serving_02_enabled ? 1 : 0
  project_id         = var.wallet_pir_project_id
  region             = var.region
  image              = var.image
  vpc_id             = digitalocean_vpc.wallet.id
  ssh_key_ids        = var.ssh_key_ids
  cloudflare_zone_id = var.cloudflare_zone_id
  size               = "m-16vcpu-128gb"
  data_volume_gib    = 512
  hostname           = "wallet-pir-query-02.valargroup.dev"
}

output "query_serving_node_02" {
  value = try(module.query_serving_02[0].node, null)
}
