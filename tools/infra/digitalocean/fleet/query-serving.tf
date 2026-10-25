variable "query_serving_enabled" {
  description = "Opt in to the separate unified query candidate; preserves the existing fleet."
  type        = bool
  default     = false
}
variable "query_serving_size" {
  type    = string
  default = "m-24vcpu-192gb"
}
variable "query_serving_volume_gib" {
  type    = number
  default = 1024
}
variable "query_serving_hostname" {
  type    = string
  default = "wallet-pir-query-01.valargroup.dev"
}

module "query_serving" {
  source             = "../query-serving"
  count              = var.query_serving_enabled ? 1 : 0
  project_id         = var.wallet_pir_project_id
  region             = var.region
  image              = var.image
  vpc_id             = digitalocean_vpc.wallet.id
  ssh_key_ids        = var.ssh_key_ids
  cloudflare_zone_id = var.cloudflare_zone_id
  size               = var.query_serving_size
  data_volume_gib    = var.query_serving_volume_gib
  hostname           = var.query_serving_hostname
}

output "query_serving_node" {
  value = try(module.query_serving[0].node, null)
}

# The smaller replacement for wallet-pir-query-01 (ams3 offers no 16 vCPU,
# 96 GB size). It runs beside the first node until routing moves to it, so
# retiring the first node is a separate step.
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
