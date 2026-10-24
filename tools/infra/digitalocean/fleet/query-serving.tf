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
