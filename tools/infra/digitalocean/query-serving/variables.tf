variable "project_id" {
  description = "Existing Valargroup Wallet PIR project; this module creates no project."
  type        = string
}
variable "vpc_id" {
  type = string
}
variable "region" {
  type = string
}
variable "image" {
  type    = string
  default = "ubuntu-24-04-x64"
}
variable "ssh_key_ids" {
  type = list(string)
  validation {
    condition     = length(var.ssh_key_ids) > 0
    error_message = "Install at least one existing approved public SSH key."
  }
}
variable "size" {
  description = "The approved starting capacity: 24 dedicated vCPUs and 192 GiB RAM."
  type        = string
  default     = "m-24vcpu-192gb"
}
variable "data_volume_gib" {
  type    = number
  default = 1024
  validation {
    condition     = var.data_volume_gib >= 512 && floor(var.data_volume_gib) == var.data_volume_gib
    error_message = "The full-history node needs an integer volume size of at least 512 GiB."
  }
}
variable "cloudflare_zone_id" {
  type = string
}
variable "hostname" {
  description = "Dedicated candidate DNS only; never replace the existing wallet endpoint."
  type        = string
  default     = "wallet-pir-query-01.valargroup.dev"
  validation {
    condition     = can(regex("^wallet-pir-query-[0-9]{2}\\.valargroup\\.dev$", var.hostname))
    error_message = "Use a dedicated wallet-pir-query-NN.valargroup.dev hostname."
  }
}
