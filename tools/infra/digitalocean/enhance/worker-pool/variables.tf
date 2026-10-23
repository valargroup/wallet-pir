variable "digitalocean_token" {
  type      = string
  sensitive = true
}
variable "project_id" {
  description = "Existing wallet-pir project in the verified Valargroup account."
  type        = string
}
variable "vpc_id" {
  description = "Existing VPC shared with the v4 coordinator."
  type        = string
}
variable "region" {
  type    = string
  default = "ams3"
}
variable "group_count" {
  type    = number
  default = 0
  validation {
    condition     = var.group_count == floor(var.group_count) && var.group_count >= 0 && var.group_count <= 4
    error_message = "Use zero to four complete replica pairs."
  }
}
variable "ssh_key_ids" {
  description = "Registered public key IDs/fingerprints, including Roman's ~/.ssh/id_ed25519.pub."
  type        = list(string)
  validation {
    condition     = length(var.ssh_key_ids) > 0 && length(distinct(var.ssh_key_ids)) == length(var.ssh_key_ids)
    error_message = "Supply distinct registered SSH keys including Roman's key."
  }
}
