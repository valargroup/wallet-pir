variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for valargroup.dev."
  type        = string
  default     = "d3ac9657be6101818fed439c62fdcadf"
}

variable "coordinator_id" {
  type = string
}

variable "coordinator_tag" {
  type = string
}

variable "enable_backups" {
  type    = bool
  default = false
}

variable "image" {
  type    = string
  default = "ubuntu-24-04-x64"
}

variable "region" {
  type    = string
  default = "ams3"
}

variable "ssh_key_ids" {
  description = "Existing public SSH key IDs or fingerprints. Never supply a private key."
  type        = list(string)
}

variable "transparent_txid_display_archive_count" {
  description = "Txid display archive hosts: zero or one."
  type        = number
  default     = 0

  validation {
    condition     = var.transparent_txid_display_archive_count >= 0 && var.transparent_txid_display_archive_count <= 1
    error_message = "The txid display archive is a single host."
  }
}

variable "transparent_txid_display_archive_size" {
  description = "Txid display archive host size."
  type        = string
  default     = "m-8vcpu-64gb"
}

variable "transparent_txid_display_site_enabled" {
  description = "Whether the one-query txid display site host and its DNS record exist."
  type        = bool
  default     = false
}

variable "transparent_txid_display_site_size" {
  description = "One-query txid display site host size."
  type        = string
  # The AMD m-8vcpu-64gb was not offered in ams3 on 2026-10-12.
  default = "m-8vcpu-64gb-intel"
}

variable "transparent_worker_deploy_public_key" {
  description = <<-EOT
    Public half of the fleet deploy key, installed for root at first boot.

    The existing Enhance hosts got this key by a hand edit of authorized_keys
    rather than through DigitalOcean's ssh_keys, so a freshly provisioned
    droplet would not accept the deploy workflow's key and every deploy would
    need a manual fixup first. Rendering it through cloud-init makes a new
    worker deployable the moment it boots. Public half only; never the private.
  EOT
  type        = string
  default     = ""

  validation {
    condition     = var.transparent_worker_deploy_public_key == "" || can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-) ", var.transparent_worker_deploy_public_key))
    error_message = "Must be an OpenSSH public key line, or empty. A private key starts with '-----BEGIN'."
  }
}

variable "transparent_worker_tag" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "apm_sidecar_boot_script" {
  description = "First-boot pir-apm-sidecar install appended to new droplets' user_data by the fleet root (empty leaves user_data unchanged). Public binary reference only; never key material."
  type        = string
  default     = ""
}
