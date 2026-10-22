variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for valargroup.dev."
  type        = string
  default     = "d3ac9657be6101818fed439c62fdcadf"
}

variable "coordinator_tag" {
  type = string
}

variable "enable_backups" {
  type    = bool
  default = false
}

variable "enhance_group_count" {
  description = "Desired Enhance range count; use the autoscale state wrapper for every plan/apply."
  type        = number
  default     = 1
  validation {
    condition     = var.enhance_group_count >= 1 && var.enhance_group_count <= 4 && floor(var.enhance_group_count) == var.enhance_group_count
    error_message = "Enhance supports one to four groups (two replicas each)."
  }
}

variable "enhance_legacy_worker_count" {
  description = "Initial c-4 migration only: retain the two original Basic workers after moving their state addresses. Return to zero only after rollout acceptance."
  type        = number
  default     = 0
  validation {
    condition     = contains([0, 2], var.enhance_legacy_worker_count)
    error_message = "Retain either zero or both legacy workers."
  }
}

variable "enhance_packing_router_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_packing_router_count == floor(var.enhance_packing_router_count) && var.enhance_packing_router_count >= 0 && var.enhance_packing_router_count <= 8
    error_message = "Use zero to eight independently assigned packing routers."
  }
}

variable "enhance_pool_worker_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_pool_worker_count == floor(var.enhance_pool_worker_count) && var.enhance_pool_worker_count >= 0 && var.enhance_pool_worker_count <= 14
    error_message = "Add zero to fourteen individually placed workers."
  }
}

variable "enhance_worker_deploy_public_key" {
  description = "Public deployment key installed on new Enhance workers; the private key stays in Infisical."
  type        = string
  default     = ""
  validation {
    condition     = var.enhance_worker_deploy_public_key == "" || can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-) ", var.enhance_worker_deploy_public_key))
    error_message = "Supply an OpenSSH public key, never a private key."
  }
}

variable "enhance_worker_size" {
  description = "Enhance worker size: four dedicated vCPUs and 8 GiB RAM."
  type        = string
  default     = "c-4"
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

variable "vpc_id" {
  type = string
}

variable "wallet_pir_coordinator_dns_ipv4" {
  description = "Stable public IPv4 published for the shared Wallet PIR coordinator. Update this when the coordinator address changes."
  type        = string
  default     = "167.99.42.60"
}
