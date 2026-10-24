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

variable "transparent_archive_names" {
  description = "Archive-tier owner Droplet names. The inventory's partition assigns each live owner one contiguous range of archive shards; losing an owner makes its range unavailable until rebuilt. Remove a name only after the inventory has retired that owner."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for name in var.transparent_archive_names : can(regex("^transparent-pir-archive-[0-9]{2}$", name))])
    error_message = "Archive owner names must match transparent-pir-archive-NN."
  }

  validation {
    condition     = length(distinct(var.transparent_archive_names)) == length(var.transparent_archive_names)
    error_message = "Archive owner names must be unique."
  }
}

variable "transparent_archive_size" {
  description = "Archive owner size: the accepted 8 vCPU / 64 GiB regular memory-optimized host (48 GiB runtime cache, 56 GiB MemoryMax)."
  type        = string
  default     = "m-8vcpu-64gb"
}

variable "transparent_loadgen_count" {
  description = "Dedicated-CPU load generators for the Gate 6 series; the coordinator's shared vCPUs saturate as a client at 128 wallets. Zero when not measuring."
  type        = number
  default     = 0
}

variable "transparent_loadgen_size" {
  description = "Load generator size: dedicated CPU, so the client's own PIR work does not bound what the fleet is measured at."
  type        = string
  default     = "c-16"
}

variable "transparent_recent_count" {
  description = "Recent-tier replicas. Every replica holds the whole recent tier."
  type        = number
  default     = 0
}

variable "transparent_recent_size" {
  description = "Recent replica size: the accepted 4 vCPU / 8 GiB regular Basic host (5 GiB runtime cache, 7 GiB MemoryMax)."
  type        = string
  default     = "s-4vcpu-8gb"
}

variable "transparent_router_count" {
  description = "Routers in front of the fleet: zero or one. The public DNS record follows the router."
  type        = number
  default     = 0

  validation {
    condition     = var.transparent_router_count >= 0 && var.transparent_router_count <= 1
    error_message = "The router is a single host; the deployment accepts it as a single point of failure."
  }
}

variable "transparent_router_size" {
  description = "Router size: a 4 GiB Basic host; it proxies and terminates TLS only."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "transparent_v12_enabled" {
  description = "Whether the v12 worker and its staging DNS record exist."
  type        = bool
  default     = false
}

variable "transparent_v12_size" {
  description = "Transparent v12 worker size."
  type        = string
  default     = "m-8vcpu-64gb"
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

variable "vpc_id" {
  type = string
}

variable "apm_sidecar_boot_script" {
  description = "First-boot pir-apm-sidecar install appended to new droplets' user_data by the fleet root (empty leaves user_data unchanged). Public binary reference only; never key material."
  type        = string
  default     = ""
}
