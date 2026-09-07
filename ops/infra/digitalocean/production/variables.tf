variable "digitalocean_token" {
  description = "DigitalOcean API token supplied through TF_VAR_digitalocean_token."
  type        = string
  sensitive   = true
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token with DNS edit access to valargroup.dev, supplied through TF_VAR_cloudflare_api_token."
  type        = string
  sensitive   = true
}

variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for valargroup.dev."
  type        = string
  default     = "d3ac9657be6101818fed439c62fdcadf"
}

variable "coordinator_dns_ipv4" {
  description = "Stable public IPv4 published for the production coordinator. Update this when the coordinator address changes."
  type        = string
  default     = "167.99.42.60"
}

variable "project_id" {
  description = "Existing enhance-pir DigitalOcean project ID."
  type        = string
  default     = "85639967-fecb-4c8d-88be-c0e3dee3f86c"
}

variable "region" {
  type    = string
  default = "ams3"
}

variable "coordinator_size" {
  type    = string
  default = "m-8vcpu-64gb-intel"
}

variable "worker_size" {
  description = "Worker Droplet size. Four-vCPU workers use the closest AMS3 bundled size; keep process RSS below 2 GiB."
  type        = string
  default     = "s-4vcpu-8gb"
}

# Transparent PIR shard workers. A separate fleet from the Enhance workers
# above: different binary, different tag, different firewall, and sized on a
# measurement of its own rather than on the Enhance worker's 2 GiB.
variable "transparent_worker_count" {
  description = "Transparent PIR shard workers to provision. One serves the whole pilot set."
  type        = number
  default     = 1
}

variable "transparent_worker_size" {
  description = <<-EOT
    Transparent shard worker size. A shard costs ~257 MiB resident (128.5 MiB
    per table segment: a 32 MiB u16 database plus three 32 MiB pack matrices),
    so the 21-shard pilot needs 5.3 GiB and 16 GB leaves it room to double.
    Evaluation is memory-bandwidth-bound and saturates at two threads, so this
    buys memory rather than cores, from the $5.25/GB line.
  EOT
  type        = string
  default     = "s-4vcpu-16gb-amd"
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

variable "image" {
  type    = string
  default = "ubuntu-24-04-x64"
}

variable "ssh_key_ids" {
  description = "Existing public SSH key IDs or fingerprints. Never supply a private key."
  type        = list(string)
}

variable "allowed_ssh_cidrs" {
  description = "Operator CIDRs allowed to SSH to all three hosts."
  type        = list(string)
}

variable "enable_backups" {
  type    = bool
  default = false
}
