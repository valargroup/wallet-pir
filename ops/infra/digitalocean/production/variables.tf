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

variable "wallet_pir_coordinator_dns_ipv4" {
  description = "Stable public IPv4 published for the shared Wallet PIR coordinator. Update this when the coordinator address changes."
  type        = string
  default     = "167.99.42.60"
}

variable "wallet_pir_project_id" {
  description = "Existing wallet-pir DigitalOcean project ID."
  type        = string
  default     = "85639967-fecb-4c8d-88be-c0e3dee3f86c"
}

variable "region" {
  type    = string
  default = "ams3"
}

variable "wallet_pir_coordinator_size" {
  type    = string
  default = "m-8vcpu-64gb-intel"
}

variable "enhance_worker_size" {
  description = "Enhance worker size: four dedicated vCPUs and 8 GiB RAM."
  type        = string
  default     = "c-4"
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
  description = "Operator CIDRs allowed to SSH to firewalled workers and auxiliary hosts; the coordinator has no cloud firewall."
  type        = list(string)
}

variable "enable_backups" {
  type    = bool
  default = false
}

# The transparent two-tier fleet accepted in transparent/docs/deployment.md:
# recent replicas serving the tier from the cutoff, archive owners each holding
# a contiguous half of the archive tier, and one router in front of both. All
# counts default to zero so a checkout that has not adopted the fleet plans no
# change; the counts are raised through TF_VAR_* when the hosts are provisioned.
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

variable "transparent_recent_cache_bytes" {
  description = "Runtime cache reservation per recent replica, bytes. Deployment accepts 5 GiB."
  type        = number
  default     = 5368709120
}

variable "transparent_recent_memory_max" {
  description = "systemd MemoryMax for the recent replica unit. Deployment accepts 7 GiB."
  type        = string
  default     = "7G"
}

variable "transparent_archive_count" {
  description = "Archive-tier owners. Each owns a disjoint contiguous range of archive shards; losing one makes its range unavailable until rebuilt."
  type        = number
  default     = 0
}

variable "transparent_archive_size" {
  description = "Archive owner size: the accepted 8 vCPU / 64 GiB regular memory-optimized host (48 GiB runtime cache, 56 GiB MemoryMax)."
  type        = string
  default     = "m-8vcpu-64gb"
}

variable "transparent_archive_cache_bytes" {
  description = "Runtime cache reservation per archive owner, bytes. Deployment accepts 48 GiB."
  type        = number
  default     = 51539607552
}

variable "transparent_archive_memory_max" {
  description = "systemd MemoryMax for the archive owner unit. Deployment accepts 56 GiB."
  type        = string
  default     = "56G"
}

variable "transparent_router_count" {
  description = "Routers in front of the fleet: zero or one. The public DNS record follows the router when it exists, else the pilot worker."
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

variable "transparent_public_dns_target" {
  description = <<-EOT
    Which host transparent-pir.valargroup.dev points at: "worker" keeps the
    pilot worker serving while the fleet is provisioned and staged; "router"
    is the activation, switched by the operator after fleet-deploy has the
    router answering.
  EOT
  type        = string
  default     = "worker"

  validation {
    condition     = contains(["worker", "router"], var.transparent_public_dns_target)
    error_message = "Must be \"worker\" or \"router\"."
  }
}

variable "transparent_archive_build_slots" {
  description = <<-EOT
    Runtime builds an archive owner runs at once, for the prewarm and for
    requests. Deployment records 1 as the initial value; the first fleet
    activation measured one slot at about 4.4 s per table runtime, 12 minutes
    per owner. Raising it is a measured decision recorded in deployment.md.
  EOT
  type        = number
  default     = 1
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

variable "transparent_loadgen_extra_ssh_cidrs" {
  description = "Additional existing load-generator SSH sources, preserved during shared-state Enhance expansion."
  type        = list(string)
  default     = []
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
