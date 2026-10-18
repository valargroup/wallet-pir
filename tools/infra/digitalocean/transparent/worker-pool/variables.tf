variable "digitalocean_token" {
  description = "DigitalOcean API token supplied through TF_VAR_digitalocean_token."
  type        = string
  sensitive   = true
}

variable "region" {
  type    = string
  default = "ams3"
}

variable "vpc_uuid" {
  description = "The fleet VPC the production root owns (wallet-pir-production), or a bench VPC."
  type        = string
  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.vpc_uuid))
    error_message = "Supply the VPC's UUID."
  }
}

variable "project_id" {
  description = "Existing wallet-pir DigitalOcean project; each member joins it additively."
  type        = string
  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.project_id))
    error_message = "Supply the project's UUID."
  }
}

variable "ssh_key_ids" {
  description = "Registered public key IDs or fingerprints, including Roman's ~/.ssh/id_ed25519.pub. Never a private key."
  type        = list(string)
  validation {
    condition     = length(var.ssh_key_ids) > 0 && length(distinct(var.ssh_key_ids)) == length(var.ssh_key_ids)
    error_message = "Supply distinct registered SSH keys including Roman's key."
  }
}

# Referenced by name only. The production root owns this tag and the firewall
# that selects it, so an elastic member gets exactly the static workers' ingress
# and this root can never change it.
variable "worker_tag" {
  type    = string
  default = "transparent-pir-worker"
  validation {
    condition     = can(regex("^[A-Za-z0-9_:-]{1,255}$", var.worker_tag))
    error_message = "Supply an existing DigitalOcean tag name."
  }
}

variable "deploy_public_key" {
  description = "Public half of the fleet deploy key, installed for root at first boot, as in the production root."
  type        = string
  default     = ""
  validation {
    condition     = var.deploy_public_key == "" || can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-) ", var.deploy_public_key))
    error_message = "Must be an OpenSSH public key line, or empty. A private key starts with '-----BEGIN'."
  }
}

# Every elastic recent replica, keyed by its permanent name. One droplet and
# one project entry per key, so adding or removing a member never updates a
# shared resource. The image is pinned per member and ignored after creation.
variable "members" {
  type = map(object({
    size  = string
    image = string
  }))
  default = {}
  validation {
    condition = alltrue([for name, member in var.members :
    can(regex("^transparent-pir-recent-[0-9]{2,3}$", name)) && member.size != "" && member.image != ""])
    error_message = "Members are recent replicas named transparent-pir-recent-NN, each with a size and a pinned image."
  }
}

# Pinned ed25519 host keys, written by cloud-init so the first SSH connection
# is verified against a key the actuator recorded before the droplet existed.
# Needed only for members being created: user data is ignored afterwards and
# bootstrap rotates the key, since user data stays readable from the droplet's
# metadata service and from provider state.
variable "host_keys" {
  type = map(object({
    private = string
    public  = string
  }))
  default   = {}
  sensitive = true
  validation {
    condition = alltrue([for name, key in var.host_keys :
      contains(keys(var.members), name)
      && startswith(key.public, "ssh-ed25519 ")
    && startswith(trimspace(key.private), "-----BEGIN OPENSSH PRIVATE KEY-----")])
    error_message = "Host keys must be OpenSSH ed25519 key pairs, one per listed member."
  }
}
