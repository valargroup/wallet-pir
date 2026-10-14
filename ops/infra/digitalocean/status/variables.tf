variable "allowed_ssh_cidrs" {
  description = "Operator CIDRs allowed to SSH to firewalled workers and auxiliary hosts; the coordinator has no cloud firewall."
  type        = list(string)
}

variable "coordinator_tag" {
  type = string
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

variable "status_control_public_key" {
  description = "Public half of the coordinator's Status forwarding key; the private key stays in Infisical."
  type        = string
  default     = ""
  validation {
    condition     = var.status_control_public_key == "" || can(regex("^ssh-ed25519 [A-Za-z0-9+/=]+( [^\\s]+)?$", var.status_control_public_key))
    error_message = "Supply an OpenSSH ed25519 public key, never a private key."
  }
}

variable "status_host_count" {
  type    = number
  default = 0
  validation {
    condition     = contains([0, 1], var.status_host_count)
    error_message = "Use zero or one Status host."
  }
}

variable "status_host_size" {
  type    = string
  default = "s-8vcpu-16gb"
}

variable "vpc_id" {
  type = string
}
