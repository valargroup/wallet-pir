# Status router and worker share one CPU droplet. The coordinator reaches them only
# through the forwarding-only `status-control` SSH account; no PIR port is exposed.
variable "status_host_count" {
  type    = number
  default = 0
  validation {
    condition     = contains([0, 1], var.status_host_count)
    error_message = "Use zero or one Status host."
  }
}


# The CPU worker's retained generations outgrew 6 GiB under sustained 20 QPS
# with live blocks, and preparation saturated four vCPUs at block arrivals.
variable "status_host_size" {
  type    = string
  default = "s-8vcpu-16gb"
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


resource "digitalocean_project_resources" "status_host" {
  count     = var.status_host_count
  project   = var.wallet_pir_project_id
  resources = [module.status.digitalocean_droplet_status_host[0].urn]
}


output "status_host" {
  value = [for host in module.status.digitalocean_droplet_status_host : {
    name         = host.name
    public_ipv4  = host.ipv4_address
    private_ipv4 = host.ipv4_address_private
  }]
}
