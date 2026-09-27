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

resource "digitalocean_tag" "status_host" {
  name = "status-pir-host"
}

resource "digitalocean_droplet" "status_host" {
  count  = var.status_host_count
  name   = "status-pir-01"
  image  = var.image
  region = var.region
  size   = var.status_host_size
  # CPU/RAM-only resizes stay reversible; a disk resize cannot be undone.
  resize_disk = false
  ssh_keys    = var.ssh_key_ids
  vpc_uuid    = digitalocean_vpc.wallet.id
  tags        = [digitalocean_tag.status_host.name]
  monitoring  = true
  ipv6        = true
  user_data = templatefile("${path.module}/status/cloud-init.yaml.tftpl", {
    packages           = jsonencode(local.common_packages)
    deploy_public_key  = var.enhance_worker_deploy_public_key
    control_public_key = var.status_control_public_key
  })
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
    precondition {
      condition     = var.status_control_public_key != ""
      error_message = "Set status_control_public_key before creating the Status host."
    }
  }
}

resource "digitalocean_project_resources" "status_host" {
  count     = var.status_host_count
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.status_host[0].urn]
}

resource "digitalocean_firewall" "status_host" {
  name = "status-pir-host"
  tags = [digitalocean_tag.status_host.name]
  inbound_rule {
    protocol    = "tcp"
    port_range  = "22"
    source_tags = [digitalocean_tag.coordinator.name]
  }
  dynamic "inbound_rule" {
    for_each = var.allowed_ssh_cidrs
    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

output "status_host" {
  value = [for host in digitalocean_droplet.status_host : {
    name         = host.name
    public_ipv4  = host.ipv4_address
    private_ipv4 = host.ipv4_address_private
  }]
}
