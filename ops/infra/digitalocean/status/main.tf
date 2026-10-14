

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
  vpc_uuid    = var.vpc_id
  tags        = [digitalocean_tag.status_host.name]
  monitoring  = true
  ipv6        = true
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
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


resource "digitalocean_firewall" "status_host" {
  name = "status-pir-host"
  tags = [digitalocean_tag.status_host.name]
  inbound_rule {
    protocol    = "tcp"
    port_range  = "22"
    source_tags = [var.coordinator_tag]
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

locals {
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}
