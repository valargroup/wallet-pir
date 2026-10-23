

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


locals {
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}
