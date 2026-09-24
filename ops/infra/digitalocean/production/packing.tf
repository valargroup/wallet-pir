# Packing scales independently of evaluation workers. Zero preserves the existing
# deployment until the standalone HTTP path is qualified on the intended host.
variable "enhance_packing_router_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_packing_router_count == floor(var.enhance_packing_router_count) && var.enhance_packing_router_count >= 0 && var.enhance_packing_router_count <= 8
    error_message = "Use zero to eight independently assigned packing routers."
  }
}

resource "digitalocean_tag" "enhance_packing_router" {
  name = "enhance-pir-packing-router"
}

resource "digitalocean_droplet" "enhance_packing_router" {
  count      = var.enhance_packing_router_count
  name       = format("enhance-pir-packing-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = "c-4"
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.enhance_packing_router.name]
  monitoring = true
  ipv6       = true
  user_data  = <<-YAML
    #cloud-config
    package_update: true
    packages: [ca-certificates, curl, jq]
    runcmd:
      - [useradd, --system, --home-dir, /srv/enhance-pir-router, --shell, /usr/sbin/nologin, enhance-pir]
      - [install, -d, -o, enhance-pir, -g, enhance-pir, /srv/enhance-pir-router]
  YAML
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}

resource "digitalocean_project_resources" "enhance_packing_router" {
  count     = var.enhance_packing_router_count
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.enhance_packing_router[count.index].urn]
}

resource "digitalocean_firewall" "enhance_packing_router" {
  name = "enhance-pir-packing-routers"
  tags = [digitalocean_tag.enhance_packing_router.name]
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
  inbound_rule {
    protocol    = "tcp"
    port_range  = "8092-8093"
    source_tags = [digitalocean_tag.coordinator.name]
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

output "enhance_packing_routers" {
  value = [for router in digitalocean_droplet.enhance_packing_router : {
    name         = router.name
    public_ipv4  = router.ipv4_address
    private_ipv4 = router.ipv4_address_private
  }]
}

# Additional evaluation workers are independent of the legacy paired inventory.
variable "enhance_pool_worker_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_pool_worker_count == floor(var.enhance_pool_worker_count) && var.enhance_pool_worker_count >= 0 && var.enhance_pool_worker_count <= 14
    error_message = "Add zero to fourteen individually placed workers."
  }
}
resource "digitalocean_droplet" "enhance_pool_worker" {
  count      = var.enhance_pool_worker_count
  name       = format("enhance-pir-pool-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = "c-4"
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.enhance_worker.name]
  monitoring = true
  ipv6       = true
  user_data = templatefile("${path.module}/enhance/cloud-init-worker.yaml.tftpl", {
    packages          = jsonencode(local.common_packages)
    deploy_public_key = var.enhance_worker_deploy_public_key
  })
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}
resource "digitalocean_project_resources" "enhance_pool_worker" {
  count     = var.enhance_pool_worker_count
  project   = var.wallet_pir_project_id
  resources = [digitalocean_droplet.enhance_pool_worker[count.index].urn]
}
output "enhance_pool_workers" {
  value = [for worker in digitalocean_droplet.enhance_pool_worker : {
    name         = worker.name
    public_ipv4  = worker.ipv4_address
    private_ipv4 = worker.ipv4_address_private
  }]
}
