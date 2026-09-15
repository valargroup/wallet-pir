resource "digitalocean_tag" "transparent_worker" {
  name = "transparent-pir-worker"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_tag" "transparent_loadgen" {
  count = var.transparent_loadgen_count > 0 ? 1 : 0
  name  = "transparent-pir-loadgen"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_tag" "transparent_router" {
  name = "transparent-pir-router"

  lifecycle {
    create_before_destroy = true
  }
}

# The transparent two-tier fleet. Both tiers share the worker tag and firewall;
# roles differ in size and in the roster the outputs render. Names carry the
# role so an assignment and a `/v1/ready` body read the same as the droplet list.
resource "digitalocean_droplet" "transparent_recent" {
  count      = var.transparent_recent_count
  name       = format("transparent-pir-recent-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_recent_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/transparent/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_droplet" "transparent_archive" {
  count      = var.transparent_archive_count
  name       = format("transparent-pir-archive-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_archive_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/transparent/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

# Load generators: measurement clients inside the VPC with dedicated CPUs,
# reaching the workers and the router's internal listener. Not part of the
# serving fleet; provisioned for a measurement and destroyed after.
resource "digitalocean_droplet" "transparent_loadgen" {
  count      = var.transparent_loadgen_count
  name       = format("transparent-pir-loadgen-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_loadgen_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_loadgen[0].name]
  monitoring = true
  backups    = false
  ipv6       = false

  user_data = templatefile("${path.module}/transparent/cloud-init-transparent-router.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["build-essential", "pkg-config"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

# The router: the only public host of the fleet. It terminates TLS and routes
# each shard id to its owner or the replica pool from a Caddyfile the deploy
# renders out of the assignment.
resource "digitalocean_droplet" "transparent_router" {
  count      = var.transparent_router_count
  name       = format("transparent-pir-router-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_router_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.wallet.id
  tags       = [digitalocean_tag.transparent_router.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/transparent/cloud-init-transparent-router.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

# Same shape as the Enhance worker firewall: SSH from the coordinator and from
# operator CIDRs, the service port from the coordinator tag alone, and no public
# ingress at all.
resource "digitalocean_firewall" "transparent_worker" {
  name = "transparent-pir-workers"
  tags = [digitalocean_tag.transparent_worker.name]

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

  # The shard retrieval port itself stays private. The coordinator verifies
  # deployments, the router proxies wallets, and optional load generators
  # exercise the fleet from inside the VPC.
  inbound_rule {
    protocol   = "tcp"
    port_range = "8093"
    source_tags = concat(
      [digitalocean_tag.coordinator.name, digitalocean_tag.transparent_router.name],
      digitalocean_tag.transparent_loadgen[*].name,
    )
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
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# The router: SSH from the coordinator and operators, public TLS, nothing else
# inbound. Its egress to the workers' 8093 is governed by their firewall.
resource "digitalocean_firewall" "transparent_router" {
  name = "transparent-pir-router"
  tags = [digitalocean_tag.transparent_router.name]

  inbound_rule {
    protocol    = "tcp"
    port_range  = "22"
    source_tags = [digitalocean_tag.coordinator.name]
  }

  # The router's internal plain-HTTP listener, the same routes as the public
  # site without TLS, for the load harness and deploy verification from the
  # coordinator before the public name points here. VPC only.
  inbound_rule {
    protocol   = "tcp"
    port_range = "8080"
    source_tags = concat(
      [digitalocean_tag.coordinator.name],
      digitalocean_tag.transparent_loadgen[*].name,
    )
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
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }
  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
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
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_firewall" "transparent_loadgen" {
  count = var.transparent_loadgen_count > 0 ? 1 : 0
  name  = "transparent-pir-loadgen"
  tags  = [digitalocean_tag.transparent_loadgen[0].name]

  dynamic "inbound_rule" {
    for_each = toset(concat(var.allowed_ssh_cidrs, var.transparent_loadgen_extra_ssh_cidrs))
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
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# Taken from the router attribute rather than a hand-maintained variable, so a
# rebuilt router cannot leave the record pointing at an address nothing answers
# on. The Enhance record predates this and still carries a hardcoded IP.
resource "cloudflare_dns_record" "transparent" {
  count   = var.transparent_router_count
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_public_hostname
  type    = "A"
  content = digitalocean_droplet.transparent_router[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = "Transparent PIR router; managed by Terraform"
}

