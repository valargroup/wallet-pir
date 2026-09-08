locals {
  coordinator_name = "enhance-pir-coordinator-01"
  public_hostname  = "enhance-pir.valargroup.dev"
  # The transparent fleet terminates TLS on the worker rather than behind the
  # Enhance coordinator's Caddy. That Caddyfile is rendered and installed by
  # deploy-enhance-pir.sh, and a token it does not substitute would fail
  # `caddy validate` on the next Enhance deploy -- a live service broken by a
  # change with nothing to do with it. When the coordinator daemon lands and
  # there is more than one worker, fronting moves there and this record follows.
  transparent_public_hostname = "transparent-pir.valargroup.dev"
  # Group order is stable shard placement. Replica membership may change
  # without moving shards; append groups before the next six-shard boundary.
  worker_groups = [
    {
      name = "shard-group-01"
      replicas = [
        "enhance-pir-worker-01",
        "enhance-pir-worker-02",
      ]
    },
  ]
  worker_names    = flatten([for group in local.worker_groups : group.replicas])
  common_packages = ["ca-certificates", "curl", "jq", "htop"]
}

resource "digitalocean_vpc" "enhance" {
  name     = "enhance-pir-production"
  region   = var.region
  ip_range = "10.142.0.0/24"
}

resource "digitalocean_tag" "coordinator" {
  name = "enhance-pir-coordinator"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_tag" "worker" {
  name = "enhance-pir-worker"

  lifecycle {
    create_before_destroy = true
  }
}

resource "digitalocean_tag" "transparent_worker" {
  name = "transparent-pir-worker"

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

resource "digitalocean_droplet" "coordinator" {
  name       = local.coordinator_name
  image      = var.image
  region     = var.region
  size       = var.coordinator_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.coordinator.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-coordinator.yaml.tftpl", {
    packages = jsonencode(concat(local.common_packages, ["xfsprogs"]))
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_droplet" "worker" {
  count      = length(local.worker_names)
  name       = local.worker_names[count.index]
  image      = var.image
  region     = var.region
  size       = var.worker_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-worker.yaml.tftpl", {
    packages = jsonencode(local.common_packages)
  })
}

# Transparent PIR shard workers.
#
# Serves a published transparent shard set over the VPC. Deliberately not
# reachable from the internet: the wallet-facing API is not designed yet, and
# the firewall below opens its port to the coordinator tag alone.
resource "digitalocean_droplet" "transparent_worker" {
  count      = var.transparent_worker_count
  name       = format("transparent-pir-worker-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_worker_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  # cloud-init runs once, at first boot. A plan that renders the template
  # without the deploy key must not read as a reason to rebuild the host.
  lifecycle {
    ignore_changes = [user_data]
  }
}

# The transparent two-tier fleet. Same VPC, tag and firewall as the pilot
# worker above, so the deploy reaches every worker the same way; roles differ
# only in size and in the roster the outputs render. Names carry the role so
# an assignment and a `/v1/ready` body read the same as the droplet list.
resource "digitalocean_droplet" "transparent_recent" {
  count      = var.transparent_recent_count
  name       = format("transparent-pir-recent-%02d", count.index + 1)
  image      = var.image
  region     = var.region
  size       = var.transparent_recent_size
  ssh_keys   = var.ssh_key_ids
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
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
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.transparent_worker.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-worker.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
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
  vpc_uuid   = digitalocean_vpc.enhance.id
  tags       = [digitalocean_tag.transparent_router.name]
  monitoring = true
  backups    = var.enable_backups
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init-transparent-router.yaml.tftpl", {
    packages          = jsonencode(concat(local.common_packages, ["caddy"]))
    deploy_public_key = var.transparent_worker_deploy_public_key
  })

  lifecycle {
    ignore_changes = [user_data]
  }
}

resource "digitalocean_volume" "zakura" {
  region                  = var.region
  name                    = "spendability-memo-pir-zakura"
  size                    = 1024
  initial_filesystem_type = "xfs"
  description             = "Zakura archive and canonical Ironwood Enhance records"

  # DigitalOcean cannot rename a volume in place. Keep the historical provider
  # name so this production data volume is never replaced for branding alone.
  lifecycle {
    prevent_destroy = true
    ignore_changes  = [name, description]
  }
}

resource "digitalocean_volume_attachment" "zakura" {
  droplet_id = digitalocean_droplet.coordinator.id
  volume_id  = digitalocean_volume.zakura.id
}

resource "digitalocean_firewall" "coordinator" {
  name = "enhance-pir-coordinator"
  tags = [digitalocean_tag.coordinator.name]

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

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8233"
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

resource "digitalocean_firewall" "worker" {
  name = "enhance-pir-workers"
  tags = [digitalocean_tag.worker.name]

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
    port_range  = "8091"
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
  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
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

  # The shard retrieval port itself stays private. The only things that reach
  # 8093 are the coordinator, for deploy verification, the router, which
  # proxies wallets to it, and Caddy on the pilot host over loopback, which a
  # firewall does not govern.
  inbound_rule {
    protocol    = "tcp"
    port_range  = "8093"
    source_tags = [digitalocean_tag.coordinator.name, digitalocean_tag.transparent_router.name]
  }

  # Public TLS for transparent-pir.valargroup.dev. Port 80 is needed for the
  # ACME HTTP challenge and the redirect to 443; Caddy serves nothing else on it.
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

resource "digitalocean_project_resources" "enhance" {
  project = var.project_id
  resources = concat(
    [digitalocean_droplet.coordinator.urn, digitalocean_volume.zakura.urn],
    [for worker in digitalocean_droplet.worker : worker.urn],
    [for worker in digitalocean_droplet.transparent_worker : worker.urn],
    [for worker in digitalocean_droplet.transparent_recent : worker.urn],
    [for worker in digitalocean_droplet.transparent_archive : worker.urn],
    [for router in digitalocean_droplet.transparent_router : router.urn],
  )
}

resource "cloudflare_dns_record" "enhance" {
  zone_id = var.cloudflare_zone_id
  name    = local.public_hostname
  type    = "A"
  content = var.coordinator_dns_ipv4
  ttl     = 300
  proxied = false
  comment = "Enhance PIR production coordinator; managed by Terraform"
}

# Taken from the droplet attribute rather than a hand-maintained variable, so a
# rebuilt worker cannot leave the record pointing at an address nothing answers
# on. The Enhance record predates this and still carries a hardcoded IP.
#
# The record moves to the router only when the operator says so: a router
# that exists but has no Caddyfile yet must not take the public name from
# the pilot that is serving it.
resource "cloudflare_dns_record" "transparent" {
  count   = var.transparent_router_count + var.transparent_worker_count > 0 ? 1 : 0
  zone_id = var.cloudflare_zone_id
  name    = local.transparent_public_hostname
  type    = "A"
  content = var.transparent_public_dns_target == "router" && var.transparent_router_count > 0 ? digitalocean_droplet.transparent_router[0].ipv4_address : digitalocean_droplet.transparent_worker[0].ipv4_address
  ttl     = 300
  # Unproxied, so Caddy can answer the ACME challenge directly.
  proxied = false
  comment = var.transparent_public_dns_target == "router" && var.transparent_router_count > 0 ? "Transparent PIR router; managed by Terraform" : "Transparent PIR shard worker; managed by Terraform"

  lifecycle {
    precondition {
      condition     = var.transparent_public_dns_target != "router" || var.transparent_router_count > 0
      error_message = "transparent_public_dns_target is \"router\" but no router is provisioned."
    }
  }
}

# Keep the existing production resources while renaming Terraform addresses.
moved {
  from = digitalocean_vpc.memo
  to   = digitalocean_vpc.enhance
}

moved {
  from = digitalocean_project_resources.memo
  to   = digitalocean_project_resources.enhance
}
