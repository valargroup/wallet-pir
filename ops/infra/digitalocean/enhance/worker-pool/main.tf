terraform {
  required_version = ">= 1.10.0"
  backend "s3" {}
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.68"
    }
  }
}

provider "digitalocean" {
  token = var.digitalocean_token
}

resource "digitalocean_tag" "worker" {
  name = "enhance-pir-v4-worker"
  lifecycle { prevent_destroy = true }
}

resource "digitalocean_droplet" "worker" {
  count      = var.group_count * 2
  name       = format("enhance-pir-v4-g%02d-r%d", floor(count.index / 2) + 1, count.index % 2 + 1)
  image      = "ubuntu-24-04-x64"
  region     = var.region
  size       = "c-4"
  vpc_uuid   = var.vpc_id
  ssh_keys   = var.ssh_key_ids
  tags       = [digitalocean_tag.worker.name]
  monitoring = true
  ipv6       = true
  user_data  = file("${path.module}/cloud-init-worker.yaml")

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [user_data]
  }
}

# Each resource owns only its worker's additive membership in the existing project.
resource "digitalocean_project_resources" "worker" {
  count     = var.group_count * 2
  project   = var.project_id
  resources = [digitalocean_droplet.worker[count.index].urn]
  lifecycle { prevent_destroy = true }
}

resource "digitalocean_firewall" "worker" {
  name = "enhance-pir-v4-workers"
  tags = [digitalocean_tag.worker.name]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = concat(var.operator_ssh_cidrs, ["${var.coordinator_private_ipv4}/32"])
  }
  inbound_rule {
    protocol         = "tcp"
    port_range       = "8291"
    source_addresses = ["${var.coordinator_private_ipv4}/32"]
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
  lifecycle { prevent_destroy = true }
}
