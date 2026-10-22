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
