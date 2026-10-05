terraform {
  required_version = ">= 1.10.0"

  # The production root's DigitalOcean constraint. This root manages nothing
  # else, so it needs no other provider.
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
