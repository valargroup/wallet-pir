mock_provider "digitalocean" {
  mock_resource "digitalocean_droplet" {
    defaults = {
      id                   = "12345"
      ipv4_address         = "192.0.2.10"
      ipv4_address_private = "10.142.0.10"
      urn                  = "do:droplet:12345"
    }
  }
  mock_resource "digitalocean_volume" {
    defaults = {
      id  = "00000000-0000-0000-0000-000000000002"
      urn = "do:volume:00000000-0000-0000-0000-000000000002"
    }
  }
}
mock_provider "cloudflare" {}

variables {
  project_id         = "00000000-0000-0000-0000-000000000001"
  vpc_id             = "00000000-0000-0000-0000-000000000003"
  region             = "ams3"
  ssh_key_ids        = ["12345"]
  cloudflare_zone_id = "00000000000000000000000000000001"
}

run "approved_capacity_and_candidate_routes" {
  command = apply
  assert {
    condition     = digitalocean_droplet.query.size == "m-24vcpu-192gb" && digitalocean_volume.data.size == 1024
    error_message = "The initial node must use the approved 192 GiB size and a 1 TiB data volume."
  }
  assert {
    condition     = digitalocean_volume_attachment.data.droplet_id == tonumber(digitalocean_droplet.query.id) && digitalocean_volume_attachment.data.volume_id == digitalocean_volume.data.id
    error_message = "The publication volume must attach to the query node."
  }
  assert {
    condition     = cloudflare_dns_record.query.name == "wallet-pir-query-01.valargroup.dev" && !cloudflare_dns_record.query.proxied && cloudflare_dns_record.query.content == output.node.public_ipv4
    error_message = "Only the candidate hostname may point at the new HTTPS edge."
  }
  assert {
    condition     = digitalocean_project_resources.query.project == var.project_id && length(digitalocean_project_resources.query.resources) == 2
    error_message = "Both resources belong to the existing Wallet PIR project."
  }
  assert {
    condition     = startswith(digitalocean_droplet.query.user_data, "#cloud-config") && !strcontains(digitalocean_droplet.query.user_data, "pir-apm-sidecar") && strcontains(digitalocean_droplet.query.user_data, "scsi-0DO_Volume_wallet-pir-query-01-data") && strcontains(digitalocean_droplet.query.user_data, "reverse_proxy 127.0.0.1:8090") && strcontains(digitalocean_droplet.query.user_data, "respond 404")
    error_message = "Bootstrap must mount the named volume and expose only wallet routes through Caddy."
  }
}

run "refuse_missing_ssh_access" {
  command = plan
  variables { ssh_key_ids = [] }
  expect_failures = [var.ssh_key_ids]
}
run "refuse_small_data_volume" {
  command = plan
  variables { data_volume_gib = 128 }
  expect_failures = [var.data_volume_gib]
}
run "preserve_existing_public_endpoint" {
  command = plan
  variables { hostname = "wallet-pir.valargroup.dev" }
  expect_failures = [var.hostname]
}

run "monitoring_boot" {
  command = apply
  module {
    source = "../apm-sidecar"
  }
  variables {
    binary = {
      url    = "https://releases.example/pir-apm-sidecar"
      sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  }
}
run "sidecar_installs_at_first_boot" {
  command   = apply
  state_key = "sidecar-boot"
  variables {
    apm_sidecar_boot_script = run.monitoring_boot.script
  }
  assert {
    condition = (startswith(digitalocean_droplet.query.user_data, "Content-Type: multipart/mixed")
      && strcontains(digitalocean_droplet.query.user_data, "systemctl enable pir-apm-sidecar.service")
      && strcontains(digitalocean_droplet.query.user_data, "export PIR_APM_SIDECAR_SHA256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
      && strcontains(digitalocean_droplet.query.user_data, "wallet-pir-query-mount")
    && !strcontains(digitalocean_droplet.query.user_data, "PRIVATE KEY"))
    error_message = "New query nodes install the pinned sidecar and retain the query bootstrap without credentials."
  }
}
