mock_provider "digitalocean" {
  mock_resource "digitalocean_droplet" {
    defaults = { id = "12345" }
  }
}
mock_provider "cloudflare" {}
variables {
  digitalocean_token   = "mock-provider-placeholder"
  cloudflare_api_token = "mock-provider-placeholder"
  ssh_key_ids          = ["12345"]

  status_control_public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFixture"
}
run "all_services" {
  command = apply
  variables {
    enhance_group_count          = 2
    enhance_legacy_worker_count  = 2
    enhance_packing_router_count = 1
    enhance_pool_worker_count    = 1
    status_host_count            = 1
    transparent_recent_count     = 2
    transparent_archive_names    = ["transparent-pir-archive-01", "transparent-pir-archive-02"]
    transparent_router_count     = 1
    transparent_loadgen_count    = 1
    transparent_v12_enabled      = true

    transparent_txid_display_archive_count = 1
    transparent_txid_display_site_enabled  = true
    receiver_pir_enabled                   = true
    pir_monitor_enabled                    = true
  }
  assert {
    condition     = length(digitalocean_project_resources.wallet.resources) == 12
    error_message = "Shared membership must retain the coordinator, node volume, first worker pair, legacy pair, history hosts, router and load generator."
  }
  assert {
    condition = (cloudflare_dns_record.wallet_pir.name == "wallet-pir.valargroup.dev"
      && cloudflare_dns_record.wallet_pir.content == var.wallet_pir_coordinator_dns_ipv4
      && cloudflare_dns_record.wallet_pir.type == "A" && !cloudflare_dns_record.wallet_pir.proxied
    && output.wallet_pir_public_url == "https://wallet-pir.valargroup.dev")
    error_message = "The unified host must point unproxied at the coordinator edge that serves the Enhance host."
  }
  assert {
    condition     = length(output.enhance_worker_groups) == 2 && length(output.enhance_worker_public_ipv4) == 4
    error_message = "Replica group outputs must keep their original shape."
  }
  assert {
    condition     = output.transparent_txid_display_site.name == "transparent-pir-txid-k8-01" && output.status_host[0].name == "status-pir-01"
    error_message = "Optional services must keep their provider identities."
  }
  assert {
    condition     = startswith(digitalocean_droplet.coordinator.user_data, "#cloud-config") && !strcontains(digitalocean_droplet.coordinator.user_data, "pir-apm-sidecar")
    error_message = "Without apm_sidecar_boot, user_data stays the plain cloud-config."
  }
}
run "sidecar_at_first_boot" {
  command   = apply
  state_key = "first-boot"
  variables {
    enhance_group_count          = 2
    enhance_legacy_worker_count  = 2
    enhance_packing_router_count = 1
    enhance_pool_worker_count    = 1
    status_host_count            = 1
    transparent_recent_count     = 2
    transparent_archive_names    = ["transparent-pir-archive-01", "transparent-pir-archive-02"]
    transparent_router_count     = 1
    transparent_loadgen_count    = 1
    transparent_v12_enabled      = true

    transparent_txid_display_archive_count = 1
    transparent_txid_display_site_enabled  = true
    receiver_pir_enabled                   = true
    pir_monitor_enabled                    = true

    apm_sidecar_boot = {
      url    = "https://releases.example/pir-apm-sidecar"
      sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  }
  assert {
    condition = alltrue([for data in [digitalocean_droplet.coordinator.user_data, digitalocean_droplet.pir_monitor[0].user_data] : (
      startswith(data, "Content-Type: multipart/mixed")
      && strcontains(data, "Content-Type: text/cloud-config")
      && strcontains(data, "#cloud-config")
      && strcontains(data, "export PIR_APM_SIDECAR_SHA256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
      && strcontains(data, "systemctl enable pir-apm-sidecar.service")
    && !strcontains(data, "PRIVATE KEY"))])
    error_message = "New managed droplets append the pinned first-boot sidecar install, and never key material."
  }
}
run "existing_droplets_keep_their_user_data" {
  command = apply
  variables {
    enhance_group_count          = 2
    enhance_legacy_worker_count  = 2
    enhance_packing_router_count = 1
    enhance_pool_worker_count    = 1
    status_host_count            = 1
    transparent_recent_count     = 2
    transparent_archive_names    = ["transparent-pir-archive-01", "transparent-pir-archive-02"]
    transparent_router_count     = 1
    transparent_loadgen_count    = 1
    transparent_v12_enabled      = true

    transparent_txid_display_archive_count = 1
    transparent_txid_display_site_enabled  = true
    receiver_pir_enabled                   = true
    pir_monitor_enabled                    = true

    apm_sidecar_boot = {
      url    = "https://releases.example/pir-apm-sidecar"
      sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  }
  assert {
    condition     = startswith(digitalocean_droplet.coordinator.user_data, "#cloud-config") && !strcontains(digitalocean_droplet.coordinator.user_data, "pir-apm-sidecar")
    error_message = "Setting apm_sidecar_boot changes no existing droplet: each ignores user_data changes."
  }
}

run "sidecar_boot_rejects_an_unpinned_binary" {
  command = plan
  variables {
    apm_sidecar_boot = { url = "https://releases.example/pir-apm-sidecar", sha256 = "latest" }
  }
  expect_failures = [var.apm_sidecar_boot]
}
