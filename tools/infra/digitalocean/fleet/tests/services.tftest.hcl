mock_provider "digitalocean" {
  mock_resource "digitalocean_droplet" {
    defaults = { id = "12345" }
  }
}
mock_provider "cloudflare" {}
variables {
  digitalocean_token        = "mock-provider-placeholder"
  cloudflare_api_token      = "mock-provider-placeholder"
  ssh_key_ids               = ["12345"]
  allowed_ssh_cidrs         = ["192.0.2.20/32"]
  status_control_public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFixture"
}
run "all_services" {
  command = apply
  variables {
    enhance_group_count                    = 2
    enhance_legacy_worker_count            = 2
    enhance_packing_router_count           = 1
    enhance_pool_worker_count              = 1
    status_host_count                      = 1
    transparent_recent_count               = 2
    transparent_archive_names              = ["transparent-pir-archive-01", "transparent-pir-archive-02"]
    transparent_router_count               = 1
    transparent_loadgen_count              = 1
    transparent_v12_enabled                = true
    transparent_txid_display_port_enabled  = true
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
    condition     = length(output.enhance_worker_groups) == 2 && length(output.enhance_worker_public_ipv4) == 4
    error_message = "Replica group outputs must keep their original shape."
  }
  assert {
    condition     = output.transparent_txid_display_site.name == "transparent-pir-txid-k8-01" && output.status_host[0].name == "status-pir-01"
    error_message = "Optional services must keep their provider identities."
  }
}
