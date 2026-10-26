mock_provider "digitalocean" {}
mock_provider "cloudflare" {}
variables {
  digitalocean_token   = "mock-provider-placeholder"
  cloudflare_api_token = "mock-provider-placeholder"
  ssh_key_ids          = ["12345"]
}
run "query_node_disabled_by_default" {
  command = plan
  assert {
    condition     = length(module.query_serving_02) == 0 && output.query_serving_node_02 == null
    error_message = "Existing fleet plans must not provision a query node without opting in."
  }
}
run "shared_query_node_preserves_public_routing" {
  command = plan
  variables { query_serving_02_enabled = true }
  assert {
    condition     = length(module.query_serving_02) == 1 && cloudflare_dns_record.wallet_pir.content == var.wallet_pir_coordinator_dns_ipv4
    error_message = "The shared query node must preserve the existing public wallet route."
  }
}
