mock_provider "digitalocean" {}
variables {
  digitalocean_token       = "mock-provider-placeholder"
  project_id               = "00000000-0000-4000-8000-000000000001"
  vpc_id                   = "00000000-0000-4000-8000-000000000002"
  coordinator_private_ipv4 = "192.0.2.10"
  operator_ssh_cidrs       = ["192.0.2.20/32"]
  ssh_key_ids              = ["12345"]
}
run "bootstrap" {
  command = plan
  variables { group_count = 1 }
  assert {
    condition     = length(digitalocean_droplet.worker) == 2 && digitalocean_droplet.worker[0].size == "c-4"
    error_message = "Bootstrap must provision one complete c-4 replica pair."
  }
  assert {
    condition     = digitalocean_droplet.worker[0].name == "enhance-pir-v4-g01-r1"
    error_message = "Worker names must preserve stable group/replica identity."
  }
}
run "two_pairs" {
  command = plan
  variables { group_count = 2 }
  assert {
    condition     = length(digitalocean_droplet.worker) == 4 && length(digitalocean_project_resources.worker) == 4
    error_message = "Expansion must add a pair and only additive project membership."
  }
}
run "reject_fifth_pair" {
  command = plan
  variables { group_count = 5 }
  expect_failures = [var.group_count]
}
run "reject_open_ssh" {
  command = plan
  variables { operator_ssh_cidrs = ["0.0.0.0/0"] }
  expect_failures = [var.operator_ssh_cidrs]
}
