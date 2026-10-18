mock_provider "digitalocean" {}

variables {
  digitalocean_token = "mock-provider-placeholder"
  project_id         = "00000000-0000-4000-8000-000000000001"
  vpc_uuid           = "00000000-0000-4000-8000-000000000002"
  ssh_key_ids        = ["12345"]
}

run "no_members" {
  command = plan
  assert {
    condition     = length(digitalocean_droplet.recent) == 0 && length(digitalocean_project_resources.recent) == 0
    error_message = "An empty member map must plan nothing."
  }
}

run "two_members" {
  command = plan
  variables {
    members = {
      "transparent-pir-recent-05" = { size = "s-4vcpu-8gb", image = "ubuntu-24-04-x64" }
      "transparent-pir-recent-06" = { size = "s-4vcpu-8gb", image = "ubuntu-24-04-x64" }
    }
    host_keys = {
      "transparent-pir-recent-06" = {
        private = "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture\n-----END OPENSSH PRIVATE KEY-----\n"
        public  = "ssh-ed25519 AAAAfixture"
      }
    }
  }
  assert {
    condition     = length(digitalocean_droplet.recent) == 2 && length(digitalocean_project_resources.recent) == 2
    error_message = "Each member needs exactly one droplet and one project entry."
  }
  assert {
    condition     = digitalocean_droplet.recent["transparent-pir-recent-06"].name == "transparent-pir-recent-06"
    error_message = "A droplet's name is its member key."
  }
  assert {
    condition     = digitalocean_droplet.recent["transparent-pir-recent-05"].tags == toset(["transparent-pir-worker"])
    error_message = "Members carry only the production worker tag, referenced by name."
  }
  assert {
    condition     = digitalocean_droplet.recent["transparent-pir-recent-05"].backups == false && digitalocean_droplet.recent["transparent-pir-recent-05"].monitoring
    error_message = "Members run with monitoring and without backups."
  }
}

run "reject_archive_member" {
  command = plan
  variables {
    members = { "transparent-pir-archive-01" = { size = "m-4vcpu-32gb", image = "ubuntu-24-04-x64" } }
  }
  expect_failures = [var.members]
}

run "reject_host_key_without_member" {
  command = plan
  variables {
    host_keys = {
      "transparent-pir-recent-07" = {
        private = "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture\n-----END OPENSSH PRIVATE KEY-----\n"
        public  = "ssh-ed25519 AAAAfixture"
      }
    }
  }
  expect_failures = [var.host_keys]
}
