run "no_artifact" {
  command = plan
  assert {
    condition     = output.script == ""
    error_message = "An unconfigured artifact leaves cloud-init unchanged."
  }
}

run "pinned_artifact" {
  command = plan
  variables {
    binary = { url = "https://example.invalid/pir-apm-sidecar", sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
  }
  assert {
    condition     = strcontains(output.script, "PIR_APM_SIDECAR_SHA256=aaaaaaaa") && !strcontains(output.script, "PIR_MONITORING_CA_KEY")
    error_message = "Boot installs a pinned public artifact without monitoring issuer credentials."
  }
}

run "reject_unpinned_artifact" {
  command = plan
  variables {
    binary = { url = "https://example.invalid/pir-apm-sidecar", sha256 = "latest" }
  }
  expect_failures = [var.binary]
}
