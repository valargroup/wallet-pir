# Newly provisioned managed droplets install the per-host APM sidecar at first
# boot: the pinned binary, its unit as the `apm-sidecar.observer` renderer
# renders it, and a provisional configuration (droplet name, private address),
# enabled but not started. Nothing secret is in user_data or state: the
# certificate is issued and installed from Infisical afterwards
# (`wallet-pir-monitoring.py install-credentials --role sidecar`), and the
# next `apm-sidecar` deploy writes the inventory's configuration. Every
# droplet ignores user_data changes, so this reaches new droplets only.
variable "apm_sidecar_boot" {
  description = "Binary new droplets install at first boot: an http(s) URL anyone may read and its sha256. Null leaves user_data unchanged."
  type = object({
    url    = string
    sha256 = string
  })
  default = null
  validation {
    condition = var.apm_sidecar_boot == null || (
      can(regex("^https?://[A-Za-z0-9./_~:-]+$", var.apm_sidecar_boot.url))
    && can(regex("^[0-9a-f]{64}$", var.apm_sidecar_boot.sha256)))
    error_message = "apm_sidecar_boot is {url = a plain http(s) URL, sha256 = the binary's lowercase sha256}."
  }
}

locals {
  apm_sidecar_boot_script = var.apm_sidecar_boot == null ? "" : join("\n", [
    "#!/bin/bash",
    "export PIR_APM_SIDECAR_URL='${var.apm_sidecar_boot.url}'",
    "export PIR_APM_SIDECAR_SHA256=${var.apm_sidecar_boot.sha256}",
    "export PIR_APM_SIDECAR_UNIT_TEMPLATE=${base64encode(file("${path.module}/../../../deploy/monitoring/pir-apm-sidecar.service.in"))}",
    file("${path.module}/../../../deploy/monitoring/pir-apm-sidecar-boot.sh"),
  ])
}
