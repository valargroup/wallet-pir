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

module "monitoring_boot" {
  source = "../apm-sidecar"
  binary = var.apm_sidecar_boot
}

locals {
  apm_sidecar_boot_script = module.monitoring_boot.script
}
