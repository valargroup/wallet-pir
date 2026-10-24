# Shared by the fleet and both standalone worker pools. Only public artifact
# identity enters Terraform. Credentials and enrollment are deployment steps.
variable "binary" {
  type = object({
    url    = string
    sha256 = string
  })
  default = null
  validation {
    condition = var.binary == null || (
      can(regex("^https?://[A-Za-z0-9./_~:-]+$", var.binary.url)) &&
    can(regex("^[0-9a-f]{64}$", var.binary.sha256)))
    error_message = "Sidecar boot requires a public artifact URL and its lowercase SHA-256 digest."
  }
}

output "script" {
  value = var.binary == null ? "" : join("\n", [
    "#!/bin/bash",
    "export PIR_APM_SIDECAR_URL='${var.binary.url}'",
    "export PIR_APM_SIDECAR_SHA256=${var.binary.sha256}",
    "export PIR_APM_SIDECAR_UNIT_TEMPLATE=${base64encode(file("${path.module}/../../../deploy/monitoring/pir-apm-sidecar.service.in"))}",
    file("${path.module}/../../../deploy/monitoring/pir-apm-sidecar-boot.sh"),
  ])
}
