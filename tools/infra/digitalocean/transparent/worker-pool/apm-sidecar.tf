variable "apm_sidecar_boot" {
  description = "Pinned public sidecar artifact for new workers; the fleet actuator installs credentials and enrolls before serving."
  type = object({
    url    = string
    sha256 = string
  })
  default = null
}

module "monitoring_boot" {
  source = "../../apm-sidecar"
  binary = var.apm_sidecar_boot
}

output "monitoring_enrollment_required" {
  description = "The fleet actuator must install credentials and return enrollment for each new member."
  value       = { for name in keys(var.members) : name => true }
}
