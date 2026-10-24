variable "apm_sidecar_boot" {
  description = "Pinned public sidecar artifact for new workers; enroll and install credentials through the deployment tool."
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
  description = "New workers need deploy-managed certificates and explicit APM enrollment before monitoring is active."
  value       = { for worker in digitalocean_droplet.worker : worker.name => true }
}
