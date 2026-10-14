# Packing scales independently of evaluation workers. Zero preserves the existing
# deployment until the standalone HTTP path is qualified on the intended host.
variable "enhance_packing_router_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_packing_router_count == floor(var.enhance_packing_router_count) && var.enhance_packing_router_count >= 0 && var.enhance_packing_router_count <= 8
    error_message = "Use zero to eight independently assigned packing routers."
  }
}


resource "digitalocean_project_resources" "enhance_packing_router" {
  count     = var.enhance_packing_router_count
  project   = var.wallet_pir_project_id
  resources = [module.enhance.digitalocean_droplet_enhance_packing_router[count.index].urn]
}


output "enhance_packing_routers" {
  value = [for router in module.enhance.digitalocean_droplet_enhance_packing_router : {
    name         = router.name
    public_ipv4  = router.ipv4_address
    private_ipv4 = router.ipv4_address_private
  }]
}


# Additional evaluation workers are independent of the legacy paired inventory.
variable "enhance_pool_worker_count" {
  type    = number
  default = 0
  validation {
    condition     = var.enhance_pool_worker_count == floor(var.enhance_pool_worker_count) && var.enhance_pool_worker_count >= 0 && var.enhance_pool_worker_count <= 14
    error_message = "Add zero to fourteen individually placed workers."
  }
}

resource "digitalocean_project_resources" "enhance_pool_worker" {
  count     = var.enhance_pool_worker_count
  project   = var.wallet_pir_project_id
  resources = [module.enhance.digitalocean_droplet_enhance_pool_worker[count.index].urn]
}

output "enhance_pool_workers" {
  value = [for worker in module.enhance.digitalocean_droplet_enhance_pool_worker : {
    name         = worker.name
    public_ipv4  = worker.ipv4_address
    private_ipv4 = worker.ipv4_address_private
  }]
}
