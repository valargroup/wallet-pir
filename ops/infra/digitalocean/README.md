# DigitalOcean infrastructure by service

| Repository directory | Ownership and state |
| --- | --- |
| [fleet](fleet/README.md) | Existing production state: backend, providers, coordinator, VPC, node storage, monitoring, Receiver and aggregate project membership |
| `enhance/` | Enhance service module, including evaluation workers and packing |
| `status/` | Status service module |
| `transparent/` | Transparent history module and activity storage |
| `transparent-tx-detail/` | Transaction-detail hosts, DNS and journal storage |
| [enhance/worker-pool](enhance/worker-pool/README.md) | Independent provisioning root, formerly `enhance-v4`; unchanged resource addresses and provisioning contract |
| [transparent/worker-pool](transparent/worker-pool/README.md) | Independent provisioning root, formerly `transparent-elastic`; unchanged resource addresses and provisioning contract |

Only the three roots own state. Service modules share the fleet root's existing
state and provider configuration. Root input names, defaults, validations,
sensitivity and output shapes stay unchanged. Provider resource names, backend
keys and writer locks retain their historical names.

Repository paths differ from retained production installation paths. The
production Terraform wrapper continues to use `/opt/enhance-pir/infra/production`
and `/run/lock/wallet-pir-production.lock`. A staged installation must include
all four sibling service modules so `../enhance`, `../status`, `../transparent`
and `../transparent-tx-detail` resolve from that root. Receiver's cloud-init
also retains its repository-relative path; preserve the complete source-tree
layout in the operations artifact. Keep every deployed `*.auto.tfvars` file,
including transaction-detail settings. Do not rename runtime paths or use the
worker-pool roots against the fleet state.

No production activation is implied by these source files. Follow the
[fleet activation procedure](fleet/activation.md). No infrastructure replacement,
endpoint change or serving-process restart is needed for this reorganization.
