"""Product-neutral operations primitives shared by Enhance and transparent PIR.

Standard library only. Scripts import this package by adding the checkout's
`ops/lib` directory, computed from their own path, to `sys.path`. The only
modules installed on hosts are `control_sessions` and the modules it imports
(`control_sessions.SHIPPED_MODULES`), copied into `lib/` beside
`wallet-pir-control-sessions.py`.

- `durable`: canonical digests and atomic, fsynced JSON files.
- `hostlock`: the root-only lock pinned to one machine that serializes writers
  of a Terraform state whose backend cannot lock (DigitalOcean Spaces).
- `terraform`: a saved-plan runner that passes that lock to Terraform.
- `digitalocean`: a read-only API client that never follows redirects.
- `pinned_ssh`: SSH and SCP against a verified, pinned known-hosts file.
- `control_sessions`: supervised SSH control masters with restricted forwards,
  and the forwarding-only account they log in to.
- `transparent_unit`: rewriting a serving transparent worker's systemd unit for
  a new worker, and the per-role memory limits it carries.
- `deploy`: the transactional Enhance and Status deploy CLI
  (`ops/scripts/wallet-pir-deploy.py`).
"""
