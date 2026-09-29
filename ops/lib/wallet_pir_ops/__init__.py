"""Product-neutral operations primitives shared by Enhance and transparent PIR.

Standard library only. Scripts import this package by adding the checkout's
`ops/lib` directory, computed from their own path, to `sys.path`; nothing here
is installed on hosts by itself.

- `durable`: canonical digests and atomic, fsynced JSON files.
- `hostlock`: the root-only lock pinned to one machine that serializes writers
  of a Terraform state whose backend cannot lock (DigitalOcean Spaces).
- `terraform`: a saved-plan runner that passes that lock to Terraform.
- `digitalocean`: a read-only API client that never follows redirects.
- `pinned_ssh`: SSH and SCP against a verified, pinned known-hosts file.
- `transparent_unit`: rewriting a serving transparent worker's systemd unit for
  a new worker, and the per-role memory limits it carries.
"""
