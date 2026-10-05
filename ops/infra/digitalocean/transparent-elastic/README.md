# Transparent elastic recent replicas

This Terraform root owns the elastic recent replicas of transparent PIR and
nothing else: one `digitalocean_droplet.recent["<name>"]` and one
`digitalocean_project_resources.recent["<name>"]` per member, keyed by the
member's permanent name `transparent-pir-recent-NN`. The
[elastic recent contract](../../../../transparent/docs/elastic-recent.md)
describes the actuator, the inventory and the invariants this root serves.

## Ownership

- **Actuator only.** The transparent fleet actuator is the only writer. It
  plans with a sensitive variables file, validates the saved plan with
  [`transparent-plan.py`](../../../../transparent/ops/scripts/transparent-plan.py)
  and applies exactly that plan, by digest, through
  [`elastic.py`](../../../../transparent/ops/scaler/elastic.py). No operator
  applies this root by hand, and nothing else ever plans against it.
- **Its own state.** The backend is the production Spaces bucket
  `enhance-pir-terraform` under its own key,
  `transparent-elastic/terraform.tfstate`. Never initialize it against
  `production/terraform.tfstate`; the runner refuses any other key.
- **Its own lock.** Spaces does not enforce Terraform's lock writes, so
  `use_lockfile = false` and every writer holds
  `/run/lock/transparent-elastic-terraform.lock` as root on the coordinator
  whose machine id the actuator pins. Terraform inherits the descriptor, so a
  killed actuator cannot release the lock while an apply still runs. It is a
  different lock from the production root's
  `/run/lock/wallet-pir-production.lock`: the two roots never share state.
- **Never the production root.** The production root keeps the archive
  owners, the static recent replicas, the router, the VPC, the worker tag and
  its firewall. This root refers to the tag by name (`worker_tag`, default
  `transparent-pir-worker`), so an elastic member gets exactly the static
  workers' ingress, and it takes the VPC and project as inputs. The same root
  serves a bench fleet with a bench VPC, project and tag.

## Members

`members` maps each name to its `size` and pinned `image`. Adding a key
creates that droplet and its project entry; removing one destroys both. Names
are never reused and must not collide with the production root's static
recent names. After creation the image and user data are ignored: a member's
host facts never change during its life. A changed size plans an update,
which the plan validator refuses.

`host_keys` supplies an ed25519 host key pair for each member being created.
Cloud-init installs it (`ssh_keys`, `ssh_deletekeys: true`), so the
actuator's first SSH connection is checked against a key pinned before the
droplet existed. User data stays readable from the droplet's metadata service
and is kept in plans, so bootstrap rotates that key; saved plans, their JSON
and the variables file are secrets. The cloud-init template is the production
transparent worker template plus that block, and a test keeps them in step.

## Credentials

The runner is started under `ops/scripts/wallet-pir-runtime.py`, which
injects the production runtime credential. It passes Terraform only
`TF_VAR_digitalocean_token` (from `DO_TOKEN_NEW_ORG`), `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` (from the bucket-scoped `WALLET_PIR_TF_STATE_*` keys)
and `AWS_EC2_METADATA_DISABLED=true`, and strips ambient `TF_*`, `AWS_*` and
`DIGITALOCEAN_*` variables.

## Local checks

Provider and schema checks use no credentials and no backend:

```sh
terraform fmt -check -recursive
terraform init -backend=false -input=false -lockfile=readonly
terraform validate
terraform test
```

The provider lock pins the production root's DigitalOcean provider version.
