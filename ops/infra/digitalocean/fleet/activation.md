# Activate the service-module extraction

This is a state-address update within the existing production state, with an
intended downtime of zero. Source preparation is separate from approval to
activate. No provider action or serving-process restart is allowed.

## Prepared source and local evidence

The old source baseline is commit
`4f49394aeb2e08f02a52e4e0bc2594549bae0d2b`. `moves.tf` contains 36 whole-resource
mappings into four service modules. The historical rename chains and the two
archive index-to-name mappings remain in `shared.tf`. Project memberships stay
in this root with their original resource sets.

`ops/tests/fixtures/digitalocean-layout-contract.json` pins the original root
interfaces, resource arguments, templates and independent worker-pool files.
The tests reconstruct the old references before comparison. The fleet mock
suite enables every service. A private two-version Terraform mock rehearsal
applied the original configuration into test-only state and then planned the
new source against that same state: 66 instances, 42 moves, all resource actions
`no-op`, unchanged output values. These counts describe the mock fixture,
not the currently deployed fleet.

Operations bundles include all module Terraform files and cloud-init resources;
private state, plans, populated variables and provider caches are excluded.
Immutable evidence and historical source manifests retain their original paths.

## Read-only live gate

The coordinator failed SSH host-key verification during preparation. Have its
owner verify the SSH identity and repair `known_hosts`, then run:

```sh
ssh-reachable root@enhance-pir.valargroup.dev
```

Do not disable host-key checking or try another key to get past the refusal.
Use `ssh-check` for the read-only host checks once the probe passes. Capture
service health and running binary identities, state lineage and serial, backend
bucket/key, provider locks, and the enabled variable-file inventory. Retain the
raw inputs privately. Preserve every deployed `*.auto.tfvars` byte-for-byte,
especially `txid-display.auto.tfvars` and the site/archive settings; use the same
explicit variable files and environment inputs for both configurations. Record
the current source/runtime installation before preparing any replacement.

## Review the exact artifact and plans

Assemble and verify the operations bundle for the final clean source SHA using
`ops/scripts/wallet-pir-bundle.py`. The bundle manifest supplies its source SHA
and checksums. Keep it together with the reviewed move files, protected state
snapshot, variable-file hashes and rollback source artifact in a private
activation package. Packaging alone does not install it.

Stage the complete source tree so the retained runtime root
`/opt/enhance-pir/infra/production` has all four sibling modules. Preserve
Receiver's relative cloud-init path. Keep the backend bucket
`enhance-pir-terraform`, key `production/terraform.tfstate`, and existing writer
lock; do not initialize a new state or migrate the backend. Do not refresh a
root by deleting its deployed variable files.

Compare the old and new configurations against the same protected state
snapshot and inputs, first without refresh to isolate the extraction, then
with a fresh read-only live plan to detect drift. Save plans privately and
export them with `terraform show -json`; never reuse any historical saved plan.
Run the read-only comparator from the reviewed source tree:

```sh
python3 ops/scripts/wallet-pir-infra-moves.py \
  --before /private/activation/before.json \
  --after /private/activation/after.json
```

The baseline must be clean. The comparator rejects any provider action,
undeclared/missing move, unknown value, changed ID, changed resource value,
changed output, incomplete inventory or reported drift. Stop and resolve
unrelated drift separately. A mock rehearsal does not replace this live gate.

## Confirmation, locked activation and rollback

Request production confirmation only after the source SHA, artifact digest,
reviewed mappings, live plans, variable-file preservation and health receipts
are concrete and reviewable. Production installation must use the existing
`ops/scripts/wallet-pir-deploy.py` plan/preflight and locked operations path.
The manual Terraform writer remains `ops/scripts/wallet-pir-terraform.sh`,
which holds `/run/lock/wallet-pir-production.lock`; no direct remote apply or
manual host mutation is permitted.

Apply only the newly reviewed moves-only plan under that same production lock,
then require a subsequent no-change plan and unchanged service health/identities.
Report locked operation status and its exact rollback command from the existing
deployment tool. Rollback has no service restart: restore the verified old
source artifact and variable files, and reverse only the address mappings that
were actually applied, under the same lock, from the retained state receipt.
A rollback plan must also contain no provider actions. Do not push an old state
snapshot over newer infrastructure or use a historical saved plan. Stop on a
serial/lineage mismatch or any resource action.

The actual rollback command depends on the locked deployment operation and live
state receipt. Those cannot be established until verified host access returns;
production activation remains gated until they are available and approved.
