# Operating the Wallet PIR fleet

Start here for the shared operating model. Product runbooks own their publication,
authority, resource and encrypted-answer checks. Source and release preparation do
not establish what is running; inspect the fleet and retained deployment evidence.

The standard deployment uses [one shared publisher](publisher.md) for all five
products and [shared query hosts](query-server.md) for serving. Product-specific
roles also support distributed workers and isolated qualification. Packaging an
executable alone does not establish what is running.

## Deploying a build

Every main commit publishes the GitHub release `build-<sha12>` from
`.github/workflows/release.yml`. It contains `pir-publisher`, `pir-query-server` (every
query role runs this one executable), `pir-probe` with its clients and pinned answers,
`contract.json`, `pirctl` and `SHA256SUMS`.

Each host runs [`pirctl`](../tools/deploy/pirctl). It downloads its asset from the
release, checks it with `--check`, points the unit's `current` link at it and restarts
the unit. Verification is the process running the new file, `/health` ready, and
[`pir-probe`](../tools/deploy/pir-probe) passing within 15 s on components that serve
wallets. `pir-probe` sends one encrypted query per product to the local listener, all at
once, and needs no chain node:

- Enhance, Transparent history and transaction details must return the answers pinned
  in [`tools/deploy/probe/`](../tools/deploy/probe).
- Receiver must return the payment built into `receiver-probe`.
- Status covers only recent blocks, so it has no fixed answer: a random txid must decode
  as not found under the service's own fresh manifest.

The pinned Transparent rows come from the oldest archive shards. A declared re-cut
replaces their revisions, so refresh that fixture from the monitor's current one when
you deploy a re-cut. If verification fails,
`pirctl` restores the previous release on its own. `/etc/wallet-pir/pirctl.conf` lists
the components on that host.

[`pir-deploy`](../tools/deploy/pir-deploy) runs `pirctl` over SSH, from a workstation
or from the [Deploy workflow](../.github/workflows/deploy.yml). It reads an inventory
kept outside the repository. Before it starts it takes a lease on the publisher host,
so two deploys never overlap, and it opens APM's deployment window (see below):

| Mode | Command | What restarts |
|---|---|---|
| Publisher only | `pir-deploy publisher <sha>` | The shared publisher; query roles keep serving |
| One product | `pir-deploy product <group> <sha>` | That product's roles, in inventory wave order |
| Breaking product change | `pir-deploy product <group> <sha> --breaking` | Publisher, then unified nodes and that product |
| Unified nodes | `pir-deploy unified <sha>` | Unified query nodes |
| Everything | `pir-deploy all <sha>` | Publisher, then every query role |
| Monitoring | `pir-deploy product monitoring <sha>` | `pir-apm` on the coordinator and `pir-monitor` on the monitor host |

Every host is prepared before anything restarts. If any component in a wave fails, the
deploy stops: the components of that wave that had already moved are rolled back, and
later waves never start. A partial mode refuses a release whose contract major differs
from the live publisher's. `pir-deploy --dry-run <mode> ...` takes no lease and restarts
nothing. On every selected host it unpacks the build, has the new binary accept the
host's config, and runs that build's `pir-probe` against the service running now. Then
it prints the waves that would restart. Every deploy and dry run ends with one probe
per product through the public origins wallets use (Enhance and Status through
`enhance-pir`, Transparent history and transaction details through `transparent-pir`,
Receiver through `receiver-pir`), run from the publisher host with the deployed build's
`pir-probe`; a failed probe is retried within its 15 s budget while query roles catch up. Components that share a host deploy one after another. `pir-deploy rollback <group|host>` swaps components back to
their previous release, and `pir-deploy status` prints what each host runs. A
component's previous release must still read what the publisher now sends: query
servers built before `e9848f1e4157` refuse the Receiver distribution of later
publishers, so a unified node cannot roll back past it while the publisher runs a
later build. Deploying the release a component already runs changes nothing.
`pirctl adopt <component>` moves a unit that names a release path onto the `current`
link without restarting it. It sets aside drop-ins that only repeat `ExecStart`.

The Deploy workflow is dispatched by hand and runs on the coordinator's self-hosted
runner, inside the private network. Its inputs mirror the command line (mode, target,
sha, cold, breaking) and it is a dry run unless `dry_run` is unchecked. It runs
`pir-deploy self-update <sha>` first, so every host's `pirctl` matches the build, and
ends with `pir-deploy status`. The inventory, the SSH host aliases and the pinned host
keys are the `production` environment's `PIR_DEPLOY_INVENTORY`, `PIR_DEPLOY_SSH_CONFIG`
and `PIR_DEPLOY_KNOWN_HOSTS` variables; the key is the `WALLET_PIR_DEPLOY_SSH_KEY`
secret, whose public half every inventory host must authorize for root. A workstation
deploy and a workflow deploy share the lease, so they never overlap.

## Architecture and ownership

| Product | What runs | Product authority |
|---|---|---|
| Enhance | Ingestion/publication coordinator, query ingress, packing router, evaluation workers | [Architecture](enhance/architecture.md), [deployment](enhance/deployment.md) |
| Status | Separate observation controller, packing router and evaluation worker; implementation shares the Enhance package | [Architecture](status/architecture_status.md), [distributed topology](status/status_distributed.md) |
| Transparent history | Event journal writer, publication controller, recent/archive workers and public routing | [Architecture](transparent/architecture.md), [deployment](transparent/deployment.md) |
| Transparent display | Its own transaction journal, publication controller and display workers | [Display contract](txid/txid-display.md) |
| Receiver | Indexes and publishes inside the shared publisher; shared query hosts fetch prepared publications over HTTPS | [Overview](receiver/README.md), [deployment](../tools/receiver/digitalocean/README.md) |

The [serving contract](serving-contract.md) records their shared process identity
and deliberate differences. A running process is not necessarily authorized,
warm, publishing or answering correctly. Preserve those distinctions in status
reports. [Control sessions](control-sessions.md) records what dated captures show
of the shared supervisor (observed carrying Status; its restricted account and the
other products not adopted); packaging it does not deploy or adopt it.

Shared operations live in `tools/lib/wallet_pir_ops`. Product adapters live with the
product under `tools/<product>/`: `tools/enhance`, `tools/status`, `tools/transparent`,
`tools/txid` (transaction-detail display) and `tools/receiver`, including
`tools/transparent/lib/deploy_cli.py` and the txid display driver it dispatches to. The shared
[role registry](../tools/deploy/services.toml) includes product-owned descriptors.
Each inventory role names its actual host, unit, private addresses and artifact
variant. The [example inventory](../tools/enhance/deploy/deploy-inventory.example.json)
uses documentation addresses and is not the live fleet.

## Contract releases

Both executables support configuration-free `--contract-info`. It reports the
compiled discovery version, products and optional CUDA backend without opening
state or initializing CUDA. Each build release's `contract.json` records it for
every executable. These are distinct from the products enabled in the installed
configuration and from qualification.

A release within the live publisher's contract major can move one component group
at a time. A major change moves the publisher and every consumer together:
`pir-deploy product <group> <sha> --breaking` or `pir-deploy all <sha>`. Partial
modes refuse it. Private query-to-query protocol qualification remains a separate
release requirement.

## Operations releases

Full CI on main produces `wallet-pir-ops-<sha>` containing
`wallet-pir-ops.tar.gz`. Its own job (`release-ops`) tests, assembles and runs the
unpacked package without compiling Rust, so publishing it never waits on the
binary build; like the binaries, it is eligible only when that whole run on main
succeeds. It contains the standard
library Python modules, wrappers, product descriptors, templates, remote helpers
and this guide. It contains no operator inventories, credentials, runtime state,
tests or retained evidence. It needs Python 3.11+, OpenSSH and the existing host
tools; it does not need a source checkout, Cargo or a Python dependency install.

Use the existing artifact resolver to select the successful full-CI artifact for
the exact source revision. Verify and unpack it into a new release directory:

```sh
python3 tools/ci/release.py resolve --sha <full-revision> --kind wallet-pir-ops
python3 tools/scripts/wallet-pir-bundle.py verify --sha <full-revision> --archive wallet-pir-ops.tar.gz
python3 tools/scripts/wallet-pir-bundle.py extract --sha <full-revision> --archive wallet-pir-ops.tar.gz --output /path/to/new-release
```

Verification refuses unsafe paths, links, duplicates, incomplete inventories,
wrong revisions and checksum mismatches before creating the destination. These
checks establish artifact integrity; the manifest's `"qualification":
"unqualified"` labels that integrity artifact, not the CI run. The resolver's
full-CI provenance and product qualification remain separate requirements. The unpacked tool checks its resources
before operating. Dirty operations releases support inspection only. When a binary
archive names a source revision, it must match the operations release's revision.

For local development, assemble an inspection bundle without compiling Rust:

```sh
python3 tools/scripts/wallet-pir-bundle.py assemble --sha <checkout-revision> --archive /tmp/wallet-pir-ops.tar.gz
```

Use wrappers from the unpacked release's directory tree. Do not copy individual
Python files out of it. `wallet-pir-bundle.py self-check --sha <revision>` checks
the unpacked release it belongs to, wherever it was moved. Retained installed
standalone scripts have their recorded source identities.

New releases place their commands under `tools/scripts/`. Retained releases keep
their original `ops/scripts/` paths. Package verification and rollback check the
recorded tree first, then run that release's own entrypoints; installed files and
past transaction records are never rewritten for a source directory move.

## Installing the package on the coordinator

`ops-package-*` installs a package release on the coordinator as one journaled
transaction under the production lock:

- `/opt/wallet-pir-ops/releases/<full-sha>`: root-owned, `0755` directories,
  `0755` scripts and modules, `0644` other files; never overwritten.
- `/opt/wallet-pir-ops/current`: a link to `releases/<full-sha>`, replaced atomically.
- `/etc/wallet-pir/fleet.json`: the maintained operator inventory (`0600`), outside
  every release, validated as a complete fleet by the target package itself.
- `/etc/wallet-pir/known_hosts`: optional reviewed public host-key pins (`0644`).
  Pins come from a trusted census or from Roman, never from `ssh-keyscan`; the file
  never holds a private key. Inventory entries that name it must pin its digest.
- `/opt/wallet-pir-ops/staging/<transaction>`: the transaction's own partial copy.
- `/opt/wallet-pir-ops/transactions/<transaction>/`: the coordinator's recovery
  records: the plan, the previous inventory's bytes, per-step intent and completion
  markers and the final status. `latest.json` names the latest transaction.

A package is *installed* when `current` selects it. That says nothing about which
code runs a service: a package transaction writes no unit, reloads no daemon and
restarts nothing. Each product's locked deployment adopts services separately, and
status documents must keep the two apart.

```sh
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory <inventory> ops-package-plan \
    --sha <full-revision> --ci-run-id <full-CI run on main> --fleet fleet.json [--known-hosts known_hosts]
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory <inventory> ops-package-preflight ...same...
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory <inventory> ops-package-install ...same... \
    --expect-plan-sha256 <reviewed plan digest>
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory <inventory> ops-package-status
```

Plan and preflight change nothing on the coordinator. The archive is the one inside
the `wallet-pir-ops-<sha>` artifact of the named run: the run must be a successful
full CI on main for that revision, and the artifact ZIP is downloaded with `gh` and
checked against the digest GitHub reports (or, for an artifact without one, the
downloaded bytes are used as acquired). A local `--archive` is accepted only when it
equals that archive; caller-supplied run metadata or local bytes never qualify a
package. The plan binds the archive and manifest digests, the source revision, the
run and artifact identities (`provenance`, separate from the artifact's
`integrity`), the coordinator's machine id, the candidate inventory and pins, the
current selection, managed files, release directory and latest package transaction,
and the planned changes. They refuse a dirty-source archive, altered, missing or
unsafe files, unsafe or foreign-owned package paths, a different or unrecorded
directory at the final path, staging left by another transaction, an unfinished
transaction, an inventory that is not a complete fleet, pins that are not public
host keys or do not match the inventory, and too little space. Install re-plans under the
lock, behind the Transparent schema fence, and refuses unless the digest equals
the reviewed one. It stages, checks the staged tree's files, modes and owner, runs
its entrypoints in place, then promotes the directory, installs the inventory and
selects the release. A release kept by an earlier rollback is selected again
without staging when the transaction that promoted it is recorded and its files
still match; that rehearsal (install, rollback, plan, install) reuses the exact
retained bytes and never overwrites the directory.

A clean installed package may install a newer package revision. The remaining
binary commands (`transparent-binary-*`) keep requiring the operations package and
the binary archive to name the same source revision.

**Recovery.** Before each step the runner journal and the coordinator record the
intent, and after it the completion. A transport failure leaves the step's outcome
unknown; the command prints the exact `ops-package-reconcile` and
`ops-package-rollback` commands and nothing is guessed. Reconcile finishes only
where each step's observed state is exactly its recorded before or after state
(a partial staging copy is discarded and staged again) and refuses anything else.
A refusal after the first step (for example an altered staged file) selects
nothing and needs rollback. Rollback refuses if the selection or inventory changed
outside the transaction, or if the release it would select again is missing,
differs in any file, owner or mode from the record of the transaction that
promoted it, has no such record, or fails its own self-check; a refusal changes
nothing and the current selection stays. Otherwise it restores the previous selection and
inventory and pins, or removes them when the transaction installed the first ones.
It keeps the release directory and every record. An earlier transaction rolls back
only after every later one has.
Rollback and reconcile work from another runner using the coordinator's records.
`ops-package-status` reports the selected revision, whether its files still match
its manifest (integrity), the provenance recorded by the transaction that promoted
it, the managed files' digests, the latest transaction and the next command.

## Fleet configuration and lifecycle

`pir-deploy` pauses normal Slack notifications through the running `pir-apm` for
10 minutes (an hour with `--cold`), or until the deploy ends. APM keeps collecting
metrics and evaluating incidents meanwhile, and `pir-monitor` reads the window from
APM's monitor status and pauses its own canary notifications with it. Only a failed
deploy posts to Slack; a start or a successful finish posts nothing. After the window:

- an incident already announced before the deploy is not announced again;
- an incident that fired during the window and still fails is announced once;
- a five-minute rate or latency alert waits until its whole window was sampled after
  the deploy ended, so restart errors inside the deploy never fire it.

APM expires a window that is never finished and posts an expiry warning. If APM refuses
the window, the deploy goes ahead with alerts armed.

The inventory owns operator choices: hosts, units, connection settings and artifact
variants. Descriptors own role order, templates and readiness checks. Dynamic
membership, publication assignments and their persisted revisions remain owned by
the current controllers and Transparent inventory; the shared view does not write
them or introduce another state database.

`fleet --complete` checks the whole fleet: every registered service and role; the
journal writer as a responsibility of exactly one controller per Transparent
service, not a unit of its own; shared routing (the coordinator edge, the public
Transparent router, the receiver edge) and supervision (control sessions) listed
once under `ancillary` with the services that consume them, inspected only
(`ancillary-status`); one owner per unit on each machine, counting host names that
reach the same address; explicit CUDA variants on hosts marked
`"accelerator": "cuda"`, verified through health; and no field that copies
controller-owned membership. References to `retired_hosts` and unknown hosts are
refused. A role repeated on one host (the history archive and recent workers) is
keyed `role@host:unit`. A host may name its own existing `key` and a
`known_hosts`/`known_hosts_sha256` pair, falling back to the inventory-wide `ssh`
settings; only the private-key path is recorded, never key material. A product
inventory that lists only its own services keeps working for that product's
commands. `ancillary-status [NAME]` is read-only and reports an unreachable host
as a result instead of leaving it out. The example inventory is complete in shape but uses
documentation values; it is not the live fleet.

```sh
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json fleet --complete
```

`fleet` reads configured roles without contacting hosts. Binary rollouts and their
status go through `pir-deploy` (see [Deploying a build](#deploying-a-build)), not
this inventory tool.

Transparent history and display appear in the same role view and process status,
but their publication drivers remain product recipes: `schema-*` and
`txid-display-*`, because changing a publication is more than replacing an
executable. Their retained request, plan-digest, authority, quiescence and recovery
requirements continue to apply.

## Recover an interrupted rollout

A failed `pirctl deploy` restores the previous release on that host by itself, and
`pir-deploy` rolls back the components of the failed wave. Start from what the
tools report, not from a new plan of your own:

1. `pir-deploy status` prints the release each component runs.
2. `pir-deploy rollback <group|host>` swaps a component back to its `previous`
   release and verifies it. On one host, `pirctl rollback <component>` does the
   same.
3. An interrupted run leaves the deploy lease on the publisher host until it
   expires (20 minutes, 70 with `--cold`), so a second deploy cannot start
   meanwhile. APM ends an unfinished alert window on its own.

Remote checks during recovery go through `ssh-check`. Never rerun a failed
`ssh-check` unchanged: read its JSON result (the last stderr line) and change
something first. A `negative` result is an answer to act on; `check_failed`
needs its cause fixed; `unreachable`, `auth_refused`, `host_key_mismatch` and
`approval_required` stop the step until that is resolved. After a change, rerun
it with `--fresh`.

## Configure a publisher and its sandbox together

`role_config.RenderedRole` holds unit text and the configuration files that unit
consumes. Transparent's publisher adapter derives journal access from
`follow_journal`: a writer gets a writable journal; a follower gets a read-only
journal separate from writable publication/snapshot/state paths. It preserves the
template's other service settings and credential-file restrictions.

Render both for review without writing files or contacting a host:

```sh
python3 <release>/tools/scripts/wallet-pir-roles.py --service transparent-history --role publisher --config /path/to/controller.json --config-path /etc/transparent-pir/controller.json --state-dir /srv/transparent-publisher-state
```

Configuration must live outside the role's writable directories. Invalid paths,
ambiguous writer/follower modes and writable access through a follower journal's
ancestor are refused. The publisher installer consumes the same renderer.
Adopt its generated unit and configuration together through the product's locked,
journaled deployment; rendering alone changes no process.

Other binary roles retain their captured complete effective units during this
pass. The shared renderer is available when an owning product adopts complete
configuration rendering; it does not silently replace captured production units.

## External workers

Every product registers one optional external worker role that an operator can
add on a host of their own: `enhance.external-worker`, `status.external-worker`,
`transparent-history.external-worker`, `transparent-display.external-worker`
and `receiver.query`. Its whole unit and configuration are rendered
by the product's adapter (`tools/enhance/lib/roles.py`, `tools/status/lib/roles.py`,
`tools/transparent/lib/roles.py`, `tools/txid/lib/roles.py`, `tools/receiver/lib/roles.py`) from the
inventory entry's `settings`; the template beside it owns the sandbox. The
inventory owns hosts, paths, listeners, budgets (`limits`: `MemoryMax`,
`MemoryHigh`, `MemorySwapMax`, `CPUQuota`, `CPUWeight`, `IOWeight`, `Nice`,
`User`, `Group`) and credential file references (`credentials`: installed
`EnvironmentFile` paths whose contents are never read). Templates carry no
fleet address and no optional provider credential; the standalone Receiver
never carries node or partner credentials. Inputs (publications, assignments,
configuration, credentials) are read-only and may not overlap the writable
state and cache directories, which an install creates.

```json
{"host": "edge-01", "install": true, "vars": {"listen": "127.0.0.1:18390"},
 "settings": {"config_path": "/etc/receiver-query/config.json",
              "state_dir": "/srv/receiver-query", "listen": "127.0.0.1:18390",
              "management_listen": "127.0.0.1:18391",
              "publisher": "https://receiver-pir.valargroup.dev",
              "genesis": [8, 206, 61, 151, 49, 176, 0, 192, 131, 56, 69, 92, 138, 74, 107, 208, 93, 161, 110, 38, 177, 29, 170, 27, 145, 113, 132, 236, 232, 15, 4, 0],
              "activation_height": 3428143}}
```

Review a rendering before any host is touched:

```sh
python3 <release>/tools/scripts/wallet-pir-roles.py --service receiver --role query \
  --settings settings.json --release /opt/receiver-pir/releases/<sha256>
```

Install the rendered unit and configuration on the operator's host, then list the
worker in that host's `/etc/wallet-pir/pirctl.conf` so `pir-deploy` rolls its
releases like any other component. With `config=` on that line, `pirctl prepare`
runs the new release's own check against the installed configuration before anything
restarts (`pir-query-server --config FILE --check`). Receiver downloads immutable objects
over HTTPS and requires fresh publisher authority before activating them.
Verification proves the process and its identity, never serving authority:

| Role | Controller and dependencies | Ready when |
|---|---|---|
| Enhance worker | the worker downloads published rows from its configured HTTPS publisher and activates them itself; a packing router must list it | health `ready`, once its active publication's lease holds |
| Status worker | the worker downloads prepared database units from its configured HTTPS publisher; wallet/evaluation forwarding remains separate | control health `ready`, with a fresh, verified and eligible publication |
| Transparent history worker | the worker downloads verified publications and runtimes from its configured HTTPS publisher and activates them itself | `/v1/ready` once its runtimes are warm |
| Transparent display worker | the worker downloads the full display publication from its configured HTTPS publisher and activates it itself; starts empty | `/v1/ready` after its first activation |
| Receiver query | HTTPS publisher objects and durable authority floors; no node or partner credentials | root `/ready` after an eligible directory activates |

`worker-report SERVICE [--only ROLE]` reads only existing private endpoints and
reports each worker on separate axes: process (systemd state, running digest),
executable (reported digest and incarnation, and whether they match), assigned
and loaded publication, latest preparation or refusal, and readiness split into
`verified`, `warm`, `authorized` and `ready`. What a product does not expose is
`unsupported` (Status control health is a closed schema; Transparent assignment
and preparation live on the root-only socket); encrypted correctness is never
inferred and needs an exact-answer check. CUDA workers keep their artifact kinds
and need separate hardware qualification; the CPU rehearsal qualifies no GPU.
The local operator rehearsal exercises staged publications and encrypted lookup
through the library-owned clients. It does not qualify the production sandbox,
GPU hardware, or fleet capacity.

## Qualification and retirement

Qualification, candidate-build, census and benchmark tooling is advanced product
tooling, not the normal fleet lifecycle. The retained Transparent source-bound
recipes still require their exact reviewed source trees and inputs; an operations
bundle does not bypass those requirements or qualify a new runtime.

The [October 13 retirement](../evidence/transparent/v1-sunset-2026-10-13/README.md)
records v1 withdrawal and the v12 journal handover. This consolidation removes the
superseded standalone filter deploy workflow/installer and historical implicit
publication-path defaults. Worker verification helpers and source-bound recovery
tools remain because current tests and retained procedures consume them. The v1
410 route remains an explicit retirement response, not a second serving runtime.
No retired host, journal, rollback artifact or immutable evidence is deleted here.

Rust packages, serving protocols and persisted journals remain product-owned.
The next fleet-management frontend can consume the lifecycle and inventory APIs;
this pass adds no new provisioning service, paid infrastructure or serving lease.

## Infrastructure source layout

The [DigitalOcean service layout](../tools/infra/digitalocean/README.md) keeps the
existing production and worker-pool state boundaries. Source paths are
`fleet/`, the four service modules, and the two service-owned `worker-pool/`
roots. Installed runtime paths, backend keys and writer locks retain their
existing names. Follow the [activation gate](../tools/infra/digitalocean/fleet/activation.md)
before installing this source or updating production state.

### Recover transaction-detail query-cache capacity

The HTTPS transaction-detail worker collects obsolete publication trees, their
unreferenced shared revisions, and downloaded objects. It preserves active,
prepared, retained and request-held publications, plus the saved predecessor
before the first activation after restart. Collection runs before another
download and after failed synchronization attempts. Check collection logs and
filesystem free space to verify reclamation; hard-linked directory sizes do not
measure reclaimable bytes.

Use `wallet-pir-deploy.py cache-storage-plan` and `cache-storage-prune` to
retire an explicitly selected, unused HTTPS cache beside the installed worker's
active cache. Cleanup checks configuration, process handles, mappings and active
links, binds the reviewed metadata digest, and records intent on the coordinator
and operator disk before deleting anything. This also works when the query host
has no space left to write its own journal.

For an approved additional data volume, `cache-volume-plan` and
`cache-volume-provision` verify the existing Valargroup project and droplet,
record the new volume, and reuse it on retry. Supply the production credential
through `DIGITALOCEAN_ACCESS_TOKEN` at runtime. `cache-data-plan` and
`cache-data-prepare` verify that provisioning receipt and copy the running cache.
`cache-data-activate` stops only the selected worker for the final copy, retains
the original directory, and mounts the new copy at the existing path. Persistent
mounts and unit dependencies prevent the worker from falling back to the root
disk after a reboot. `cache-data-status` reads the retained transaction. Failures
hold their recorded state; activation never automatically rolls back. Verify
readiness, fresh publication advancement and the independent encrypted-answer
probe before retiring the original cache with the same guarded cleanup command.
