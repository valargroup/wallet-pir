# Operating the Wallet PIR fleet

Start here for the shared operating model. Product runbooks own their publication,
authority, resource and encrypted-answer checks. Source and release preparation do
not establish what is running; inspect the fleet and retained deployment evidence.

See [the unified publisher](publisher.md) for the opt-in executable that composes
these product owners into one process. Packaging it does not establish deployment.

## Architecture and ownership

| Product | What runs | Product authority |
|---|---|---|
| Enhance | Ingestion/publication coordinator, query ingress, packing router, evaluation workers | [Architecture](enhance/architecture.md), [deployment](enhance/deployment.md) |
| Status | Separate observation controller, packing router and evaluation worker; implementation shares the Enhance package | [Architecture](status/architecture_status.md), [distributed topology](status/status_distributed.md) |
| Transparent history | Event journal writer, publication controller, recent/archive workers and public routing | [Architecture](transparent/architecture.md), [deployment](transparent/deployment.md) |
| Transparent display | Its own transaction journal, publication controller and display workers | [Display contract](txid/txid-display.md) |
| Receiver | One process indexes, publishes and serves receiver payments | [Overview](receiver/README.md), [deployment](../tools/receiver/digitalocean/README.md) |

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

A clean installed package may install a newer package revision. Binary deploys
keep requiring the operations package and the binary archive to name the same
source revision.

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

Deploy, restart, rollback and deployment-verification commands pause normal Slack
notifications through the running `pir-apm` sidecar. A single fleet-wide window
starts under the production lock; APM sends the service names and keeps collecting
metrics and evaluating incidents. Existing readiness and exact-answer validation
run once as before. Success sends “Deployment finished”; failure sends “Deployment
failed” and restores normal notifications before automatic rollback. Events that
recover inside the window are not replayed; fresh, still-failing observations are
announced after it ends.

The deadline is fixed at one hour and survives runner or monitor restarts. APM
expires it itself and sends an expiry warning; retries never extend it. The
running sidecar must support `pir-apm deployment` and have active alerting with
`PIR_APM_STATE_PATH`. Failure to open the window refuses the deployment before
service changes. Read-only inspection and preparation commands do not pause alerts.
The sidecar update that first introduces this feature uses the previously
installed deploy tool, before adopting the updated operations package.

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
commands. `status SERVICE --only ROLE[@HOST]` and `ancillary-status [NAME]` are
read-only and report an unreachable host as a result instead of leaving it out. The example inventory is complete in shape but uses
documentation values; it is not the live fleet.

```sh
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json fleet --complete
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json status enhance
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json status transparent-history
```

`fleet` reads configured roles without contacting hosts. `status` reports observed
systemd state and executable identity plus the latest transaction, not encrypted
query correctness. Structured results name the service, operation, roles, hosts,
phases, changes and refusal reasons. The importable `deploy.lifecycle.Lifecycle`
interface supports plan, preflight, deploy, status and rollback over the existing
engine; the CLI consumes that interface. Mixed CPU/GPU releases emit one JSON
result per artifact group, each with its binary digest and separate transaction.

Immutable binary staging uploads to up to four hosts concurrently under the
production lock. Every upload and binary self-check finishes before activation.
A staging failure waits for the other uploads to finish before releasing the
lock; no units restart. Activation and rollback keep their descriptor order.

Enhance, Status and Receiver use the ordinary binary lifecycle:

```sh
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json capture-baseline enhance
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json --json plan enhance --archive enhance-pir-native.tar.gz --kind enhance-pir-native --sha <full-revision>
python3 <release>/tools/scripts/wallet-pir-deploy.py --inventory /path/to/inventory.json preflight enhance --archive enhance-pir-native.tar.gz --kind enhance-pir-native --sha <full-revision>
```

Review the complete captured unit and its drop-ins, the planned differences and
the binary identities. Supply the matching CUDA artifact for selected GPU roles;
missing variants are refused. Staging preflight honors baseline and configuration
refusals before uploading anything. Deployment holds the existing production lock,
records effects in the existing journal and retains the existing rollback rules.
After deployment, report status and the rollback command from that product's runbook.

If restoration restarted a process but readiness timed out, rerun rollback. The
journal retains the acknowledged systemd invocation. Recovery can keep that same
process preparing when the restored files and loaded unit still match; it repeats
the full readiness and product authority checks before marking restoration done.
A lost restart reply, changed files or a different invocation requires another
restart. The receipt alone never establishes readiness.

Transparent history and display appear in the same role view and process status,
but their mutation drivers remain product recipes: `schema-*` and
`txid-display-*`. Ordinary binary deploys refuse those services because changing
a publication is more than replacing an executable. Their retained request,
plan-digest, authority, quiescence and recovery requirements continue to apply.

## Recover an interrupted rollout

Recovery repeats only reviewed steps for the checkpoint the transaction recorded.
Start from what the tools report, not from a new plan of your own.

1. Read the latest transaction (`status <service>`, or `publisher-cutover-status`
   for the unified publisher). When a publisher deploy fails and its automatic
   rollback fails too, the journal keeps both errors apart:
   `failure.deploy_error` says why the rollout stopped and
   `failure.rollback_error` says where restoration stopped. The command's error
   names both.
2. For a publisher cutover whose restoration stopped before verification
   finished, use the staged Status repair path:

   ```sh
   python3 <release>/tools/scripts/wallet-pir-deploy.py publisher-cutover-repair-probe-plan --request <request.json> --request-sha256 <digest>
   python3 <release>/tools/scripts/wallet-pir-deploy.py publisher-cutover-repair-probe-verify --request <request.json> --request-sha256 <digest> --expect-plan-sha256 <plan digest>
   ```

   It applies only when all of these hold:
   - the continuous-load checker is stopped;
   - the transaction is this request's restored transaction: restoration
     recorded the publisher start and both serving roles passed their restored
     identity checks;
   - no effect is pending (no recorded intent);
   - the replacement Status probe client is the staged publisher executable,
     unchanged since staging.

   The plan pins the restored processes and the replacement client; verify
   repeats all four exact-answer checks and finishes the rollback without
   stopping or starting any service. [The unified publisher](publisher.md)
   describes what it checks.
3. Outside those preconditions the repair refuses and names the check that
   failed. That is an unsupported checkpoint: report it with the refusal and the
   transaction's recorded phase. Do not bypass the guard, and do not fall back to
   a generic `rollback`, which repeats restoration effects. Use another recovery
   command only where this runbook or [the unified publisher](publisher.md) names
   it for that exact phase (`publisher-cutover-continue` for a lost stop reply,
   `publisher-cutover-verify` for verification recovery).

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

The existing `plan`/`preflight`/`deploy`/`rollback` install, update and remove
them with the usual exact artifact identity, pinned hosts, lock and journal:

1. `capture-baseline SERVICE --only ROLE` records the unit as absent.
2. `plan`/`preflight` show the action `install` and every file written. A unit
   is installed only where the entry says `"install": true`; a fresh install
   refuses to overwrite a configuration file it did not render.
3. `deploy` stages the release, then runs the product's own check on the
   staged release against a staged copy of the configuration before any unit
   changes: `pir-query-server --config FILE --check`. Receiver downloads
   immutable objects over HTTPS and requires fresh publisher authority before
   activating them. Other workers use their registered configuration checks. The
   unit is then written, enabled, started and verified: active, the release's
   digest running, health answering with that `binary_sha256`, and the role's
   own readiness where it has one.
4. A failure, or `rollback`, returns a fresh install to absence: the unit is
   stopped, disabled and removed with the configuration it wrote. State
   directories stay; they hold no authority.

Transparent services keep their product recipes for every other role; the
shared engine admits them only when `--only` selects external workers (and a
rollback only of such a transaction). Verification proves the process and its
identity, never serving authority:

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
