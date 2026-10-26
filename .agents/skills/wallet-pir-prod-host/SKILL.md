---
name: wallet-pir-prod-host
description: Build, debug, repair, and deploy Wallet PIR directly on production hosts over SSH, reusing host build caches, and deploy build releases with pir-deploy and pirctl. Use for authorized production rollouts, startup failures, slow publication, and interrupted deployments.
---

# Wallet PIR production host workflow

Prefer building and investigating on the affected Linux host, then activating a
build release through `tools/deploy/pir-deploy`, which runs `tools/deploy/pirctl` on
each host. Keep the iteration short: one pinned
source checkout, one reusable build cache, focused checks, and one acceptance pass
per deployed component. Read the relevant sections of [operations](../../../docs/operations.md)
and [publisher deployment](../../../docs/publisher.md) for the operation selected.

## Establish the scope once

- Carry forward the user's deployment authorization. A request to debug or monitor
  alone does not authorize activation. Do not ask again for an already-authorized
  build, repair, or deployment within the same scope.
- Pin one source revision for the rollout. New merges do not silently change an
  in-flight candidate. Record later changes for the next candidate.
- Discover the current `pir-deploy` inventory, installed configuration, what each
  host runs (`pir-deploy status`), and its previous release. Obtain hosts and roles from that inventory;
  historical IPs, process IDs, scratch paths, hashes, and receipts are not defaults.
- Keep the requested products and transport. “HTTP” is not permission to replace
  existing HTTPS. A shared publisher restart must preserve unselected products;
  it does not authorize their protocol migrations or query-server upgrades.
- Use Valargroup credentials from their existing authorized runtime sources. Keep
  credentials and private inventories out of Git, logs, and the skill itself.

## Build on the host

Check `ssh-reachable` before remote work. Use `ssh-check` for short diagnostic
checks. Preserve the pinned key, known-host checks, and existing routing; do not
try another key or disable verification after an access refusal.

Batch independent host observations into a short remote program. Capture the
actual unit, PID, executable digest, configuration, publication identity, resource
headroom, and recent relevant errors. Avoid dozens of fresh SSH sessions; the
deployment executor already multiplexes its helper connections.

Use a clean, visible host checkout pinned to the candidate commit, outside live
publication and wallet-state directories. Fetch normally when the host already
has authorized Git access; otherwise transfer a verified source bundle. Never
copy a private SSH key onto a host to make Git or deployment work.

Reuse a compatible `CARGO_TARGET_DIR` on that host. Read `rust-toolchain.toml` and
the deployed build settings; retain the target CPU, native compiler flags, and
feature set. Keep one Cargo writer per target directory. Choose build parallelism
from measured spare RAM and CPU; leave room for the running services. Do not stop
the old service to make room for compilation.

Build deploy binaries with `--locked --release`; `release-fast` is for iteration
and focused checks. Capture each binary in a separate immutable candidate path
before another feature build can overwrite Cargo's output. Record source SHA,
clean/dirty state, features, flags, toolchain, binary SHA-256 and size. Commit a
host fix before producing its deployable artifact, then carry that exact fix back
to a follow-up PR against main. Do not commit exploratory output.

Bound builds and tests explicitly and retain their logs and process identity.
Reuse focused test evidence for the same source/build inputs. Filesystem refusal
tests that depend on permissions must run as an unprivileged user, even when
compilation runs as root. Build and check the candidate before stopping its
predecessor.

For Transparent feature examples and recovery details, read
[the Transparent recipe](references/transparent.md).

## Activate with pir-deploy

`pir-deploy` installs only published `build-<sha12>` releases. Commit a host fix,
push it, and dispatch the Release workflow on that branch
(`gh workflow run release.yml --ref <branch>`) to get a deployable release; a binary
built on the host is for diagnosis, not activation. Then, from the operator
machine:

- `pir-deploy --dry-run <mode> <sha>` checks every selected host without restarting
  anything; then run the mode itself (`publisher`, `product <group>`, `unified`,
  `all`; `--breaking` for a contract-major change). Deploy the publisher before
  dependent query servers.
- `pirctl` verifies readiness and the release's `pir-probe` and restores the previous
  release on failure. `pir-deploy rollback <group|host>` and `pirctl rollback
  <component>` swap back later; `pir-deploy status` shows what runs.
- On loss of SSH, check `pir-deploy status` and the running process before acting.
  A lost connection is neither success nor proof of rollback. The deploy lease
  expires on its own; never remove another session's lease or kill its owner.

Changes to units, configuration or routes are host configuration, not a release:
make them deliberately and verify the service afterwards.

## Diagnose the affected service and accept once

Separate compilation, shutdown, publication construction, distribution hashing,
transfer, runtime loading, background validation, and encrypted queries. Measure
the actual affected process over a stated interval. Use a short low-frequency
CPU profile plus I/O and wait observations when necessary; disclose profiling
load. Attribute shared-publisher work to its product. A production profile finds
hot paths; an isolated same-host baseline/candidate comparison proves a speedup.
Do not provision benchmark infrastructure without the required spending approval.

After each activation, use the operation's readiness contract and its independent
encrypted HTTPS probe with retained fixture/oracle pins. Do not invent an extra
gate requiring every background hash or warmup task to finish. Report background
progress separately from readiness, corruption/withdrawal failures, answer
mismatches, publication age, and disk capacity. A listening port is not acceptance.

For alerts, summarize active/firing incidents and their observed values, rather
than dumping metric history or assuming every alert is startup noise. Preserve
critical latches, incident records, wallet recovery state, and high-water marks.
Recovered disk space does not itself acknowledge a stopped checker's incident.
Use filesystem free-space measurements to establish disk recovery; hard-linked
directory sizes alone do not establish reclaimable bytes.

Finish with the deployed source and release identities, `pir-deploy status`,
passed acceptance, rollback command, follow-up PRs, and specific remaining alerts
or stopped observers. Keep receipts and raw output in ignored `.local-pir/`.
Distinguish deployed, ready, encrypted-answer accepted, and fully qualified.
