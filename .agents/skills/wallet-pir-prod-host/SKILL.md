---
name: wallet-pir-prod-host
description: Build, debug, repair, and deploy Wallet PIR directly on production hosts over SSH, reusing host build caches and the existing transactional deployment tool. Use for authorized production rollouts, startup failures, slow publication, and interrupted deployments.
---

# Wallet PIR production host workflow

To ship a main commit's release, use the `wallet-pir-deploy` skill. This workflow
covers host builds, repairs and investigations that a release does not.

Prefer building and investigating on the affected Linux host, then activating
through `tools/scripts/wallet-pir-deploy.py`. Keep the iteration short: one pinned
source checkout, one reusable build cache, focused checks, and one acceptance pass
per deployed component. Read the relevant sections of [operations](../../../docs/operations.md)
and [publisher deployment](../../../docs/publisher.md) for the operation selected.

## Establish the scope once

- Carry forward the user's deployment authorization. A request to debug or monitor
  alone does not authorize activation. Do not ask again for an already-authorized
  build, repair, or deployment within the same scope.
- Pin one source revision for the rollout. New merges do not silently change an
  in-flight candidate. Record later changes for the next candidate.
- Discover the current operator inventory, installed configuration, transaction
  state, and rollback artifacts. Obtain hosts and roles from that inventory;
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

## Activate with the existing transaction

Run the current CLI's `--help` and the selected operation's help before composing
arguments. Use its plan/preflight, production lock, request/plan digests, journal,
status, and rollback. Keep the same state directory throughout the transaction.
Use a host-local runner when its existing inventory and credentials support it;
otherwise run the CLI from the authenticated operator machine while the build
and expensive diagnostics stay on the host. Do not invent a local-runner flag,
copy credentials to enable one, or recreate the deployment engine in a wrapper.

Reuse an already-staged executable when the tool verifies its digest. Avoid
unnecessary artifact round trips, cold target directories, repeated full suites,
and additional verification ceremonies around checks the transaction already
performs. Retain affected-service correctness and encrypted-answer acceptance;
unrelated CI is not a deployment blocker when the user has explicitly waived it.

Deploy the publisher before dependent query servers. Verify publisher discovery
and publication availability over the existing HTTPS origin before restarting
consumers. A CPU Transparent-only query binary must not replace an Enhance or
Status serving executable. Changes to units, binaries, routes, or ownership go
through the locked tool, including repairs and bounded shutdown handling.

On loss of SSH, inspect the recorded transaction and the actual running process
before acting. A lost connection is neither success nor proof of rollback. Use
the supported continuation or verification operation for the observed phase;
do not rebuild, restart, or roll back a healthy candidate merely to rerun the
workflow. A command named `reconcile` may restore the predecessor: check its
documented behavior first. Never remove another session's lock or kill its owner.

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

Finish with the deployed source and artifact identities, transaction status,
passed acceptance, rollback command, follow-up PRs, and specific remaining alerts
or stopped observers. Keep receipts and raw output in ignored `.local-pir/`.
Distinguish deployed, ready, encrypted-answer accepted, and fully qualified.
