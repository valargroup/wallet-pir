# CI and deployment latency

The target is **under three minutes from PR update or main push to fast checks completing**
for routine changes on warm runners, and **two minutes from deployment dispatch
to healthy service** when an exact-SHA qualified artifact is already available.
Cold builds, dependency/toolchain changes, initial data preparation, schema
migrations and first fleet activation are measured separately. These are targets,
not measured guarantees; hosted fallback and cold fleet warming can exceed them.

## Complete CPU correctness budget

`make check-correctness` runs all ordinary CPU correctness targets, reference
arithmetic and encrypted HTTP restart checks, doctests, the standalone sizing
exporter, operations/tooling checks and native CPU lint. The budget is 30 seconds
per test and 180 seconds for the complete warm command, including compilation,
fixture setup and teardown. CUDA and explicit soak/hardware campaigns keep their
existing qualification commands and durations. This command does not replace
exact-SHA release qualification.

The complete command requires Linux: the sizing exporter deliberately verifies
the executing binary through `/proc/self/exe`. Other platforms can run focused
checks or diagnostic mode; a missing Linux check cannot qualify the full budget.

Use `python3 tools/ci/correctness.py --record-only` to diagnose a complete run
that exceeds the target; it runs the same correctness selection. Receipts and
individual case times go to ignored `.local-pir/correctness/`. Record-only
success establishes correctness, not compliance with the timing budget. Cold
dependency preparation and CI queue/setup time must be reported separately;
local timings do not establish CI-runner performance.

Use `--case-seconds 5` to measure or enforce the tighter local per-test target.
The same limit applies to Rust and Python, including each case's setup and
cleanup. `--record-only --case-seconds 5` retains every case and lists all
five-second overruns in its receipt. Diagnostic success establishes correctness;
only an enforced passing run establishes compliance with the selected budget.

Native tests compile once with a consistent feature set, then run Cargo-reported
executables with at most three programs active and one case per program. Mutable
service state is fresh; only immutable public runtime material is shared within
the run. Serving HTTP and packing tests live beside the serving library and
avoid the application's chain ingestion dependencies. Reference arithmetic,
encrypted HTTP, frozen v7 vectors and Status checks run under the reference
profile. Recovery decision tests use real durable controller state and small
HTTP peers; separate worker tests
check durable reservation, abort and commit behavior. Real encrypted HTTP,
restart, independent wallet, publication, packing and revocation checks remain.

## Parallel release qualification

Service qualification targets five minutes from dispatch to qualified artifacts
for routine source changes with compatible caches. Cold dependency/toolchain
builds are measured separately. These are targets, not established guarantees.

The checked-in [test matrix](../tools/ci/test-matrix.json) assigns every enabled
family target exactly once: seven Transparent jobs, eight Enhance jobs and two
Receiver jobs. Receiver lint runs separately. Hosted test shards and lint start after selection,
alongside helper checks. The final completion and artifact publication gates still
require every selected helper check to pass. Whole test executables retain their
fixture reuse and existing thread limits. Reference arithmetic and CUDA checks
have separate feature/cache partitions. The reference shard retains the independent
generic-hint and damaged-artifact checks; encrypted lifecycle smokes run with
reference arithmetic in fast correctness and with the native backend in full
qualification. Deployment builds remain on the dedicated
build runner. Example builds, doctests, binary tests and the standalone sizing
exporter are included. New or removed targets require an explicit matrix update.

Each shard reports compilation/execution timings and writes a success receipt
bound to the source SHA, matrix and workflow attempt. Family aggregates reject
missing, duplicate, compile-only or mismatched receipts before artifact promotion.
Rerun the complete workflow rather than only failed jobs: receipts belong to one
workflow attempt, and earlier attempts remain available as evidence.
Common/shared checks overlap service work, but all required checks must finish
successfully before candidates become qualified artifacts. Hosted source snapshots
reuse verified unchanged inputs; Cargo still checks every compiled unit.

`python3 tools/ci/test_matrix.py validate` checks target coverage; `plan` prints
the runner matrix. The existing cache-primer workflow can compile those shards
without executing or qualifying tests. The manual **Audit qualification matrix**
workflow executes the original and split suites at the same revision, then compares
test names, ignored cases and package feature sets. It publishes audit evidence,
not deployment artifacts. Retain failed/cancelled attempts and report queue, setup,
compile, execution and promotion time separately when assessing the five-minute
qualification and ten-minute deployment targets.

## Fast feedback and full qualification

`CI / Fast checks` runs on pushes and PRs. It selects changed workspace packages
and their transitive reverse dependencies. Deleted files are included; build
configuration, dependency manifests and unknown paths select the whole workspace.
Workspace-only membership and local lockfile changes select the changed packages
and their consumers. Global manifests, dependency locks, Cargo configuration and
the Makefile retain conservative native coverage and run tooling/workflow
contracts; they do not by themselves select unrelated product operations fixtures.
External dependency, profile, resolver, inherited-setting,
removed-member and unrecognized changes retain complete workspace coverage.
CI tooling changes run tool tests and workflow contracts; they retain broad full
qualification without starting unrelated operations fixtures in fast feedback.
Documentation-only changes run documentation checks without installing Rust.
Operations-only changes run helper checks without compiling Rust. Main compares against
the last successful main fast run or a newer completed full fast preparation on
main's ancestry. Preparation reuse requires successful complete-workspace compile
and validation plus affected-graph validation from the same successful attempt
and the canonical repository/workflow.
Partial diagnostic dispatches, canceled or skipped validation, unrelated branches
and stale ancestry cannot leave unchecked changes behind.

Rust feedback compiles the selected unit tests, CLI test binaries and named fast
integration targets together in one `release-fast` Cargo invocation. Campaign
targets are excluded from that build. Local and CI checks then execute at most
three Cargo-reported test programs concurrently. The publisher library builds
full archive runtimes on every core of a 4-core hosted runner, so it runs alone
there before the pool; beside it, the encrypted HTTP smokes miss the 30-second
case deadline. The 8-core fast runner keeps that library in the pool with four
build threads. The Enhance encrypted HTTP executable runs alone before the
pool on every runner: concurrent runtime preparation exceeded its per-case
deadline even on eight cores. Other HTTP integration programs use the pool
ahead of ordinary units. Within each group, binaries with more discovered cases start first,
independently of Cargo's artifact order. Large unit executables are divided into
at most three disjoint groups of exact discovered case names, within that same
process limit. Each group retains dozens of serial cases and immutable fixture
reuse; no case is omitted or repeated. HTTP integrations and the exclusive
small-runner publisher are not partitioned. The working-set ceiling remains
6 GiB. Explicit feature or test-filter checks use one program at a time.
The fast runner enforces the 30-second limit for every Rust case, including
setup and teardown inside the case. Each program keeps its package directory and Cargo environment; fast integration
targets must contain an enabled, unignored case. Ordinary unit discovery has no
named exclusions. Cold cache preparation compiles the same target selection.
Enhance smoke cases reuse immutable prepared input bytes for the same record count,
then copy them into separate writable worker directories. Workers still verify
their copies and both 67/68-record generic oracles build independently. The encrypted
restart and pending-commit publication checks retain the required two-worker quorum.
Transparent fixture exit tests use tiny file trees with the actual ownership and
copying helpers. Manager behavior tests still prepare and verify real publications.
Fixture construction, copying, verification and cleanup stay inside measured work.

Ordinary cache, identity, preparation-failure, publication, TLS fault and recovery
assertions all run in fast feedback. The non-default `qualification-tests` feature
restores their original larger geometry combinations and longer histories for
full qualification. Binary unit tests run as well as library targets. Affected
packages also run integrations classified as `fast` in
[`tools/ci/integration-tests.json`](../tools/ci/integration-tests.json). Complete
correctness includes independent reference oracles, doctests and CPU lint. Larger
integration campaigns and hardware qualification remain separate release gates.
The aggregate completion check fails if a
selected job fails or is skipped. This avoids compiling every integration target just
to exclude its tests at runtime. Affected helper
checks overlap both Rust compilation and execution; completing helper checks
is not a prerequisite to starting native tests. The gate still requires both
suites to pass. Cargo writers remain sequential in each target; tooling oracles
use separate fixture targets. Selected operations suites run in
four parallel make slots. Deployment unittest modules use two separate processes,
retaining each module's fixtures. Fast deployment feedback selects exact discovered
case IDs from `tools/ci/deploy-fast-tests.json`: installation and upgrade, lock
refusal, interrupted recovery, tampered retained artifacts, failed verification
and rollback. Empty, duplicate or renamed selections fail. Full qualification
runs every discovered deployment case without that selection. The package
transaction module has independent test classes and is explicitly split by class;
class fixtures stay together, and modules with module fixtures cannot opt in. Each case keeps its
own mutable deployment tree; negative variants may restore a real transaction
checkpoint after proving that an untampered copy still recovers. Failures in any
task fail fast CI. Fast native runs report each executable's wall time and the
15 slowest completed cases; overlapping program times must not be added to infer
the job's elapsed time. Tooling cache fixtures cannot write to the outer job's
GitHub environment files or timing receipts.

Every workspace integration target must be classified exactly once.
`make check-tools` validates the registry against manifests and integration source files without invoking Rust, rejecting new
unclassified targets and stale entries. Full CI runs both tiers serially, including
the real-crypto `packing_http` path; it clears `QUALIFY_*` overrides so that CI uses
the bounded default fixture. Existing ignored hardware/GPU cases remain explicit
manual checks. The boundary recovery target is a reference model, not evidence that
the implementation follows every modeled transition.

`Qualify services` selects cumulative changes since each service's last qualified
ancestor on main. Missing history, unknown inputs and truncated GitHub comparisons
broaden coverage. A manual run can qualify one family or all families at the exact
main revision captured by dispatch; newer pushes do not cancel manual runs.

Receiver, Transparent, Enhance/Status and the operations package qualify
independently, using the existing full family suites, shared tests and helper
checks. Common helper, shared-library and infrastructure checks run once per qualification,
alongside service-specific native work. Full completion still requires every
selected helper, shared and infrastructure check. Helpers run on hosted runners
to reserve the fast queue for main. Operations archives
assemble on hosted runners without Rust. Full tests and lint run on separate
Ubuntu 24.04 hosted runners; CPU release builds use the dedicated build runner.
CPU and Ubuntu 22.04 CUDA candidate builds start alongside family testing;
candidate archives are uploaded under
an ineligible name and re-verified before qualified publication. Each family
publishes immediately after its required checks pass. Failure in another family
cannot authorize or prevent that publication. CPU artifacts do not wait for CUDA.
CUDA candidates use an ineligible artifact
name until Enhance qualification and candidate compilation both pass; publication
rechecks archive contents, ABI metadata and clean source provenance.

Only **CI / Fast checks** validates automatically on main pushes and PRs.
Dependency cache priming runs independently on relevant main changes.
Both PR and main feedback target three minutes; queueing also counts toward the
latency target. During the current repair, validation finishes before main's
final timing check enforces 180 seconds. PRs and manual runs report timings.
A completed over-budget run provides diagnostic coverage, not a timing pass.
The pre-push health guard follows this workflow, rather than long qualification.

Whole-repository **CI full** runs nightly at 04:17 UTC and on explicit dispatch.
**Qualify services** is dispatch-only and pins its main revision. It retains
full service suites and exact-SHA artifact checks; a green fast gate cannot
replace it. Hosted cache priming runs after main dependency/build-configuration
changes, nightly at 03:37 UTC or on request, without
launching qualification or holding the main result open. Dependency/toolchain
validation and cold preparation belong before landing an upgrade. Unexpected
missing prerequisites must fail visibly rather than skip required tests.

To reproduce selection without running tests:

```sh
python3 tools/ci/fast.py --base <base-sha> --select-only
```

To run the fast gate locally, omit `--select-only`. Local checks include working
changes and untracked files; `--all` selects every package. CI passes
`--committed-only`. See [development workflow](development.md).
The workflow fetches only the current checkout and comparison commit, rather than
all historical evidence and build artifacts in repository history.

For a hosted timing replay, dispatch `CI` on main with `hosted=true` and
`comparison-base` set to the parent of a representative change. This uses the
hosted PR environment without occupying or canceling main's reserved queue.
Manual diagnostic runs allow cold-build measurement beyond the routine ceiling;
they do not qualify the latency target merely by completing successfully.

Enhance full qualification runs common service assertions once with the native
backend, including the independent wallet decoder and the bounded packing
publication. Reference arithmetic and one encrypted HTTP/restart smoke remain
separate; CUDA compilation and
configuration checks use one native-plus-CUDA feature set. Commit/abort decision
tests use durable controller state and small HTTP peers; worker tests separately
check reservation accounting and restart behavior. The encrypted HTTP smoke separates publication progress from durable restart.
One case retains a failed commit delivery, a later healthy publication and retry
with two real workers. The other checks encrypted answers before and after a
durable coordinator restart with a fresh wallet. Both keep production runtimes
and a 30-second deadline.

Transparent concurrency coverage pairs four publication layouts with four wallet
shapes instead of running their Cartesian product. Each pair still compares real
encrypted sequential and concurrent syncs against an independent ledger traversal;
all layouts, one-shard and multi-shard wallets, and request/charge assertions remain.
The deployment goal is under ten minutes for all services, including qualification,
artifact preparation, rollout and query verification. These reductions do not by
themselves establish that end-to-end timing; record a complete run before claiming it.

## Warm runners

The shared CI host is **wallet-pir-ci-01** (`164.90.197.110`), DigitalOcean droplet
`600509951` in project `wallet-pir`, region `ams3`. It runs Ubuntu 24.04 on the
`s-8vcpu-16gb-amd` plan: 8 vCPU, 16 GB RAM and 320 GB root disk ($112/month at
provisioning). It is separate from the production coordinator and production VPC.
SSH administration uses `ssh wallet-pir-ci-01` and Roman's public key.

Two repository runners share this host: `wallet-pir-ci-fast` and
`wallet-pir-ci-build`. The variables below select them once these workflows are
published. Unset variables retain the **ubuntu-24.04** hosted fallback.

| Repository variable | Configured value |
|---|---|
| `WALLET_PIR_MAIN_RUNNER` | `["self-hosted","Linux","X64","wallet-pir-fast"]` |
| `WALLET_PIR_BUILD_RUNNER` | `["self-hosted","Linux","X64","wallet-pir-build"]` |

Use separate Unix users and 0700 homes for the two services:
`wallet-pir-fast` and `wallet-pir-build`. Neither has sudo or a deployment SSH key.
The fast runner executes main and explicit cold preparation, with no production access or
access to the build user's release caches or runner credentials. All PRs and branch
dispatches use hosted runners to reserve main's queue. The legacy
`WALLET_PIR_FAST_RUNNER` variable is `"ubuntu-24.04"`, so a workflow stored
on an older branch cannot take main's runner through the previous setting. Explicit full runs from branches other than main
also use hosted runners; only main builds use the trusted persistent build pool. The fast service has CPU/I/O weight 1000 and a 6-GiB memory ceiling; the build
service has weight 100, nice level 10 and a 9-GiB ceiling. Both use
`MemoryHigh=infinity`: a soft threshold previously trapped tests and the runner
listener in memory reclaim for hours without reaching the hard limit. The hard
caps remain enforced. `OOMPolicy=kill`, `KillMode=control-group`, and
`Restart=always` clean up the entire service group and reconnect the runner after
an out-of-memory failure. Service stops allow 30 seconds before forced cleanup.
Both limit Cargo to four
build processes. A build cannot occupy the fast service's queue, but both services
share the host CPU and disk, so latency must also be measured under contention.
The single build slot serializes CPU release builds, while full tests and lint
use independent hosted Ubuntu runners with their own lane/scope caches. Initial
hosted qualification may need cold compilation; a successful hosted cache prime
for fast feedback alone does not establish warm full-test or lint caches.
Transparent full tests run two test cases at a time
(`RUST_TEST_THREADS=2`), while Enhance tests run serially: the simulation fixtures each start multithreaded services,
and too many simultaneous fixtures can exhaust a hosted runner
or trigger service memory admission refusals. Each test retains its internal concurrency. `tools/ci/full-test.sh` compiles
the suite first, then asks Linux to discard clean file-cache pages for Cargo
outputs before running tests. Build artifacts remain on disk. This prevents
compiler cache from consuming the memory headroom used by service admission
checks without throttling the live integration fixtures.

Bootstrap scripts: `tools/ci/bootstrap-runner.sh` installs tools and creates the
users; `tools/ci/register-runners.py` accepts short-lived GitHub registration
tokens on stdin and installs the services with resource limits. Tokens are not
stored by either script. The runner itself stores its normal credentials in its
private home directory.

Runner prerequisites: Ubuntu 24.04 x86-64 with x86-64-v3 CPU support, Git, Python
3.11+, Node, make, jq, shellcheck, Clang/libclang-dev, protobuf-compiler, standard
native build tools, and Rust **1.97.1** with rustfmt and clippy. The setup action
checks the pinned tools and OS instead of reinstalling them each run.

## Cargo cache identity and reuse

[`tools/ci/cargo_cache.py`](../tools/ci/cargo_cache.py) computes each job's
compatibility identity from the actual compiler (`rustc -vV`, including its
commit), Cargo version, OS/glibc, CPU architecture and target, native C/C++,
clang and protoc versions, compile-affecting environment (`RUSTFLAGS`, `CFLAGS`,
`CARGO_PROFILE_*`, `CARGO_BUILD_*` and similar), the version of any `rustc`
wrapper, toolchain files, and every Cargo configuration file Cargo reads: the
checkout's and each ancestor directory's `.cargo/config(.toml)`, `CARGO_HOME`'s,
and files they `include`, recorded by relative position. The compiler and
wrapper are probed as Cargo selects them: environment first, then `[build]`
settings by Cargo's precedence (deeper directory over ancestor over `CARGO_HOME`;
an including file over its includes; a later include over an earlier one), with
config-relative paths resolved as Cargo does. The full identity
also covers the lane, its package/feature scope and profile, the root manifest's
`[profile.*]` definitions, and `Cargo.lock`. Without Python's `tomllib` (the
Ubuntu 22.04 CUDA container), the whole `Cargo.toml` is hashed instead and
`include`s and `[build]` tool settings in config files are not followed. It excludes the checkout SHA, workflow text and runtime-only
variables such as `RUST_TEST_THREADS` and `CARGO_TERM_COLOR`. Every
`rust-setup` use names a lane and scope; an unclassified pair fails the job.

Persistent Cargo output lives outside checkout at
`~/.cache/wallet-pir/<runner-name>/<lane>/<toolchain-identity>/`, where the
toolchain identity omits scope, profile definitions and lockfile. Fast, lint, full test, q48 release
and native CPU release lanes cannot contend for the same Cargo target lock.
Cargo tracks source, lockfile, features and profile changes inside a lane.
Prune unused lane directories only while the runner is idle. Do not share
writable caches between trust boundaries.

Hosted fast snapshots partition by the existing compiler/native tools, OS/ABI,
flags, Cargo configuration, lane, scope and profile compatibility identity. Their
`v2-wallet-pir-workspace-<compatibility>-<lock>-<source-tree>` keys add the committed
Git tree to permit writeback after source changes; the restore prefix stops at
compatibility, retaining dependency fallback without crossing build or trust
boundaries. Checkout SHA remains excluded from compatibility. Other hosted lanes
retain rust-cache and exact identities. GitHub scopes PR saves to the merge ref;
a PR can restore its own or base/default-branch entries, while main cannot read
PR entries. No ref is inferred from fingerprint counts or restore duration.

Fast snapshots use `actions/cache` for the Cargo registry, Git dependencies and
complete fast target, including test executables. Only successful jobs save;
canceled/failed builds cannot archive targets while orphaned Cargo writers are
still changing them. An exact immutable hit does not write back. The source-tree
suffix permits a new successful snapshot after edits, but does not make a partial
exact entry complete for a later broader selection. Other hosted lanes keep
`cache-on-failure`; persistent targets and release caches are unchanged.

[`workspace_cache.py`](../tools/ci/workspace_cache.py) captures tracked regular
files before compilation. After work, it seals their content hashes, modes and mtimes
alongside a checksum and a signature of Cargo fingerprint bytes **and mtimes**.
On restore, only unchanged bytes and modes at the same checkout path and compatibility
identity regain their captured source timestamps. Changed files, or missing,
corrupt, stale or incompatible metadata, receive timestamps newer than restored
fingerprints. Metadata schema 2 rejects older records lacking mode identity.
Before setup and again after archive extraction, validate the lexical target and
all parents: aliases, checkout/ancestor targets, Git internals and overlap with
tracked source paths fail closed before mkdir, move or timestamp normalization.
Git internals include the actual Git directory and common directory discovered
read-only through Git, including linked worktrees and separate-Git-dir checkouts;
targets inside or containing either directory are refused.
Resolving a target alias first would lose ownership and could move another
directory. Invalid paths are errors, not cold misses; their destinations remain
untouched by the helper.
Any symlink in the checkout outside Git internals and the validated target
disables target reuse entirely, including ignored and untracked input links:
the restored target is moved to a uniquely named `fast-unused-*` sibling outside
the saved path, and Cargo starts with an empty target. This includes symlinks to
external or untracked destinations; neither link nor destination is touched.
These unused directories live only on the ephemeral hosted runner. This chooses
extra compilation over trying to prove alias topology safe. External/untracked
inputs and directory timestamps are otherwise not normalized. Cargo independently
checks every unit, feature, environment,
build-script dependency and output; conservative extra rebuilds remain possible.
The signature prevents an old source record from blessing newer outputs built
from different bytes even when Cargo fingerprint contents are identical.

The new cache layout needs a separately measured first successful cold preparation
before repeat hosted timing can qualify. It does not restore older rust-cache archives with a different path/version layout. Complete
targets may cost more archive/storage time than rust-cache's pruned targets.
Reverting the action/helper restores the old keys and behavior; v2 snapshots stay
ref-scoped and expire under GitHub's existing retention policy, without deletion
or cache migration. No dependent product format or published artifact changes.

For an unmerged PR, apply the `prepare-hosted-cache` label to explicitly run
[Prepare hosted PR cache](../.github/workflows/ci-pr-prepare.yml). It uses the real
`pull_request` merge ref on the existing hosted runner, read-only repository
permissions and no persisted checkout credentials. It compiles **and validates**
all fast units with a separate 30-minute cold-preparation allowance; every test
keeps its 30-second limit. It then compiles and validates the ordinary affected
selection against the event's actual PR base SHA before sealing. Cargo unifies
dependency features across selected packages: an all-workspace build can enable
extra features (for example `clap/env` and `reqwest/stream`) and does not prepare
every narrower affected graph. A verified source-mtime/cache-key hit alone
therefore does not establish fresh Cargo units. All-workspace preparation and
validation remain intact, as do the separate main and complete warm correctness
gates. Only successful preparation saves a ref-isolated
snapshot. This is not the routine fast gate or a latency pass. Remove/reapply the
label after a head update; no synchronize event automatically repeats preparation.
Concurrency belongs to the label-filtered job: unrelated label events cannot
cancel preparation; genuine same-PR preparation requests still supersede it.
Then rerun the ordinary CI job at the same PR merge SHA and verify actual Cargo
fresh flags, executed tests, matched key/ref and complete warm duration including
archive/cleanup under 180 seconds. Branch dispatch does not warm the PR merge ref.
An already-existing partial exact snapshot cannot be filled in: prepare the next
source-tree key before routine Rust validation, without deleting old entries.
The parent owns this hosted sequence and independent review; local tooling-only
checks do not prove it. Reversal removes this optional workflow/label trigger;
existing snapshots remain ref-scoped and expire normally, with no cache deletion,
release artifacts or product state migration. The main-only primer guard and
routine limits remain unchanged.

Main uses persistent targets; the separate
[Prime hosted Cargo caches](../.github/workflows/ci-cache.yml) workflow can prepare
one successful main fast snapshot for hosted PRs. It compiles fast units only
when the exact key is missing, runs no tests and is not a gate. Full qualification
retains persistent build lanes. A missing cache never relaxes validation or the
180-second warm feedback target.

## Cold preparation and representative latency

Dispatch **Prepare fast validation** on a dependency/toolchain upgrade branch
before landing it. Branch preparation uses hosted runners and a branch-specific
concurrency group, so it cannot occupy main's runner or scheduling slot. Dispatch
on `main` to prepare its trusted persistent cache. Main pushes preempt that
preparation; preparation cannot cancel a running main check.
Main rejects an entirely empty cache with an explicit
preparation error instead of spending its three-minute budget on native builds.
Preparation validates the complete fast selection, then validates the affected
selection using the same command as routine CI: Cargo can unify dependency
features differently for those roots. The optional `comparison-base` input pins
a full comparison SHA; otherwise preparation uses successful main history.
Cargo still validates dependencies and rebuilds changed units on every run.
The optional `stress` input repeats each family's encrypted HTTP/recovery smoke
50 times with fresh service state and no test retries. It remains preemptible
and does not publish release artifacts.

Fast coverage includes every bounded unit test and an encrypted HTTP smoke for
Receiver, Transparent and Enhance. The former named slow-unit exclusions have
been removed. Default fixtures keep complete failure/recovery assertions with
supported narrow inputs; explicit full runners enable `qualification-tests` to
restore deployed geometry combinations and larger history/publication runs.
Complete CPU correctness uses the same bounded native selection and retains its
reference, exporter, documentation and lint checks under the 180-second limit. Transparent checks a nonzero row against public source bytes, refuses
malformed input before preparation, and repeats after a cache restore and
corruption. Enhance verifies encrypted records after draining the old listener
and reopening durable coordinator state. Listener readiness/shutdown use bound
sockets and completion signals, without timing sleeps. Full suites retain the
larger publication, failover, geometry and independent-reference campaigns.

Wallet walk fixtures cache their immutable manifests, returning owned copies
for mutation cases. Full Transparent wallet suites also reuse verified public
runtime snapshots in a bounded, temporary directory owned by the test runner;
services, wallets and mutable stores remain fresh. Explicit cold/corruption
cases use their own cache directories. No client secrets are cached.

To assess the target using completed main runs:

```sh
python3 tools/ci/timings.py --workflow ci.yml --limit 100 --slo --since-run <first-updated-run-id>
```

The explicit starting run excludes older pipeline versions and their failures.
This needs at least 20 representative runs, coverage of all service families,
shared changes, operations and docs, and a p95 below 180 seconds. Workflow-only,
canceled, cold and unknown-cache samples cannot qualify the target. Failed runs
remain visible. Warm means Cargo reported no third-party compilation; workspace
source can still rebuild. Queueing and final job cleanup count. A missing sample
population is an unqualified target, not a measured guarantee.

## Prepared-artifact deployment

Qualify the required service on the desired main revision before dispatching
a deployment. The existing `ref` input still specifies a full SHA. Enhance still
requires current main; Transparent still accepts an ancestor of main.

Deploy workflows require a completed successful service release job for that exact SHA,
download its product bundle, verify the complete inventory, SHA and checksums,
restore executable permissions, then invoke the existing deployment scripts.
There is no deployment-time Cargo build or fallback compilation. Missing,
expired, mismatched or unqualified artifacts fail before activation. Retention is
14 days; dispatch service qualification if artifacts have expired.

`tested_locally` was removed from Transparent deployments: local tests cannot
substitute for a qualified downloadable release. Enhance `artifact_only=true`
now fetches and verifies the prepared bundle rather than compiling it. For
isolated qualification, download the bundle directly from the service-qualification run.

Publisher `shadow` consumes prepared binaries. Its `activate` and `rollback`
modes use installed/saved state and do not require a fresh artifact download.
The existing revision checks remain in effect. Historical revisions without these bundles cannot use the new download path;
create and qualify a new main revision carrying the desired code.

Compatible shard data and runtime caches remain reusable. Existing parallel
staging, unchanged-worker skips, canary/replica serving floors, router settling
and rollback remain in place. Cold startup and the 35-second replica-batch router
settle can still exceed the deployment target. Do not remove them to make the
clock green; measure warm routine rollouts before changing readiness behavior.

## Timing evidence and rollout

Fast and deployment workflows include queue/job/step timing summaries. The last
reporting step captures elapsed time through that step; use completed-run reports
to include final cleanup. Queue figures include dependencies and environment
waiting, not just runner scheduling. Reruns are measured from the current attempt
start, not the original workflow creation; the report includes its attempt number.

```sh
python3 tools/ci/timings.py --run <run-id>
python3 tools/ci/timings.py --run <run-id> --cache-records
python3 tools/ci/timings.py --workflow ci.yml --limit 20
python3 tools/ci/timings.py --workflow deploy-transparent-shard.yml --limit 20
```

`dispatch_to_job_start_seconds` is measured from the current attempt's start and
includes waiting for `needs`, concurrency groups and environments; it is not
pure runner queue time. Each job's `phases` groups its steps into setup (including
Rust setup and hosted cache restore), work, post steps (including cache save) and
reporting. With `--cache-records`, the tool also reads each job's sanitized
`CI_CACHE_IDENTITY`, `CI_CACHE_RESTORE`, `CI_CACHE_REPORT`, `CI_CARGO_ARTIFACTS`
and `CI_STAGE_REPORT` lines. They record the identity digest and key, restore
status (`miss`, `hit-current-ref`, `hit-main`, `primed`, or
`persistent-new`/`persistent-existing`), the refs that held the key before
restore, and setup and restore seconds (null when not recorded).

`CI_CARGO_ARTIFACTS` is the attribution of Cargo work. In CI, `tools/ci/stage.py`
adds `--message-format=json-diagnostic-rendered-ansi` to every Cargo
build/check/clippy/test command it runs, prints diagnostics and other output as
before, and logs each `compiler-artifact` message's package name, version,
source kind and `fresh` flag. A unit is a package target with its profile,
features and output file names (basenames only), so `cargo check` (`.rmeta`) and
`cargo build` (`.rlib`) of one target are separate units. A unit is compiled if
any command reported it not fresh. Units no command needed are not counted. Cargo commands outside
`stage.py` (for example `make` helper targets) are not attributed.
`fingerprint_inventory` in `CI_CACHE_REPORT` compares fingerprint directories at
restore and job end by content; it is an inventory, not attribution.

`CI_STAGE_REPORT` sums leaf stage wall time: `compile_stage_seconds` covers whole
compile/lint Cargo commands, including resolution, downloads, build scripts and
linking. Test stages may also build units no compile stage needed. A class with
no stage is null. Records contain no environment values, tokens or absolute paths.

Reused jobs in partial reruns have a null current-attempt queue time and an
explicit reuse flag. Total workflow and current-attempt elapsed times are separate.
The history command reports nearest-rank p95 for successful runs and retains
individual results. Separate warm/cold populations and deployment modes before
using this aggregate as an SLO. Failed runs remain visible in Actions and must
also be reviewed; a fast failure is not successful feedback or rollout.

After publishing these workflows, verify runner routing and warm each lane
with full selection. Record documentation, operations, leaf Rust,
shared-library and dependency-change runs. Rehearse deployment with prepared data
on isolated infrastructure, including artifact rejection, unhealthy replicas and
rollback. Publish achieved p95 only after collecting representative warm runs.
If branch protection is added, require **Fast checks** for feedback; full release
qualification is enforced independently by artifact provenance.

## Runner storage and validation launchers

Persistent cache setup marks the current generation before maintenance, retains
at least the two most recently used generations per lane, and skips generations
used in the last day or referenced by a readable process. Older inactive
generations and unopened runner diagnostic logs older than fourteen days can be
removed. Maintenance refuses to start expensive work with less than 10 GiB free;
it never clears a current target to make a check appear healthy.

For long remote validation, start a tracked hub job running
`python3 tools/ci/launch.py --receipt <outside-checkout-attempt.json> -- <command>`.
It requires a clean checkout, chooses the pinned Rust toolchain, checks native
prerequisites, sets `umask 022`, and writes the commit, configuration digest,
measured duration and exit status atomically. A changed source/configuration
invalidates success. Use `--no-rust` for operations-only commands. Each attempt
needs a new receipt path. Inspect the tracked job with short SSH checks; do not
run validation through an inspection connection.

The initial warm targets are under one minute of queueing, three minutes for
operations qualification and five minutes for Receiver. These are acceptance
targets, not measured guarantees. Enhance and Transparent need separate measured
fixture improvements to reduce their existing roughly 25-minute test runtimes.

### Canonical library integration tests

The service integration registry names workspace-owned executables only. Native
primitive, receiver-directory/PIR oracle, filter, txid-client, recovery-engine and
SQLite-store tests run in wallet-libraries CI for the exact revision pinned by
the service. They are not selected as Git dependency packages from this workspace.
The service retains its production-router smoke and HTTP suites, including the
independent library receiver client, plus the indexer's receiver-store tests.
Moving a package removes only its registry ownership here; its library assertions
and the service interoperability suites remain separate qualification requirements.
