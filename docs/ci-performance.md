# CI and deployment latency

The target is **three minutes from main push to fast checks completing**
for routine changes on warm runners, and **two minutes from deployment dispatch
to healthy service** when an exact-SHA qualified artifact is already available.
Cold builds, dependency/toolchain changes, initial data preparation, schema
migrations and first fleet activation are measured separately. These are targets,
not measured guarantees; hosted fallback and cold fleet warming can exceed them.

## Fast feedback and full qualification

`CI / Fast checks` runs on pushes and PRs. It selects changed workspace packages
and their transitive reverse dependencies. Deleted files are included; build
configuration, dependency manifests and unknown paths select the whole workspace.
Documentation-only changes run documentation checks without installing Rust.
Operations-only changes run helper checks without compiling Rust. Main compares against
the last successful main fast run, so canceled or failed runs cannot leave unchecked changes behind.

Rust feedback compiles sequentially in `release-fast`, then CI executes at most
two Cargo-reported unit test programs concurrently. Cases within each program stay
serial, preserving process-local immutable public fixtures within the fast runner’s
6-GiB ceiling. Local focused checks default to one program at a time. The package-qualified full-geometry/cache, multi-unit preparation, RPC publication
and real HTTP retry cases run only in full CI;
`tools/ci/slow-tests.json` records the exact names and reasons. Binary unit tests run as well as library targets. For affected packages, fast CI also
runs the integration targets classified as `fast` in
[`tools/ci/integration-tests.json`](../tools/ci/integration-tests.json). The remaining
integration targets, doctests, binary unit tests and all-feature lint run in
**CI full**, nightly or on request. The aggregate completion check fails if a
selected job fails or is skipped. This avoids compiling every integration target just
to exclude its tests at runtime. Affected helper
checks run concurrently with Rust; selected operations suites run in
four parallel make slots, and failures in any task fail fast CI.

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
checks. Common helper, shared-library and infrastructure checks run once per qualification, before service-specific native work. Helpers run on hosted runners to reserve the fast queue for main. Operations archives
assemble on hosted runners without Rust, so they never queue behind native tests. Candidate release builds may run
alongside testing in separate target lanes; candidate archives are uploaded under
an ineligible name and re-verified before qualified publication. Each family
publishes immediately after its required checks pass. Failure in another family
cannot authorize or prevent that publication. CPU artifacts do not wait for CUDA.

Only **CI / Fast checks** runs automatically on main pushes and PRs. The main
job has a three-minute execution ceiling; queueing also counts toward the latency
target. A timeout is a failure, never evidence of successful fast validation.
The pre-push health guard follows this workflow, rather than long qualification.

Whole-repository **CI full** runs nightly at 04:17 UTC and on explicit dispatch.
**Qualify services** is dispatch-only and pins its main revision. It retains
full service suites and exact-SHA artifact checks; a green fast gate cannot
replace it. Hosted cache priming runs nightly at 03:37 UTC or on request, without
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
The single build slot serializes full-CI jobs. Enhance and Transparent share
its `full-test` and `full-lint` lanes so common dependencies are compiled once
per profile instead of once per product. Transparent full tests run two test cases at a time
(`RUST_TEST_THREADS=2`), while Enhance tests run serially: the simulation fixtures each start multithreaded services,
and eight simultaneous fixtures can trigger memory admission overloads inside
the build service’s 9-GiB limit. Each test retains its internal concurrency. `tools/ci/full-test.sh` compiles
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

Hosted jobs restore one exact key, `v1-wallet-pir-<lane>-<scope>-<identity>-Linux-x64`,
with no partial fallback to another identity. Lanes, scopes, CPU, native CPU and
Ubuntu 22.04 CUDA builds therefore never share entries. GitHub scopes a PR's saves
to its merge ref; a PR restores its own entry first and otherwise main's. Main
never reads PR entries. Because main uses persistent targets, the separate
[Prime hosted Cargo caches](../.github/workflows/ci-cache.yml) workflow warms
one fast feedback entry from main for hosted PRs. It looks up the exact key and
compiles fast units only when missing. It runs no tests and is not a gate.
Full qualification uses persistent build lanes; its former six hosted PR cache
primers are no longer automatic work. Explicit branch full runs may build cold.

A restored cache can only save work. Cargo still checks every unit; a fresh
checkout's workspace sources are newer than cached outputs, so workspace crates
rebuild on hosted runners, while unchanged third-party units can be fresh.
A missing cache is a separately measured cold build; it does not relax main validation or its deadline.

## Cold preparation and representative latency

Dispatch **Prepare fast validation** on a dependency/toolchain upgrade branch
before landing it. It compiles and validates the same fast units using the fast
runner's cache. Main pushes preempt preparation; preparation cannot cancel a
running main check. Main rejects an entirely empty cache with an explicit
preparation error instead of spending its three-minute budget on native builds.
Cargo still validates dependencies and rebuilds changed units on every run.
The optional `stress` input repeats each family's encrypted HTTP/recovery smoke
50 times with fresh service state and no test retries. It remains preemptible
and does not publish release artifacts.

Fast coverage includes an encrypted HTTP smoke for Receiver, Transparent and
Enhance. Transparent checks a nonzero row against public source bytes, refuses
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
