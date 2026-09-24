# CI and deployment latency

The target is **30 seconds from workflow creation to fast checks completing**
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
Operations-only changes run helper checks without compiling Rust.

Rust feedback runs library unit tests in `release-fast`. Six full-geometry/cache, RPC publication
and real HTTP retry tests embedded in library targets run only in full CI;
`tools/ci/slow-tests.json` records the exact names and reasons. Packages with only
binaries get `cargo check --bins`. When the Enhance server is selected, fast CI also
runs the integration targets classified as `fast` in
[`tools/ci/enhance-tests.json`](../tools/ci/enhance-tests.json). The remaining
integration targets, doctests, binary unit tests and all-feature lint run in
**CI full**, including on PRs. This avoids compiling every integration target just
to exclude its tests at runtime. Helper
checks run concurrently with Rust; independent operations suites also run in
four parallel make slots, and failures in any task fail fast CI.

Every Enhance server integration target must be classified exactly once.
`make check-tools` validates the registry against Cargo metadata, rejecting new
unclassified targets and stale entries. Full CI runs both tiers serially, including
the real-crypto `packing_http` path; it clears `QUALIFY_*` overrides so that CI uses
the bounded default fixture. Existing ignored hardware/GPU cases remain explicit
manual checks. The boundary recovery target is a reference model, not evidence that
the implementation follows every modeled transition.

CI full on main runs both Transparent and Enhance suites and offline deployment
acceptance checks. After all pass, it builds optimized release binaries and uploads
four checksummed bundles tied to that SHA. A green fast run is never release
qualification. Full qualification of an older main commit is not cancelled by a
newer main push. No tests were marked ignored or removed.

To reproduce selection without running tests:

```sh
python3 tools/ci/fast.py --base <base-sha> --select-only
```

To run the fast gate locally, omit `--select-only`. Omit `--base` for all packages.
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
| `WALLET_PIR_FAST_RUNNER` | `["self-hosted","Linux","X64","wallet-pir-fast"]` |
| `WALLET_PIR_BUILD_RUNNER` | `["self-hosted","Linux","X64","wallet-pir-build"]` |

Use separate Unix users and 0700 homes for the two services:
`wallet-pir-fast` and `wallet-pir-build`. Neither has sudo or a deployment SSH key.
Fast runners execute same-repository PR code and have no production access or
access to the build user's release caches or runner credentials. Fork PRs always use disposable
hosted runners. Full CI on PRs also uses hosted runners; only main builds use the
trusted persistent build pool. The fast service has CPU/I/O weight 1000 and a 6-GiB memory ceiling; the build
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
per profile instead of once per product. Full tests run two test cases at a time
(`RUST_TEST_THREADS=2`): the simulation fixtures each start multithreaded services,
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
native build tools, and Rust **1.91.0** with rustfmt and clippy. The setup action
checks the pinned tools and OS instead of reinstalling them each run.

Persistent Cargo output lives outside checkout at
`~/.cache/wallet-pir/<runner-name>/<lane>/<compiler-flags-hash>/`. Fast, lint, full
test and release lanes cannot contend for the same Cargo target lock. Cargo
tracks source, lockfile, features and profile changes; changes to compilation
flags also get separate directories. Prune unused lane directories only while
the runner is idle. Do not share writable caches between trust boundaries.

## Prepared-artifact deployment

Run CI full on the desired main revision and wait for success before dispatching
a deployment. The existing `ref` input still specifies a full SHA. Enhance still
requires current main; Transparent still accepts an ancestor of main.

Deploy workflows locate a successful main **CI full** run for that exact SHA,
download its product bundle, verify the complete inventory, SHA and checksums,
restore executable permissions, then invoke the existing deployment scripts.
There is no deployment-time Cargo build or fallback compilation. Missing,
expired, mismatched or unqualified artifacts fail before activation. Retention is
14 days; rerun full CI for that revision if artifacts have expired.

`tested_locally` was removed from Transparent deployments: local tests cannot
substitute for a qualified downloadable release. Enhance `artifact_only=true`
now fetches and verifies the prepared bundle rather than compiling it. For
isolated qualification, download the bundle directly from the full CI run.

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
python3 tools/ci/timings.py --workflow ci.yml --limit 20
python3 tools/ci/timings.py --workflow deploy-transparent-shard.yml --limit 20
```

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
