# Development workflow

Use `make check-fast BASE=origin/main` for affected feedback. One planner selects
both local checks and full PR job families. Local selection includes commits
since BASE plus staged, unstaged and untracked files. CI uses committed changes
only. Without BASE, local selection checks the working changes; use `python3
tools/ci/fast.py --all` to request all fast checks. Every invocation prints its
selected checks and reasons. Unknown paths, dependencies and build configuration
broaden coverage. Package-owned fixtures also select their package.

```sh
make doctor
make prepare-dev  # explicit, one-time fetch of pinned dependencies
make check-fast BASE=origin/main OFFLINE=1
make check-package PACKAGE=pir-apm TEST=dashboard FEATURES= OFFLINE=1
make check-package PACKAGE=transparent-shard-server TEST_TARGET=wallet_sync TEST=a_choice OFFLINE=1
make check-full   # comprehensive local validation, not the default inner loop
```

Prerequisites: Rust 1.91.0 with rustfmt/clippy, Python 3.11+, Git, Clang/libclang,
protoc, jq, shellcheck, Node and make. Linux additionally needs the standard C/C++
build tools, pkg-config and OpenSSL development headers. macOS uses Xcode Command
Line Tools. `make doctor NETWORK=1` checks authenticated GitHub/dependency access;
it suppresses subprocess output and never prints tokens. Missing dependencies are
fixed explicitly, not by interactive repair in a test command.

Fast Rust checks run library and binary unit tests in `release-fast`, plus the
classified Enhance fast integration targets. Required CLI features are enabled
for binary unit targets; protocol and hardware modes remain explicit choices. Package-qualified slow names are
validated against executable test discovery before skips apply; removed/renamed
entries fail. Full tests retain these cases. An explicit TEST filter can run a slow case; custom
FEATURES select unit targets with those features and do not add default-feature
integration targets. Full package families are validated against every workspace
member in `tools/ci/full-packages.json`; new or stale assignments fail. Real HTTP, crypto, fault injection,
restart and deployment qualification remain distinct from short model tests.

Fast scaler feedback runs decision, forecast, signal, daemon and metrics unit
tests. Seeded fleet simulations and exhaustive membership/mutant checks remain
in full operations qualification; their module registry rejects missing or
renamed assignments. No production clocks or thresholds change.

Full PR jobs select affected product/shared/operations/infrastructure families.
The same helper selection is used for full PR checks: documentation runs only
link validation, and operations run their affected consumers. Main selects every
helper and restores exhaustive scaler tiers. Shared library changes expand
through reverse dependencies, while unknown paths
request complete coverage. `Full checks complete` verifies that every selected
job succeeded and unrelated jobs were skipped. On main, every family must pass
before release preparation can publish artifacts. Test builds use `release-fast`;
deployment binaries retain fat LTO and native/q48/CUDA isolation.

## Build and monitor ownership

`check-fast` and `check-package` automatically lease reusable local target lanes
under `target/dev-lanes/<compatibility>/lane-N`. A local `CARGO_TARGET_DIR`
override sets the pool root. The first free lane is reused; a busy lane is skipped
using a nonblocking OS lock. Toolchain, Cargo configuration and build flags
partition compatible pools; feature, profile and dependency changes retain
Cargo's own fingerprint validation. Documentation-only checks need no lease or
compiler. Leases cover compilation, test discovery, execution and nested Enhance
checks. Child processes inherit the lease, so killing the wrapper cannot release
a lane while Cargo or its children remain alive. Lease files must never be
deleted to force reuse. `TARGET_LEASE` output identifies the chosen directory.

Raw Cargo commands and other make targets still require separately owned targets;
they do not participate in these wrapper leases. Registry/download locks are
shared across lanes; `OFFLINE=1` avoids downloads but does not eliminate every
registry-cache lock. GitHub Actions keeps its own target/cache lanes; see
[Cargo cache identity and reuse](ci-performance.md#cargo-cache-identity-and-reuse).
Trusted CI release-native and q48 targets are separate lanes.
CUDA caches are separate by the Ubuntu 22.04 ABI, pinned compiler and CPU flags.
Caches cannot cross from PR code into trusted release users.

Use the installed `misc-create-pr` skill to publish and monitor requested PRs,
and `roman-dev-ux` for slow-command observations and detached local checks.
Run durable validation on a committed, clean snapshot. Invoke the helper with
`python3 tools/ci/snapshot_check.py -- <check command>` so dirty edits as well as
head changes invalidate completion. Monitor records preserve ownership and
results; automatic agent wake requires a verified runtime mechanism. Avoid
foreground polling and duplicate broad local/CI checks.

## Timing evidence

`CHECK_STAGE` records identify helpers, formatting, compilation and test phases.
Actions reports label jobs reused from earlier attempts and leave their current
queue duration null. Total workflow elapsed time and current-attempt time are
reported separately; dependency/environment waiting is not runner queue time.

Measure an explicit population on a stable committed checkout:

```sh
python3 tools/ci/measure.py --category docs --population warm --runs 20 \
  --output /tmp/wallet-pir-docs-warm.json -- make check-docs
```

Categories are docs, ops, leaf, shared, dependency and artifact. Cold/warm labels
are operator assertions, not inferred from speed. The recorder never purges a
cache. `TEST_TARGET` selects a named integration executable; `TEST` filters test
names in that executable (or unit targets when TEST_TARGET is omitted). It reports p95 only after 20 successful, unchanged-snapshot runs and keeps
failures visible. Compare the same workload, host, flags and population before
and after; local execution excludes Actions queue/setup. The warm CI target is
30 seconds; deployment's prepared-artifact target remains two minutes. These
are targets until measured on the updated workflow.
