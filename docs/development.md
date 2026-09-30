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
make check-full   # comprehensive local validation, not the default inner loop
```

Prerequisites: Rust 1.91.0 with rustfmt/clippy, Python 3.11+, Git, Clang/libclang,
protoc, jq, shellcheck, Node and make. Linux additionally needs the standard C/C++
build tools, pkg-config and OpenSSL development headers. macOS uses Xcode Command
Line Tools. `make doctor NETWORK=1` checks authenticated GitHub/dependency access;
it suppresses subprocess output and never prints tokens. Missing dependencies are
fixed explicitly, not by interactive repair in a test command.

Fast Rust checks run library and binary unit tests in `release-fast`, plus the
classified Enhance fast integration targets. Package-qualified slow names are
validated against executable test discovery before skips apply; removed/renamed
entries fail. Full tests retain these cases. Real HTTP, crypto, fault injection,
restart and deployment qualification remain distinct from short model tests.

Full PR jobs select affected product/shared/operations/infrastructure families.
Shared library changes expand through reverse dependencies, while unknown paths
request complete coverage. `Full checks complete` verifies that every selected
job succeeded and unrelated jobs were skipped. On main, every family must pass
before release preparation can publish artifacts. Test builds use `release-fast`;
deployment binaries retain fat LTO and native/q48/CUDA isolation.

## Build and monitor ownership

Sequential compatible commands reuse the worktree's target. Concurrent writers
use separate worktrees or persistent target lanes; do not create a cold directory
for each command. Trusted CI release-native and q48 targets are separate lanes.
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
cache. It reports p95 only after 20 successful, unchanged-snapshot runs and keeps
failures visible. Compare the same workload, host, flags and population before
and after; local execution excludes Actions queue/setup. The warm CI target is
30 seconds; deployment's prepared-artifact target remains two minutes. These
are targets until measured on the updated workflow.
