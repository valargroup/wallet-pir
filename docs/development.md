# Development workflow

Use `make check-fast BASE=origin/main` for affected feedback. One planner selects
both local checks and full PR job families. Local selection includes commits
since BASE plus staged, unstaged and untracked files. CI uses committed changes
only. Without BASE, local selection checks the working changes; use `python3
tools/ci/fast.py --all` to request all fast checks. Every invocation prints its
selected checks and reasons. Unknown paths, dependencies and build configuration
broaden coverage. Package-owned fixtures also select their package. CI orchestration
edits run helper checks without recompiling unchanged Rust in the fast gate;
full service qualification still broadens to every family.

```sh
make doctor
make prepare-dev  # explicit, one-time fetch of pinned dependencies
make check-fast BASE=origin/main OFFLINE=1
make check-package PACKAGE=pir-apm TEST=dashboard FEATURES= OFFLINE=1
make check-package PACKAGE=transparent-shard-server TEST_TARGET=wallet_sync TEST=a_choice OFFLINE=1
make check-full   # comprehensive local validation, not the default inner loop
```

Prerequisites: Rust 1.97.1 with rustfmt/clippy, Python 3.11+, Git, Clang/libclang,
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

Only fast checks run automatically for pushes and PRs. Full regression runs
nightly or on explicit dispatch, and service qualification is dispatch-only.
Qualification runs common helper, shared and infrastructure gates once, then
qualifies each requested service independently. A failed service blocks its own
artifacts. `Full checks complete` rejects failures or unexpected skips. Release
and hardware qualification remain separate from fast feedback.

Before landing a dependency/toolchain upgrade, dispatch **Prepare fast
validation** on its branch. Cold preparation and hosted cache priming run
separately from main CI. A main push preempts fast preparation.

## Keep changes focused

Each change has one outcome. Include only files that implement it, test it,
document it, or preserve essential evidence. Split independently useful
refactors, operations features, benchmarks and unrelated fixes. Before pushing,
inspect additions by area and remove files with no direct role in the outcome.
Update existing documentation instead of adding task narratives or duplicate plans.
Review findings, self-review verdicts and task summaries belong in chat or the PR.

Run `make check-hygiene BASE=origin/main` for an inexpensive artifact check;
`python3 tools/ci/hygiene.py --all` audits the current tree. The same check runs
before builds in affected validation and CI, and before branch pushes through
the guard installed by `make prepare-dev`. Existing custom hooks are preserved;
if setup reports a custom hook, compose its invocation of
`tools/ci/main_health.py --pre-push` as directed. That entry point checks hygiene
on every pushed branch before checking main's CI health.

The checker blocks captured logs, run dumps, exploratory scripts in evidence,
recognizable agent reports and undeclared data files. JSON configuration,
consumed fixtures and essential proofs have bounded locations and reasons in
`tools/ci/hygiene.toml`. Add an allowance only with its consumer or retained claim;
never allow a whole product or every JSON file. It does not identify prose by
its author or decide whether independent features belong together.

Changes exceeding 1,500 added text lines, 30 added/modified files, or a 1 MiB
data file produce warnings, not failures. Pure deletions and unchanged renames
add no review content to these budgets. The grouped summary is a prompt to
split unrelated work; there is no universal line limit or extra approval gate.

Use ignored `.local-pir/runs/<product>/<run-id>/` for local run output and
`.local-pir/scratch/` for exploratory scripts. Temporary CI output stays in CI
artifacts. Promote reusable tools with tests, and promote only essential evidence
under the [retention policy](../evidence/README.md). An expiring artifact is not
a durable acceptance record. Local report defaults stay ignored; explicit output
options remain available. Production runtime paths are independent of these
local development defaults.

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
three minutes; deployment's prepared-artifact target remains two minutes. These
are targets until measured on the updated workflow.
