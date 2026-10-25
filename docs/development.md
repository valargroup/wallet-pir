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
make check-package PACKAGE=pir-query-server FEATURES=transparent-history TEST_TARGET=wallet_sync TEST=a_choice OFFLINE=1
make check-full   # comprehensive local validation, not the default inner loop
```

Prerequisites: Rust 1.97.1 with rustfmt/clippy, Python 3.11+, Git, Clang/libclang,
protoc, jq, shellcheck, Node and make. Linux additionally needs the standard C/C++
build tools, pkg-config and OpenSSL development headers. macOS uses Xcode Command
Line Tools. `make doctor` runs the protoc that builds will use (`PROTOC` when set,
else the first on `PATH`) and imports the Python test suites' third-party modules
(`cryptography`); when a Homebrew protoc fails to load its libraries, set
`PROTOC` to a working one or repair it. `make doctor NETWORK=1` checks authenticated GitHub/dependency access;
it suppresses subprocess output and never prints tokens. Missing dependencies are
fixed explicitly, not by interactive repair in a test command.

The service workspace uses the published Common 4.0.0 release, with no Common
patch, and its root patch keeps the wallet-library family on one revision. The
dependency boundary check rejects older Common releases and copies resolved from
another source, including stale lockfile entries. The released frozen-client fixture retains its independent historical
dependencies for interoperability checks.

PIR primitives come from the role packages in wallet-libraries. Publishers use
`zakura-pir-publisher`, query evaluation uses `zakura-pir-query-server`, and
shared records use `zakura-pir-protocol`. Native preparation shared by the two
server roles uses `zakura-pir-server-core`. Probe clients and integration tests
use `zakura-pir-client`; the query libraries expose their local client helpers
only with the explicit `client` feature (or in unit tests).

The chain reader `zakura-server-chain` (`src/shared/pir-chain`) is the only
package that declares direct Common and node dependencies; `tools/ci/boundary.py`
lists each permitted edge and `tools/ci/chain_graph.py` checks its fresh
consumers. Fast feedback resolves those consumers and applies the graph rules.
The standalone tool also type-checks each one; that compile stays out of the
fast job. Fast feedback runs the Enhance and Transparent reader RPC targets.
Those targets stay named on every fast compile, including `testing-node`,
the RPC transport, and the pipeline. A diff that does not touch the reader
must not drop them: that toggles feature unification and rebuilds every
consumer. Receiver, status and RocksDB reader targets stay in full
qualification so a fast compile does not toggle those features either.
Each `zakura-server-chain` consumer selects its product (`transparent`, `enhance`,
`status` or `receiver`) with default features disabled. Test dependencies add
`testing` alongside that same product; fixture support alone does not enable an
adapter. Database readers additionally enable the existing `rocksdb-reader` feature.

Fast Rust checks run every library unit test, and every binary that declares one,
in `release-fast`, plus classified bounded integration targets. The compile names
a stable dev-dependency closure so a smaller affected set reuses the warm
dependency graph; only affected packages run. Transparent publication tests
stay in that closure. History and detail reach the query host as explicit
features of that command. The publisher host binary does not enable them:
those features link the same library again. The query host's dev-dependency
does not force those roles, so the Enhance and Status harness can leave them
off. Publisher fixtures stay off this command. The publisher and query hosts
stay on that command for every diff. The in-process Enhance hint target stays
on a second command with the query host's Enhance and Status roles only. That
command names the same roots and the stable features other than history and
detail, so shared crates stay warm and one rustc does not build both stacks.
The stable feature set does not change with the diff. Moving the hosts onto
another command rebuilds the closure. Publisher chain-node fixtures stay in
qualification, as do the reader's receiver, status, and RocksDB targets. A
binary with no unit tests is left out of that compile; qualification still
builds every binary.
There are no named slow-unit exclusions.
Required CLI features are enabled for binary unit targets. Ordinary cache,
identity and fault matrices exercise one native column instance with the real
ring parameters. These component fixtures are not advertised publication
geometries. Independent history oracles retain the 16-bit native profile, and
HTTP checks retain registered publication geometries and real history rows. Publisher tests share immutable
producer bytes and generated certificate material; writable state, listeners,
trust checks and fault controls remain fresh. A query reused within one recovery
scenario is first decoded against producer bytes, and every later check still
sends it over HTTP and compares the complete answer. Large unit executables
use up to three disjoint groups of exact discovered case names inside the same
three-process limit; each group keeps fixture reuse and serial case deadlines.
Status cases stay together in one process and share prepared material
and exported object bytes across publication identities. Each publisher still
has its own object map, and each consumer downloads into its own writable cache.
On the 8-core fast runner that process is one of the three pool programs.
Smaller hosts run it alone. Splitting those cases copies their large native
fixtures and can exhaust the runner's memory limit. On the 8-core fast runner
the hint oracle and Enhance publication library lead the other light programs
beside the one memory-heavy program; below eight CPUs they still run alone.
Receiver's encrypted HTTP smoke and focused storage, witness and cache-budget
units stay in fast feedback.
Full publication reload/restart, staged loading and witness-finality campaigns
use `qualification-tests`; adjacent witness reorg checks share one history
through provisional and completed witnesses there.
HTTP integrations and the small-runner publisher protection remain intact.

`make check-correctness` runs that complete bounded unit/integration population
with native arithmetic, the independent reference checks, exporter contracts,
doctests, formatting, lint and Python helpers. Its 300-second deadline includes
warm compilation, fixture construction, execution and cleanup. Every CPU case
retains its 30-second deadline. `--record-only` records overruns without omitting
cases and never establishes an enforced timing pass.

Explicit full commands (`make test`, `make check-full`, full package families and
the qualification matrix) enable the declared `qualification-tests` features.
These restore the original deployed geometry combinations, mixed-size archive
preparation, full publication sequence, longer display histories and the larger
Enhance multi-unit failure fixture and 16K monolithic comparison, and Receiver's served
sealing campaigns. Repeated
campaigns that rebuild one runtime only to assert an additional property stay
behind that feature; fast feedback keeps a single case for each property.
Ordinary
unequal-unit checks use a supported 8K composed tail and independently assemble
the whole table, including its zero-filled gap. Cold and cached preparation
failures stay separate only when their retries match different direct raw-row
column sums. Fast keeps the cold retry; qualification repeats the cached retry
against those same sums, and repeats hint-layout, shipped-load and failed-update
campaigns that rebuild a runtime already covered by a fast case. The same contracts run in both configurations; full integration campaigns and
released-client checks remain classified separately. Fast builds reject this
feature if it leaks into their actual executable artifacts. Use
`make check-package PACKAGE=<name> FEATURES=qualification-tests TEST=<filter>`
to diagnose one expanded unit case. Custom FEATURES do not select default
integration targets. Full package families are validated against every workspace
member in `tools/ci/full-packages.json`; new or stale assignments fail. Real HTTP,
crypto, fault injection, restart and deployment qualification remain required.

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
separately from main CI. The optional `correctness` input first diagnoses the
complete CPU run (including cold native/reference and lint preparation) and
requires three enforced warm runs on that same source. It also repeats complete
fast feedback three times with its 300-second command deadline. This explicit
preparation rebuilds the changed test crates once, retaining dependency caches;
that cold rebuild is separate from the enforced warm measurements. Their per-case, stage
and source receipts are retained as an attempt-specific Actions artifact; cold
diagnostics never count as a five-minute pass. A main push preempts fast preparation.

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
CUDA checks have their own cache lane. CUDA release caches are additionally
separate by the Ubuntu 22.04 ABI, pinned compiler and CPU flags.
Caches cannot cross from PR code into trusted release users.

CPU lint enumerates every non-CUDA feature instead of enabling `--all-features`.
The optional `pir-cuda` library owns CUDA dependencies, and boundary checks reject
other features that enable CUDA indirectly. CUDA has a separate affected-change
selector and workflow; ordinary full and nightly CPU checks do not imply CUDA.
Explicit qualification uses `ci-qualify.yml` with `include-cuda=true`. A revision
without CUDA qualification has no deployable CUDA artifact. See
[CUDA validation](enhance/cuda_workers.md#validation) for triggers and commands.

Use the installed `misc-create-pr` skill to publish and monitor requested PRs,
and `roman-dev-ux` for slow-command observations and detached local checks.
Run durable validation on a committed, clean snapshot. Invoke the helper with
`python3 tools/ci/snapshot_check.py -- <check command>` so dirty edits as well as
head changes invalidate completion. Monitor records preserve ownership and
results; automatic agent wake requires a verified runtime mechanism. Avoid
foreground polling and duplicate broad local/CI checks.

For Transparent PIR runtime or artifact changes, start with
`make check-package PACKAGE=transparent-shared`. For HTTP handling and
worker control, use `make check-package PACKAGE=pir-query-server
FEATURES=transparent-history,transparent-detail`; the query server's `transparent`
module serves both roles, and its default features are empty. The host binary is
`pir-query-server`, and the worker administration tools are in `transparent-tools`.
These packages do not pull in the publisher's chain reader or RocksDB, including
when compiling their tests. Publisher-to-worker shipping tests live in the
tools package; select them with
`make check-package PACKAGE=transparent-tools FEATURES=publisher-tools TEST_TARGET=txid_shipped_runtimes`.
The existing `make transparent-txid-demo` command still exercises confirmed raw
blocks through extraction, publication and encrypted retrieval. Add `OFFLINE=1`
once dependencies are fetched. Finish a change with `make check-fast BASE=<sha>`
and `make check-hygiene BASE=<sha>`; manifest edits broaden affected checks.

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

Temporarily, CI lets PR and main validation run to completion without a
three-minute cancellation. Pushes to main enforce that target in a final timing
check; PRs and manual runs only report timings. The check measures job start
through validation and reporting, excluding queue time and final cleanup.
A warm run, which recompiled no workspace crate, still has to finish in three
minutes. A run that relinked workspace crates and no third-party crate may
take that long plus the release-fast compile; a compile over five minutes
still fails.

Direct node database readers in the publisher and tools require
`--features rocksdb-reader`: `pir-publisher ingest-events`, `shard-cutoff`,
`journal-inventory` and `compact-export`. Ordinary RPC-based server builds omit
that feature and do not compile RocksDB. Full qualification and the workflows
that build the database-reader tools enable it explicitly.

`transparent-journal` owns the saved event journal and its display sidecars.
Journal consumers such as `transparent-measure` depend on it directly, so they
do not compile the publisher, node parser or database reader. `transparent-measure`,
`shard-assign` and `parent-evaluation-server` link the query server through the
`query-host` feature; fast diagnostic binaries leave that feature off.

## Publisher source layout

The publisher host lives at `src/pir-publisher` and the query-server host at
`src/pir-query-server`. Each product publisher lives beside that product's
query server. `src/shared` holds shared libraries, including `pir-pipeline`.
Cargo package names and standalone commands are preserved. See the
[unified publisher](publisher.md) for configuration and service ownership.

## Shared HTTPS synchronization

`pir-sync-manager` is the internal HTTPS transport and discovery library under
`src/shared/`; it accounts all consumed response bytes against one attempt
budget. The shared `pir-pipeline` consumer polling module
schedules complete passes sequentially with bounded backoff; Enhance,
Transparent history, transaction-detail and Status supply its product tasks.
Neither grants serving authority by itself. The transport-neutral `ConsumerEngine` runs the shared publication lifecycle with the HTTPS envelope adapter in `pir-sync-manager` and the transport-free
`pir-publication` contract (object references, pointer succession, durable
floors, object cache and publisher object store); Status is its first adopter,
and Enhance composes its existing lease and replay protocol with the same
consumer engine. Product code retains schemas, directory locks, validation,
preparation and activation. Transparent History and Detail use the same consumer engine and guarded owner
activation. Their bulk artifacts carry length-only receipts until the existing
background validator hashes them after serving starts; disk reload checks size.
Receiver consumes HTTPS publication envelopes through the same adapter and
consumer engine. Its lazy payment, witness and filter loaders retain checked open
files, while the root publisher retains the independent canonicality guard and
correction checks. Local publisher table reads use the shared bounded reader
without adding a digest pass. Status retains its own observation deadlines.

Every product publishes through `PublisherEngine`, including Receiver's live
collection and metadata-only audit refreshes. Product preparation can store one
shard at a time to preserve its memory budget. The shared engine orders typed
preparation, source rechecks and guarded commits; there is no legacy publication
pass or alternate publisher polling loop. Shared consumer reservations distinguish
incoming candidates from the acknowledged serving view. Roles without peers use
an explicit no-peer reservation service.
