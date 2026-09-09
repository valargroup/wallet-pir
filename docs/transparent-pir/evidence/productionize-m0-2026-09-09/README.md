# M0 — Wallet and deployment baseline

Captured 2026-09-09. Responsible execution role: Codex, acting as integration
engineer and read-only operator for Roman. This record closes baseline discovery,
not fleet acceptance or release readiness. See the [execution plan](../../productionize-plan.md).

## Baseline and preservation

The integration baseline is `zakura-core/wallet-libraries` commit
`2c7cee52caff03af52e21903fa44ac972659d787`, plus the captured working-tree patch
and one untracked source file, `zakura/wallet-app/example/lib/screens/heights.dart`.
M0 did not commit, reset, edit or reformat the user's staged or unstaged changes. `.vscode/settings.json` is excluded as unrelated editor configuration.
The [source inventory](wallet-source-baseline.json) records exact hashes for all
508 tracked files, the extra source file, Git index/working diffs and the base
archive. The patch is [preserved separately](wallet-working.patch.gz).

Tests ran on `/tmp/transparent-m0-20260909/wallet-source`, with diagnostic probes
added only to that copy. A second independent reconstruction verified every
baseline source hash. Cargo reused the original checkout's build cache, not its
source files or wallet databases. No existing application wallet was opened.

During execution, wallet HEAD independently advanced to `8267008b4` (forget-wallet
changes). All captured source bytes still matched at the final preservation check;
the old HEAD plus patch remains a valid exact reconstruction. Index/unstaged hashes
changed with that concurrent commit. See [preservation-check.json](preservation-check.json).

The PIR dependency pin resolves to
`22e6becacdd0107683cda7280c5585fac9f4f4b2`; the current PIR repository code baseline
is `14f3919a86fab1c2be44877026a9d50270f70e88`. The wallet implements `PirStore`
over its own `WalletDb`; it does not use `transparent-wallet-store` for persistence.
Consequently updating a client pin alone does not apply the reference store's
`1d95ebf` append correction to the wallet database.

## Capability matrix

Paths below are relative to the captured wallet repository. "Locally tested"
means native tests or model/widget tests as specified, not a live application
recovery or a release build. Prior live evidence is linked separately.

| Capability | Current implementation / entrypoint | M0 verification | Disposition |
|---|---|---|---|
| Native launch and bindings | `zakura/wallet-app/example/lib/main.dart` → `NativeBindings.load()` → generated bridge → facade | Source traced; example tests use test bindings | Implemented, but beta must refuse native-load failure instead of falling back to demo |
| Endpoint configuration | `ZAKURA_TRANSPARENT_FILTERS` and `ZAKURA_TRANSPARENT_SHARDS`; lightwalletd defaults to `us.zec.stardust.rest:443` | Source traced; public maps and independent anchor checked | Explicit paired configuration exists; production configuration acceptance remains M2 |
| Recovery engine | Facade `sync.rs` attaches `TransparentPir`; engine runs transparent recovery before enhancement | Native adapter recovery tests | Implemented and locally tested; whole-wallet incremental timing unmeasured |
| Accepted target and reorg | `wallet-transparent/src/source.rs` obtains target from scanned local chain; `chain.rs` supplies accepted hashes | Native recovery/store contract tests | Implemented and locally tested; broad live lifecycle gate remains M3 |
| Script derivation/discovery | `scripts.rs`, `source.rs` and wallet address maintenance extend actual account windows | Native recovery cases include moving gap window and unsupported script | Implemented; full application matrix remains M3 |
| Persistence and layout | `PirStore::commit_shard` delegates to `WalletDb::commit_transparent_shard`; layout 4 | Store contracts, direct checkpoint probe and layout rejection test | Atomic persistence exists; quadratic checkpoint rewrite remains |
| Balance with coverage | Facade `query.rs::balance_with_coverage` uses a single SQLite snapshot; bridge/client/provider expose it | Native facade and Dart/state tests | Implemented; balance display semantics need M2 correction below |
| Incomplete UI state | `TransparentCoverage.synchronized` is strict, but `statusAgainst` suppresses incomplete reasons within 10 blocks of the tip | Targeted Dart diagnostic reproduces query-budget, chain-unknown and sync-in-progress suppression | Core completion remains false; M2 must preserve explicit incomplete reasons in presentation |
| Restart/cancellation | Durable files and facade start/stop synchronization are present | Native facade persistence and state tests; no mid-PIR process-kill test in M0 | Implemented; interruption latency and crash matrix remain M2/M3 |
| Profile isolation | Example uses `$HOME/.zakura-example`, with temporary-directory fallback if HOME is unavailable | Source traced; no real wallet files opened | Separate beta namespace is missing |
| Shadow versus opt-in mode | Development demo flag exists; configured native recovery attaches PIR directly | Source traced | Explicit shadow/opt-in policy and independent comparison store are missing |
| Sending | Send screen generates a fresh phrase for confirmation; facade still exposes send | Source traced; existing send-model tests are not custody acceptance | Placeholder flow; disable UI and native send for recovery beta |
| History and destructive UI | Native history/provider/UI wiring and newly added forget/height screens | Facade persistence tests and example/state tests | Present; existing profiles must remain outside beta operations |
| Real-user recovery / signed release | Prior adapter live fixture evidence exists | Not exercised in M0 | Do not claim a particular user's balance or a new signed macOS release |

Prior live adapter validation is in
[hardening evidence](../hardening-2026-09-08/README.md). Those historical fixture
results are not fresh validation of this captured uncommitted application baseline.

## Measured persistence finding

[coverage-probe.rs](coverage-probe.rs) calls the actual wallet database commit path
used by `PirStore`, appending one empty-event checkpoint after N existing
checkpoints for one script. It uses SQLite `total_changes()` around only the final
commit. Account creation and preparation are outside the counter delta.

| Prior checkpoints | SQL row changes for one append | Resulting coverage rows |
|---:|---:|---:|
| 0 | 2 | 1 |
| 10 | 22 | 11 |
| 100 | 202 | 101 |
| 1,000 | 2,002 | 1,001 |

The implementation reads all prior ranges, deletes all coverage for that script,
and reinserts the retained ranges. The observed final-append work grows as
`2N + 2`; a sequence of appends therefore has quadratic row-write growth. This is
a row-change measurement on a synthetic local database, not filesystem bytes,
a full recovery benchmark or evidence that ledger results are wrong. Fix the
wallet's own store in M2 and preserve replacement/replay/reorg contracts.

[status-probe.dart](status-probe.dart) records a separate presentation issue:
with coverage 100 and tip 105, incomplete reasons produce only "Transparent
coverage through block 100" even though `synchronized` is false. This does not
change core completion but can conceal the reason the wallet is incomplete.

## Validation result

All final commands passed: 18 native facade tests, 25 recovery tests, 3 adapter
store-contract tests, 15 database tests (including the checkpoint probe), 47 Dart
client tests (including the status probe), 38 Flutter state tests and 12 example
widget tests. The database suite includes layout-3 rejection with wallet data
preserved. These targeted checks do not claim a full workspace release check.

## Public deployment observation and limits

Both public map endpoints returned identical bytes, SHA-256
`dfc89f953813c17266720d5e6ad93e8115cba360e535ce96118e62ec600b1d49`, covering
mainnet through **3,477,098**, hash
`00000000007e760bac455c24dc26b85dc8b43ac58c3459041133436df42f1b11`.
A separate `GetTreeState` call to the wallet's lightwalletd source corroborated
that exact height/hash; its sampled latest block was also that height. See
[public-observation.json](public-observation.json) and the compressed raw responses.
This is independent-server corroboration, not local consensus validation or proof
of index completeness.

Private SSH to coordinator `167.99.42.60` and generator `152.42.137.247` timed out.
[ssh-observations.json](ssh-observations.json) preserves the commands and failures.
No firewall, service, rollout or host was changed. Therefore **current worker
readiness, running binary hashes, configuration and the supervised soak result
remain unverified**. Public map agreement cannot establish those facts.

The last recorded deployment identities and settings remain historical evidence
in [managed preparation](../managed-preparation-2026-09-09/README.md) and
[the comparison's fleet captures](../block-comparison-2026-09-09/README.md).
M1 must restore operator access and retrieve
`/opt/transparent-publisher-build/managed-hardening-rollout-3/status.json` and its
matching raw results before restarting or promoting anything. An existing
supervisor may have advanced; do not infer its phase from older documentation.

## Reproduction and evidence

Reconstruct the exact integration source without touching the working checkout:

```sh
python3 docs/transparent-pir/evidence/productionize-m0-2026-09-09/reproduce-wallet.py \
  --repo /Users/roman/projects/wallet-libraries \
  --out /tmp/m0-wallet-reproduction
```

The destination must not exist. The helper verifies the base archive, captured
patch and every reconstructed source file. [commands.md](commands.md) records
probes and test commands; [manifest.json](manifest.json) records results and
provenance. `SHA256SUMS` binds the published evidence files.

The first example-test attempt omitted the untracked heights screen from the
snapshot and failed compilation. Its raw log is retained; after including that
source file, the example tests were rerun. A separate command was accidentally
issued from the PIR repository and found no test directory; that invocation is
also retained and is not counted as application validation. These are M0 harness
errors, not attributed to wallet code.

M0 closes discovery with explicit gaps. It does not close M1's sustained fleet
acceptance, M2's app changes or M3's live wallet-correctness matrix.
