# CI Cargo cache identity and reuse — October 1, 2026

This note validates the change that keys hosted Cargo caches by compatibility
identity ([`tools/ci/cargo_cache.py`](../../../tools/ci/cargo_cache.py), described in
[CI performance](../../../docs/ci-performance.md#cargo-cache-identity-and-reuse)).
It is development tooling evidence, separate from qualification.

Status: **validated on the task branch, not integrated**. The finding is not
addressed until the change is on `main` and the primed-main comparison below
has run.

There are three rounds:

- **Round 1** tested the first implementation. Review found two identity gaps
  and an attribution defect.
- **Round 2** tested the corrected code (e56d818f) with cold, restore and
  workflow/env-only runs. Review then found two measurement defects:
  - a `check` and a `build` of one target were counted as one unit;
  - with several config `include`s, the identity probed a compiler Cargo did
    not run.
- **Round 3** runs the final code (8d6d613a). Its unit counts are the current
  attribution.

Raw files from every round are retained unchanged.

## Original report

The finding is p0mvn/ai-runbook PR #30, finding `4a4fd347a4012a81e5b3d13d`. It
was based on the runs below, captured before the change in `raw/baseline-*`:

- `baseline-restore.json`: rust-cache configuration and restore lines from PR 122
  run 36723820479 and PR 123 runs 36728359128 and 36729873650.
  - Keys used rust-cache's environment hash. That hash included every installed
    toolchain (1.91.0 and 1.98.1) and the runtime-only `RUST_TEST_THREADS`.
  - Enhance tests and Shared tests shared one key despite different package sets.
  - PR 123's first full run restored nothing.
- `baseline-caches.json`: the 22 cache entries before the change. Main never saved
  hosted full-lane entries because main CI full runs on the persistent pool.
- `baseline-timings-*.json`: step timings for the PR 123 runs.

## Round 3: final code (8d6d613a)

The fix changes only include ordering, config-relative program paths and the
artifact unit key. This repository has no config `include`, so every hosted key
and the persistent toolchain directory are unchanged. Both round 3 runs
therefore restored round 2's entries.

| Run | Workflow | Case | Gate |
| --- | --- | --- | --- |
| 36854240061 | CI full | restore (`hit-current-ref` for all five keys) | Full checks complete: success |
| 36854242979 | CI | persistent restore | Fast checks: success |

Fresh / compiled units from Cargo's `compiler-artifact` messages, with the
third-party split in parentheses:

| Job | Units | Fresh / compiled | Compile / execution stage s |
| --- | --- | --- | --- |
| Transparent lint | 572 | 460 / 112 (third-party 460 / 0) | 14.9 / none |
| Transparent tests | 570 | 460 / 110 (460 / 0) | 225.9 / 671.5 |
| Enhance lint | 419 | 380 / 39 (380 / 0) | 10.2 / none |
| Enhance tests | 967 | 887 / 80 (887 / 0) | 265.7 / 1006.7 |
| Shared lint and tests | 282 | 268 / 14 (268 / 0) | 3.0 / 5.6 |
| Fast checks (persistent) | 925 | 924 / 1 (805 / 0) | 12.6 / 64.2 |

Enhance tests and Fast checks now report more units than in round 2: 967 vs 870,
and 925 vs 655. Their `cargo check` units (`.rmeta`) are no longer merged with
`build`/`test` units of the same target. Round 2's totals for those two jobs are
undercounts. Its zero-fresh cold result and all-third-party-fresh restore result
still hold, because a merged unit was fresh only if every occurrence was fresh.
The other jobs run a single mode per target, and their counts are identical in
rounds 2 and 3. A cold run with the final unit key was not repeated. The cache
keys did not change, so there was no missing-cache state to observe without
deleting entries.

Both review counterexamples are now real-Cargo oracle tests in
`RealCargoOracleTests` (`tools/tests/test_ci.py`), and both fail on e56d818f's code:

- `check` then `build` of one dependency-free crate counts two compiled units;
- `include = ["one.toml", "two.toml"]`, with each file naming a different
  `rustc`, makes Cargo run the second, and the identity probes that same
  program.

## Round 2: acceptance runs (e56d818f)

All runs were `workflow_dispatch` on branch `ai-dev/t-76147dba2c4c47d1/a1/wallet-pir`.
Hosted full jobs ran on GitHub-hosted `ubuntu-24.04`, so their entries are scoped
to that branch ref. Fast checks ran on the persistent fast runner. The release and
CUDA jobs are main-only and were skipped.

| Run | Workflow | Head | Case | Gate |
| --- | --- | --- | --- | --- |
| 36845912455 | CI full | e56d818f | cold: no entry for the new identities | Full checks complete: success |
| 36845915560 | CI | e56d818f | first build in the new persistent toolchain directory | Fast checks: success |
| 36848898795 | CI full | f826bf42 | probe: workflow comment plus runtime-only `RUST_BACKTRACE` | Full checks complete: success |
| 36848927411 | CI full | fc84e9b8 | restore; probe reverted, tree identical to e56d818f | Full checks complete: success |
| 36848930204 | CI | fc84e9b8 | persistent restore | Fast checks: success |

`raw/timings-<run>.json` is the output of
`python3 tools/ci/timings.py --run <run> --cache-records`. `raw/v1-caches-round2.json`
lists the resulting hosted entries; all ten are on the branch ref.

### Attribution from Cargo

Units were keyed by package, target, profile and features. Round 3 found that
this merged `check` and `build` units of one target, so the Enhance tests and
Fast checks totals below are undercounts.

Each cell is **units fresh / compiled** from Cargo's `compiler-artifact` messages
(`CI_CARGO_ARTIFACTS`), with the third-party split in parentheses. These cover
only the commands each job ran, so restored units a job never needed are not
counted.

| Job | Cold e56d818f | Restore fc84e9b8 | Workflow/env probe f826bf42 |
| --- | --- | --- | --- |
| Transparent lint | 0 / 572 (third-party 0 / 460) | 460 / 112 (460 / 0) | 460 / 112 (460 / 0) |
| Transparent tests | 0 / 570 (0 / 460) | 460 / 110 (460 / 0) | 460 / 110 (460 / 0) |
| Enhance lint | 0 / 419 (0 / 380) | 380 / 39 (380 / 0) | 380 / 39 (380 / 0) |
| Enhance tests | 0 / 870 (0 / 793) | 793 / 77 (793 / 0) | 793 / 77 (793 / 0) |
| Shared lint and tests | 0 / 282 (0 / 268) | 268 / 14 (268 / 0) | 268 / 14 (268 / 0) |
| Fast checks (persistent) | 0 / 655 (0 / 577) | 654 / 1 (577 / 0) | not run |

- **Cold/restore:** every hosted key missed on the cold run and hit exactly on
  the restore run (`hit-current-ref`). After a restore, Cargo reported every
  third-party unit fresh. On hosted runners every workspace unit compiled
  because a fresh checkout makes its sources newer than the cached output. On the
  persistent fast runner, one workspace unit compiled.
- **Workflow/env-only change:** the probe changed workflow text and added a
  runtime-only variable. All five identities and keys were unchanged, every job
  hit, and the fresh/compiled counts equal the restore run's.
- **Missing cache:** the cold run is the missing-cache case; all gates passed.
- **Incompatible inputs:** the five lint/test jobs received five distinct keys in
  the same run. `CacheIdentityTests` in `tools/tests/test_ci.py` covers changes
  to the following, using a temporary directory rather than remote runs:
  - compiler, Cargo-relevant environment flags, target, OS/glibc and native tools;
  - wrapper versions and features/scope;
  - root-manifest profile settings, such as `lto` and `codegen-units` for `release-fast`;
  - ancestor-directory, checkout, `CARGO_HOME` and included Cargo configuration;
  - `Cargo.lock`.

  Both review counterexamples were also reproduced against this checkout. A
  `release-fast` edit to `lto` and `codegen-units` changes the identity. So does
  an ancestor `.cargo/config.toml`. Removing either edit restores the original
  identity.

### Timing on the same Rust tree (n=1 per side)

Times are leaf stage wall time in seconds (`CI_STAGE_REPORT`). The compile stage
covers whole Cargo compile/lint commands, including resolution, downloads, build
scripts and linking. Execution stages may also compile units that no compile
stage needed. In the Enhance tests job, the five inline test commands are now
timed as execution stages.

| Job | Cold compile / execution | Restore compile / execution |
| --- | --- | --- |
| Transparent lint | 708.2 / none | 12.6 / none |
| Transparent tests | 801.2 / 663.0 | 219.4 / 667.7 |
| Enhance lint | 165.7 / none | 12.8 / none |
| Enhance tests | 532.0 / 1040.0 | 262.3 / 979.4 |
| Shared lint and tests | 84.3 / 3.2 | 4.2 / 3.9 |
| Fast checks (persistent) | 1419.8 / 76.0 | 13.4 / 70.8 |

Hosted Rust setup took 20.4–35.4 s per job, including identity computation and
the cache-ref lookup. Hosted restore took 4.4–29.7 s on hits and 0.4–2.2 s on
misses. Each comparison is one paired run on the same tree with the same inputs.
The rows show which work the cache removes, not a typical saving. No PR population
or before/after p95 was measured, so this note claims no savings.

## Round 1: first implementation (superseded attribution)

Round 1 used runs 36834774107 and 36834777102 (cold, 856a419a), 36837792640 and
36837789214 (restore, 3fb732d2), and 36837847229 (probe, 4167c3ce, reverted in
ba8ad735). All gates passed. Their `raw/timings-*.json` files and
`raw/v1-caches.json` are retained unchanged.

That code used keys that did not cover profile definitions or ancestor Cargo
configuration. Its unit counts compared fingerprint mtimes, not Cargo's own
`fresh` flags:

- "reused" included restored units the job never needed, and units whose
  fingerprint bytes changed while their mtime was preserved;
- a timestamp-only touch counted as "rebuilt".

The round 1 counts therefore do not establish which units Cargo compiled. Two
statements in this note's earlier version were overclaims:

- "No third-party unit rebuilt after a restore" rested on those mtime counts.
- "Rust setup took 21–35 s" omitted the cold Transparent lint job's 45.1 s; the
  measured range was 20.9–45.1 s.

Round 2 supersedes both statements.

## Not demonstrated here

- **Equivalent PR/main against a primed main cache.** The main-only
  `Prime hosted Cargo caches` workflow can save entries only from `main`, so this
  needs reviewed integration first. Then:
  1. Wait for that workflow to finish on the integrated SHA.
  2. Run CI full on a PR or branch whose Rust tree equals `main`.
  3. Expect `restore: hit-main`, `refs_before_restore: ["refs/heads/main"]` and
     third-party units fresh in `CI_CARGO_ARTIFACTS`.
  4. Record the result with `--cache-records`.

  The trust boundaries (PRs may read main entries, and main never reads PR entries)
  also follow from GitHub's cache scoping and the main-only priming and release
  jobs. They are covered by workflow tests, not by a remote cross-ref run.
- **No cross-ref reuse between branches or PR merge refs.** The branch entries
  above can be read by no other branch or PR.
- **Release and CUDA jobs** are main-only. They still build their own release
  binaries in separate lanes (`release`, `release-native`, `native-cuda`). They do
  not reuse test or PR output, and their caches are written only from main.
  - The CUDA container has Python 3.10, which lacks `tomllib`. Its identity hashes
    the whole `Cargo.toml` and does not follow config `include` or `[build]` tool
    settings.
- **Unattributed Cargo commands:** commands outside `tools/ci/stage.py`, such as
  `make` helper targets, are not in `CI_CARGO_ARTIFACTS`.
- **Self-hosted persistent directories** are named by toolchain identity. Each lane
  builds cold once after integration, and these branch runs created extra fast-lane
  directories. Old directories remain until pruned while the runners are idle.
- Fork-PR fast lane and native/CUDA feature checks are not primed.

`manifest.json` records the source, commands and run identities. `SHA256SUMS`
covers `raw/`.
