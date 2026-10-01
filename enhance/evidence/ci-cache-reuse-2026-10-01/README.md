# CI Cargo cache identity and reuse — October 1, 2026

Validation of the change that keys hosted Cargo caches by compatibility identity
([`tools/ci/cargo_cache.py`](../../../tools/ci/cargo_cache.py), described in
[CI performance](../../../docs/ci-performance.md#cargo-cache-identity-and-reuse)).
This is development tooling evidence, separate from qualification. Status:
**validated on the task branch, not integrated**. The finding is not addressed
until the change is on `main` and the primed-main comparison below has run.

## Original report

The finding (p0mvn/ai-runbook PR #30, finding `4a4fd347a4012a81e5b3d13d`) was
based on these runs, captured before the change in `raw/baseline-*`:

- `baseline-restore.json`: rust-cache configuration and restore lines from PR 122
  run 36723820479 and PR 123 runs 36728359128 and 36729873650. Keys used rust-cache's
  environment hash, which included every installed toolchain (1.91.0 and 1.98.1)
  and runtime-only `RUST_TEST_THREADS`. Enhance tests and Shared tests shared one
  key with different package sets. PR 123's first full run restored nothing.
- `baseline-caches.json`: the 22 cache entries before the change. Main never saved
  hosted full-lane entries because main CI full runs on the persistent pool.
- `baseline-timings-*.json`: step timings for PR 123 runs.

## Runs (valargroup/wallet-pir, GitHub-hosted `ubuntu-24.04` for full jobs)

All runs were `workflow_dispatch` on branch `ai-dev/t-76147dba2c4c47d1/a1/wallet-pir`,
so hosted entries are scoped to that branch ref. The release and CUDA jobs are main-only
and were skipped. Fast checks ran on the persistent fast runner.

| Run | Workflow | Head | Purpose | Gate |
| --- | --- | --- | --- | --- |
| 36834774107 | CI | 856a419a | first build in new persistent toolchain directory | Fast checks: success |
| 36834777102 | CI full | 856a419a | cold: no `v1` entries existed | Full checks complete: success |
| 36837792640 | CI | 3fb732d2 | persistent restore | Fast checks: success |
| 36837789214 | CI full | 3fb732d2 | restore (only Python tooling changed since 856a419a) | Full checks complete: success |
| 36837847229 | CI full | 4167c3ce | probe: workflow comment plus runtime-only `RUST_BACKTRACE`; reverted in ba8ad735 | Full checks complete: success |

`raw/timings-<run>.json` is `python3 tools/ci/timings.py --run <run> --cache-records`
output. `raw/v1-caches.json` lists the resulting entries, all on the branch ref.

## Results

Per job: restore status, then Cargo units at job end (reused / rebuilt / new;
third-party rebuilt), leaf compilation and execution stage seconds.

| Job | Cold 856a419a | Restore 3fb732d2 | Env-only probe 4167c3ce |
| --- | --- | --- | --- |
| Transparent lint | miss; 0/0/627; comp 727.5 | hit-current-ref; 513/114/0 (tp 0); comp 13.5 | hit-current-ref; 513/114/0 (tp 0); comp 15.7 |
| Transparent tests | miss; 0/0/628; comp 840.9, exec 668.2 | hit-current-ref; 516/112/0 (tp 0); comp 221.0, exec 665.9 | hit-current-ref; same counts; comp 220.9, exec 654.1 |
| Enhance lint | miss; 0/0/461; comp 211.4 | hit-current-ref; 422/39/0 (tp 0); comp 12.3 | hit-current-ref; same counts; comp 11.1 |
| Enhance tests | miss; 0/0/1057; comp 447.6, exec 752.2 | hit-current-ref; 977/80/0 (tp 0); comp 277.9, exec 800.9 | hit-current-ref; same counts; comp 274.5, exec 800.2 |
| Shared lint and tests | miss; 0/0/320; comp 107.7, exec 3.1 | hit-current-ref; 306/14/0 (tp 0); comp 4.0, exec 7.5 | hit-current-ref; same counts; comp 4.1, exec 3.9 |
| Fast checks (persistent) | persistent-new; 0/0/1013; comp 1326.8, exec 67.9 | persistent-existing; 1011/2/0; comp 13.5, exec 66.5 | not run |

- **Cold/restore:** every hosted key missed on the cold run and hit exactly on
  the restore run. No third-party unit rebuilt after a restore. Workspace crates
  rebuild because a fresh checkout makes their sources newer than the cached output.
- **Workflow/env-only change:** the probe changed workflow text and added a
  runtime-only variable. Every identity and key was unchanged, and every job hit.
- **Missing cache:** the cold run is the missing-cache case; all gates passed.
- **Incompatible inputs:** lanes and scopes received distinct keys in the same run
  (five keys for five lint/test jobs). Changes to compiler, flags, target, OS/glibc,
  native tools, features, profile, Cargo configuration and `Cargo.lock` are covered
  by `CacheIdentityTests` in `tools/tests/test_ci.py`, not by remote runs.
- **Restore cost:** hosted restore took 5–26 s per job, and Rust setup took 21–35 s
  including identity computation.

The cold and restore rows are one paired measurement of the same Rust tree. The
setup and Cargo inputs are identical; only Python CI tooling changed. With n=1 per
side, they show which work the cache removes, not a typical saving. No PR
population or before/after p95 was measured, so this note claims no savings. Test
execution time is unaffected by the cache.

## Not demonstrated here

- **Equivalent PR/main against a primed main cache.** The main-only
  `Prime hosted Cargo caches` workflow can save entries only after integration. After the
  change reaches `main`, wait for that workflow on the integrated SHA, then run
  CI full on a PR or branch whose Rust tree equals `main`. Expect `restore: hit-main`
  and `refs_before_restore: ["refs/heads/main"]`; record the result with `--cache-records`.
- **No cross-ref reuse between PR merge refs or branches.** Each scope used only its
  own ref and main. The branch entries above are not readable by other branches.
- **Release and CUDA jobs** are main-only. The release jobs still build their own
  release binaries in separate lanes (`release`, `release-native`, `native-cuda`).
  They do not reuse test or PR output, and their caches are written only from
  main. The CUDA identity is unit-tested only.
- **Self-hosted persistent directories** are now named by toolchain identity, so each
  lane builds cold once after integration, as Fast run 36834774107 did. Old directories
  remain until pruned while runners are idle.
- Fork-PR fast lane and native/CUDA feature checks are not primed.

`manifest.json` records the source, commands and run identities. `SHA256SUMS`
covers `raw/`.
