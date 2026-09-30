# Development-speed follow-up to PR 122

This is local tooling evidence, not protocol, hardware or deployment qualification.
The report is [ai-runbook PR 3](https://github.com/p0mvn/ai-runbook/pull/3).
The implementation is [wallet-pir PR 123](https://github.com/valargroup/wallet-pir/pull/123).

## Acceptance assessment

| Criterion | Result and remaining work |
| --- | --- |
| Verify PR 122 final checks before changing code | Final head `ecc987fa1e830c120e36be7e1de29b8808df6dbc` passed Fast checks and Full checks complete, including native and optional CUDA checks. No attributable final-head failure was found. See [full results](raw/pr122-full.json) and [fast timings](raw/pr122-actions.json). |
| Preserve equivalent final validation | Main was verified at merge SHA `a73c85f0ff63376839e75fbd6d582930454c84e0`. Its Fast checks passed; [full qualification](https://github.com/valargroup/wallet-pir/actions/runs/36724363704) remained pending at this observation. The durable follow-up checks this exact run and PR 123's exact-head aggregate. A PR gate does not establish main artifact or hardware qualification. |
| Automatic target ownership and reuse | Local fast/package wrappers use nonblocking OS leases, compatible pools and released numbered lanes. Compilation, discovery and execution share one lease; inherited descriptors preserve it after wrapper death. GitHub Actions retains its existing targets. Raw Cargo and other make targets still require manual ownership. |
| Two actual concurrent focused Cargo checks without a shared build lock | Both warm `pir-control` identity checks succeeded in separate lanes, with overlapping compilation/test intervals and no build-directory lock messages. Package-cache contention was present and retained; registry-cache contention is not eliminated. |
| Comparable focused feedback before/after | Same checkout, host, command, flags, Rust toolchain and warm cache population; 20 successful clean-SHA samples on each implementation. Local p95 increased from 1.259s to 1.431s. This shows 0.172s ownership overhead, not a serial speedup. |
| Time to first reviewable PR | This follow-up took 593.524s from the request to PR creation, including verification and initial implementation. A comparable historical baseline is unavailable; the report's summed tool durations cannot reconstruct it. No before/after PR-time improvement is claimed. |
| Warm Actions / dependency / native artifact latency targets | Still unqualified by this local sample. The initial PR fast run took 100s from workflow creation to job completion, with 3s to job start, 1s Rust setup and 85s affected execution. It selected broad checks for tooling changes, unlike the focused local command. It is one sample, not a comparable warm p95 population or proof of the 30s target. Native/CUDA artifact populations and hardware gates remain separate. |

## Local population and concurrency

The exact workload was:

```sh
CARGO_TARGET_DIR=<private-evidence>/target \
  make check-package PACKAGE=pir-control TEST=identity OFFLINE=1
```

Both populations used this same pool-root environment value. Before the change,
Cargo wrote directly into that directory. Afterward it wrote into compatible
`dev-lanes` beneath it. Cache population means the focused artifacts were warmed
before samples; it does not mean cold artifacts were silently mixed into the
warm population. The baseline warmup took 7.534s. Two newly allocated lanes were
explicitly warmed concurrently before measurement; their compilation took
7.340s and 8.058s. These preparation runs are retained but excluded from p95.
Neither pool was deleted between measured runs. The 20 after samples reuse the
released first lane. No protocol/profile flags, fixtures or production values
were modified to obtain these results.

| Warm concurrent invocation | Elapsed | Target | Compile interval | Test interval | Build-directory waits |
| --- | ---: | --- | --- | --- | ---: |
| 0 | 1.448s | lane-0 | 0.972–1.133s | 1.288–1.441s | 0 |
| 1 | 1.439s | lane-1 | 0.973–1.126s | 1.279–1.432s | 0 |

Intervals are monotonic seconds since the pair's shared start, recorded while
reading each command's output. Compile intervals overlapped for 0.152s, and test
intervals for 0.144s. There were six package-cache lock messages across these
warm invocations, despite offline mode. The logs retain those messages and the
distinct target paths; this proves target isolation, not absence of all locking.

The measured implementation SHA is `4f0c98cdec73fd6132d3e7842d1998889d7f8452`.
Subsequent changes add the actual-Cargo lifetime regression and these retained
records; they do not change the measured runtime. Eight focused lease tests
passed locally with `WALLET_PIR_CARGO_LEASE_TEST=1`, including SIGKILL while a real
dependency-free Cargo check waits in its build script. The default helper suite
leaves this actual-Cargo probe opt-in; the shell/grandchild regression always runs.
Selection regressions passed (19 tests plus one existing opt-in skip).
Final local helper validation passed 50 tests with the same one opt-in skip;
documentation validation checked 232 files without broken links. The planner
still selects broad CI coverage for changes to the tooling itself; broad Rust
suites were left to CI rather than duplicated locally.

## Provenance and reproduction

- [Run manifest](manifest.json) and [comparison metadata](comparison.json): exact source SHAs, compiler, host,
  flags, workload, cache assertions and p95 arithmetic.
- [Before samples](raw/before.json), [after samples](raw/after.json): all 20
  successful clean-snapshot rows in each population.
- [Baseline warmup](raw/baseline-warmup.json) and [log](raw/baseline-warmup.log).
- [Concurrent warmup](raw/after-warmup-pair.json) and its
  [first](raw/after-warmup-pair-0.log)/[second](raw/after-warmup-pair-1.log) logs.
- [Warm concurrency record](raw/after-concurrent.json) and its
  [first](raw/after-concurrent-0.log)/[second](raw/after-concurrent-1.log) logs.
- [Pair harness](harness/measure_pair.py): run from a clean checkout. It retains
  output under `~/.ai-runbook/dev-ux/checks/wallet-pir-122-followup/`; use a fresh
  evidence path in a copy of the harness before reproducing so retained inputs
  are not overwritten.
- [PR creation timing](raw/first-pr.json): extracted task/creation timestamps,
  with unavailable historical baseline explicitly null. No transcript retained.
- [Initial PR Actions timings](raw/initial-pr-actions.json),
  [PR 122 Actions timings](raw/pr122-actions.json) and
  [main full observation](raw/main-full-observed.json): exact-SHA snapshots.
- [Checksums](SHA256SUMS.json): immutable retained measurement inputs and harness.

To repeat a serial population, warm the workload once, then use
`python3 tools/ci/measure.py --category shared --population warm --runs 20
--output <new-json> -- make check-package PACKAGE=pir-control TEST=identity
OFFLINE=1` with the same target-root environment. Local execution excludes
Actions queue, setup and runner scheduling. Report a new source SHA and preserve
the old records rather than substituting new observations into these files.
