# Schema-11 placement and full-shard width comparison — September 23, 2026

The target is five total shards in an active/lending group or seven sealed shards
per replica after qualification. Six sealed remains the default; select seven
explicitly with `--sealed-shards 7`. No native 8 GiB qualification is claimed.

## Correctness evidence

- [Full-size HTTP campaign](seven-shard-http.log): seven sealed shards on the
  destination replica pair, injected seventh-slot reservation refusal, canonical
  fallback, successful retry, and exact retained/current queries. Passed in
  1,101.18 seconds on the local 128 GiB ARM64 macOS host while other builds ran.
  This is functional evidence, not a latency or memory qualification campaign.
- [Pinned wallet interoperability](wallet-interop.log): all three tests passed at
  PR #28 commit `de3ec78f31b6fd184596fc952fe4f78d3a63cd0a`, using the seven-sealed
  policy. Actual HTTP requests and SQLite batch application preserve duplicate
  outcomes. Wallet local `.gitignore` and `.vscode` modifications were untouched.
- [Capacity tests](capacity-tests.log): six passed, including both packing policies,
  restart persistence, memory demand, first-pair behavior and fleet ceilings.
- [Operations tests](operations-tests.log): 121 passed with three platform skips (124 total).
  Bootstrap units/receipts and campaign evidence bind the selected policy.

- [Targeted serving tests](targeted-tests.log): 13 passed across old-format rejection,
  canonical extraction, HTTP recovery, authenticated note recovery and RPC reorg/restart.
  Three large HTTP tests are ignored by default; the seven-sealed test was run
  explicitly as recorded above.
- [Load-test checks](load-tests.log): six passed. Workspace Clippy with warnings
  denied passed. The broad workspace run was stopped during unrelated legacy
  topology tests; this evidence does not claim a complete workspace test pass.

The first expanded forecast test exposed the pinned wallet's 24-query-shard
ceiling. The corrected forecast respects that ceiling: blocked boundaries for
one through four groups are 5/12/19/24 full-shard spans with seven-sealed packing,
versus 5/11/17/23 with six. The last compatible record count is 25,952,255.
The server and pinned wallet boundary test verifies this limit directly.

## Equal-geometry measurements

These are synthetic width comparisons in the same server build, not a replay of
an old production binary. Both use 32,768 rows, 33 records per row, the existing
cryptographic profile and public setup, three warmups and ten measured queries.
The benchmark materializes raw input, preparation and query state in one process;
its peak RSS is not worker residency. Concurrent builds make latency descriptive
only. Do not infer a throughput regression or improvement from these samples.

| Measurement | 737 bytes | 653 bytes |
|---|---:|---:|
| Raw row bytes | 24,321 | 21,549 |
| Raw full-shard bytes | 796,950,528 | 706,117,632 |
| PIR instances | 6 | 6 |
| Encoded columns (`u16`) | 12,288 | 12,288 |
| Encoded database bytes | 805,306,368 | 805,306,368 |
| Artifact bytes | 1,006,682,601 | 1,006,682,601 |
| Query bytes | 274,460 | 274,460 |
| Response bytes | 30,748 | 30,748 |
| Peak benchmark RSS bytes | 4,211,703,808 | 4,139,991,040 |
| Median server evaluation/packing ms | 78.609 | 109.689 |
| Maximum measured server ms | 137.063 | 177.615 |
| Exact measured queries | 10/10 | 10/10 |

Raw storage shrank 11.3976%; encoded storage and responses did not shrink.
See [machine-readable comparison](comparison.json), [737-byte output](width737.json),
[653-byte output](width653.json), and their `*.time` process measurements.

Reproduce from the repository root:

```sh
cargo build --locked --release -p enhance-pir-server --example suffix_width
/usr/bin/time -l target/release/examples/suffix_width 737 32768
/usr/bin/time -l target/release/examples/suffix_width 653 32768
cargo test --release -p enhance-pir-server --test v4_http seven_sealed_consolidation_preserves_retained_queries -- --ignored --nocapture
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries
```

On Linux, replace `time -l` with `time -v`. Run measurements without competing
workloads for performance decisions. The HTTP campaign needs at least 32 GiB RAM
and 64 GiB free disk. Source fingerprints and host identity are retained here;
this local evidence is not a clean Linux release or authorization to claim the
six-hour hardware qualification passed. Production deployment uses six sealed
until the seven-shard hardware gate is satisfied.
