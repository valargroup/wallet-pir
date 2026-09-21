# Enhance wider-row encoding study — September 20, 2026

This offline study uses the 450,163-record snapshot measured in `../live-pir-2026-09-20/`. It constructs fresh real queries with pinned `ipir-sp` revision `accc424e879d8da425fa620aad80f0f2c4e0defd`; upload lengths are taken from serialized keys and query coefficients. Response lengths are calculated with that library's response encoder sizing function. Wider layouts were not deployed; server latency, cold-build cost and peak RSS were not benchmarked. No production files were changed.

Each record is 737 bytes. One instance carries 3,584 plaintext bytes and contributes 5,120 response bytes. The response adds a single 16-byte prefix. All widths use the current 8,192-row shard minimum and power-of-two logical row count.

| Records/row | Instances | Logical rows | Upload KiB | Calculated download KiB | Combined KiB | Records before next row-count step |
|---:|---:|---:|---:|---:|---:|---:|
| 9 | 2 | 65,536 | 420.01 | 10.02 | 430.02 | 589,824 |
| 10 | 3 | 65,536 | 420.01 | 15.02 | 435.02 | 655,360 |
| 14 | 3 | 32,768 | 252.01 | 15.02 | 267.02 | 458,752 |
| 18 | 4 | 32,768 | 252.01 | 20.02 | 272.02 | 589,824 |
| 19 | 4 | 32,768 | 252.01 | 20.02 | 272.02 | 622,592 |
| 29 | 6 | 16,384 | 166.01 | 30.02 | 196.02 | 475,136 |
| 36 | 8 | 16,384 | 166.01 | 40.02 | 206.02 | 589,824 |
| 38 | 8 | 16,384 | 166.01 | 40.02 | 206.02 | 622,592 |
| 58 | 12 | 8,192 | 125.01 | 60.02 | 185.02 | 475,136 |
| 72 | 15 | 8,192 | 125.01 | 75.02 | 200.02 | 589,824 |
| 77 | 16 | 8,192 | 125.01 | 80.02 | 205.02 | 630,784 |

The original design rationale is retained in git commit `7f6e24a9b693ab1e9c4914e2b09a676c6308abf1`, `docs/roman_notes.md`, “Why 9 records per row?”. At the then-current 724-byte record size, nine was the densest packing in two instances. For 136,425 records, moving eight to nine records per row crossed a padding boundary from 32,768 to 16,384 logical rows without increasing instance count. Today's 737-byte records still fit nine in two instances, while ten require three. This establishes the recorded rationale, not a globally optimal layout or empirical superiority over all wider shapes.

The current client rejects a records-per-row value other than its compile-time constant. Changing width requires coordinated protocol/client/server changes, rebuilt shard artifacts and reconsideration of shard/group capacity. Wider rows preserve whole-database privacy when all groups are still queried; no public target-shard selection is needed.

Fewer rows reduce the query vector, while more instances increase response size, public setup, intermediate width and response-packing work/preprocessing. Matrix scan cost follows both rows and columns, so halving rows does not automatically halve server time. Packing keys remain 86,016 bytes for every width tested. The 14-record layout crosses back to 65,536 rows above 458,752 records; 19 and 38 retain capacity through 622,592 records. Selecting width for a single present-day padding boundary can therefore give short-lived savings.

Reproduce in a new evidence directory with `cargo run --offline --release --manifest-path <study>/Cargo.toml --target-dir target`. Raw output and helper source are retained here. The larger response sizes and upload reductions are encoding results, not full-system latency measurements.
