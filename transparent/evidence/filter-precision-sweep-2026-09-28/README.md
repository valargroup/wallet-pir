# Range-filter precision sweep, 2026-09-28

Exact encoded sizes of the recent tier's range filters under alternative
Golomb-Rice parameters (architecture update §3), with a false-match cost model.
Offline; no profile is implemented or published.

| Field | Value |
|---|---|
| Tool | `filter-sweep` (`transparent/services/transparent-filter-server/src/bin/filter-sweep.rs`), same commit as this note, built on `roman-ipir-bench-8vcpu` |
| Input | The recent journal slice from the [census run](../single-lookup-census-2026-09-27/README.md) (heights 3,262,749–3,473,686) and that run's recent-8k `per_shard` boundaries; shard ids 160–173, keyed exactly as published |
| Command | `filter-sweep --data-dir <slice> --census census-recent-8k.txt --params 19:784931,16:98304,14:24576,13:12288,12:4096,11:2048,10:1024,9:512,8:256` |
| Output | `sweep.tsv`: bytes per shard and parameter pair, totals, bits per element |

## Results

At the current profile (P=19, M=784,931), the 14 recent filters total **1,293,378
bytes**. That is exactly the per-sync filter download in
[fleet series r3](../runs/fleet-series-2026-09-08-r3/README.md), so the sweep
reproduces the published filters.

| P | M | Bytes (14 shards) | Bits per element | False match per absent test |
|---:|---:|---:|---:|---:|
| 19 | 784,931 | 1,293,378 | 21.05 | 1.3e-6 |
| 16 | 98,304 | 1,109,246 | 18.06 | 1.0e-5 |
| 14 | 24,576 | 986,384 | 16.06 | 4.1e-5 |
| 13 | 12,288 | 924,952 | 15.06 | 8.1e-5 |
| 12 | 4,096 | 834,430 | 13.58 | 2.4e-4 |
| 11 | 2,048 | 773,003 | 12.58 | 4.9e-4 |
| 10 | 1,024 | 711,577 | 11.58 | 9.8e-4 |

The false-match rate is `1/M` by construction. Measured sizes agree with the
geometric-gap approximation in the architecture update (21.05 and 13.58 bits at
P=19 and P=12).

**Expected recent public cost per wallet** (derived). This is the filter bytes
plus false matches × 161,544 B. That figure is one single-lookup 8K directory
query (133,144 B), plus a directory setup (about 19.4 KB) and a manifest carrying
a table (about 9 KB), for a shard the wallet would not otherwise open.

| Scripts tested | P=19 | P=13 | P=12 | P=10 |
|---:|---:|---:|---:|---:|
| 10 | 1.293 MB | 0.927 | 0.840 | 0.734 |
| 40 | 1.293 | 0.932 | 0.857 | 0.800 |
| 100 | 1.294 | 0.943 | 0.890 | 0.932 |
| 200 | 1.294 | 0.962 | 0.945 | 1.153 |
| 1,000 | 1.296 | 1.109 | 1.387 | 2.920 |

## Reading

- P=13 (M=12,288) costs less than the current profile at every tested count up
  to about 2,000 scripts. It saves 28% of recent filter bytes for ordinary
  wallets.
- P=12 saves about 35% for wallets testing up to about 200 scripts, but costs 7%
  more than today at 1,000.
- For a six-month restore that already uses directory choice tables (2.39 MB on
  the [bench fleet](../single-lookup-fleet-2026-09-27/README.md)), P=13 would
  bring the recent public floor to about 2.03 MB. That is −35% against the r3
  baseline, a projection and not a measurement.

A profile change alters every filter, so it needs:
- a new profile name with its own P and M;
- wallets that select P and M by profile and refuse unknown ones (current
  wallets decode with compiled constants, so a changed filter would not be
  refused cleanly);
- a full republication.
It belongs with the next full publication.
