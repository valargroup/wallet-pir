# 8-bit native profile on every v12 table, 2026-10-12

What ipir-sp rc.7's 8-bit native profile would cost and save if every
`transparent-shard-v12` table used it, measured on the real tables of the v12
staging publication and certified table by table. Each table was built three
ways on the same host:

- `baseline`: today's served runtime, the 16-bit profile (two bytes per entry,
  2,048 entries per 4,096-byte row), 44-bit dithered queries, 22-bit responses
  and 29-bit masks.
- `p8-19x2`: the 8-bit profile (one byte per entry, so 4,096 entries per row and
  two packing blocks instead of one), byte storage, 28-bit dithered queries,
  10-bit responses, 22-bit published masks, and today's two-digit packing key.
- `p8-27x1`: the same, with the one-digit packing key of half the size.

**This is a measurement. Nothing served changed.** The 8-bit runs use trial
seeds, so their masks and certificates belong to this trial, not to a profile a
server publishes.

## Provenance

| Item | Value |
|---|---|
| Tables | Active v12 staging publication `d1e449175fe1ddc5…`, copied with `rsync` from `transparent-pir-v12-01:/srv/transparent-pir/v12/publications/`; heights 0–3,515,568, tip `00000000001a05a5…0626375e`, mainnet. All 241 table files matched their manifest SHA-256. 12 archive directories and 174 archive page tables (`decoupled-32k-32k`, 32,768 rows), 25 recent directories (`decoupled-2k-8k`, 2,048 rows) and 30 recent page tables (8,192 rows); [raw/tables.tsv](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/raw/tables.tsv). |
| Tool | `transparent/services/transparent-shard-server/examples/p8_trial.rs` at `52a2045c`, the commit adding this directory on top of `af491534` (removed once the 8-bit profile was served, since its baseline variant measured the 16-bit history tables); ipir-sp and reinspiring `v0.1.0-rc.7`; `release-fast`, `RUSTFLAGS=-C target-cpu=native`, rustc 1.97.1. |
| Checker | ipir-sp `v0.1.0-rc.7` `reinspiring/tools/security/certify_native.py`, SHA-256 `eb4e5799…dd64918`, with its `src/native_gaussian_cdf.txt`. |
| Host | Temporary DigitalOcean `m-8vcpu-64gb-intel` in ams3 (8 vCPU Intel Xeon Gold 6548N, AVX-512 VNNI and VBMI; 62 GiB; Ubuntu 24.04.5, kernel 6.8), deleted after the run. Staging runs the `m-8vcpu-64gb` size (Xeon Platinum 8358), which ams3 could not provision; both variants ran on this one host, so the comparison is like for like, but absolute times may differ from staging. |
| Commands | [run.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/run.sh) then [run_parallel.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/run_parallel.py), see Method; [summarize.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/summarize.py) wrote [raw/summary.md](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/raw/summary.md) and [raw/summary.json](raw/summary.json). |

## Method

One process per table and variant, so each peak memory figure is that table's
own. A timed run builds the runtime (`TableRuntime::build` with 8 build threads
for baseline; `NativeServer::build_u8_with_concurrency` with two blocks in
flight for 8-bit), answers 10 queries one at a time and then 10 more two at a
time (a worker's two query slots), and decodes every answer against the table's
row. Every one of the 4,680 answers decoded exactly. Upload is the 8-byte
binding plus key plus selection; response is the 16-byte header plus body,
both in today's framing. A certificate run builds the table with
snapshot-specific noise statistics (`build_u8_analyzed`), writes the
`native_noise` report and runs the checker on it.

`run.sh` first ran all five variants serially, table by table. It was stopped
after 27 tables (23 archive, 4 recent) to get results sooner. `run_parallel.py`
then timed every remaining recent table serially, and certified every remaining
table four processes at a time. So:

- **Timings and memory** cover all 55 recent tables, but only 23 of the 186
  archive tables (2 directories, 21 page tables). Each archive table has the
  same shape, so its held bytes, upload and build time vary little; archive
  totals multiply the per-kind median by the table count. The scaled baseline,
  28.2 GiB, matches the 28.5 GiB RSS staging measured for the same archive
  worker.
- **Certificates** cover all 241 tables with both keys: 482 reports. Analyze
  times from the parallel phase ran four at a time and are not build timings.

Killing the serial run cut off one in-flight process, which left a broken-pipe
panic in [raw/stderr.log](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/raw/stderr.log); no result was recorded for it, and
the parallel phase redid that work. Raw per-run lines are in
[raw/results.jsonl](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/transparent/evidence/p8-trial-2026-10-12/raw/results.jsonl); reports and checker verdicts are in
[raw/raw-reports.tar.gz](raw/raw-reports.tar.gz).

## Results

### Upload and response per table query (measured)

| Table | Today | 8-bit, two-digit key | 8-bit, one-digit key |
|---|---:|---:|---:|
| Archive directory or pages, 32,768 rows | 207,880 B | 142,344 B (−32%) | 128,520 B (−38%) |
| Recent directory, 2,048 rows | 38,920 B | 34,824 B (−11%) | 21,000 B (−46%) |
| Recent pages, 8,192 rows (median prefix query) | 72,712 B (67,080 B) | 56,328 B (52,744 B) | 42,504 B (38,920 B) |
| Response per segment | 5,648 B | 5,136 B (−9%) | 5,136 B (−9%) |
| Published masks per table | 14,848 B | 22,564 B (+52%) | 22,564 B (+52%) |

One query to every directory shard (12 archive, 25 recent, as a full restore's
directory pass does): 3,468 KB today, 2,579 KB two-digit (−26%), 2,067 KB
one-digit (−40%). The recent prefix figures are what a page query may send
when the page table holds fewer rows than its height (`PageChild::query_rows`);
the 8-bit prefix sizes are computed from the same row count, since
`NativeServer` does not take a prefix query yet.

### Server time per query (measured, medians across tables)

| Table | Today p50 / max p99 | Two-digit | One-digit | Two at a time, p50 (today / two-digit / one-digit) |
|---|---:|---:|---:|---:|
| Archive, 32,768 rows | 4.77 / 6.01 ms | 5.05 / 5.36 ms | 5.17 / 6.31 ms | 9.13 / 8.48 / 9.08 ms |
| Recent directory | 0.96 / 1.82 ms | 1.46 / 1.62 ms | 1.73 / 2.34 ms | 1.69 / 2.54 / 3.34 ms |
| Recent pages | 1.76 / 3.50 ms | 2.20 / 2.48 ms | 2.50 / 3.14 ms | 3.33 / 3.84 / 4.77 ms |

The scan of the 8-bit table takes about as long as today's; packing is what
grows, because there are two blocks per row instead of one, and the one-digit
key packs more slowly. These are server compute times only; staging's mean
request times are about 200 ms.

### Memory (measured per table, totals scaled)

| Worker | Today | Two-digit | One-digit |
|---|---:|---:|---:|
| Archive, 186 tables (per table) | 28.2 GiB (155.0 MiB) | 33.1 GiB (182.1 MiB), +17% | 34.9 GiB (192.1 MiB), +24% |
| Recent, 55 tables (per directory / page table) | 2.6 GiB (35.0 / 59.0 MiB) | 4.0 GiB (62.1 / 86.1 MiB), +56% | 4.6 GiB (72.1 / 96.1 MiB), +76% |
| Both | 30.8 GiB | 37.1 GiB | 39.5 GiB |

The database is the same size either way: today's two-byte entries already
hold two plaintext bytes each. The increase is the packing data, which doubles
with the block count; the one-digit key's compiled matrices are stored as
64-bit integers, against 27-bit packing for the two-digit key. All three fit
the 64 GB host. Peak memory of any single table build was 0.67 GiB.

### Preprocessing (measured per table, totals scaled)

| | Today | Two-digit | One-digit |
|---|---:|---:|---:|
| Archive table build (median) | 2.34 s | 2.70–3.01 s | 2.62–2.96 s |
| Archive cold build, 186 tables one at a time | 435 s | 506 s (+16%) | 491 s (+13%) |
| Recent directory / page table build (median) | 0.69 / 0.78 s | 1.33 / 1.43 s | 1.23 / 1.49 s |
| Recent cold build, 55 tables | 41 s | 76 s | 75 s |
| Rebuild of the 8 unsealed recent tables | 5.9 s | 10.9 s | 10.4 s |

The tail rebuild after each block roughly doubles. Today's recent runtime
leaves trailing all-zero row blocks out of the hint (`batched_hint`);
`NativeServer` computes the hint over every row, so part of that gap is not
inherent to the 8-bit profile.

### Certificates (every table, 28-bit dithered queries)

| Tables | Key | Tables | Min bits | Median bits | Below 50 | Below 78 | Below 128 |
|---|---|---:|---:|---:|---:|---:|---:|
| Archive directory | two-digit | 12 | 276 | 306 | 0 | 0 | 0 |
| Archive directory | one-digit | 12 | 168 | 182.5 | 0 | 0 | 0 |
| Archive pages | two-digit | 174 | 182 | 312 | 0 | 0 | 0 |
| Archive pages | one-digit | 174 | 125 | 185 | 0 | 0 | 4 |
| Recent directory | two-digit | 25 | 743 | 771 | 0 | 0 | 0 |
| Recent directory | one-digit | 25 | 257 | 268 | 0 | 0 | 0 |
| Recent pages | two-digit | 30 | 503 | 686 | 0 | 0 | 0 |
| Recent pages | one-digit | 30 | 238 | 257.5 | 0 | 0 | 0 |

Bits are `certified_failure_bits`: a table's certified decoding-failure
probability is at most 2^-bits. Every table passes Roman's 50-bit floor for this
profile with both keys, and the 78-bit level the checker also reports. With the
two-digit key every table clears 128 bits. With the one-digit key, 4 archive
page tables fall between 125 and 128 bits. Each certificate covers this
publication's bytes under the trial seeds; another snapshot, or the seeds of a
served profile, needs its own.

## Reading

The 8-bit profile cuts the upload a wallet sends by 11–46% per table query,
most with the one-digit key, and responses by 9%. It costs 17–24% more archive
memory and 56–76% more recent memory (6–9 GiB in all, still within the host),
about 15% longer archive cold builds, about twice the recent rebuild time, and
0.3–1 ms more server time per query. Every table certifies above the 50-bit
floor with both keys.

Not covered here: the wire and client changes (a new `NativeScheme` and profile
names, a move from `IPIRServer<u16>` to `NativeServer`, decoding in the wallet
and txid clients, a wallet-libraries repin), prefix queries in `NativeServer`,
the zero-tail hint skip, certificates bound to served seeds and checked at
publication, and whole-worker load tests.
