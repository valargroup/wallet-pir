# Txid display tables at the 8-bit profile, 2026-10-13

Certificates and costs of the one-query txid display (`txid-2k8`) at the 8-bit
native profile, taken on the live txid site `transparent-pir-txid-k8-01` right
after it began serving that profile (release `3e705bb3`). Every table of one
served map was certified against the masks the worker actually serves, and one
runtime's memory, rebuild time and query time were measured on the same host.

**This is a read-only measurement.** It ran beside the live worker at nice 19 and
the idle IO class, in a scratch directory that was removed afterwards. No
service, unit or configuration changed.

## Result

| Gate | Criterion | Measured | Result |
|---|---|---|---|
| Certificates | Every served table at least 50 certified failure bits | 190 of 190 tables: archives min 247, median 266, max 277 (189 tables); recent 258 (1 table). None below 78 or 128. Every report's rebuilt masks equal the worker's served masks | Pass |
| Recent rebuild, one table at 4 build threads | p95 at most 10 s | p50 1.20 s, p95 1.27 s, max 1.40 s (30 builds) | Pass |
| Held memory per runtime | Planned 144.17 MiB (151,171,248 B) | The live worker charges exactly 151,171,248 B per runtime (191 runtimes, 28,873,708,368 B); its whole process RSS divided by 191 is 149.6 MiB; the residency tool's steady increments are 144.0–144.25 MiB | Matches plan |
| Server time per query | No gate; recorded | 2.85–2.91 ms p50, 3.18–3.28 ms p95 one at a time; 6.1–6.4 ms p50 two at a time | Recorded |

## Provenance

| Item | Value |
|---|---|
| Tables | Served map `633d46af77efe287…` ([raw/inputs/txid-shards.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/inputs/txid-shards.json), whose SHA-256 is the map digest), the worker's active publication at 01:14 UTC. 189 sealed archives (shards 0–188, heights 1–3,506,111, 90,000–90,422 entries each) and the recent shard 189 (heights 3,499,159–3,515,819, 90,011 entries; its window reaches back into the newest archive). `txid-2k8` geometry, one bucket, one 2,048 × 8,192 B segment (16 MiB) per table. Copied from `/srv/transparent-txid-display/publications/<map>/` to scratch at 01:14:06–01:14:11 UTC; every manifest digest and segment SHA-256 matched the map ([raw/inputs/plan.tsv](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/inputs/plan.tsv)). |
| Served masks | `GET /v2/txid/{archive,recent}/shards/<id>/revisions/<digest>/setup/directory-0/0` on the worker for all 190 tables ([raw/inputs/served-setup.tsv](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/inputs/served-setup.tsv)); each certificate was run with `--public-sha256` of that response and refuses to report if its rebuilt masks differ. |
| Source | The 8-bit tree synced to `roman-dev-2:/root/wallet-pir-txid-p8` (no Git metadata). Its profile, display, runtime and residency sources equal `c50c91dd`; `3e705bb3`, the deployed release, differs from them only by formatting (`270ef60b`). `release-fast`, `--target x86_64-unknown-linux-gnu`, `RUSTFLAGS=-C target-cpu=x86-64-v4 -C target-feature=+pclmulqdq,+aes` (the repository's `target-cpu=native` would have targeted the build host, which lacks AVX-512; the kernels pick AVX-512 at run time either way), rustc 1.97.1, ipir-sp and reinspiring `v0.1.0-rc.7` (`35069f1e`). Binary digests: [raw/binaries.sha256](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/binaries.sha256). |
| Tools | `native_certificate segment --geometry txid-2k8 --table txdirectory`, with the one-line fix in [native_certificate-screens.patch](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/native_certificate-screens.patch) (see Failed first run); the same fix is in the source from the commit that adds this directory; `shard-residency`; `hint_bench --runtime-only`; and [p8_query_bench.rs](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/p8_query_bench.rs), a scratch example written for this run and not committed. |
| Checker | ipir-sp `v0.1.0-rc.7` `reinspiring/tools/security/certify_native.py`, SHA-256 `eb4e5799…dd64918`, with its `src/native_gaussian_cdf.txt` (`4c6e3f51…c2266`), run as `certify_native.py --require-bits 50 <report>`. |
| Host | `transparent-pir-txid-k8-01`: DigitalOcean 8 vCPU Intel Xeon Gold 6548N (AVX-512 VNNI and VBMI), 62.8 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, shared with the live combined txid worker (`MemoryMax` 48G, `CPUQuota` 600%, cache budget 32 GiB) serving 190 runtimes; no released wallet uses the site. Host load average 0.5–2.6 during the timings. |
| Commands | [measure.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/measure.sh), then [setup_fetch.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/setup_fetch.py) and [certify.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/certify.sh), then [summarize.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/summarize.py). They ran in `/root/txid-p8-measure` on the host, where `inputs/` is what is here [raw/inputs/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/inputs). |

## Certificates

`native_certificate` rebuilds each table's hint and two-mask preprocessing from
the segment bytes under display's native schema (`transparent-txid-display-p8-v1`)
and the served seeds, checks the published masks against the served digest, and
writes the 28-bit dithered report the 8-bit profile uses. Four ran at a time;
each took 11.7–16.7 s (median 13.9 s) and at most 1.38 GB of memory. At the
spot checks taken during the run, host memory in use, worker included, was at
most 35,057 MiB (34.2 GiB, 55%) of 62.8 GiB.

| Tier | Tables | Min bits | Median bits | Max bits | Below 50 | Below 78 | Below 128 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sealed archives | 189 | 247 | 266 | 277 | 0 | 0 | 0 |
| Recent | 1 | 258 | 258 | 258 | 0 | 0 | 0 |

Every report is `p_bits` 8, one-digit 27-bit gadget, 22-bit published masks,
28-bit dithered query, 10-bit response. The lowest three were archives 160
(247), 109 (248) and 20 (250). Per table: [raw/results.tsv](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/results.tsv);
totals: [raw/summary.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/summary.json); every report and verdict:
[raw/reports-and-verdicts.tar.gz](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/reports-and-verdicts.tar.gz).

A certificate covers that snapshot's bytes. The archives are sealed and keep
these certificates; the recent shard is rebuilt every block, so its figure is
one sample of a table that changes.

### Failed first run

The first certification run failed on all 16 tables it reached, before any
report was written: `native_certificate` panicked with `selected precision
screen` ([raw/failed-first-run/native.log](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/failed-first-run/native.log)).
For each table it had already passed the served-mask check. The tool called
reinspiring's `build_two_mask_analyzed`, whose noise analysis only screens
published-mask widths 27–32 bits, so the 8-bit profile's 22-bit masks had no
screen to select. ipir-sp's own 8-bit analysis screens 16–32 bits for these
profiles. The patch passes that range for 8-bit tables and keeps the old one
otherwise; it changes which widths are analysed, not how. The patched binary
is `76e3f487…`; the failed one was `fd665179…`
([raw/build-binaries.sha256](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/build-binaries.sha256)). **The tool in the
synced tree still has this bug and needs the same fix before it is committed.**

## Held memory

The live worker is itself the measurement. Its `/metrics` and systemd
accounting, read before and after the timings ([raw/worker/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/worker)):

| | Measured | Per runtime |
|---|---:|---:|
| Cache charge, 191 runtimes (190 served + 1 retired recent) | 28,873,708,368 B | 151,171,248 B = 144.17 MiB, exactly the plan |
| Process RSS | 29,967,912,960 B | 149.6 MiB, including about 1.0 GiB of process overhead |
| cgroup `MemoryCurrent` (peak since start) | 30.02 GB = 27.96 GiB (28.30 GiB) | 58% of the 48G `MemoryMax` |

`shard-residency --geometry txid-2k8 --table txdirectory --runtimes 12`
([raw/residency/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/residency/txid-2k8-txdirectory-12.txt)) plans
144.17 MiB and reserves 144.17 MiB per runtime. Runtimes 8, 11 and 12 each
added 144.00–144.25 MiB of RSS; the others ranged 72–201 MiB as the allocator
settled, so its 162.5 MiB slope over 12 runtimes overstates the steady cost.
The live worker's figure is the one to use.

Against the 16-bit `txid-2k8` runtime of the
[2026-10-11 gates](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-2k8-gates-2026-10-11/README.md#runtime-memory)
(80.04 MiB measured, 80.09 MiB planned; 15.2 GiB planned for 191 runtimes),
an 8-bit runtime holds 1.80× as much: 27.96 GiB for the same 191. The table
bytes are the same; the growth is packing data, as the
[8-bit trial](../../transparent/p8-trial-2026-10-12/README.md#memory-measured-per-table-totals-scaled)
found for history tables.

## Recent rebuild

`hint_bench --runtime-only --geometry txid-2k8 --repeats 10
txdirectory=<recent table>` with `TRANSPARENT_BUILD_THREADS=4`, three rounds
([raw/rebuild/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/rebuild)), on the served recent table rather than a
synthetic one. This is `TableRuntime::build`, what the worker runs for each new
recent revision.

| | n | p50 | p95 | max | mean | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| 8-bit, this host | 30 | 1.199 s | 1.271 s | 1.400 s | 1.212 s | 849–862 MB |
| 16-bit, 2026-10-11 gates (other host) | 30 | 2.359 s | 3.079 s | 3.247 s | 2.537 s | 747–778 MB |

The 16-bit figures came from a development worker capped at 3 CPUs, so the two
rows are not like for like; on this host the 8-bit build is well inside the
10 s gate. `hint_bench` derives its parameters under the history schema, so its
masks (`7b6c0bc9…`) are not the served ones; the work has the same shape.
`p8_query_bench`, which uses display's schema, built the same table in 1.35 s
and reproduced the served masks exactly. The runtime's build threads lower
themselves to nice 10 whatever the parent's niceness, as in the worker.

## Server time per query

[`p8_query_bench`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/p8_query_bench.rs) builds one runtime from a served table,
then times `TableRuntime::evaluate` (parse plus answer, the worker's per-query
work) for 40 fresh client queries one at a time and 40 two at a time, on the
default 8-thread query pool. Every answer decoded to the table's row
([raw/query/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-p8-2026-10-13/raw/query)).

| Table | One at a time p50 / p95 / max | Two at a time p50 / p95 / max |
|---|---:|---:|
| Recent, shard 189 | 2.85 / 3.28 / 4.43 ms | 6.41 / 7.45 / 8.08 ms |
| Archive, shard 100 | 2.91 / 3.18 / 3.33 ms | 6.11 / 6.97 / 9.23 ms |

A query is 21,000 B up and 10,256 B down, against 38,920 B and 11,280 B for one
16-bit `txid-2k8` query in the 2026-10-11 demo. These are server compute times
only. No 16-bit query timing was taken on this host, so there is no like-for-like
comparison here.

## Not covered

Load tests of the whole worker, client latency, and certificates of later maps.
The recent shard's certificate is one sample; a check at publication would cover
every revision.
