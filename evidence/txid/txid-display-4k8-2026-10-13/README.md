# Txid display on `txid-4k8` tables, 2026-10-13

The one-query txid site (`transparent-pir-txid-k8-01`, `/v2/txid`) moved from
2,048-row `txid-2k8` tables to 4,096-row `txid-4k8` tables
([design](../../../docs/txid/txid-display.md#lookup-one-row-of-one-table-per-bucket)).
Rows are unchanged: 8,192 bytes, 72 entries, the 8-bit native profile. Archives
seal at 180,000 entries and the recent window holds the newest 180,000, so each
lookup hides among twice as many transactions; the site holds about half as
many tables. Roman approved the deploy directly; the read-only trial the
[spec](../../../docs/transparent/remaining-work.md#taller-txid-display-tables-txid-4k8-deployed-2026-10-13) asked
for first was folded into it. `/v1/txid` was not touched.

## Result

| Check | Criterion | Measured | Result |
|---|---|---|---|
| Placement census | Every archive one segment | 94 archives from genesis, 180,000–180,787 entries each, all one segment; first seed fits 81 (86%), second 12, third 1; recent shard 180,011 entries, first seed | Pass |
| Certificates | Every served table at least 50 bits | 95 of 95 tables, each bound to the worker's served masks: archives min 246, median 260, max 269; recent 269. None below 128 | Pass |
| Worst-case table, before cutover | 50 bits | All-0xff synthetic `txid-4k8` table 259 bits, random 253 | Pass |
| Held memory per runtime | Planned 160.17 MiB | The live worker charges 16,123,052,544 B for 96 runtimes: 167,948,464 B = 160.17 MiB each. Process memory 16.6 GB of the 48G limit (30.0 GB on `txid-2k8`) | Matches plan |
| Recent rebuild, 4 build threads | p95 at most 10 s | p50 1.32 s, p95 1.48 s, max 1.49 s (30 builds) | Pass |
| Server time per query | Recorded | 3.30–3.42 ms p50, 3.61 ms p95 one at a time; 7.1–7.4 ms p50 two at a time | Recorded |
| Live lookups | Exact entry | 5 of 5 cold lookups through `https://transparent-pir.valargroup.dev/v2/txid`: three archives, two recent; about 1.7 s each | Pass |
| Downtime | — | `/v2/txid` down about six minutes (02:39–02:45 UTC) | — |
| Controller verify | Exit 0 | `txid-display-verify` (transaction `txid-display-verify-20261013T025435Z-6da493`) re-read the journal against the `v4` root, 02:54–03:44 UTC: exit 0, status committed, 16 min CPU, 4.0 GB peak | Pass |
| Live load, 20 lookups/s for 10 minutes | Every answer exact | 11,999 of 11,999 lookups exact, no errors, no missed slots; warm lookups p50 0.21 s, p99 0.77 s; fresh clients p50 1.56 s, p99 1.71 s; worker 0.56 cores on average | Pass |

## Placement census

[`raw/census/`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/census): the bootstrap's own seal records
(`timeline.jsonl`), its root parameters and log. This is the real-journal census
the [one-query gates](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-2k8-gates-2026-10-11/README.md) could not
run: the bootstrap placed every archive with the production rule over the
site's journal copy (`journal-v3`), heights 1–3,515,756.

| Geometry | Archives | Entries per archive | First seed | Second | Third | Fourth | Two or more segments |
|---|---:|---|---:|---:|---:|---:|---:|
| `txid-4k8`, 180,000 | 94 | 180,000–180,787 | 81 (86.2%) | 12 | 1 | 0 | 0 |
| `txid-2k8`, 90,000 (the 2026-10-12 bootstrap of the same journal) | 189 | 90,000–90,422 | 182 (96.3%) | 6 | 0 | 1 | 0 |

Both tables are 61% full. A `txid-4k8` table has twice the rows, so twice the
chances that one row overflows, and the first seed fits less often; the
2026-10-11 model put the per-table rate near 90% at 2,048 rows, which squared
is 81%. Each extra seed costs one more placement pass at bootstrap or seal.
Nothing overflowed.

## Certificates

[`certify.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/certify.sh) ran `native_certificate segment --geometry txid-4k8
--table txdirectory --public-sha256 <served>` on every table of the served map
`980a74b7…` ([raw/inputs/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/inputs); the map file's SHA-256 is that
digest), four at a time at nice 19, then ipir-sp `v0.1.0-rc.7`
`certify_native.py --require-bits 50`. [`summarize.py`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/summarize.py) checked
that every report's rebuilt masks equal the served ones and its table digest
equals the manifest's. Each certificate took 9–18 s and at most 1.39 GB.

| Tier | Tables | Min bits | Median | Max | Below 50 | Below 128 |
|---|---:|---:|---:|---:|---:|---:|
| Sealed archives | 94 | 246 | 260 | 269 | 0 | 0 |
| Recent | 1 | 269 | 269 | 269 | 0 | 0 |

The lowest were archives 26 and 60 (246) and 1 (248), against 247 for the
lowest `txid-2k8` table in the [8-bit evidence](../txid-display-p8-2026-10-13/README.md):
twice the rows cost about one bit. Every report is `p_bits` 8, 22-bit published
masks, 28-bit dithered query, 10-bit response. Per table:
[raw/results.tsv](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/results.tsv); totals: [raw/summary.json](raw/summary.json);
reports and verdicts: [raw/reports-and-verdicts.tar.gz](raw/reports-and-verdicts.tar.gz).
Before the cutover, `native_certificate synthetic --geometry txid-4k8 --fill max`
(the worst-case table) certified at 259 bits ([raw/precheck/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/precheck)).

The archives are sealed and keep these certificates. The recent shard is rebuilt
every block, so its figure is one sample.

## Memory, rebuild and query time

[`measure.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/measure.sh) on k8-01, at nice 19 and the idle IO class, beside the
live worker ([raw/worker/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/worker), [raw/residency/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/residency),
[raw/rebuild/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/rebuild), [raw/query/](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/query)).

- **Held memory.** The worker's cache charge is the measurement: 96 runtimes (95
  served and one retired recent revision) hold 16,123,052,544 B, exactly the
  planned 160.17 MiB each. Its cgroup holds 17.0 GB (peak 17.2 GB) and the
  process 16.6 GB, against 30.0 GB for 191 `txid-2k8` runtimes. `shard-residency`
  plans and charges 160.17 MiB; its later runtimes added 159.9–160.1 MiB of
  memory each, and its 179 MiB average includes the allocator settling.
- **Recent rebuild.** `hint_bench --runtime-only --geometry txid-4k8` on the
  served recent table at 4 build threads, three rounds of ten: p50 1.32 s, p95
  1.48 s, max 1.49 s, peak 0.89 GB. The `txid-2k8` rebuild on the same host was
  p50 1.20 s, p95 1.27 s. The host load average was 11–19 during these rounds
  (the last certificates and the residency run), so they are upper bounds.
- **Query time.** [`display_query_bench.rs`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/display_query_bench.rs) (a scratch
  example, not committed) builds a runtime from a served table and times
  `TableRuntime::evaluate` for 40 fresh queries; every answer decoded to its row.

| Table | One at a time p50 / p95 / max | Two at a time p50 / p95 / max |
|---|---:|---:|
| Recent, shard 94 | 3.42 / 3.61 / 4.07 ms | 7.43 / 8.88 / 9.83 ms |
| Archive, shard 50 | 3.30 / 3.61 / 3.68 ms | 7.10 / 9.25 / 9.69 ms |

A query uploads 28,168 B (21,000 at `txid-2k8`) and the reply is 10,256 B, as
planned. Server time rose about 15% over `txid-2k8`'s 2.85–2.91 ms.

## Cutover

Release `e1f94158` (the geometry; its display code equals the
`transparent-v0.1.0-rc.3` tag, `c0852aa5`), built on roman-dev-2 with the CI
release flags (bundle `8a606676…`); request [raw/request-k8-4k8.json](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/request-k8-4k8.json)
changes geometry, `archive_target`, `recent_window`, `archives`, the publication
root (`/srv/txid-display-genesis/v4/root`) and the release from the
`txid-2k8` request. The `txid-2k8` root (`v3/root`) is kept for rollback.

1. `stage` at 02:00 UTC.
2. **Bootstrap beside the live lineage, 02:02–02:38.** The site's controller
   writes its journal, and bootstrap refuses a journal being written. The
   journal appends, and moves its 16-byte checkpoint only after the data is on
   disk, so a hardlinked snapshot is a consistent read-only copy: data files
   hardlinked, checkpoint and metadata copied, sidecar directory linked. The
   bootstrap ran from it through height 3,515,756, 100 blocks below the
   snapshot's end, so no reorg could touch what it read; `txid-2k8` kept
   serving meanwhile.
3. `stop` 02:38:55, `workers --replace-active 2e4fd35d…` 02:39:47 (candidate
   shipped and verified, worker restarted 02:41:52, 96 runtimes warm by
   02:45), `route` 02:45:23, `controller` 02:46:22. The controller replayed
   from 3,515,757 and the map was at height 3,515,886 by 02:49.

[`cutover.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/cutover.sh) ran step 3 once the bootstrap exited 0.

**Bootstrap time.** The 2026-10-12 `txid-2k8` bootstrap took 67 minutes with
1,017 s of CPU: it reads the journal twice (counts, then each range), and its
reads waited on the volume one at a time (1,660 reads/s, 0.56 ms each). Two
read-only page-cache warmers cut this one to 36 minutes:
[`prefetch_sidecars.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/prefetch_sidecars.sh) read the 3.5M per-block sidecar
files, 32 at a time (14,900 reads/s); when the cache dropped them,
[`paced_ahead.py`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/paced_ahead.py) kept the next 4 GB of the event file and its
blocks' sidecars read just ahead of the bootstrap's own position. The volume
then ran at its ceiling (about 9,000 reads/s).

## Live lookups

[`raw/lookups/`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/lookups): `txid-inventory fixture` built on the coordinator
from the controller's newest candidate (map `52a31095…`) and its heights index,
never from a server answer. `txid-quality-canary` built from the
`transparent-v0.1.0-rc.3` tag looked up five entries cold through the public
origin: archives 3, 8 and 58 and two from the recent shard. All five returned
exactly the fixture's entry in 1.69–1.75 s. pir-monitor runs no txid probe, so
no deployed canary needed a rebuild.

## Live load

[`raw/load/`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/load): `txid-rate` on roman-dev-2 sent lookups to
`https://transparent-pir.valargroup.dev/v2/txid` on a fixed schedule, whatever
the service did, and checked every answer against the fixture's entry digest.
The fixture is 4,000 txids drawn at the chain's own class mix plus 200 absent
controls, built with `txid-inventory fixture --natural 2000 --absent 200 --seed
20261013` on the coordinator from the controller's candidate at 3,515,903, never
from a server answer. The mix was 80% recent and 20% archive; every 25th lookup
used a fresh client and every 20th asked an absent control. A watcher would have
stopped a window on any inexact answer or 20 errors
([`load_plan.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/load_plan.sh), [`load_run.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/load_run.sh), which it calls as
`run.sh`). [`load_sample.sh`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/load_sample.sh) sampled the worker on k8-01 every
15 s; [`summarize_load.py`](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/summarize_load.py) wrote each `summary.json`.

| Window (UTC) | Rate | Lookups | Exact | Errors | Warm p50 / p95 / p99 / max | Fresh client p50 / p99 / max | Worker CPU mean / max 15 s |
|---|---:|---:|---:|---:|---|---|---|
| V0, 03:30:00–03:31:02 | 5/s | 301 | 301 | 0 | 0.23 / 0.76 / 1.02 / 1.34 s | 1.56 / 1.84 / 1.84 s | 0.17 / 0.25 cores |
| A, 03:31:05–03:41:06 | 20/s | 11,999 | 11,999 | 0 | 0.21 / 0.57 / 0.77 / 1.43 s | 1.56 / 1.71 / 1.88 s | 0.56 / 0.99 cores |

In window A, warm archive lookups were p50 0.21 s and p99 0.81 s, recent p50
0.21 s and p99 0.75 s, and absent controls p50 0.21 s and p99 0.79 s. The HTTP
round trip of a warm query was p50 0.16 s and p99 0.32 s from roman-dev-2, so
most of a warm lookup is the network and the client's own work. A warm lookup
sends 28,451 B and receives 10,423 B. A fresh client also fetches the map,
manifest and setup: 134,155 B down and 29,107 B up. The dispatcher's lag stayed
under 10 ms at p99, and the worker's memory stayed at 17.0–17.2 GB. The map moved
seven times during A as blocks arrived.

Caveats:
- The run's cold lookups all landed on archives, because the fresh-client
  interval (25) is a multiple of five, and the tool sends every fifth lookup (sequence 0, 5, 10, …) to an archive. Fresh-client
  figures are archive figures.
- During V0, a second copy of the load, left from a launch that was stopped,
  ran for about 50 s, so V0 saw roughly 10 lookups/s. It was killed before A
  started; A ran alone.
- The first attempt failed every lookup before any request reached a table: the
  tool's example client derived the served setup seed under the history schema.
  The shipped `transparent-txid-client` does not have this bug. The fix is
  `cf6273ee` on `main`, and the run used that source (binary digest in
  [raw/load/txid-rate.sha256](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/load/txid-rate.sha256)).

## Provenance

| Item | Value |
|---|---|
| Source | Geometry `45810a9a` on `main` (built from its pre-rebase twin `e1f94158`); release tag `transparent-v0.1.0-rc.3` = `c0852aa5`. Measurement tools from `e1f94158` plus `display_query_bench.rs`, `release-fast`, `--target x86_64-unknown-linux-gnu`, `RUSTFLAGS=-C target-cpu=x86-64-v4 -C target-feature=+pclmulqdq,+aes`, rustc 1.97.1, ipir-sp `v0.1.0-rc.7`. Digests: [raw/binaries.sha256](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/raw/binaries.sha256). |
| Checker | ipir-sp `v0.1.0-rc.7` `reinspiring/tools/security/certify_native.py` `eb4e5799…dd64918`, CDF `4c6e3f51…c2266`. |
| Host | `transparent-pir-txid-k8-01`: DigitalOcean 8 vCPU Intel Xeon Gold 6548N, 62.8 GiB, Ubuntu 24.04.5, kernel 6.8.0-142, shared with the live combined worker (48G `MemoryMax`, 600% CPU quota, 32 GiB cache). |
| Bootstrap | Coordinator, transient unit at the request's bootstrap limits (8G, 400% CPU, nice 10), 954 s of CPU. |
| Commands | [snapshot.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/snapshot.sh) (hardlinks the served publication, writes `inputs/plan.tsv` after checking every table digest), [setup_fetch.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/setup_fetch.py), [certify.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/certify.sh), [summarize.py](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/summarize.py), [measure.sh](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/txid/txid-display-4k8-2026-10-13/measure.sh), in `/root/txid-4k8-measure` on k8-01; removed afterwards. |

## Not covered

Load above 20 lookups/s, and certificates of later recent revisions. Wallets
read the new tables from transparent 0.1.0-rc.3: wallet-libraries main pins it
(zakura-core/wallet-libraries#174, #175), and Vizor's v12 branch
(chainapsis/vizor-wallet#977) pins that main.
