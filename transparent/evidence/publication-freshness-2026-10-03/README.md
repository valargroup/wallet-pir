# Transparent tail publication profile and compute fix, 2026-10-03

A local release-fast benchmark on the development hub. It is not a production,
fleet or freshness qualification, and nothing was deployed. Production attempt 14
remains the failed freshness observation: serial cycles of 23–25 s and burst
block visibility of 43–45 s against the unchanged 30 s target.
[manifest.json](manifest.json) records sources, binaries, commands, workload and
host; [host.json](host.json) records the shared 8-vCPU DO-Premium-Intel hub
(no AVX-512 or SHA-NI, other tenants active).

## Workload

`publication_bench` (committed under the filter server's examples) replays the
retained [mainnet day](../baselines/mainnet-study/mainnet-day.jsonl.gz) into a
v3 journal as consecutive copies. Later copies get new txids and give about 70%
of scripts a new identity. Fees are exact where every input value is known. The
result is representative synthetic data derived from real blocks, not a replay of
production's tail. One sealed `recent-8k` shard then holds 9,416 blocks, 80,369
scripts, 7,936 page rows and 573,493 events. That is within the v10 census range
for real recent shards: 57k–88k scripts and 604k–648k events.

Each measured cycle appends a block and then does what the controller does:
- copies the journal snapshot;
- publishes the grown tail beside the previous publication;
- reopens the candidate's filters.

Directory choice tables are on and txid display is off. The tail fills tested
are 24%, 66% and 97% of the page-row target. After the cycles, the tail's
directory and pages runtimes are built in the runtime build pool with two
threads, the default on a 4-vCPU worker.

## Result

The baseline (`4c85b6c2`) and final (`921e1642`) binaries alternated twice
per fill, three cycles each ([runs](runs/), [summary](summary.json)):

| Tail fill | Baseline publish, median (range) | Final publish, median (range) | Change |
|---|---|---|---|
| 24% (1,919 rows, 23.6k scripts) | 2.51 s (2.42–2.70) | 1.30 s (1.23–1.79) | −48% |
| 66% (5,266 rows, 56.2k scripts) | 6.45 s (5.97–8.11) | 2.68 s (2.54–3.04) | −58% |
| 97% (7,715 rows, 78.1k scripts) | 9.67 s (8.99–11.23) | 3.78 s (3.63–4.33) | −61% |

The 97% baseline is close to production's measured 9.781–10.175 s native
publication. That suggests the workload is representative; it is not proof.

Every one of the 80 files in each fill's working tree is byte-identical between
baseline and final ([digests](published-trees.json)). That covers the journal,
the base publication with its archive shard, and every candidate: maps,
manifests, filters, directory, page and choice tables. The map digests at every
height and the runtime public-parameter digests also match.

### Where the time went

At 97% fill, with temporary phase timers ([instrument.py](instrument.py), never
committed; [baseline](profile/phase-base.log), [final](profile/phase-final.log)):

| Phase | Baseline | Final |
|---|---|---|
| Sealer replay of the whole tail | 5.59–6.08 s | 0.97–1.09 s |
| `build_shard` (filter, pages, directory, choice) | 2.11–2.20 s | 1.70–1.98 s |
| Manifest table digests | 0.66–0.73 s | 0.33–0.36 s |
| Writes | 0.16–0.17 s | 0.16–0.19 s |

The sealer replays every tail block at every publication. For each block it
computed the exact best-fit page-row count twice, over cloned BTreeMaps. That
computation is now a dense count over the bounded free-space values, with the
map form kept as the definition and fallback. The demand is projected in place
and restored only if the shard closes first, so the count runs once per block.
Seal decisions are unchanged.

Retention of superseded tails parsed every previous manifest once per map entry.
At [census scale](retention-probe.jsonl), with 86 shards and 3.87 MB of
manifests, that took 0.23–0.33 s; reading each manifest once takes 0.003–0.004 s.
The small benchmark set does not show this.

### Worker runtime

Native construction is external code and unchanged. A partly filled tail's page
table ends in all-zero rows, which add nothing to the hint. The runtime now
leaves trailing all-zero 2,048-row blocks out of the hint product; the published
masks are identical ([stages](runtime-stages.json), medians):

| Fill | Pages hint, baseline | Pages hint, final | Blocks used |
|---|---|---|---|
| 24% | 1.125 s | 0.445 s | 1 of 4 |
| 66% | 1.349 s | 0.988 s | 3 of 4 |
| 97% | 1.121 s | 1.120 s | 4 of 4 |

Directory tables fill every row block, so their hint is unchanged. The other
stages are unchanged within host noise: encoding takes about 0.1 s and two-mask
preprocessing 1.1–1.7 s per table. Whole-build totals in [summary.json](summary.json)
vary by up to 1.6 s between runs because of host load.

## Reproduce

In a working directory, decompress the day to `day.jsonl`. Build
`publication_bench` (release-fast) at each source, copied to
`bin/publication_bench.base` and `bin/publication_bench.final`. Then run
[compare.sh](compare.sh) and `python3 summarize.py cmp`. Phase timers come from
[instrument.py](instrument.py) applied to a checkout before building.

## Rejected and failed attempts

- **Reusing the previous revision's hint or rows.** Consecutive tail revisions
  share no nonzero row in either table ([row stability](row-stability.json)),
  because script tags are salted by the endpoint hash.
- **A two-level free-space bitmap in the packer.** It saved 0.2–0.3 s at 97%
  fill, inside host noise, so it was reverted.
- **First packer equivalence test.** It used an impossible size above one row,
  which also divides by zero in the reference; the case now uses an out-of-range
  free space.
- **First sealer fixture.** It never paged during a close before an
  over-capacity block. A mutation removing the demand restore passed until the
  fixture was strengthened; it now fails.

## Projection and limits

The following is arithmetic, not a measurement. Suppose production scales as the
97% bench did. Native publication would then take about 3.8–4.2 s, and cycles
about 17–19 s instead of 23–25 s. Burst visibility was about 1.8 cycles in
attempt 14, so that would give roughly 31–34 s. That is still above the 30 s
target for full tails.

The pages-hint saving depends on fill and on worker speed. Here it is 0–0.7 s;
production workers prewarm roughly twice as slowly.

The remaining cycle is dominated by the recent workers' prewarm: two sequential
native tail builds, about 10–11 s in production, under `build_slots` 1 and
mandatory table verification. Neither was changed. Changing build slots or
threads, or overlapping publication with preparation, would be a capacity or
pipeline decision. Either one needs its own memory, reorg and cancellation
evidence.

Other limits:
- One host was used, with other tenants active.
- Txid display was not exercised, although the changed code is display-agnostic.
- The production controller and workers may differ in CPU features.

Freshness acceptance remains open until a deployment is measured.
