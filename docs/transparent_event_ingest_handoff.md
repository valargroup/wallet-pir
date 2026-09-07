# Transparent event ingest: handoff

Date: 2026-09-07. Status: running, optimized once, bottleneck moved.

## What is running

A genesis-to-tip backfill of the transparent event journal, on the coordinator,
as a transient systemd unit named `transparent-event-ingest`.

| | |
|---|---|
| journal | `/srv/zakura/transparent-event-data`, pinned to **start height 0** |
| binary | `/usr/local/bin/transparent-event-ingest`, built from `1e507be` |
| target | height 3,474,631 (the node's tip when the run began) |
| at handoff | height ~341,000, 161.8M events stored |
| rate | 1.46 blocks/s, roughly 25 days remaining |
| control | `.github/workflows/backfill-transparent-events.yml`, actions `status`, `stop`, `start` |

The coordinator answers no SSH from a developer machine; the workflow is the
only interface. `status` is read-only and safe to run at any time.

## The one way to destroy this journal

`EventStore::open` takes a start height and **sets the journal aside and begins a
new one** if it differs from the pinned value (`events.rs:101-130`). The
workflow's `start_height` input defaults to **3,428,143** (Ironwood). This
journal is pinned to **0**. Dispatching `start` without overriding that input
would silently discard the whole backfill — no error, just a `superseded-*`
directory and a run that begins again.

`1e507be` added a guard: the start step reads `meta.json` and refuses a
mismatch. Do not remove it. To resume, always pass `start_height=0`.

`start_height` is **not** where indexing begins. The loop resumes from
`store.next_height()`, which is the last committed height plus one
(`event-ingest.rs:146`). Interrupting is safe: coverage advances only at a
commit, every 1,000 blocks.

## Resuming after a stop

```
gh workflow run backfill-transparent-events.yml --ref main \
  -f action=stop
gh workflow run backfill-transparent-events.yml --ref main \
  -f action=start -f ref=<sha on main> -f start_height=0
```

`start` refuses while a unit is active, so `stop` first. It rebuilds the binary
from `ref` before launching, so the SHA decides what runs.

## What was changed, and what it bought

Measured at height ~340,000, where a block took 1.34 s and needed 907
previous-output lookups against 250 cache hits.

| Change | Where | Why |
|---|---|---|
| `PREVOUT_BATCH` 16 → 256 | `prevout.rs:27` | 907 lookups at a batch of 16 is 57 sequential round trips for one block. The node does the same work either way; the batch removes the waiting. The old value matched a Python collector's, which aligned two tools without either measuring. |
| `DEFAULT_CACHE_OUTPUTS` 2M → 8M | `prevout.rs:50` | 2M outputs is a window of ~8,700 blocks at this density, so a spend reaching further back paid RPC — which is why only 22% of lookups hit. Also now settable per run via the `cache_outputs` workflow input, because the cost of guessing high is the OOM killer and that has already happened once over this range. |
| Pipelined block fetch | `ingest.rs`, `event-ingest.rs` | The next block is fetched while the current one is resolved. Fetching is the only part of a block's work that reads no cache state, so it is the only part that can run ahead. |

Result, comparing the last old-binary interval with the first new one:

| | before | after |
|---|---:|---:|
| minutes per 1,000 blocks | 21.6 | 11.4 |
| blocks/s | 0.77 | 1.46 |
| RPC lookups per block | 907 | 484 |
| cache hits per block | 250 | 730 |
| cache hit rate | 21.6% | 60.1% |
| remaining | ~47 days | ~25 days |

One interval each, on a host that also serves PIR queries under `CPUWeight=20`.
Treat 1.9x as indicative, not as a benchmark.

## Where the time goes now

Round trips are no longer the constraint: 484 lookups at a batch of 256 is about
two per block. What remains is **volume** — the node reads 484 raw transactions
per block from its index, and at 0.68 s a block that plausibly dominates.

Next levers, in the order worth trying:

1. **A larger cache.** 8M outputs is a ~35,000-block window. Scripts are reused
   heavily on this chain — mean 27 shards per script, max 2,748 — so spends
   reach far back. 32M outputs is roughly 4 GB and about a 140,000-block window.
   Check the coordinator's free memory first; a transaction-bounded cache was
   OOM-killed over this range before, which is why the bound is outputs now.
   Try it with `cache_outputs=32000000` rather than by changing the default.
2. **Concurrent batches.** `prefetch_previous_outputs` walks chunks
   sequentially (`prevout.rs:208`). At two batches a block this is small now,
   but it grows again if the cache is not enlarged.
3. **Measure before more.** Nothing here has been profiled on the host. The
   claim that node fetch dominates is inference from counters, not a profile.

## What is unresolved, and matters more than the speed

**The event count is unexplained by one of the repo's own figures.** The journal
holds 161.8M events by height 341,000. `transparent_pir_design.md:288` records
29,409,580 transparent outputs created at height 3,471,419 — for the *whole*
chain — which cannot coexist with this. That figure is labelled there as copied
and not independently verified, and it contradicts the repo's other, measured,
figure: 288,426,459 address-transaction associations genesis to 3,471,098
(`transparent_pir_mainnet_study.md:15`), which the journal *is* consistent with.

What has been checked:

- extraction emits exactly one event per output and one per non-coinbase input,
  with no nesting (`extract.rs:83-170`) — read line by line;
- the events are really stored, not a stale counter: the census reaches the same
  total by walking every height;
- distinct indexable scripts are **2,444,894**, under the study's 9,254,567
  chain-wide addresses, so nothing is duplicating scripts;
- `prevout.rs:24` independently records early-chain coinbases carrying thousands
  of outputs, which is the mechanism for 450 events a block.

The 29.4M figure also reads like a UTXO snapshot rather than a history: it says
95.32% of outputs are unspent, which is what a live set looks like, not a
cumulative record.

**What would settle it:** count the transparent outputs of one real block from
the density burst near height 106,500 — where the census reports 2,171 events a
block — against an explorer or `getblock` directly. If it has hundreds of
outputs, the journal is right and `transparent_pir_design.md` should stop
quoting 29.4M without a caveat. If it has a dozen, the journal is wrong and
every sizing figure derived from it is void. **Do this before anything is
published from this journal.**

Also unreported by the census: the journal's network and genesis hash.
`EventStore` holds both. Printing them would make provenance checkable from the
output rather than inferred.

## Related work in flight

The geometry decision this journal feeds is written up in
[genesis-geometry-notes.md](transparent-pir-evaluation/shard-utilisation/genesis-geometry-notes.md).
It is not settled: the wallet-cost input arrived late and argues for wider
shards than the note recommends. Nothing should be published until the
end-to-end measurement runs.
