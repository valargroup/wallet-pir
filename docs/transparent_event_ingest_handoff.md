# Transparent event ingest: handoff

Date: 2026-09-07. Status: running on the state path; the count question is
closed.

## What is running

A genesis-to-tip backfill of the transparent event journal, on the coordinator,
as a transient systemd unit named `transparent-event-ingest`.

| | |
|---|---|
| journal | `/srv/zakura/transparent-event-data`, pinned to **start height 0** |
| binary | `/usr/local/bin/transparent-event-ingest`, built from `7ad1100` |
| source | the node's own RocksDB at `/root/.cache/zakura`, opened read-only |
| target | height 3,473,686 (the finalized tip the run could see when it began) |
| at handoff | height ~365,000, 174.5M events stored |
| rate | ~160 blocks/s over the early chain; hours, not weeks |
| control | `.github/workflows/backfill-transparent-events.yml`, actions `status`, `stop`, `start` |

The workflow is the sanctioned interface and `status` is read-only, so it is
safe to run at any time. SSH to the coordinator also works, from an address the
`enhance-pir-coordinator` DigitalOcean firewall admits.

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
`store.next_height()`, which is the last committed height plus one.
Interrupting is safe: coverage advances only at a commit, every 1,000 blocks.

## Resuming after a stop

```
gh workflow run backfill-transparent-events.yml --ref main \
  -f action=stop
gh workflow run backfill-transparent-events.yml --ref main \
  -f action=start -f ref=<sha on main> -f start_height=0 \
  -f state_dir=/root/.cache/zakura -f workers=6
```

`start` refuses while a unit is active, so `stop` first. It rebuilds the binary
from `ref` before launching, so the SHA decides what runs. **Omitting
`state_dir` selects the old RPC path**, which is roughly a hundred times slower;
it is kept only so an existing dispatch does not change meaning.

## What was changed, and what it bought

The backfill used to read the chain over Zakura's JSON-RPC: `getblock` per
height, then `getrawtransaction` for every previous output an input spends. At
height 344,000 that was 387 lookups a block. The node answers all of them out of
its own transaction index, so the work was already a database read, and the RPC
added a round trip, a hex encoding and a re-parse to it.

`7ad1100` reads the database instead. `zakura_state::init_read_only` opens the
same RocksDB as a **secondary instance**, so a running node is undisturbed, and
resolving a previous output becomes two local point lookups through
`tx_loc_by_hash` and `tx_by_loc`. The output cache, its batching pre-pass and
the pipelined fetch are all gone — they existed only to hide the round trip.

Removing the cache also removed the reason blocks had to be handled one at a
time. Nothing carries from one height to the next now, so `--workers` heights
extract at once and append in order.

| | RPC path | state path |
|---|---:|---:|
| blocks/s, around height 360,000 | 1.5 | ~160 |
| resolution | 387 RPC lookups/block | local database lookups |
| resident memory | ~530 MB peak | ~1.3 GB peak |

Measured on the coordinator, `workers=6`, under the same `CPUWeight=20`. Host
load *fell* across the cutover, from 3.44 to 1.74: the old path spent its time
waiting.

Do not read a single rate as the finishing time. Throughput tracks block
density, not height: 1,000 blocks around 356,000 took 4 seconds and 1,000
around 362,000 took 6, because the second thousand held nearly three times the
events. Density rises a long way toward the tip, so the run will slow. The
useful claim is the change of scale — weeks to hours — not a number of hours.

**Verified against the old journal before cutting over.** A scratch run over
heights 340,000-341,000 produced the same block hashes, the same event counts
and the same event bytes as the RPC-built journal — 1,000 blocks, 1,022,211
events, byte for byte. The two paths share `extract_events`, so what this
checks is the source, not the extraction.

Rollback: `/root/transparent-event-ingest.rpc-path.bak` is the binary the first
344,000 blocks were built by.

### What the state path deliberately does not read

Zakura also maintains `tx_loc_by_transparent_addr_loc`, keyed by address
location and transaction location — already grouped by address and ordered by
height, which is the shape a shard wants. It is not usable as the event source.
That index is built with `filter_map(|utxo| utxo.output.address(network))`, so
it holds only P2PKH and P2SH, while `extract.rs` indexes every nonempty output
script that does not begin with `OP_RETURN`, raw. Sourcing events from it would
silently drop every nonstandard script, and a wallet told it has no activity
does not look again.

## Building it

`zakura-state` is pinned to `1faf150fc3648aae22c55a6b30f8f5a9b9ce934e`, the same
revision as `zakura-chain` — and, not by coincidence, the revision the deployed
`zakurad 1.3.0+g1faf150fc364` was built from. **That pin must track the node.**
This code reads the on-disk format that node wrote; a node upgrade that changes
the format needs this pin moved with it.

The build needs `libclang-dev` for rocksdb's bindgen. It is installed on the
coordinator now; a fresh host needs it before the workflow's build step will
succeed.

## Where the time goes now

Unprofiled, and at this scale it no longer dominates anything. If it ever does:
the work per block is a database read per spent output plus the parse, which is
why throughput follows density. `--workers` is the knob, held at 6 rather than
the host's 8 cores because the coordinator also answers live PIR queries.

One property to keep in mind near the tip: a secondary instance sees only what
the primary has flushed, so the visible finalized tip trails the node's, and the
non-finalized state is invisible. For a backfill of buried history that is
irrelevant. A height the secondary cannot see yet surfaces as a missing-block
error rather than as a wrong answer.

## What was unresolved, and now is not

**The journal's event count was checked against the node and is correct.**
Comparing `blocks.bin` records with the node's verbose `getblock` — an
independent path from the ingester's raw parse — at three heights:

| height | outputs | non-coinbase inputs | node total | journal |
|---|---:|---:|---:|---:|
| 106,500 | 803 | 13,305 | 14,108 | 14,108 |
| 250,000 | 963 | 461 | 1,424 | 1,424 |
| 340,000 | 961 | 4,614 | 5,575 | 5,575 |

Exact at all three, and the mechanism is visible: height 106,500 carries 13,305
non-coinbase inputs across 65 transactions. Events are dominated by **inputs**,
which is why an outputs-only figure cannot bound them.

So `transparent_pir_design.md:288`'s 29,409,580 transparent outputs is not a
chain-cumulative count and should not be quoted as one. It reads as a UTXO
snapshot — it says 95.32% unspent — which is what a live set looks like, not a
history. The repo's other, measured figure (288,426,459 address-transaction
associations, `transparent_pir_mainnet_study.md:15`) is the one the journal
agrees with.

Sizing derived from this journal is sound. Two caveats worth keeping: this
checks event *counts*, not script bytes; and it is three heights, not a proof.

Provenance is settled too. The journal's `meta.json` carries mainnet genesis
`00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08`. The census
still does not print it, and printing it would make this checkable from the
output rather than by reading the file.

## Related work in flight

The geometry decision this journal feeds is written up in
[genesis-geometry-notes.md](transparent-pir-evaluation/shard-utilisation/genesis-geometry-notes.md).
It is not settled: the wallet-cost input arrived late and argues for wider
shards than the note recommends. Nothing should be published until the
end-to-end measurement runs.

That decision is now much cheaper to defer. Re-running the journal after a
geometry change used to be a 25-day commitment; it is an afternoon.

## Leftovers on the coordinator

- `/srv/zakura/state-ingest-check` — the scratch journal from the equivalence
  check. Safe to delete.
- `/root/state-ingest-build` — the tree the running binary was built from.
  Delete once the change has landed on main and the workflow can rebuild it.
