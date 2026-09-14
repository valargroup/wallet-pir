# Dataset inventory, 2026-09-08

Run: [inventory-transparent-dataset.yml run 34181566124](https://github.com/valargroup/enhance-pir/actions/runs/34181566124), tools built from commit `6ff2bfe`, executed on `enhance-pir-coordinator-01` at 2026-09-08T03:01Z. Files are the run's artifact, unedited; `SHA256SUMS` pins them.

| File | What it records |
|---|---|
| `journal.json` | `journal-inventory`: chain identity, pinned start, committed end, event count, on-disk state, hash confirmations against the node's RocksDB |
| `cutoff.json` | `shard-cutoff`: anchor block time, the six-calendar-month rule, the derived cutoff height and hashes, the scanned window |
| `spotcheck.json` | `event-spotcheck`: eleven sampled blocks re-derived from the node's verbose RPC and compared with the journal event by event |
| `coordinator.txt` | Host, memory, disks, published sets, units, binaries, filter service state on the coordinator |
| `worker.txt` | The pilot worker's unit, release, rollback, set, service state, and both public origins' map digests |

## Findings

- Journal `/srv/zakura/transparent-event-data`: mainnet genesis `00040fe8…dce08`, start height 0, committed through **3,473,686**, 3,473,687 blocks, **352,873,356 events**, 43.4 GB of events, no uncommitted tail. Start, end and genesis hashes agree with the node. The node's finalized tip at the time was 3,474,759.
- Anchor **3,473,686** = `0000000000755137ff64d560a32b9190e62ae085923c9e69930c05550f50be1d`, header time 2026-09-06T07:39:01Z. Cutoff time 2026-03-06T07:39:01Z. Cutoff height **3,262,749** = `0000000000b300ab318a0589588cbfd3fa889c38b3dc68c67519f5d05151e8d9`; block 3,262,748 is the last stamped before the cutoff time and the first crossing is also 3,262,749, so no non-monotone tail crossed the boundary. Tiers: archive `[0, 3262748]`, recent `[3262749, 3473686]`. Every publish must pass `recent_from=3262749` and the tool re-derives it.
- Spot-check: 11 blocks, 0 disagree. The three densest blocks in the journal (108,326; 220,127; 224,191, each 16–19k events dominated by spends), a seeded random sample and the last three blocks agree with the node's verbose RPC in every event field. This closes the historical output-count question: the journal's density is real and the 29.4M "transparent outputs" figure was a UTXO snapshot, not history.
- Coordinator: 8 vCPU Xeon Gold 6548N, 62 GiB (50 GiB available), `/srv/zakura` 447 GB free, root 183 GB free. Seven published sets `transparent-shards` through `-v7`; the filter service unit reads `-v7` and serves per-block filters through 3,475,763. `zakurad` active; the inventory's RPC probe omitted a content type and was refused with 415, which is a probe defect, not a node fault.
- Pilot worker `transparent-pir-worker-01`: 4 vCPU, 15 GiB, release `d1f86c3`, binary `b95baba2…`, rollback binary/unit/Caddyfile present, unit reads the flat `/srv/transparent-pir/shards` (the v7 set, `shards.json` file digest `654beb8d…`), 3 shards, `recent-8k`, cache 8 GiB, `MemoryMax=12G`, covered through 3,473,474. The served map digest `581cb0f5…` is identical at the worker loopback, the retrieval origin and the filter origin.

## Limits

This is the state at one instant. The worker serves the earlier three-shard v7 set, not the full-chain publication; the full-chain set does not exist yet. The RPC probe line in `coordinator.txt` is empty for the reason above.
