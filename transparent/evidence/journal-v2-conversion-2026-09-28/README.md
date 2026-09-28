# Version-2 event journal derived on production, 2026-09-28

Schema `transparent-shard-v9` changed the event record from 96 to 87 bytes and
bumped the event journal to version 2. The v9 binaries refuse the version-1
production journal. This run built a version-2 journal in a new directory on
the coordinator. It did not touch the version-1 journal, change any service, or
publish anything. The live publisher kept appending to the version-1 journal
throughout.

Source `b693cfde`. Metadata, binaries and exact commands are in
[manifest.json](manifest.json).

## Method

1. `journal-convert-v1` re-encoded the committed version-1 prefix through
   height 3,498,196 (1,000 blocks below the committed tip) in 3 minutes 4
   seconds. It decodes each record with the frozen v1 codec, encodes it with the
   current codec, and keeps scripts, order and block hashes. The bin's tests pin
   its output byte for byte to what `EventStore` writes for the same blocks.
2. `transparent-event-ingest` extended the result from the node's state
   database (2 blocks, up to the finalized height) and then over RPC to
   3,499,198, the node's tip when the run began. Both runs reconcile the
   journal's last block against the node on start.

## Results

| Check | Result |
|---|---|
| Conversion ([convert.json](convert.json)) | 3,498,197 blocks, 353,766,447 events; 43,477,252,777 source bytes to 40,293,354,754 (−9 bytes per event exactly) |
| Final journal ([journal.json](journal.json)) | Heights 0–3,499,198, 353,790,708 events, 40,296,116,882 event bytes; start and tip hashes agree with the node; no uncommitted bytes |
| Byte-exact cross-check ([xcheck.jsonl](xcheck.jsonl), [journal_cmp.py](journal_cmp.py)) | Fresh v9 ingests of 5 × 5,000 blocks (from 200,000, 1,000,000, 2,000,000, 3,000,000, 3,493,000) are identical to the converted journal: hashes, counts and event bytes, 3,494,437 events |
| Independent extraction ([spotcheck.json](spotcheck.json)) | 11 blocks (3 densest, 5 seeded random, last 3) compared as event multisets from verbose node JSON; 0 disagreements |

A first `journal-inventory` attempt with `--state-dir` failed with
`MissingBlock(3499198)`. The state database holds only finalized blocks, so it
cannot confirm the journal's tip. The retained run used RPC.

## Limits

- The version-2 journal is a snapshot at height 3,499,198. Nothing appends to
  it until a v9 publisher is pointed at it. Extending it again costs one RPC
  ingest over the blocks since that height.
- The cross-check and spot check are samples. Completeness of the whole range
  rests on the conversion being a pure re-encoding of the version-1 journal,
  whose own provenance is in the [dataset inventory](../inventory-2026-09-08/README.md).
