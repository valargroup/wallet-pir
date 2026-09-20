# Schema-8 derivation, September 20, 2026

Derived sizes for the 29-record layout, and the live schema-7 reading they are
compared against. **Nothing in this directory is a schema-8 measurement.** No
schema-8 code has run on the production fleet.

| File | Contents |
|---|---|
| `enhance-init-2026-09-20.json` | `GET https://enhance-pir.valargroup.dev/v1/enhance/init`, unmodified. HTTP 200, 40,474 bytes |
| `derive.py` | The model, with every constant attributed to its source file |
| `derived.json` | Its output |

The live reading: schema 7, `ironwood-enhance-pir-v2`, 737-byte records, nine
records per 6,633-byte row, 8,192 shard rows, 65,536 logical rows, 51,918 used
rows, 467,255 Ironwood positions, seven shards, anchor height 3,489,681,
generation 3,489,681. The same document is committed as the deployment gate's
negative fixture at `enhance/ops/fixtures/enhance/init-schema7-nine-record.json`.

## What the model is accountable to

`derive.py` asserts, and fails if any stops holding:

- the three measured schema-7 sizes (430,088 upload / 10,256 response / 28,672
  public material), which match the live 40,474-byte init body and the
  [September 14 baseline](../../evidence/public-baseline-2026-09-14/README.md);
- the three benchmarked schema-8 sizes (169,992 / 30,736 / 115,992-byte init),
  from the [layout benchmark](../layout-benchmark-2026-09-20/REPORT.md);
- the 258,056-byte upload the benchmark predicted past the schema-8 boundary;
- the instance counts the server's own unit tests assert (2 at nine records,
  6 at 29, 7 at 30).

That is the whole of its evidence. It reproduces sizes; it says nothing about
latency, memory behaviour under load, or whether the fleet can serve the layout.

## Growth observed while deriving

Two readings of `ironwood_tree_size` ten hours apart -- 450,081 at height
3,489,196 (`evidence/live-pir-2026-09-20/enhance-init.json`) and 467,255 at
height 3,489,681 -- give 35.4 positions per block across 485 blocks. That is one
interval on one day, not a rate to plan capacity from, but it is enough to date
the schema-8 boundary: 475,136 positions is about 222 blocks away, four to five
hours after the reading.

## Worker residency

`worker_residency` in `derived.json` is `((shards - 1) + 9) * runtime_bytes`:
sealed shards hold one `ShardRuntime` each, the frontier holds one per retained
generation plus the unpublished candidate, and a runtime is its packed u16
database plus its partial CRS blocks. It excludes the coordinator, the
preparation slot, allocator overhead and every transient during a publish, so it
is a lower bound on what a worker needs, not an estimate of what it will use.
[Capacity expansion](../../docs/capacity-expansion.md) draws the conclusion.
