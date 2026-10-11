# Decoupled directory/page shards: step-1 simulation (in progress)

Draft-branch prototype of the [decoupled pages design](../../docs/decoupled-pages-design.md).
**No results yet.** The simulation needs a read-only slice of the v11 journal,
which this hub cannot read: the coordinator refuses the hub's SSH key. Nothing
here has run against production data.

## Method (planned)

| Field | Value |
|---|---|
| Input | Two windows of `/srv/transparent-activity/full-v3/journal` (v3 journal, schema v11 events), cut by [slice-journal.py](slice-journal.py): `recent` 3,262,749–3,473,686 (the frozen sample's six months, cutoff to anchor) and `archive` 200,000–419,999 (contains the densest stretch of the chain, about 230k–360k at 700–2,200 events per block in the [September census](../census-2026-09-08/README.md)). Offsets are rebased; committed display sidecars (`display-v1`, `display-v2x`) are copied with their blocks. About 14 GB in total. |
| Workload | [Frozen 1,080-wallet sample](../workload-sample-2026-09-08/README.md), through [pairs.py](pairs.py) |
| Tool | `shard-census`, `release-fast`, from this branch. Coupled layouts take `--per-shard --sample`; decoupled ones take `--decoupled --directory-rows N --page-rows M --sample`. |
| Layouts | today `archive-wide`, `recent-4k-8k`; coupled 16k/64k and 8k/64k archive; bulk isolation (`--bulk-long`); decoupled 32k directory with 64k, 32k and 16k page shards; decoupled recent 4k and 8k directory with 8k page shards |

`pairs.py` is derived arithmetic over the census output, not a wallet
measurement. Its model is in its docstring: per matched (script, shard) one
directory query plus the page fragments (coupled), or one directory query plus
the chain walked newest to oldest (decoupled); filters at about 2.5 B per
script, choice tables at about 1.245 bits per script, setups per queried
table and segment, and query prices from the census's `prices` row.
