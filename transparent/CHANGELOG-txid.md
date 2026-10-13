# Transparent txid display changelog

Covers private lookups of a transaction by txid: the `transparent-txid-client`
crate, the display tables that `transparent-filter-server`'s txid display
controller publishes, and the display service in `transparent-shard-server`. History
recovery has its own [changelog](CHANGELOG.md). Both are released under one tag.

## 0.1.0-rc.3 — 2026-10-13

Taller display tables. Wallets reading `/v2/txid` need this release: the
service now publishes `txid-4k8` tables, and 0.1.0-rc.2 clients refuse a map
naming a geometry they do not know.

- **`txid-4k8` geometry.** 4,096 rows of the same 8,192-byte, 72-entry rows,
  so a table holds about 180,000 entries instead of 90,000 at the same 61%
  fill. Archives seal at 180,000 entries and the recent window holds the
  newest 180,000, so every lookup hides among twice as many transactions.
- **Bytes per lookup.** A query uploads 28,168 bytes instead of 21,000;
  replies (10,256 bytes per segment) and setups do not change.
- **Server memory.** A runtime holds 160.17 MiB instead of 144.17 MiB, with
  about half as many tables: about 15.2 GiB from genesis instead of 26.9 GiB.
- `txid-2k8` stays in the registry, so a deployment can still serve it.

## 0.1.0-rc.2 — 2026-10-12

First release published to crates.io, so wallets can depend on registry
versions (`=0.1.0-rc.2`) instead of a git tag. No behaviour changes from
0.1.0-rc.1.

- The ipir-sp family now comes from crates.io at the same versions:
  `ipir-sp 0.1.0-rc.7`, `reinspiring 0.1.3-rc.1`. A workspace can hold only one
  `ipir-sp 0.1.0-rc.*` from crates.io, so anything else in it must use rc.7 too.
- The shared helpers these crates need, `pir-native`, `pir-control` and
  `pir-observability`, are published at 0.1.0.

## 0.1.0-rc.1 — 2026-10-12

First tagged release. Earlier wallet-libraries builds pinned a bare revision;
this candidate's code matches the last such pin (`3e705bb3`), so moving to the
tag changes no behaviour, only the version requirement (`=0.1.0-rc.1`).

- **One query per lookup.** Display tables are `txid-2k8`: 2,048 rows of
  8,192 bytes, 72 entries a row. The publisher places each txid in exactly one
  row, using the smallest per-bucket seed that leaves no row over capacity, so
  a lookup sends a single query. The client checks every decoded entry sits in
  the row its seed assigns.
- **Served at `/v2/txid`** on the same origin as history. Archives hold
  sealed heights; the recent shard is a sliding window of the newest txids.
  The client routes by the map's first unsealed height.
- **8-bit native profile.** Display uses the same one-digit 8-bit profile as
  history: 28-bit dithered queries only and 10-bit responses. A lookup
  uploads 21,000 bytes and receives 10,256 bytes per segment. The display
  schema has its own name, `transparent-txid-display-p8-v1`, so a client built
  for the earlier 16-bit display finds the service unsupported at init rather
  than failing later.
- **Recent-shard anonymity floor.** A lookup reveals which shard and bucket it
  reads, so the txid hides only among that bucket's transactions. The client
  refuses to query a recent shard whose smallest bucket holds fewer than the
  larger of the map's `recent_floor` and its own `MIN_RECENT_FLOOR` (10,000)
  transactions, and returns `TxidLookup::RecentBelowFloor` without sending
  anything. A server cannot lower this floor. Archive lookups are unaffected.
- **Fresh publications and reorgs.** The client follows a fresh publication
  of the display tables, rolls back a reorg the chain shows before judging the
  map, and refuses display maps with no bucket or too many.
