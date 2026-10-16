# Design: directory shards with their own page shards

This replaces the earlier "run list in the directory entry" draft. The directory
entry stays lean and points to one place in the pages; the pages link back to
older history themselves.

Status: implemented as schema v12 on `v12/decoupled-pages` (PR #150) and
deployed to staging next to production v11
([status](status.md#schema-v12-in-staging-next-to-v11-2026-10-12)). The branch
merges to `main` only at the switch from v11 to v12, so `main` stays
redeployable for v11 until then. The constraints below governed the prototype
phase and are kept as its record.

## Goal

Decide, with measured numbers, whether sizing the directory and page tables
independently lowers what a wallet pays to recover its history. Prototype it
end to end (wallet-pir, then wallet-libraries) on draft branches.

Prototype-phase constraints (superseded by the staging deployment above):
- Draft branches and draft PRs only, with no merge to `main` before the switch.
  This overrides wallet-pir's usual commit-to-main rule.
- No production publication, fleet, Caddy, router, assignment or scaler changes.
- Reuse the hub's seeded build cache: keep `CARGO_TARGET_DIR`, run Cargo through
  the repository's iteration profile, never `cargo clean`, build the census with
  `release-fast`, not fat LTO.

## Why: what is known today

- A shard closes on whichever fills first: distinct addresses, directory bytes,
  or page rows (`wallet-libraries/zakura/transparent-shard/src/seal.rs`). Targets are
  capacity minus 1/7 for addresses and directory bytes, minus 1/32 for pages.
- On the 2026-09-08 census (older schema), pages filled first almost everywhere:
  159 of 160 archive shards and 13 of 13 recent shards sealed on page rows.
  Archive address count was 33% of slot capacity at the median, 62% at most.
- Those percentages do not carry over to v11. A v11 row holds 58 minimal entries
  (19-byte header + 51-byte receive), versus 14 then. Real entries with two
  inline events are about 110–150 bytes. Rough arithmetic puts v11 directory
  byte fill below 30%. **Nobody has measured v11 seal reasons or byte fill.**
- Why pages fill first: a few very busy addresses produce most events, and every
  event beyond an address's two newest goes to pages. The bulk-isolation census
  (2026-09-28) confirms it: separating long histories made recent shards seal on
  address count instead, with ordinary page rows at 3.4k–4.9k of 8k.

Consequence: directory tables are mostly empty, and every shard boundary forced
by pages costs wallets one more directory query.

## Mechanism

### Shards

- A **directory shard D** covers a block range. Its directory seals on directory
  bytes or address count, at a tier boundary, or when it reaches
  `MAX_PAGE_CHILDREN = 32` page shards.
- Inside D are up to 32 **page shards** P0…P(k−1): consecutive block ranges, each
  its own PIR table, each sealing on page rows. When D seals, its open page shard
  seals with it. A block is never split across page shards.
- `MAX_PAGE_CHILDREN` no longer affects entry size. It bounds chain length (round
  trips), manifest size, publisher replay length, and catch-up cost. 32 is a
  placeholder; the census decides it.

### Which events are inline and where older ones go

- Each address keeps its **two newest events in D inline** in its directory entry.
- Take an address's events in D in order, e1…en. When e(i+2) arrives, e(i) is
  **pushed out** of the inline slots and appended to that address's run in
  **whichever page shard is open at e(i+2)'s block**.
- Why the open one and not the one where e(i) happened: that page shard may have
  sealed already, and sealed tables never change.
- So an address has at most one run per page shard. Runs are in time order: older
  page shard = older events, and every paged event is older than the inline ones.

Example. D covers 1000–1999; P0 = 1000–1299, P1 = 1300–1649, P2 = 1650–1999.
Events: e1@1010, e2@1020, e3@1100, e4@1400, e5@1700, e6@1710.

| Block | Arrives | Pushed out | Goes into |
|---|---|---|---|
| 1100 | e3 | e1 | P0 |
| 1400 | e4 | e2 | P1 (P0 sealed) |
| 1700 | e5 | e3 | P2 |
| 1710 | e6 | e4 | P2 |

Result: inline e5, e6; runs P0 [e1], P1 [e2], P2 [e3, e4].

### Directory entry (schema v12): one pointer

```
tag[14] | flags_child u8 | head_row u32 | inline events
```

| Field | Bytes | Meaning |
|---|---|---|
| `tag` | 14 | Salted SHA-256 prefix of the address's script, as today (`tag.rs`). The wallet finds its entry in a row by tag. |
| `flags_child` | 1 | Bits 0–4: page shard number j of the **newest** run. Bit 7: set means exactly one inline event. Bits 5–6: must be zero. |
| `head_row` | 4 | 1-based row of the newest run's first fragment in `pages-j`. 0 = no pages. |
| inline events | varies | The one or two newest events, in today's compact encoding. |

Rules the decoder enforces:
- `head_row = 0`: page bits must be 0; bit 7 picks one or two inline events.
- `head_row ≠ 0`: exactly two inline events (bit 7 clear), and j < k.
- Reserved bits are zero; inline events are in order.

Why the count is still encoded: entries sit back to back in a row, and the next
entry starts with a random-looking tag, so the decoder cannot tell "second event"
from "next entry" without being told. Only one bit of information is needed,
since entries with pages always have two inline events. The header is 19 bytes,
**the same size as today's** (tag + `first_page` + `inline_count`), while also
naming the page shard.

Example entry for the address above, head run at row 37 of `pages-2`:

```
tag (14 B) | 0x02 | 25 00 00 00 | e5 (receive, 51 B) | e6 (spend, 79 B)
```

### Page entry header: the chain

Today's page entry header is 34 bytes: tag 14, ordinal 4, fragment count K 4,
event count 4, min height 4, max height 4 (`page_row.rs`). v12 adds:

```
prev_child u8 | prev_row u32 | older_runs u8          (header becomes 40 bytes)
```

- On ordinal 0 (a run's first fragment): `prev` points to the first fragment of
  the same address's previous run, in an earlier page shard; `prev_row = 0` ends
  the chain. `older_runs` is the number of runs older than this one.
- On ordinal > 0: all three must be zero.
- K stays where it is. Page rows, greedy fragments and packing are otherwise
  unchanged (`packing.rs`, `build.rs` long runs plus best-fit).
- Pointers only go backwards, to page shards that sealed before this run started
  receiving events, so a run's contents and its `prev` are fixed when its page
  shard seals.

Open option to measure: store the three fields only on run-start fragments
(variable-width header) if the extra 6 bytes on every entry costs material page
rows.

### Identity and routing (unchanged from the earlier draft)

- D stays the only lineage. D's manifest embeds a `PageChild` list: j, height
  range, terminal block hash, its own salt counter, sealed flag, page segments
  with sha256, occupancy.
- Page tags use a per-child salt `page_tag_salt(D id, j, P end height,
  P terminal, counter)`, because D's terminal changes every block while D is open
  and would otherwise re-tag sealed children.
- Queries go to `/v2/shards/:d/revisions/:d_digest/{setup,query}/pages-<j>`,
  reusing the `:table` parameter. Only D revisions go stale, so 409 handling and
  worker assignment stay per D. `ShardMapEntry` is unchanged; `page_segments`
  becomes k. The server keys page runtimes by child identity, so a sealed child's
  runtime is shared across D revisions.
- One geometry registry entry names both dimensions (e.g. `decoupled-32k-16k`),
  so native seeds stay keyed by (geometry, table).

### What the wallet does

1. One directory query on D (choice table unchanged). Gives the head or no pages.
2. Query the head row in `pages-j`. It yields K and `prev`. Next round trip:
   fetch the run's remaining K−1 fragments **and** the previous run's first row
   together.
3. Repeat until `prev_row = 0`. Round trips ≈ number of runs + 1 (today a paged
   address takes 2).
4. Checks:
   - page shard number strictly decreases along the chain;
   - `older_runs` drops by exactly 1 per hop and is 0 exactly when `prev_row = 0`;
   - existing per-run ordinal, count and greedy-boundary checks;
   - events strictly increase across the chain (oldest to newest) and then into
     the inline events;
   - push-out placement: for each paged e(i), the height of e(i+2) lies inside
     its run's page shard range. Walking newest to oldest, the wallet already
     holds the newer run's first two events (or the inline ones) when it reads an
     older run.
5. Coverage commits per D. Pending work is keyed by D revision only, since D's
   digest commits to every child.

**Run cache**, keyed by (page shard digest, address): the run's own events and
the page shard digest of the address's previous run, so the history is rebuilt
by following those links and no event is stored twice. The walk stops at the
first cached run, because nothing older can change. Once the wallet covers an
address over a sealed directory shard it drops that address's cached runs
there, so the cache holds runs of directory shards still being caught up. Tip catch-up becomes one directory query, plus the head run's
fragments only if the head is in the open page shard.

**Pending-work state**: a cursor `(child, row, older_runs)` plus the two events
carried for the placement check, instead of a run list.

**Privacy delta**: which page shards are queried, and how many hops, reveals
roughly when events were pushed out, at about today's shard granularity.
Addresses with no pages leak less than today, because D spans more blocks. The
sequential walk also reveals chain length through round-trip count; the run list
design revealed the same count through query count.

## Seal order per block

Deterministic, so a fresh build equals block-by-block publishing:

1. Project the block into (D, P).
2. If P would overflow, close P and re-project into (D, new P).
3. If D would overflow in either projection, close both and re-project into a
   fresh pair.
4. After absorbing the block: if D is at its target, close D and P; else if P is
   at its target, close P.
5. An oversized block uses the existing single-block multi-segment path at each
   level.

The publisher resumes the sealer at D's start, never at a child boundary, and
reuses sealed children by digest. A reorg at height h only changes pushes at
heights ≥ h, so sealed children ending before h are reproduced exactly. An empty
force-sealed child is published with zero segments. The `pages_query_rows` prefix
may apply to any single-segment child, sealed or open.

## Changes by component

The prototype changes below now belong to the listed owners. Service paths are
under wallet-pir’s `transparent/`; wallet and format packages are owned by
wallet-libraries. The schema-6 store change describes the prototype; the current
persistence contract is [schema 7](wallet-adapter.md#persistence-compatibility).

- `wallet-libraries/zakura/transparent-shard`
  - `records.rs`: v12 entry with `flags_child` and `head_row`; drop `inline_count`.
  - `page_row.rs`: 40-byte header with `prev_child`, `prev_row`, `older_runs` and
    their rules; everything that derives packing from the header width follows.
  - `tag.rs`: `page_tag_salt`.
  - `manifest.rs`: `SCHEMA` v12, `PageChild`, per-child `pages_query_rows`.
  - `seal.rs`: `DisplacementSealer` reusing `HistoryLayout` and `PackedDemand`.
  - `build.rs`: split `build_shard` into `build_directory` (reusing
    `place_directory` and the choice table) and `build_pages` (reusing the run
    plus best-fit loop), with `build_pages` writing each run's `prev` from the
    row positions in earlier, sealed children.
  - `layout.rs`: decoupled registry entries.
  - Drop v11 and v10 decoding; unreleased prototype, no migrations.
- `services/transparent-filter-server`
  - `publication.rs`: publish D plus children, resume at D's start, check emitted
    rows per child, reference-count children in retention, link children in
    `link_superseded_tails`.
  - `bin/shard-census.rs`: `--decoupled --directory-rows N --page-rows M` mode.
- `services/transparent-shard-server`
  - `shardset.rs`: `LoadedShard.children`, `Table::Pages(j)`, reference-counted
    pruning.
  - `service.rs`: parse `pages-<j>`; query binding (D digest, `pages-j`).
- `wallet-libraries/zakura/pir-transparent/src/engine`
  - `sync.rs`: child checks in `verify_manifest`, the chain walk and its checks,
    the run cache.
  - `transport.rs`: no signature change.
- `wallet-libraries/zakura/pir-transparent/src/engine_store`: schema 6 — `pending_work` gains the cursor
  carried events and saved partial fragments; add a run cache table. Keep existing table names
  (wallet-libraries resets them by name).
- Tests: update hand-built manifests in
  `services/transparent-shard-server/tests/common/mod.rs` and
  `tests/wallet_sync.rs`.

Wallet integration in wallet-libraries (`zakura/pir-transparent`) covers
`src/catalog/store_tables.rs`, `testing/service.rs`, fixtures for the manifest
and table names, and `tests/private_recovery.rs` (a history spanning two page
children plus tip catch-up). The PIR packages and wallet engine are owned by
wallet-libraries; wallet-pir consumes them at a pinned library revision. A
wallet-libraries release does not require a wallet-pir source pin or tag.

## Sequence and gate

1. **Simulate first.** Build `DisplacementSealer` and the census mode.
   - Data: a read-only slice of the v11 journal
     (`/srv/transparent-activity/full-v3/journal` on the coordinator), copied with
     nice/ionice and rebased offsets as in the single-lookup census. Include
     committed blocks' display sidecars. Cover the recent six months plus an
     archive window with a dense stretch; disk budget 60 GB. If the worker cannot
     reach the coordinator, stop and report the exact copy command.
   - Workload: the frozen 1,080-wallet sample
     (`transparent/evidence/workload-sample-2026-09-08`) through `pairs.py`.
   - Layouts to compare:
     - today: `archive-wide`, `recent-4k-8k`;
     - smaller archive directories: 16k/64k, 8k/64k;
     - bulk isolation (existing `--bulk-long` census mode);
     - decoupled: directory 32k with pages 64k, 32k, 16k; recent directory 4k or
       8k with pages 8k.
   - Report, per layout:
     - v11 seal reasons per shard (first, as the baseline fact);
     - directory fill by bytes and by address count, per shard;
     - shard and page-child counts; how often D sealed on the child limit versus
       a full directory;
     - page fill, including the cost of the 40-byte page header;
     - runs per address and chain-length histogram (p50/p95/p99) as round trips;
     - one-shot query bill per wallet (median/p95/p99) and tip catch-up bill;
     - memory reservation per tier.
   - **Gate:** if no decoupled option beats both today's one-shot bill and bulk
     isolation without a worse tip catch-up bill, stop with the numbers.
2. Codecs and builders, with property tests:
   - a fresh build equals block-by-block publishing;
   - sealed children are unchanged by tail growth and by a reorg above a child's
     end;
   - the decoder refuses reserved bits set, the one-event flag with pages, and a
     page shard number out of range;
   - the wallet refuses a chain that is cut short (`older_runs` disagrees with
     where `prev` ends), points forward or to the wrong page shard, or places an
     event in a page shard other than the one open when it was pushed out.
3. Publisher, then server, then wallet and store with the run cache.
4. End to end in wallet-pir:
   - `wallet_sync`, plus a test where one address's history spans three children
     with exact ledgers;
   - a tip catch-up test showing sealed runs are not re-queried (the walk stops at
     the cached run);
   - a decoupled published set served over HTTP and recovered by
     `transparent-measure` on a slice.
5. wallet-libraries: repin, adapt, pass `python3 scripts/dev.py test --config
   transparent-pir-testing` and `--config transparent-pir-sqlite`.
6. Evidence README at `transparent/evidence/decoupled-pages-prototype-2026-10-<dd>/`
   on the draft branch: method, numbers, privacy delta, limits.

## What this will not build

- No production publication, deploy, fleet, Caddy, router-rendering, assignment
  or scaler changes.
- No native correctness certificates.
- No Vizor changes, parent filters or txid display changes.
- No v11 compatibility, decoding or migration.
- No edits to `main` in either repository.
- No run list in the directory entry (superseded by the chain).

## Open questions and risks

- **Inline loss.** One directory shard keeps 2 inline events per address where
  today several shards each kept 2, so medium-activity addresses may move from
  zero page queries to some. The census has to show the net bill, not assume it.
- **Chain latency.** First recovery costs about one round trip per run. A p99
  chain near 32 means a slow first sync for the busiest addresses; lowering
  `MAX_PAGE_CHILDREN` caps it.
- **Page header cost.** 6 extra bytes on every page entry, including shared-row
  short histories. Measure; fall back to start-fragment-only fields if material.
- **Publisher cost.** The open D holds more addresses and spans more blocks, so
  per-block rebuild and restart replay grow.
- **Concurrent work.** Another hub task (txid display re-layout,
  `t-2cef2a026c314a7a`) touches both repositories. Branch from current
  `origin/main` and record base SHAs.
- **Main open question.** The tip catch-up bill; step 1 measures it before most of
  the build cost is spent.

## What the result should look like

- Two draft PRs, one per repository, each at an exact head SHA; the
  wallet-libraries PR pinned to the wallet-pir draft SHA.
- The evidence README with the comparison table above.
- Passing wallet-pir focused and new end-to-end tests, the two wallet-libraries
  configs, and the `transparent-measure` recovery over HTTP.
- Nothing merged until Roman decides.
