# Wallet adapter contract

Status: the boundary a wallet integrates through, 2026-09-08. The reference
implementation is owned by wallet-libraries' `zakura-pir-transparent` package
(`zakura/pir-transparent`, at the revision this repository's `Cargo.lock` pins):
its `engine` module (feature `engine`, pure Rust) with `engine_store::SqliteStore`
(feature `engine-store`, SQLite persistence). Vizor is the first
integration target; this document is what its adapter must supply and what it
may rely on. No Vizor repository change is part of this repository.

## Shape

The wallet owns four things and passes them as data. Nothing crosses the
boundary as a callback, so a binding generator can take the facade as it is.

| The wallet supplies | Reference type | Rule |
|---|---|---|
| Target | `Anchor { height, hash }` in every `SyncRequest` | The wallet accepts this block independently; publication coverage must reach it. |
| Persistence | `WalletStore` (`SqliteStore` or any implementation) | One atomic mutating boundary, `commit_shard`; a retry of a committed shard is idempotent and a differing retry is refused. |
| Scripts | `ScriptEntry { script, origin, required_from }` | Exact script bytes, not address text. `required_from` is the first height the wallet needs; it is never raised for a known script. |
| Chain view | accepted headers `(height, hash)` | A height listed with another hash is a reorg and coverage above it is rolled back; a height not listed is unknown and the sync stops short rather than guessing. |
| Transport | `ShardTransport`, `FilterSource` | The reference HTTP adapters live behind the `reqwest` feature; a wallet with its own HTTP stack implements the two traits. |

The facade `zakura_pir_transparent::engine::TransparentSync` (`src/engine/facade.rs`) binds
these: `refresh_map` fetches the map and the service parameters and returns
the tip for the wallet to validate; `sync_once(SyncRequest)` performs one
bounded sync and returns `SyncStatus`; `status`, `snapshot` and
`rollback_to(Anchor)` read or adjust the store without network work. The caller supplies an accepted common ancestor for rollback, including inside a shard.

A wallet that brings its own store proves it with the contract suite:
`zakura_pir_transparent::engine::testing::suite` (feature `engine-conformance`) is
the same suite the two reference stores, `engine::MemoryStore` and
`engine_store::SqliteStore`, pass. The init document is parsed by
`zakura_pir_transparent::engine::parse_init` whichever transport fetched it.

## What the wallet gets back

`SyncStatus.completion` is `complete` or the reason the sync stopped:
`query-budget`, `byte-budget`, `pending-limit`, `overloaded:<shard>`,
`chain-unknown:<height>`, `publication-behind:<height>`, `unresolved-spends`,
`discovery-unbounded`, `content-failed`. `content-failed` means some directory
entries' rows failed to decode or check out; `failed_entries` lists each
with its shard revision and failure count, everything else was read, and an
entry at three failures of the same revision is no longer queried. `unresolved-spends` is reported once every script in
scope is covered to the target and a spend's receive lies below the wallet's
floor; the anchor does not commit for it, so every later run re-verifies
against the target. A wallet may add words of its own for states the library
never sees — the reference integration writes `sync-in-progress`, `stopped`,
`failed`, `interrupted` and `chain-rewound` — and must show every one as a
reason, never as a bare height. The anchor commits only on a
complete sync to the explicit target the chain view accepted. Events above that target never enter the ledger or advance discovery. A subsequent target inside the same shard re-queries it and deduplicates retained events. `unresolved` counts spends
whose receive the ledger has never seen; `pending` counts page retrievals
still owed.

## What the wallet must do

- Treat the balance as synchronized only when `completion` is `complete` and
  `unresolved` is zero. Anything else is a partial view and is shown as one.
- Call `sync_once` again with the scripts it now has after gap-limit advance.
  Discovery inside one call is bounded; `discovery-unbounded` means the
  wallet's own derivation rule has to decide, not the library.
- Pass every header it has accepted for the heights the map covers. The
  library detects reorgs only through this list.
- Keep the store on the wallet's own durable storage and never share one
  store between two set identities; the store refuses a map for another
  chain, profile or genesis, and a map whose anchor is lower than the one it
  holds.
- Run beside the existing sync until the wallet owner accepts the
  [contract](contract.md)'s trusted-indexer completeness model. This library
  establishes that profile and no stronger one.

## What the wallet must not do

- Substitute a raised birthday for retained history: an old receive is needed
  to apply a recent spend, and `required_from` exists so the store keeps it.
- Present an empty UTXO set as evidence of no history.
- Display pending (unconfirmed) transactions through this path; the service
  indexes confirmed blocks only, and pending state is the wallet's.
- Look up an address or an outpoint in plaintext to fill a gap the sync left.

## Cost the adapter pays

Bytes and time are accounted per stage in `SyncReport`; the integration
measures its own transport, TLS and storage costs on top, which the
reference figures exclude. The load harness `transparent/tools/transparent-loadtest`
reports the same split for the reference adapters.

## Persistence compatibility

`engine_store::SCHEMA_VERSION` is 7: it holds schema-v12 chain walks, saved run
fragments, the run cache and per-entry content failures. A custom store must
persist the last committed fragment's `PendingPages.boundary` atomically with
events and `next_ordinal`; resumed downloads use it to reject underfilled or
reordered fragment boundaries. Store the logical event with its full outpoint,
independently of compact row references.

Content failures count per shard id, shard revision and script, committed with
the shard that observed them. `WalletStore::entry_failures(shard_id, revision,
script)` answers only for the revision the counts are bound to; a reused shard id
with another revision starts afresh, and counts without a proven revision binding
apply to none.

Schema 6 upgrades in place, adding only the empty failure-count table; the
creation and version update commit together. Other schema versions are refused,
not migrated. Changing shard schema and seal thresholds
requires a separate publication lineage and consumer migration; a database
schema migration does not authorize reuse of old shard identifiers. A lower target requires an
explicit accepted-anchor rollback. See [testing](testing.md) for regression checks.

A [declared re-cut](architecture.md#declared-re-cuts) keeps the lineage and changes no store
schema. A store keeps coverage, events and setups under the revisions it read them from, and
the sync finds them in the re-cut map by height and digest rather than by shard id. A custom
store must give the same coverage answer as the reference stores: a committed range replaces
every range of that script it contains, whatever shard id they carry; a range strictly inside
the one the script holds starting nearest at or below it changes nothing; and a store keyed by
script and start height must not refuse a range starting where one it replaces did. The
conformance suite checks this, and a custom store should keep each range's source anchor,
without which a range that ends inside a declared range is assumed to have been cut short.

`SyncError::SealedRewrite { start_height, revision_digest }` means the map replaces sealed
history the store holds with a shard whose end block the wallet's chain also accepts, at or
below the target, while the chain still accepts the block the stored history rests on, and
declares no re-cut of it. The publisher contradicts itself on the wallet's own chain, so
retrying does not resolve it while it serves that history; an adapter reports the publication
as changed. Nothing has been read, and nothing rolled back but a reorg the wallet's chain itself
showed, which the sync rolls back before judging the map. A publisher that merely got ahead of the
wallet's chain, for example by following a shallow reorg through a shard it had just sealed,
gets `chain-unknown` instead, and the next sync after the chain moves rolls it back as a reorg.
The store keeps no re-cut epoch, so a replica still serving a map from before a re-cut the store
has followed produces `SealedRewrite` too: an adapter that records the epoch it last synced at,
and treats a map with a lower one as behind before syncing, tells the two apart. A re-cut
published during a sync ends that sync with `MapDiverged`, and the next sync starts from the
re-cut map.

## Fixed Vizor provider qualification baseline

The provider plan fixes the public stable release available before approval:
[Vizor v0.0.60](https://github.com/chainapsis/vizor-wallet/releases/tag/release/v0.0.60),
macOS build 195, source `1c38c2eb0753fc6a36c16ec58cfe397696f25e4b`.
This is the released application, not the PR1021 registry demo or a rebuilt wallet.
Original assets were downloaded again without installing or executing them:

| Original release asset | Bytes | SHA-256 |
|---|---:|---|
| macOS DMG, asset 643050669 | 155631164 | `72ef8b5afd0152eccfcd66108ff659fac871396593feb4ac16f57ec6d9c63939` |
| Linux x86_64 AppImage, asset 642971073 | 89405944 | `944a9d02eecb7bc7cb725291b11bed078fe44d3e0d4d30fccae172676bf44006` |

The release source's `rust/Cargo.lock` hashes to
`c9db7eb50dc5c459931104f6a31488f7e0a35e467803ab4862fc2ae9ba9f60a7`.
It declares `zakura-pir-transparent` 0.0.1-rc0, crate checksum
`ad10db262413b0502e4782710af4eb5a76333d6a3d89adc0c50c3c130947ddf2`,
whose original `.cargo_vcs_info.json` names
`391d0b0247f0e07cfdea855ce87fcdf8ba376aae`. Native, shard and txid client
are published 0.1.0-rc.3 crates (VCS
`c0852aa558abc6cfd97640965fdda8d3e27b7334`). This is declared dependency
provenance, not independent binary dependency attestation. Do not add the old
adapter's reverse server development graph to the current release graph.

The fixed history profile is `transparent-shard-v12`,
`reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1`, with
`decoupled-2k-8k` and `decoupled-32k-32k`. Published shard manifest/layout
comparison to server pin `609cbd3` established matching source bytes; the native
adapter differs in namespaces and P16 shape-error handling. Neither comparison
proves cryptographic interoperability or application behavior. Raw release assets,
source archives and crates remain ignored local inputs.

### Endpoint and API trace

At the fixed release source,
`rust/src/wallet/sync_engine/transparent_ledger/pir.rs` sets mainnet history to
`https://transparent-pir.valargroup.dev`. `origin_override` reads
`VIZOR_TRANSPARENT_PIR_URL` only under `debug_assertions`; the release has no
independent-provider selector. Qualification must use the existing origin with
valid TLS, without rebuilding, new settings, or a wallet migration. Isolated
route/TLS fixture access is a named application qualification gate; no production
routing or DNS change is authorized.

The published adapter's `src/http.rs` resolves the following routes; discovery,
setup, revision validation, private query framing, retries and refresh retain
their existing semantics. `{table}` includes the existing child-page labels.

| Operation | Method and origin-relative path | Owning service |
|---|---|---|
| History discovery | GET `/v2/filters/shards`, `/v2/filters/shards/{id}/filter`, `/v2/shards/init` | Existing history publisher/serving boundary |
| History manifest/setup | GET `/v2/shards/{id}/revisions/{rev}/manifest`, `/v2/shards/{id}/revisions/{rev}/setup/{table}/{segment}` | History worker/router |
| Private history | POST `/v2/shards/{id}/revisions/{rev}/query/{table}` | History worker/router |
| Display discovery | GET `/v2/txid/init`, `/v2/txid/map`, `/v2/txid/map/{height}/{digest}` | Existing display service |
| Display manifest/setup/query | GET `/v2/txid/shards/{id}/revisions/{rev}/manifest`; GET `/v2/txid/{archive,recent}/shards/{id}/revisions/{rev}/setup/directory-{n}/{segment}`; POST the corresponding `/query/directory-{n}` | Existing display service |

Release `transparent_details/source.rs` imports the **same history origin** for
display. Preserve `/v2/txid/*` at the front routing boundary through that existing
service. History-only provider work does not implement a display provider.
`enhancement/mod.rs` and `enhancement/status/private.rs` use the existing
`https://enhance-pir.valargroup.dev` origin for Enhance and Status, through their
separate transports. No Receiver call was located in this fixed application
source; any existing Receiver service remains independent and unchanged.

The application still owns chain acceptance, privacy policy, complete coverage,
companion state and commit/settlement. No source comparison or provider bootstrap
test qualifies unchanged-app recent/archive recovery, continued sync, restart or
handover; those remain explicit application gates in PRs 3–5 of the plan.
