# Wallet adapter contract

Status: the boundary a wallet integrates through, 2026-09-08. The reference
implementation is `transparent/crates/transparent-wallet` (core, pure Rust) with
`transparent/crates/transparent-wallet-store` (SQLite persistence). Vizor is the first
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

The facade `TransparentSync` in `transparent/crates/transparent-wallet/src/facade.rs` binds
these: `refresh_map` fetches the map and the service parameters and returns
the tip for the wallet to validate; `sync_once(SyncRequest)` performs one
bounded sync and returns `SyncStatus`; `status`, `snapshot` and
`rollback_to(Anchor)` read or adjust the store without network work. The caller supplies an accepted common ancestor for rollback, including inside a shard.

A wallet that brings its own store proves it with the contract suite:
`transparent_wallet::testing::suite` (feature `testing`) is the same suite the
two reference stores pass. The init document is parsed by
`transparent_wallet::parse_init` whichever transport fetched it.

## What the wallet gets back

`SyncStatus.completion` is `complete` or the reason the sync stopped:
`query-budget`, `byte-budget`, `pending-limit`, `overloaded:<shard>`,
`chain-unknown:<height>`, `publication-behind:<height>`, `unresolved-spends`,
`discovery-unbounded`. `unresolved-spends` is reported once every script in
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

SQLite schema 3 retains schema 2's separation of source-publication identity
from covered endpoints and adds the last committed page's logical event and
encoded byte count. A custom store must persist this `PendingPages.boundary`
atomically with events and `next_ordinal`; resumed v10 downloads use it to reject
underfilled or reordered fragment boundaries. Store the logical event with its
full outpoint, independently of compact row references.

Migration from schema 2 preserves events, scripts and coverage and adds an empty
boundary field. Migration from schema 1 still clears unbound legacy coverage,
pending work and completion metadata. Changing shard schema and seal thresholds
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
without which a declaration is matched against the covered endpoint alone.

A map that rewrites sealed history the store holds, over a block the wallet's chain still
accepts, and declares no re-cut of it ends the sync with
`SyncError::SealedRewrite { start_height, revision_digest }` before anything is rolled back or
read. It does not resolve by retrying the same map; an adapter reports the publication as
changed. The store keeps no re-cut epoch, so a replica still serving a map from before a re-cut
the store has followed produces the same error: an adapter that records the epoch it last
synced at, and treats a map with a lower one as behind before syncing, tells the two apart. A
re-cut published during a sync ends that sync with `MapDiverged`, and the next sync starts from
the re-cut map.
