# Wallet adapter contract

Status: the boundary a wallet integrates through, 2026-09-08. The reference
implementation is `pir/transparent-wallet` (core, pure Rust) with
`pir/transparent-wallet-store` (SQLite persistence). Vizor is the first
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

The facade `TransparentSync` in `pir/transparent-wallet/src/facade.rs` binds
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
reference figures exclude. The load harness `server/transparent-loadtest`
reports the same split for the reference adapters.

## Persistence compatibility

SQLite schema 2 separates source-publication identity from covered endpoints and
records target-bound pagination validation progress. Migration preserves scripts
and event records but clears legacy coverage, pending work and completion
metadata; the next sync must re-establish coverage. A lower target requires an
explicit accepted-anchor rollback. See [testing](testing.md) for regression checks.
