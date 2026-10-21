# Changelog

## Unreleased

- Transactions are read with `zakura-transaction-wire` (wallet-libraries PR 287)
  instead of `zakura-primitives`, and block headers are framed and hashed locally.
  Transaction IDs, block hashes, Merkle roots, fees, compact fields, coinbase
  heights and expiry heights are unchanged and tested against the node's decoder.
  Shielded fields are no longer checked to be valid curve points; the node
  validated them. `zakura-orchard` is now enabled only by `receiver`, and
  `tools/ci/chain_graph.py` refuses any typed decoder or shielded backend in the
  production RPC and Transparent, Enhance and Status readers.

- Moved from wallet-libraries (`zakura/pir-chain`, revision
  `f48b62b80ec7d9d7fb7893401435a24b189b8ec2`) into wallet-pir's root workspace at
  `src/shared/pir-chain`, with unchanged behavior, features and dependency pins.
  The fresh consumer graph checks moved to `tools/ci/chain_graph.py`.

- `transparent::state::StateError` no longer exposes the node's
  `zakura_state::StateInitError`: `Open` carries the node's message as a `String`.
  A database that is not an archive database is refused with the new
  `StateError::NotArchive` instead of a panic inside the node's initializer.
  `StateReader` is documented and tested as a snapshot taken at open (including
  the primary's unflushed write-ahead log), to be reopened during long backfills;
  real databases carrying either pruning marker are tested to be refused; and the
  server graph check builds a fresh consumer with `rocksdb` enabled.

- Read transparent blocks for the transparent services: raw output and spent-output
  scripts with exact fees, resolved from the same block, a bounded cache and batched
  reads, plus compact blocks and raw JSON-RPC calls for independent checks.
- Add the opt-in `rocksdb` feature, a reader of an archive node's finalized state as
  a RocksDB secondary, tested against a real database beside its primary.
- Read an Enhance block's tree size by the hash of its extracted actions, so a
  reorg between reads cannot combine one block's actions with another's tree size.
- Document the existing mainnet-only activation gates for receiver backfill and
  the combined Enhance block reader.
- Verify production and fixture-enabled server consumers independently, and keep
  node/server packages out of each wallet facade backend graph.
