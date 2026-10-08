# Mainnet directory smoke test

Observed September 26, 2026 using `http://159.65.183.89:8232` as the canonical
RPC. This is a bounded indexing test, not full-history or PIR transport qualification.
The run metadata is in [`manifest.json`](manifest.json).

This record is historical. The `receiver-directory` binary then lived in
`enhance-pir-server`; it is now built with `cargo run -p receiver-indexer --bin
receiver-directory`. Publications then started at 4 rows; they now start at 8192,
so the same range today publishes 8192 rows with a different revision. The tests
named below were the suites of that revision.

```sh
cargo run -p enhance-pir-server --bin receiver-directory -- \
  --data-dir /tmp/receiver-smoke \
  --rpc-url http://159.65.183.89:8232 --no-auth \
  --start-height 3496100 --end-height 3496200
```

- Coverage: 101 blocks, heights 3,496,100 through 3,496,200 inclusive.
- Terminal hash: `00000000005090cbf22c88e5f9db4b710323573cae7c4ebbc70021be25430120`.
- All Ironwood Actions: 630. Excluded coinbase Actions: 14.
- Authenticated non-coinbase receiver records: 20. These are not classified as NEAR.
- Publication: 4 rows, 16,384 bytes, revision
  `346932851fd94db77e31ac522460586f262f7782601e2eb03209a18e0b2ff925`.

The known refund transaction
`2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653`
appears at height 3,496,114, transaction index 16, Action index 0, global note
position 610503. Its block hash, transaction identity, Action input nullifier,
commitment, ephemeral key, and 52-byte ciphertext prefix were compared against an
independent verbose RPC block response. All matched. The publication's row digest
was also independently recomputed. Rerunning the command yielded identical counts
and the same revision without duplicate records.

The shared crate passed six tests with `--features store`. The server's
`--test receiver` passed three tests, including a command-line restart and reorg
replacement using synthetic envelopes around public ciphertext. The shared crate
passed Clippy with warnings denied. Server Clippy completed with existing warnings
outside the new receiver files. Encrypted receiver queries and wallet insertion
are not covered by these results.
