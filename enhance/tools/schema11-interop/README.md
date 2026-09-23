# Schema-11 wallet interoperability

Run from the wallet-pir checkout:

```sh
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries
```

The runner checks that wallet-libraries is at PR #28 commit
`de3ec78f31b6fd184596fc952fe4f78d3a63cd0a` and that its tracked Rust sources and
workspace manifests are unchanged. It creates a temporary Cargo project,
uses the supplied wallet checkout and the current server sources, and writes
build artifacts under `target/schema11-interop`. It does not modify the wallet.
Cargo may download dependencies; tests use loopback servers and synthetic keys.

The server processes use the seven-sealed placement policy; these small fixtures
verify wallet interoperability, not full-size placement or hardware capacity.

The test covers:

- Actual wallet client queries against a coordinator and two workers, including
  row boundaries, out-of-range positions, expired generations and fresh acceptance.
- Two compact-scanned actions in a SQLite wallet, with the actual scanned anchor
  checked before accepting the manifest.
- One HTTP row query for reversed and duplicate action requests, with byte-exact
  records produced using the server codec.
- Atomic wallet rejection of a corrupted live response and successful application
  of the original batch with duplicate outcomes preserved.

The loopback transport and generation-expiry scenario are adapted from the
pinned wallet's historical `http_integration.rs` fixture. The new SQLite scenario
verifies schema-11 suffix reconstruction and batch application. Wallet metadata
and transaction-shape trust semantics are unchanged.
