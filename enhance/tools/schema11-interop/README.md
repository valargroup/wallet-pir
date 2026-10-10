# Schema-11 wallet interoperability

Run from the wallet-pir checkout:

```sh
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries
```

The runner uses a wallet checkout where Enhance support is part of the backend
and SQLite `orchard` feature. It prints the checkout's revision and an Enhance
client source fingerprint. Pass `--wallet-revision <commit>` to require an exact
revision. The fingerprint does not check for changes elsewhere in the checkout.
It creates a temporary Cargo project,
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
