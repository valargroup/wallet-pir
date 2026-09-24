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

## Public client check

After the latency campaign, build the pinned wallet client on a separate
machine without querying production:

```sh
RUSTFLAGS='-C target-cpu=x86-64' \
python3 enhance/tools/schema11-interop/run_public.py \
  /absolute/path/to/wallet-libraries --build-only
```

Build for the external client's Linux architecture, record the printed
binary SHA-256, and transfer `target/schema11-interop/release/public-client`
to that client before extraction. Run `enhance-chain-oracle` on the coordinator
and transfer its small `oracle.json` and `manifest.json` to the external client.
Immediately verify the copied binary hash and execute it directly:

```sh
sha256sum /absolute/path/to/public-client
/absolute/path/to/public-client \
  https://enhance-pir.valargroup.dev \
  /absolute/path/to/chain-oracle-output
```

The build runner pins PR #29, starts from its Cargo lock, rejects any resolved
package absent from that lock, and prints source, lock and binary hashes. The
copied binary verifies the oracle digest, requires the public manifest's
generation, anchor hash, height and record count to equal the chain-derived
values, then queries every oracle position through the wallet's HTTPS transport
and checks exact records. A publication between extraction and client fetch
causes a safe failure; extract a fresh oracle and retry only for that
generation-moved error. Do not rebuild in that interval: the extra build time
makes a generation race more likely. A wrong answer or any other error is a
hard failure.

The binary uses an explicit 32,768-row setup limit, matching the live
production shard at the time of qualification; a larger future shard requires
a deliberate limit review. This check uses the wallet client library but does
not open a scanned SQLite wallet or exercise restore, resume or reorg behavior.
Those remain separate release gates.
