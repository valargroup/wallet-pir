# Wallet recovery benchmark

Run a controlled comparison of ordinary scanning with 25 receiving keys and
private receiver discovery. Each run creates a fresh file-backed wallet database.
The benchmark uses actual receiver and Enhance PIR HTTP protocols on loopback.
It never opens Vizor or an existing wallet.

## Fixture

The deterministic compact chain starts at height 3428143 and contains 257 blocks,
4097 Ironwood Actions, five payments across 25 receiving keys and one later spend.
Twenty receiving keys have no payment. These are synthetic blocks from the wallet
library's test helpers, without consensus proofs. The receiver table has 8192 rows.
The Enhance fixture contains authenticated full ciphertext for the five payments.
Unused slots are zero-filled. The `main` Enhance profile identifies the wire format,
not a connection to mainnet.

Both paths scan the same compact chain from the same birthday, then retrieve and
authenticate each payment's memo through Enhance PIR. Ordinary scanning uses the
25 receiving keys during scanning. Private recovery scans with ordinary account
keys, makes 25 receiver lookups, downloads the common witness file and imports the
five payments using the wallet library's private recovery helpers.

Every run checks exact transaction/output identities, values, positions, nullifiers
and spent state. It also checks inclusion witnesses obtained from the final wallet
database for all four unspent notes. Witness validation happens after timing for
both paths. This does not construct or broadcast a spend.

## Run

Use sibling checkouts named `wallet-libraries-pir` and `ipir-sp-compat`. The root
patch table in this standalone crate pins the local wallet and native PIR adapters.
It is separate from the main workspace because the wallet and server require
different SQLite crate versions. Keep the lockfile when repeating measurements.

From the wallet-pir root, with Rust 1.98.0:

```sh
cargo build --locked -p enhance-pir-server --example recovery_fixture \
  --features native-reinspiring --profile release-fast
cargo build --locked --manifest-path receiver/tools/recovery-benchmark/Cargo.toml \
  --target-dir /absolute/path/to/existing/wallet/target
/absolute/path/to/existing/wallet/target/debug/receiver-recovery-benchmark \
  "$PWD/target/release-fast/examples/recovery_fixture" /absolute/path/to/new-results
```

The output directory must not exist. The executable runs three trials per mode,
alternating scan/PIR, PIR/scan, scan/PIR. Fresh wallets and clients are used for each
run. Fixture construction and server startup are outside timing. The parent owns
temporary server storage and removes it after terminating the child.

`results.json` records phase timings in microseconds and HTTP request/response body
bytes. Each HTTP elapsed time includes loopback server work. `fixture.json` records
the compact-chain digest and expected notes. `enhance.json` and the server log allow
inspection of the served fixture.

## Interpretation

This is a wallet pipeline correctness and timing test on a short synthetic history.
Compact blocks are preloaded into a local cache. The benchmark excludes lightwalletd
downloads, WAN/Tor latency, transaction UI enrichment, outgoing recovery, publication
lag, and transaction proving. Both paths authenticate full memos, but the baseline
does not populate extra UI metadata from them. It uses the scan's existing note and
witness state. The PIR path additionally inserts recovered notes and witnesses.

Body-byte totals exclude HTTP/TLS overhead. Directory geometry and witness size are
fixture-specific. Do not project these numbers onto a mainnet database or call them
a full seed-restore benchmark. The 25-key fixture does not change the production
50-address recovery window.
