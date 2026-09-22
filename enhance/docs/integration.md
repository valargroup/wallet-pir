# Wallet integration

Use Enhance after compact scanning has identified the Ironwood output position
and the transaction context needed to validate it. The returned record supplies
encrypted note and outgoing-recovery fields; it does not replace scanning,
transaction identification, witness construction or spend detection.

## Rust client

The `enhance-pir` crate provides an asynchronous HTTP client and a lower-level
session API. In a crate within this repository, add a path dependency pointing
to `enhance/crates/enhance-pir` and Tokio with `macros` and `rt-multi-thread`.
An external integration can pin this repository to a reviewed full commit SHA
and select package `enhance-pir`. Do not assume the package is published to crates.io.

This example connects, checks coverage and returns an encoding-validated record.
The position is a command-line input from the wallet's scan, not a transaction ID.

```rust
use enhance_pir::client::EnhancePirClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let origin = args.next().ok_or("expected origin")?;
    let position: u64 = args.next().ok_or("expected output position")?.parse()?;
    let client = EnhancePirClient::connect(&origin).await?;
    if position >= client.generation().ironwood_tree_size {
        return Err("position is outside this generation's coverage".into());
    }
    let record = client.query_position(position).await?;
    let metadata = record.metadata();
    println!("expiry={}, fee={:?}", metadata.expiry_height(), metadata.fee_zatoshis());
    // Pass the encrypted fields to the wallet's authenticated recovery path.
    // Do not treat successful PIR decoding as transaction authentication.
    Ok(())
}
```

A transaction whose trial decryption covers several actions passes every output
position to `query_positions`. The client groups those positions by
`position / 29` and issues one encrypted query per distinct row, then reads each
requested slot from that decoded row. The returned records follow the order of
the input. A repeated position in the same row still produces one query.
`query_position` is that path for a single position.

Actions in one transaction occupy consecutive output positions. A contiguous run
of at most 29 positions occupies one row, or two rows when the run crosses a
multiple of 29. The number of queries is the number of distinct rows.

`query_dummy().await` sends a fresh query for a random row and discards the
result. `query_position_with_timing` returns the same record plus preparation,
HTTP, decoding and total durations. It is useful for measurements, not a
server-only latency measurement.

For an application-owned HTTP stack, construct `QuerySession::from_session`
from the atomic initialization response. `prepare_position` returns a
`PreparedQuery` and local slot; send `PreparedQuery::body()` as the binary POST
body, pass the original prepared query and response to `decode`, then use
`record_in_row` to extract and validate the record. For several positions,
`prepare_positions` returns one `PreparedQuery` per distinct row together with
the slots to read after `decode`. `rows_for_positions` exposes the same grouping
when the caller prepares each row itself. Keep the query's private state local.
`prepare_dummy` supplies the corresponding dummy path.

## CLI smoke checks

Run these commands from the repository root. Metadata connects and validates
initialization; dummy and query each make one encrypted request.

```sh
cargo build --locked --release -p enhance-pir --features cli
./target/release/enhance-pir-cli --server https://enhance-pir.valargroup.dev metadata
./target/release/enhance-pir-cli --server https://enhance-pir.valargroup.dev dummy
./target/release/enhance-pir-cli --server https://enhance-pir.valargroup.dev query 0
```

Position zero is only a smoke-test example. The query command prints a record as
hex; it does not decrypt a note or establish that it belongs to a wallet.

## Sessions, coverage and failures

`connect` fetches `/v1/enhance/init` once and validates protocol, network, pool,
schema, geometry, setup seed, generated parameters, public-material digest and
epoch. The client holds that session until it is replaced. It neither refreshes
a session automatically nor retries failed requests. Its HTTP timeout is 120
seconds, with body limits of 1 MiB for initialization and 16 MiB for responses.

Positions are zero-based indices in the Ironwood output tree. The local mapping
is `row = position / 29`, `slot = position % 29`; only positions below the session's
`ironwood_tree_size` are covered. Padded rows do not extend real coverage. If a
newly scanned position is outside coverage, fetch a fresh session and check its
anchor and coverage before querying. If it remains outside coverage, defer it;
do not reinterpret a padded record as an output. Group the positions for one
transaction with that mapping before querying, so each row is requested once.

A session can expire as newer generations displace the eight retained snapshots.
The server currently uses HTTP 503 for several query failures, including an
unretained generation and admission failure, so the status alone cannot identify
the cause. Wallet code should use bounded retries with backoff, refresh the
session when appropriate, and recheck chain context. Do not loop immediately on
503 or bypass protocol validation. A schema mismatch requires a compatible client
and coordinated release, not a retry.

After a reorg, a previously answerable generation can still describe the old
chain. Compare its anchor with the wallet's verified scan context and invalidate
or defer affected enhancement work. Generation IDs are not guaranteed to equal
anchor heights.

## Authenticating and using the record

Use `ephemeral_key` and `enc_ciphertext` for incoming note recovery, including the
authenticated memo. Use `cv_net` and `out_ciphertext` with the outgoing viewing key
and existing action context for outgoing recovery. Bind the recovered values to
the scanned action using the wallet's normal cryptographic checks before storing
or displaying them. The record does not contain txids, note commitments,
nullifiers, mined heights or witness data.

Expiry zero is a known value meaning expiry is disabled. `fee_zatoshis()` returns
`Option<u64>`: `Some(0)` is a known zero fee; `None` is unknown, not zero. The indexer
derives a fee only for pure Ironwood transactions, without transparent, Sapling
or Orchard components. Mixed transactions need the wallet's ordinary enhancement
route for missing information. Transparent flags alone do not identify all mixed
transactions; use the optional fee and existing transaction context.

Fee, expiry and transaction-shape flags are trusted indexer metadata and are not
authenticated by note decryption. Record construction validates encoding and
ranges, not canonical-chain inclusion or correctness of those assertions.

## Privacy and fallback

The client creates fresh cryptographic query randomness for each request. Dummy
queries use the same row-query construction, but this library does not implement
a wallet-wide fixed query schedule, traffic envelope or retry policy. A wallet
must choose those policies deliberately: requests made only after interesting
scan events can reveal timing information even when the position is hidden.
`query_positions` sends one request for each distinct row. That count is visible
to the server and equals the number of rows the positions occupy. A wallet that
wants a fixed request count pads or schedules those requests itself.

A fallback that retrieves a transaction by ID exposes that identifier to the
fallback service. Treat it as a separate wallet privacy decision, including for
mixed transactions; do not describe it as preserving the PIR guarantee. Neither
TLS nor PIR hides the client's address from the service it contacts.
