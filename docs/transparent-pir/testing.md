# Transparent PIR regression and conformance tests

The production wallet accepts an explicit `Anchor { height, hash }` on every
sync. The wallet's accepted checkpoint and the service's publication tip are
separate authorities. The deployed regression runner exercises this distinction
through the reference HTTP adapters and SQLite persistence.

## Running the deployed suite

Run `make transparent-regression` against the default public origins, or set
`TRANSPARENT_LOAD_URL`, `TRANSPARENT_REGRESSION_FILTER_URL`, and
`TRANSPARENT_REGRESSION_FIXTURE`. Set `TRANSPARENT_REGRESSION_OUT` to a new
directory for each run. The manually dispatched
[workflow](../../.github/workflows/transparent-regression.yml) provides the same
post-deployment release check. It does not deploy or restart anything.

Both origins must serve the fixture's canonical compact map bytes and revision identities. The source publication file has a separate checksum because it may use pretty JSON.
Publication drift, unavailability, a deadline, any incomplete sync, or an
unexecuted required case fails the run. Fixtures never update themselves to
match a service response. Run after deployment reaches a stable publication;
ordinary PR CI uses the local deterministic tests instead.

Each case is one synthetic grouping of public scripts, not an actual wallet or
a claim about wallet population frequencies. Its sequence of exact block
heights includes checkpoints within shards. The runner syncs, closes SQLite,
reopens it and compares the saved state at every checkpoint. It then repeats
the final checkpoint and separately restores it from an empty store.

`report.json` records case outcomes, exact anchors, stage counts, payload bytes,
and elapsed time; `junit.xml` supports CI reporting. A mismatch writes both
expected and actual ledger snapshots. SQLite databases and worker logs remain
beside the report. Transport/TLS overhead and server capacity are not inferred
from payload counters. Cases run sequentially with a 60-second request timeout
and a 30-minute process deadline, configurable through the runner CLI.

## Fixture generation and oracle

`regression-export` reads the journal without changing it. It pins committed
file lengths, verifies block extents and event heights, and checks the
publication's block hashes against journal headers. It has no ingest, node or
RocksDB dependency. A case specification is a JSON list:

```json
[{
  "id": "example",
  "profile": "small-active",
  "scripts": ["76a914000000000000000000000000000000000000000088ac"],
  "required_from": 0,
  "heights": [100, 150]
}]
```

Use actual selected public scripts and heights in the pinned publication:

```sh
cargo run --release -p transparent-regression --bin regression-export -- \
  --data-dir /path/to/transparent-event-data \
  --map /path/to/publication/shards.json --cases cases.json \
  --cutoff-height CUT_OFF_HEIGHT --source-sha SOURCE_COMMIT --out fixture.json
```

The exporter refuses an existing output file, unsupported scripts, missing
checkpoint hashes, invalid height order, birthdays omitting earlier activity,
and oversized selections. Review and freeze the fixture and its checksum
before using it as a release gate. Keep the case specifications alongside it
so a future publication can receive an explicitly reviewed replacement.

Expectations contain exact script-associated events, UTXOs, spends, transaction
summaries, and confirmed balances. The reference reducer uses independent
outpoint bookkeeping, not the wallet's ledger implementation. Both the oracle
and publication share journal/ingest provenance. This detects publication,
retrieval and wallet regressions; independent archive-node extraction remains
necessary for ingest conformance. Fixed-script birthdays do not establish an
address derivation or gap-limit policy, and coinbase recovery does not establish
spendability.

## Local conformance checks

The [accepted-anchor HTTP tests](../../server/transparent-shard-server/tests/wallet_anchor.rs)
cover arbitrary endpoints, overlap, restart, bounded pagination, changed targets,
future-only discovery, and rollback within a shard. The existing wallet HTTP
suites retain geometry, segment, revision, malformed-response and overload
coverage. The [store tests](../../pir/transparent-wallet-store/tests/store_semantics.rs)
exercise memory/SQLite parity and legacy progress migration. The regression
crate tests the oracle independently.

```sh
cargo test -p transparent-wallet -p transparent-wallet-store --all-features
cargo test -p transparent-shard-server --test wallet_anchor \
  --test wallet_continuation --test wallet_sync
cargo test -p transparent-regression
```

The accepted-anchor path validates complete retrieved histories for a shard,
but persists ledger events only through the requested height. Pagination
validation counts include discarded future events and do not derive from the
ledger. Pending work belongs to a target and revision; changing the target
restarts unfinished retrieval. Coverage carries its accepted endpoint separately
from its full publication endpoint. Explicit rollback takes an accepted ancestor
hash as well as a height; it never puts a shard-end hash on a clipped range.
