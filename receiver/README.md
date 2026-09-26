# Receiver directory

Prototype receiver discovery shared by wallets and indexers. The directory maps a
canonical 43-byte Ironwood receiver to payments recoverable with the all-zero
outgoing viewing key (OVK). Refunds and swaps into Zcash use the same lookup.
Coinbase recipients are excluded. All Ironwood Actions still count toward note
positions, including coinbase and outputs that cannot be recovered with zero OVK.

## Records

Each 285-byte record contains version (1 byte), receiver (43), page (4), total
pages (4), present flag (1), height (4), block hash (32), txid (32), transaction
index (4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32), and ciphertext prefix (52). Integers are
little endian. Hashes use protocol byte order, not RPC display order.

The caller trial decrypts the compact fields with its receiving key. The note
position addresses the existing Enhance database and supports note-tree recovery.
The Action input nullifier supplies decryption context. It is not the recovered
note's spend nullifier. A directory match alone does not establish ownership,
spent status, a valid witness, or a spendable balance.

Payments are ordered by note position, with one payment per page starting at zero.
Every page repeats the same total. A wallet fetches all pages from one accepted
publication. A missing continuation is an error. An entirely zero slot represents
absence only within that publication's coverage and zero-OVK scheme.

## Publication

`receiver-directory` builds deterministic fixed-width rows suitable for a PIR
database. The initial geometry uses 4096-byte rows, 14 records per row, and zero
padding. A domain-separated SHA-256 of the genesis hash and receiver identifies
the receiver. A second domain-separated hash of publication salt, receiver tag,
and page selects a row. Full receiver bytes in the response disambiguate hash
collisions. Bucket overflow rejects the candidate publication instead of omitting
records. This geometry is a prototype, pending PIR bandwidth measurements.

The manifest binds the network, profile, inclusive block coverage, boundary block
hashes, starting and ending tree positions, row geometry, salt, record count, and
SHA-256 of the complete row data. Its SHA-256 identifies an immutable revision.
The wallet checks coverage against an independently accepted chain anchor before
using results. An indexer can still omit payments. A digest is not a completeness
proof, and PIR conceals a query rather than authenticating the chain.

No receiver-specific HTTP lookup or live PIR transport is provided by this crate.
Row selection must happen inside PIR. These rows are not the compact bulk-download
format discussed for restores.

## Validation

`cargo test -p receiver-directory` covers authenticated recovery from a public
mainnet refund Action, ciphertext tampering, strict encoding, pagination,
coverage, deterministic publications, and bucket overflow.

Outgoing recovery delegates to `zakura-orchard`. Protocol reference:
`zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`,
v2026.7.0-202-gafa086, NU6.3 proposal, section 4.19.3 (`decryptovk`).

## Local indexing

The standalone `receiver-directory` binary lives in `enhance-pir-server` to reuse
its canonical block parser and RPC client. It does not enable an Enhance route or
modify an Enhance journal. Receiver extraction, records, lookup, and SQLite
storage live in the shared crate. Wallets do not need its optional `store` feature.

```sh
cargo run -p enhance-pir-server --bin receiver-directory -- \
  --data-dir /path/to/receiver-state \
  --rpc-url http://127.0.0.1:8232 --cookie /path/to/.cookie
```

For an explicitly selected node without authentication, replace `--cookie` with
`--no-auth`. The command verifies the mainnet genesis and defaults to coverage
from Ironwood activation through the startup tip minus ten blocks. Use
`--start-height` and `--end-height` for a bounded test. Such a publication does
not cover earlier history. The node supplies consensus validation and tree sizes.

Each block commits its records and coverage together. Restarting checks saved
hashes, removes orphaned blocks and their payments, then resumes. A reorg crossing
the configured start boundary requires an explicit rebuild in a new directory.
RPC failures stop the run without advancing the failed block. Rerun the same
command to resume. This is a bounded backfill command, not a polling daemon.

Successful runs write `<revision>.rows` and `<revision>.json` under
`publications/`, then atomically replace `current.json` with that manifest.
Consumers must revalidate its terminal block against their accepted chain. Old
revision files may remain after a reorg and are not evidence of current coverage.
The prototype stops rather than dropping records if its 65,536-row limit is
exceeded. No public serving process is started.

`cargo test -p receiver-directory --features store` adds restart and transactional
rollback checks. `cargo test -p enhance-pir-server --test receiver` covers coinbase
exclusion with preserved positions, RPC anchor validation, and command-line
restart/reorg publication. Its synthetic block envelopes test indexing contracts,
not consensus validation.
