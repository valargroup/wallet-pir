# Protocol

The supported server implements schema 11, `ironwood-enhance-pir-v6`, using the
architecture-2 runtime. Its Rust modules and binary retain their `v4` names.
The wallet counterpart must use the v6/q48 profile and the same pinned IPIR
implementation. The prior [schema-11 wallet PR #28](https://github.com/zakura-core/wallet-libraries/pull/28)
uses v5/q46 and is incompatible without the q48 follow-up.

## Record format

Each Ironwood action occupies 653 bytes at its commitment-tree position:

| Offset | Length | Field |
| ---: | ---: | --- |
| 0 | 528 | `enc_ciphertext[52..580]` |
| 528 | 32 | `cv_net` |
| 560 | 80 | `out_ciphertext` |
| 640 | 1 | flags: transparent inputs/outputs (bits 0/1), fee present (bit 2) |
| 641 | 4 | expiry height, little-endian u32 |
| 645 | 8 | fee in zatoshis, little-endian u64 |

Reserved flag bits must be zero. Expiry must be below 500,000,000. An absent fee
has a zero payload; a present fee cannot exceed 21,000,000 × 100,000,000.
Expiry zero disables expiry. These fields and the transparent flags are trusted
indexer metadata; note decryption does not authenticate them.

The wallet retains the ephemeral key and first 52 ciphertext bytes from the
same compact-scanned action. It concatenates that prefix with the 528-byte PIR
suffix before note authentication. Neither omitted field is supplied by PIR.
The table contains no transaction IDs, nullifiers, commitments, or witnesses.

33 consecutive records form a 21,549-byte row. Row and slot are derived from
the action position using division and remainder by 33. Records never straddle
rows. Padding is zero, including the high byte of the final u16 plaintext.
Compared with schema 10, raw records and rows shrink by 11.4%; the row still
requires six PIR instances. This does not imply equivalent savings in query
responses, expanded databases, or latency.

## Initialization and queries

`GET /v1/enhance/init` returns the architecture-2 `Manifest`: schema/protocol,
network/pool, generation, anchor height/hash, shard geometry and coverage,
session references, and mutable-unit identities. The client validates the
manifest and binds it to locally scanned chain state before requesting setup.

`GET /v1/enhance/sessions/:generation/:shard` returns the generation-bound shard
session, including derived parameters and base64 public material. The wallet
checks parameter identity, public-material digest and length, and resource
limits before allocating a query session.

`POST /v1/enhance/query` carries an opaque PIR query. Its 28-byte header retains
`EPQ4`, followed by little-endian u64 generation, little-endian u64 shard ID,
and the eight-byte public-material digest prefix. Responses use the same
binding. Both peers reject mismatched generations, domains, epochs, and lengths.
Expired generations return HTTP 410; a refreshed manifest requires fresh wallet
acceptance. A row result is selected locally; action identities never go to the
server. Same-transaction batching needs no new server endpoint.

The PIR profile is `simplepir-p16-q48-v1`: 48-bit query transport, p16
plaintexts, and unchanged 20-bit responses. The wider query reduces rounding
noise; it does not change the decoding threshold. Query domains remain 4,096,
8,192, 16,384, or 32,768 rows, with 2,048/4,096/8,192-row mutable units.
The deterministic public setup seed and the literal domain
`ironwood-enhance-pir-v4/main/ironwood/setup\0` are intentionally unchanged to
match the wallet. Schema, protocol, parameter identities, and content hashes
separate the new records and artifacts. Do not mechanically rename the header
or setup domain to v6.

The authoritative wire types and validation are in
[the protocol module](../crates/enhance-pir/src/v4.rs); record encoding is in
[record.rs](../crates/enhance-pir/src/record.rs).

## Compatibility and trust

Schema-9, schema-10, and schema-11/v5 q46 clients are incompatible.
V6 changes query precision without changing the schema-11 record layout. The legacy serving commands
are retired. Journals validate record width, controller state is version 6,
worker state requires schema 11 and protocol v6, and preprocessing artifacts are version 9.
Use fresh data directories and rebuild publications and caches. The older
`migrate-v4-journal.py` only repacks full records and cannot prepare schema 11.

Incoming authentication and stale wallet identities remain wallet obligations.
Outgoing decryption should authenticate recoverable outputs; send-only association
may rely on server trust when decryption cannot authenticate an action. PIR
hides the chosen position, but timing and the number of row queries remain
observable. See [integration](integration.md) and [deployment](deployment.md).
