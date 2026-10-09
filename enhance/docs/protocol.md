# Protocol

The server implements schema 11. The served protocol revision is selected at
build time, not at runtime: the default build publishes `ironwood-enhance-pir-v7`
(q48 profile, control version 2); a build with the `native-reinspiring` Cargo
feature publishes `ironwood-enhance-pir-v9-native-two-mask-m29` (native two-mask
packing, control version 4). A deployment serves exactly one of them, and the
wallet library must be built with the matching feature. The interim
`ironwood-enhance-pir-v8-native-poc` revision is retired and is no longer produced
by any build. The remainder of this document describes the shared framing and
routing contract; the v7 wallet must use the v7/q48 profile and the same pinned
IPIR implementation. The v6/q48 and older clients are incompatible.

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

`GET /v1/enhance/init` returns the manifest. `generation` is the routing revision;
`placement_revision` tracks ready placement. The decimal-string `recovery_epoch`
and `domain_recovery_epochs` fence invalidated content. Coverage contains canonical
`global_start`, `global_end`, `domain_id`, and `local_start` routes. The client
validates geometry and routes and independently accepts the anchor before setup.

`GET /v1/enhance/session/:session_id` returns content-addressed session material.
The legacy-shaped `/v1/enhance/sessions/:generation/:shard` lookup also exists;
new clients use the session ID. Check parameter identity, material digest, length
and application resource limits before allocating a session.

`POST /v1/enhance/query` starts with this 116-byte header. Responses repeat the
same binding. Integers are unsigned little-endian; hashes and IDs below are raw
bytes, not hex text.

| Offset | Length | Field |
| ---: | ---: | --- |
| 0 | 4 | ASCII `EPQ7` |
| 4 | 8 | routing revision (`generation`) |
| 12 | 8 | query domain (`shard_id`) |
| 20 | 8 | packing material digest prefix (`epoch`) |
| 28 | 8 | routing recovery epoch (manifest-wide) |
| 36 | 32 | session ID |
| 68 | 16 | fresh request ID |
| 84 | 32 | accepted canonical anchor hash |

Reject wrong versions, lengths and every mismatched response field. Use fresh PIR
randomness and a fresh request ID for every request, including retries and cover.
HTTP 409 has code `stale_routing`; HTTP 410 distinguishes `noncanonical_session`
from `session_unavailable`. HTTP 429 is `overloaded`; HTTP 503 is
`temporarily_unavailable`. Refresh routing on 409/410 and at least every 30 seconds
before further work. Wallet anchor acceptance is required before rebinding cached
material. Routing refresh can reuse unchanged sessions; it does not itself prove
that the server's anchor is canonical.

### Canonical session identity

SHA-256 starts with literal bytes `enhance-pir/v7/session\0`. Strings are encoded
as their byte length (u64 LE) followed by UTF-8 bytes. Append the protocol revision
string, then u64 LE values: domain ID, domain recovery epoch, logical row count,
owned record count, and unit count. For each ordered unit append u64 LE recovery
epoch, local row start, allocated rows, followed by the content hash, setup hash,
and parameter ID as length-prefixed strings. Finally append the packing parameter
ID and public-material hash as length-prefixed strings. Hash strings are canonical
lowercase hexadecimal text here, not decoded bytes. Zero gaps are fixed by the
validated canonical domain layout; stored units commit to all padding bytes.

Routing revision, placement revision, anchor and lifecycle label are excluded.
Consequently sealing and placement alone preserve identity; changed content,
composition or domain recovery epoch does not. The server and wallet share a
[frozen vector](../crates/enhance-pir/tests/fixtures/v7-session.json) checked by
[an independent encoder](../tools/v7-interop/check_vectors.py). The vector digest is
`09d98421023fcb7801828a031db61a263a69aabba820d214d22e773cbeb12e0b`.

The PIR profile is `simplepir-p16-q48-v1`: 48-bit query transport, p16
plaintexts, and unchanged 20-bit responses. The wider query reduces rounding
noise without changing the decoding threshold. Query domains remain 4,096,
8,192, 16,384, or 32,768 rows, with 2,048/4,096/8,192-row mutable units.
The deterministic public setup seed and the literal domain
`ironwood-enhance-pir-v4/main/ironwood/setup\0` are intentionally unchanged to
match the wallet. Schema, protocol, parameter identities, and content hashes
separate the new records and artifacts. The setup domain remains unchanged in v7; the new wire header is `EPQ7`.

A `native-reinspiring` build's query body is the header, a 27,648-byte packing
key and the selection. Servers accept the selection at 49 bits rounded to nearest
(`rows * 49 / 8` bytes), which the in-repo client sends, or at 44 bits with
dithered rounding (`rows * 44 / 8` bytes), and tell them apart by the body's exact
length; any other length is refused. The width is not part of the header, the
session or the parameter identity, so v9 clients are unaffected. Dithered
rounding needs its own per-snapshot correctness certificate; see the
[dithered query screen](../evidence/dithered-query-2026-10-09/README.md).

The authoritative wire types and validation are in
[the protocol module](../crates/enhance-pir/src/protocol.rs); record encoding is in
[record.rs](../crates/enhance-pir/src/record.rs).

## Compatibility and trust

Schema-9, schema-10, v5/q46 and v6/q48 clients are incompatible. A default build
serves only v7; a `native-reinspiring` build serves only v9. v7 and v9 state,
manifests and parameter identities are mutually incompatible, and v8 state is not
readable by either build. Controller state is version 7. Use fresh controller and worker
directories and rebuild publications/caches. Existing schema-11 canonical journals
may be copied while stopped and validated; older record-width journals cannot
supply the required suffix records. Never adopt v6 serving state as v7 state.

Incoming authentication and stale wallet identities remain wallet obligations.
Outgoing decryption should authenticate recoverable outputs; send-only association
may rely on server trust when decryption cannot authenticate an action. PIR
hides the chosen position, but timing and the number of row queries remain
observable. See [integration](integration.md) and [deployment](deployment.md).
