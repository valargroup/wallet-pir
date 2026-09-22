# Protocol

Enhance PIR replaces transaction-specific retrieval for the encrypted output
data a wallet needs after compact scanning. The wallet derives an Ironwood
output position, sends a private real-or-dummy query, and receives one fixed
record without revealing the selected position.

## Record format

Schema 9 stores exactly 737 bytes per output position:

| Offset | Length | Field | Use |
| ---: | ---: | --- | --- |
| 0 | 32 | `ephemeralKey` | Note key agreement |
| 32 | 580 | `encCiphertext` | Note and authenticated memo |
| 612 | 32 | `cv_net` | OVK-based outgoing recovery |
| 644 | 80 | `outCiphertext` | OVK-based outgoing recovery |
| 724 | 1 | flags | Transparent inputs/outputs (bits 0/1), fee present (bit 2) |
| 725 | 4 | expiry height | Little-endian u32; zero disables expiry |
| 729 | 8 | fee | Little-endian u64 zatoshis; zero payload when absent |

Thirty-three consecutive records form a 24,321-byte PIR row. The client privately
retrieves the row and selects the requested record locally. The active table
does not contain txids, nullifiers, note commitments, mined heights, or witness data.

## Protocol boundary

`enhance/crates/enhance-pir` owns public record and generation types plus client query logic.
`enhance/services/enhance-pir-server` owns canonical ingestion, the append-only journal,
sealed shards, workers, and HTTP routing. The protocol identifier is
`ironwood-enhance-pir-v3`; clients must reject another identifier, schema,
PIR profile, record width, row width, setup seed, or derived parameter set. The
PIR profile is `simplepir-p16-q46-v1`: `p = 65,536`, six instances, a 46-bit
query and a 20-bit response at the 8,192-row shard geometry.

The client uses only:

- `GET /v1/health`
- `GET /v1/enhance/init`
- `POST /v1/enhance/query`

This is a breaking replacement for the former memo/action API. There are no
compatibility aliases because interpreting an old record with the new offsets
would be unsafe. Client conformance and adoption must be verified for the intended cutover; see
[status](status.md) for the limits of the committed rollout evidence.

## Atomic initialization

`GET /v1/enhance/init` returns an `EnhanceSession` JSON object containing:

- `generation`: network/pool, schema/protocol, chain anchor, tree size, generation
  ID, record/row/shard geometry, parameter ID, setup seed, public-parameter epoch
  and digest, and shard descriptors;
- `params`: the pinned PIR scheme parameters for the logical database;
- `public_params_base64`: the published public material for that same generation.

The network identifier is `main`, the pool is `ironwood`, and activation height is
3,428,143. Logical rows are the next power of two at or above the used row count,
with a minimum of 8,192. Used rows equal `ceil(ironwood_tree_size / 33)`.

The client regenerates expected scheme parameters from geometry, checks them for
exact equality, checks the SHA-256 digest of the decoded public material, and
checks that the epoch is its first eight digest bytes. It also validates the
expected public-material length. These are compatibility and consistency checks,
not an independently trusted commitment to canonical chain contents.

## Binary query and response

`POST /v1/enhance/query` takes a binary body, not JSON. All integer prefixes below
are little-endian. Use the pinned client encoder: packing keys and coefficients
are scheme-specific encodings, not a portable list of JSON numbers.

| Message | Layout |
|---|---|
| Request | 8-byte generation ID, serialized fresh packing keys, switched query coefficients |
| Response | 8-byte generation ID, 8-byte public-parameter epoch, encoded PIR response body |

The client checks both response prefixes and the exact expected body length
before decoding. Decoding yields a row; slot selection and record validation
happen locally. For a given generation, real and dummy requests use the same
construction. Each request uses fresh query randomness and fresh packing keys.
The private coordinator/worker wire protocol is separate from this public API.

Initialization returns 503 when no generation is available. Query failures,
including an unretained generation or admission failure, currently return 503;
there is no public structured error that distinguishes those causes. The
coordinator's query body limit is 64 MiB. The reference client imposes stricter
response limits described in [integration](integration.md).

## Record validation and trust

Bits 3–7 of the flags byte must be zero. When fee-present is clear, the eight fee
bytes must be zero; when set, zero is a valid known fee. Fees above
2,100,000,000,000,000 zatoshis and expiry heights at or above 500,000,000 are
rejected. Encoding validation does not authenticate a record.

Fee, expiry and transaction-shape flags are indexer assertions. The wallet must
perform its normal authenticated note/outgoing recovery and bind the result to
its scanned action. The public parameter hash does not remove that requirement.
The [integration guide](integration.md) covers sessions, reorgs, missing metadata
and the privacy consequences of fallback retrieval.
