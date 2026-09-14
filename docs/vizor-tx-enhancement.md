# Vizor Ironwood transaction enhancement

Scope: Enhance PIR. For transparent script-history recovery, use the [current transparent PIR index](transparent-pir/README.md); the retained outpoint-keyed transparent-spend protocol is a different component.

Enhance PIR replaces transaction-specific retrieval for the encrypted output
data a wallet needs after compact scanning. The wallet derives an Ironwood
output position, sends a private real-or-dummy query, and receives one fixed
record without revealing the selected position.

## Record format

Schema 7 stores exactly 737 bytes per output position:

| Offset | Length | Field | Use |
| ---: | ---: | --- | --- |
| 0 | 32 | `ephemeralKey` | Note key agreement |
| 32 | 580 | `encCiphertext` | Note and authenticated memo |
| 612 | 32 | `cv_net` | OVK-based outgoing recovery |
| 644 | 80 | `outCiphertext` | OVK-based outgoing recovery |
| 724 | 1 | flags | Transparent inputs/outputs (bits 0/1), fee present (bit 2) |
| 725 | 4 | expiry height | Little-endian u32; zero disables expiry |
| 729 | 8 | fee | Little-endian u64 zatoshis; zero payload when absent |

Nine consecutive records form a 6,633-byte PIR row. The client privately
retrieves the row and selects the requested record locally. The active table
does not contain txids, nullifiers, note commitments, mined heights, or witness data.

## Protocol boundary

`pir/enhance` owns public record and generation types plus client query logic.
`server/enhance-pir-server` owns canonical ingestion, the append-only journal,
sealed shards, workers, and HTTP routing. The protocol identifier is
`ironwood-enhance-pir-v2`; clients must reject another identifier, schema,
record width, row width, or setup seed.

The client uses only:

- `GET /v1/health`
- `GET /v1/enhance/init`
- `POST /v1/enhance/query`

This is a breaking replacement for the former memo/action API. There are no
compatibility aliases because interpreting an old record with the new offsets
would be unsafe. Production rollout remains blocked until Vizor has migrated.
