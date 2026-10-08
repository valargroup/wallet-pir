# Txid display v2x input-listing census, 2026-10-08

Raw output of `txid-sizing-export --journal-census --v2x` (wallet-pir `a368f19b`) over a
genesis-to-3,508,673 journal written by `event-ingest --txid-display-inputs` (wallet-pir
`29cd8c34`, experimental unpublished codec `transparent-txid-display-v2x`) on the coordinator's
dedicated volume on 2026-10-08. Measurement only: nothing was published or served.

- `census.json.gz`: census JSON (schema `txid-display-journal-census-v2x`), gzip -9 -n; the
  uncompressed SHA-256 is in `receipt.json`. It holds two populations over the same 17,024,724
  records: `with_inputs` (v2x bytes) and `inputs_stripped` (their v1 encoding, which matches the
  v1 census in [txid-display-genesis-census-2026-10-07](../txid-display-genesis-census-2026-10-07/README.md)).
- `receipt.json`: checkout, command, compiler (1.97.1), binary and JSON digests, ingest binary
  digest, command and window (00:47–05:48 UTC, 0 restarts).

Headline numbers (threshold 128 B, stored sizes; reservation formula, 4,096-row page tables):

| | inputs stripped (v1) | with every input |
|---|---:|---:|
| Mean record | 330 B | 931 B |
| Inline at 128 / 256 / 1,024 B | 92.5 / 94.4 / 95.4% | 47.9 / 77.8 / 87.6% |
| Page rows per 40k archive, p50 (all / Sprout / NU6.3) | 2,081 / 8,034 / 116 | 7,094 / 21,313 / 2,049 |
| Reserved memory, pages not shared / shared by 16 | 79.3 / 60.0 GiB | 134.1 / 118.3 GiB |

163,392,001 transparent inputs; one input costs 58–66 encoded bytes (p50 63). Memory is the
source reservation formula, not native RSS, and predates the reservation true-up (`3d727bfc`).
