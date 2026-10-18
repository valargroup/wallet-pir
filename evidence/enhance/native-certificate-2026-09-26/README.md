# Native two-mask correctness certificates — 2026-09-26

Per-snapshot conditional certificates for the production native profile
(29-bit rounded two-mask output) deployed from `dc0355b`. `manifest.json` records
sources, commands and snapshot identities. `inputs.sha256` hashes the raw inputs,
which are not retained here: the Enhance MPH1 hint (200 MB), its served public
masks and the Status `rows.bin` (96 MiB).

| Snapshot | Query term | Certified full-response failure |
| --- | --- | ---: |
| Enhance gen 15, shard 0 (32,768 × 12,288) | worst case, any u16 database | ≤ 2⁻¹⁵⁸ |
| Status gen 203 (8,192 × 6,144) | actual database | ≤ 2⁻²⁹⁹ |
| Status gen 203 | worst case, any u16 database | ≤ 2⁻²⁶² |

Before exporting, the tool rebuilds two-mask preprocessing from the hint and
requires the rounded public masks to equal the served bytes (Enhance) or the
manifest `public_digest` (Status). This binds the weights to the live snapshot.
Packing weights depend only on that hint and the packing setup. The Enhance query
term uses `65535 × rows` bounds, so that result does not depend on record contents.
ipir-sp's analysis rounds masks as the publisher does: ties upward, modulo q.

`published_bytes` in the reports is the RNP3-equivalent size the checker expects
(36 header bytes + packed masks); the wallet-pir wire omits that header.

## Limits

- Conditional on ipir-sp's weight exporter, the frozen sampler and fresh
  independent sampler draws. Not an independent cryptographic review.
- Each certificate covers its recorded snapshot only. Hints change every
  publication, and runtime per-snapshot certification is not implemented.
- The worst-case query bound removes dependence on database contents for that
  term only; the packing weights still come from the recorded hint.
