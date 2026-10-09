# Receiver directory evidence

Each run directory holds a results note (`README.md`), a machine-readable
`manifest.json` and a `SHA256SUMS` over its files; run `shasum -a 256 -c SHA256SUMS`
inside it. These are historical development runs, not release qualification. Only
the files listed in each run's `SHA256SUMS` were retained, so any detail, check,
log, attempt history or environment fact not in those files cannot be re-verified.
Cited commits live on development branches and may not resolve later.

- [Batched backfill](batched-backfill-2026-09-26/README.md): concurrent batched
  block reads 13.8 times faster than sequential ones under the test profile, with
  identical publication digests, over a bounded mainnet range holding the known
  NEAR refund.
- [Receiver to Enhance integration](receiver-pir-2026-09-26/README.md): an
  encrypted lookup of the full publication recovered the refund, then an Enhance
  query authenticated its output (q48 protocol).
- [Publication cache](publication-cache-2026-09-28/README.md): cached witness
  preparation and a tip-to-HTTP publication time of 12.4 seconds, locally.
