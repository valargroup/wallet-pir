# Receiver directory evidence

Each run directory holds a results note (`README.md`), a machine-readable
`manifest.json` and a `SHA256SUMS` over its files; run `shasum -a 256 -c SHA256SUMS`
inside it. These are pre-deployment development runs. They describe the source
and protocol they record, which later changes replaced, so each note says what is
historical.

- [Mainnet smoke test](mainnet-smoke-2026-09-26/README.md): a bounded mainnet
  range indexed twice to the same revision, with the known NEAR refund checked
  against an independent RPC block.
- [Batched backfill](batched-backfill-2026-09-26/README.md): concurrent batched
  block reads 13.8 times faster than sequential ones, with byte-identical
  publications.
- [Receiver to Enhance integration](receiver-pir-2026-09-26/README.md): an
  encrypted lookup of the full publication recovered the refund, then an Enhance
  query authenticated its output (q48 protocol).
- [Publication cache](publication-cache-2026-09-28/README.md): cached witness
  preparation and a tip-to-HTTP publication time of 12.4 seconds, locally.
