# Receiver directory evidence

Each run directory holds a results note (`README.md`), a machine-readable
`manifest.json` and a `SHA256SUMS` over its files; run `shasum -a 256 -c SHA256SUMS`
inside it. Apart from the deployment record, these are pre-deployment development
runs. They describe the source and protocol they record, which later changes
replaced, so each note says what is historical.

- [Mainnet smoke test](mainnet-smoke-2026-09-26/README.md): a bounded mainnet
  range holding the known NEAR refund; its reported RPC cross-check and rerun were
  not retained.
- [Batched backfill](batched-backfill-2026-09-26/README.md): concurrent batched
  block reads 13.8 times faster than sequential ones, with byte-identical
  publications.
- [Receiver to Enhance integration](receiver-pir-2026-09-26/README.md): an
  encrypted lookup of the full publication recovered the refund, then an Enhance
  query authenticated its output (q48 protocol).
- [Publication cache](publication-cache-2026-09-28/README.md): cached witness
  preparation and a tip-to-HTTP publication time of 12.4 seconds, locally.
- [Deployment record](deployment-2026-10-08/README.md): the live Droplet, DNS
  record, absent cloud firewall, host service and public session on 2026-10-08.
