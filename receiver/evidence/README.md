# Receiver directory evidence

Each run directory holds a results note (`README.md`), a machine-readable
`manifest.json` and a `SHA256SUMS` over its files; run `shasum -a 256 -c SHA256SUMS`
inside it. These are pre-deployment development runs, not release qualification:
they describe the source and protocol they record, which later changes replaced, and
each manifest's `not_retained` lists the inputs that were not kept, so those claims
cannot be re-verified. Manifests keep three kinds of fact apart: what the run
observed and retained; source configuration that git still shows (revisions,
toolchain pins, lockfiles), which declares what a build would use, not what built
the run; and what was not recorded.

- [Mainnet smoke test](mainnet-smoke-2026-09-26/README.md): a bounded mainnet
  range holding the known NEAR refund; its reported RPC cross-check and rerun were
  not retained.
- [Batched backfill](batched-backfill-2026-09-26/README.md): concurrent batched
  block reads 13.8 times faster than sequential ones under the test profile, with
  publications reported byte-identical (not retained).
- [Receiver to Enhance integration](receiver-pir-2026-09-26/README.md): an
  encrypted lookup of the full publication recovered the refund, then an Enhance
  query authenticated its output (q48 protocol).
- [Publication cache](publication-cache-2026-09-28/README.md): cached witness
  preparation and a tip-to-HTTP publication time of 12.4 seconds, locally.
- [Deployment record](deployment-2026-10-08/README.md): a point-in-time inventory,
  not a development run, of the live Droplet, DNS record, absent cloud firewall,
  host service and public session on 2026-10-08.
