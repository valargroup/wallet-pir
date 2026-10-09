# Frozen mainnet regression fixture

Journal-derived on 2026-10-09 from the coordinator's version-3 event journal
against the schema v11 map that both public origins served at export. These are
synthetic groupings of public scripts; they do not identify actual wallets or
establish population weights.

- Network: mainnet, genesis through the case anchor **3,511,700**. The pinned map
  reaches 3,511,887.
- Tier cutoff: **3,289,805**, the served map's first `recent-4k-8k` shard.
- Cases: **11**; checkpoints: **57**; sync calls including repeat and fresh-final
  checks: **79**.
- Exact scripts, required heights and checkpoint choices: [mainnet-cases.json](mainnet-cases.json).
- Expected histories and journal-derived hashes: [mainnet.json](mainnet.json).
- Artifact checksums: [SHA256SUMS](SHA256SUMS).

| Case | Scripts | Checkpoints | Final events |
|---|---:|---:|---:|
| unused-p2pkh | 4 | 5 | 0 |
| unused-p2sh | 1 | 4 | 0 |
| small-active | 1 | 5 | 2 |
| zero-balance | 1 | 5 | 4 |
| old-receive-recent-spend | 1 | 7 | 2 |
| offline-receive-spend | 1 | 7 | 2 |
| active-p2sh | 1 | 5 | 2 |
| reused-pages | 1 | 5 | 647 |
| multi-script-self-transfer | 40 | 5 | 488 |
| recent-birthday | 10 | 5 | 12 |
| coinbase | 1 | 4 | 2937 |

## Schema v11 re-cut, 2026-10-09

History has served schema v11 since 2026-10-03. Its events append transaction
metadata to the 87-byte v10 record, so the v10 fixture's expected events no
longer match what a v11 wallet stores, and its pinned sealed entries no longer
exist: the runner stopped at preflight with publication drift. The case
specification was re-cut with `recut-regression-cases.py` (scripts unchanged):

- the anchor moved from 3,473,686 to 3,511,700, which had about 190
  confirmations at export;
- the tier probes moved from 3,262,749 to 3,289,805, the archive/recent seam
  of the served map, and `recent-birthday`'s birthday with them (its scripts'
  first activity is at 3,290,754);
- one checkpoint, 3,492,693, was sampled into every case from history published
  since the last cut, inside shard 89, which continuous publication sealed.

`regression-export` at `66b0b9fb20bfd112e6f083f5097bdb1e90ba7cde` (executable
SHA-256 `2448cd1e86ba5e11f283e62432bcaf9e5b8bd2b4679116fd616386948d828bd8`)
replayed the journal read-only on the coordinator. Its map input is the exact
served bytes, so `map_sha256` and `publication_file_sha256` are equal; the map
declares no re-cuts and omits `recuts`. Every expected event carries v3 metadata.

Compared with the v10 fixture by legacy event encoding (`--event-metadata`),
every shared checkpoint reduces identically, and all 46 v10 checkpoints
reproduce from the v11 events. Four cases changed at the anchor and were
accepted: `small-active` and `active-p2sh` spent their outputs (their receive
checkpoints are unchanged), and `reused-pages` (106 to 647 events) and
`coinbase` (1,643 to 2,937) kept transacting. `multi-script-self-transfer` and
`recent-birthday` share no height with v10, because all their checkpoints follow
the cutoff or anchor; the 46-checkpoint check covers them. The public production regression then passed all
11 cases and 79 syncs against this fixture, with no failed or retried request.
See the [evidence](../../../evidence/regression-fixture-v11-2026-10-09/README.md)
for the export, comparison and regression records.

## Pinning

Both origins must still serve this set identity and these 90 sealed entries
(shards 0–89) byte for byte. Shard 90 is the unsealed tail and is not pinned
that way: it is republished on every block, so it is checked for continuity —
same shard id, geometry, start height and parent hash, and a range that only
grew — and the wallet syncs against the served map. The fixture bytes therefore
stay valid across publications and are not reissued when the tail moves. A
declared re-cut that rewrites pinned entries is drift and needs a new export.
The fixture's own `map_sha256` records the export-time whole-map digest as
provenance; it is not the gate. A fixture passing offline validation does not mean the deployed
service passed recovery. Run instructions, the exact pinning rule and oracle
limitations are in [testing](../../../docs/testing.md).

## Earlier fixtures

The v10-bound fixture is in Git history before this change. It anchored every
case at 3,473,686 with tier cutoff 3,262,749: 11 cases, 46 checkpoints and 68
sync calls, journal-derived on 2026-09-08 from the coordinator's committed
genesis journal and its full-chain candidate publication.

On 2026-09-28 `mainnet.json` was re-exported from the unchanged version-2 journal for schema v10 (86 shards, publication through 3,499,341). The full public production regression passed all 11 cases and 68 checkpoints before installing this fixture. Cases and every checkpoint expectation are identical to v9; the explicit [same-anchor comparison](../../../evidence/v10-cutover-2026-09-28/publication/qualification/fixture-compare-same-anchor.json) verifies that boundary. Its exporter source was `8e69ea75b1e0071e3b978b0c78cc0487377f9e82`; executable SHA-256 `fe959884f896efaa267fab1a9a16d7df12187d9d2854b23637508d892be32be9`. See the [production regression](../../../evidence/v10-cutover-2026-09-28/regression/report.json).

Earlier on the same day, `mainnet.json` was re-exported by `regression-export` from the version-2 journal against the served schema v9 map (139 shards), with the same case specification. Cases, checkpoints and events are byte-identical to the previous fixture; the map binding, accepted headers and source changed ([comparison](../../../evidence/v9-cutover-2026-09-28/fixture-compare.json)). The previous v7-bound fixture is in Git history before this change.

Before that, event records in `mainnet.json` were rewritten on 2026-09-28 from the 96-byte
codec to the 87-byte codec. Scripts, anchors, balances, UTXOs, spends and
histories are the same logical records. The production journal was not read
or rewritten for that step.

All expectations were generated by `regression-export` through read-only journal
replay and its independent outpoint reducer. The original September 8 export used an uncommitted
working-tree build based on `290dc62`; the fixture says this explicitly rather
than attributing the exporter to a released commit. The exporter executable SHA-256
was `7299923484234160297c7f347287cf883f318bcd93ce533694a6988b5d133038`. The publication file checksum
and canonical served-map checksum are separate fields. The source publication
is the one documented in the [publication evidence](../../../evidence/publication-2026-09-08/README.md).

The initial candidate pool came from the existing seeded workload sample.
Selection checked full histories before assigning labels: an assumed-unused
repeated-byte P2SH script was active and was replaced; the coinbase case was
selected from actual recent coinbase outputs and bounded to 1,643 events.
