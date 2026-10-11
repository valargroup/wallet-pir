# Prepared txid display cutover request (v6), 2026-10-11

Prepared, not run. Roman reviews, completes and runs it from his deploy
checkout; the phases, outage, rollback and deletions are in
[deployment](../../../docs/deployment.md#txid-display-one-query-cutover-prepared-2026-10-11).

[`request-v6.json`](request-v6.json) starts from the last request in the
repository, [`request-v4.json`](../../fleet-redeploy-9d2cbda0-2026-10-10/raw/txid/request-v4.json),
and applies the combined layout that `4bf32869` put in production: no
`recent` host, unit or baseline. Then, for the new lineage:

| Key | v4 | v6 |
|---|---|---|
| `controller.geometry` | `txid-2k` | `txid-2k8` |
| `controller.archive_target` | 40,000 | 90,000 |
| `controller.recent_floor` | 10,000 | removed |
| `controller.recent_window` | — | 90,000 |
| `controller.reorg_margin` | 100 | 100 |
| `controller.max_archive_shards` | 1,000 | 1,000 |
| `controller.archives`, `replay_seals` | 425, 4 | 189, 0 (unused: bootstrap gets explicit heights) |
| `publication.root` | `/srv/txid-display-genesis/v2/root` | `/srv/txid-display-genesis/v3/root` |
| `measure.output` | `…/v2/measure` | `…/v3/measure` |
| `journal.data_dir` | `/srv/txid-display-genesis/journal-v2x` | unchanged |
| `units.archive` | 40G, 24 GiB cache, 32 GiB disk cache, 4 threads, 2 build slots | unchanged |

Still to fill before `plan`, as for every earlier request:
- `source_sha`: the wallet-pir `main` commit with this layout;
- `release`: the `transparent-txid-display` bundle of that commit's full CI
  run (`ci_run`, `archive_sha256`, binary and file digests);
- `baseline`: available memory from `txid-display-preflight`;
- `router.live_fleet_sha256` and `history.*`, if history moved since v4.

The current production request (v5, combined) lives only in Roman's deploy
state; compare v6 with it before planning, and keep v5 for the rollback.
