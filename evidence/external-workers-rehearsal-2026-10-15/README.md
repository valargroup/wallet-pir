# External worker operator rehearsal, 2026-10-15

Every external worker role was installed, served, restarted, updated, refused
and rolled back from an unpacked operations package (`wallet-pir-ops`,
source 1cfb6bc2, clean) and copies of release-fast binaries, through the
package's own deploy engine and product adapters. The harness ran outside the
source checkout with an audit hook refusing any read under it. Real files,
processes and HTTP; the local executor emulates systemd without privileges and
does **not** enforce the sandbox (ProtectSystem, ReadOnlyPaths, users), which
only a real systemd host qualifies. CPU only: no GPU qualification.
`manifest.json` records binaries, package digest, host and contention.

| Role | Fresh install (absent baseline, product check) | Exact encrypted answers | Restart | Publication update | Failed update | Rollback to absence |
|---|---|---|---|---|---|---|
| Receiver `serve-publication` | pass (`--check` on staged config) | pass: fixture lookup vs construction, in the deploy's exact check and after restart/update | pass | pass (atomic record) | pass: refused record, still serving; refused configuration changed nothing | pass (config removed, state kept); config update rolled back |
| Enhance worker (×2) | pass (no staged inputs; no check manufactured) | pass: release's isolated synthetic coordinator, 2 publications, 9 exact boundary/retained probes | pass | pass (controller generations 1–3) | — | pass |
| Transparent history | pass (`--verify-only`) | pass: soak-query rows vs publication files | pass | pass (controller prepare/activate) | pass in run 2 (refused prepare, still exact); run 1 failed, see below | pass |
| Transparent display | pass | pass: wallet display client, 13 exact entries per phase | pass (active record restored) | pass | pass (refused prepare, still exact) | pass |
| Status worker | pass | **not demonstrated**: see below | pass | — | — | pass |

Retained failures:

- Run 1, history: right after a refused prepare the oracle got a retryable
  `no free cache capacity` and its 3-second window expired. The foundation's
  prepare evicts the active publication's unpinned runtimes before loading a
  candidate, so a loaded-only worker rebuilds on the next query. Run 2, same
  package and binaries with a 60-second oracle window, passed.
- Status exact answers: the release's `validate-distributed` spawns its own
  worker role (not the installed unit) and has a fixed 20-second prepare
  timeout, which this host exceeded at load average 21–55, in run 1 and in a
  standalone retry. Exact Status answers against an installed worker need an
  uncontended host or a fixture controller that targets an existing worker.
