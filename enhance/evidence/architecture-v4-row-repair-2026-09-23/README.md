# Offline row repair — September 23, 2026

The `repair-rows` command repairs only units referenced by the target worker's
retained publications or durable candidate. It holds the worker lock, verifies
complete source file lengths and hashes, atomically replaces missing/corrupt rows,
and syncs each replacement. It preserves valid rows and all committed metadata.
Repair is resumable per unit, not an all-or-nothing batch.

Validation:

- [Worker tests](worker-tests.log): all five passed. Coverage includes live-worker
  lock refusal, missing journal, invalid source rejection without destination
  creation, idempotence, ignoring unreferenced files, durable candidate repair,
  unchanged journal bytes, restart after repair and equivalent PIR evaluation.
- [Clippy](clippy.log): library and v4 CLI passed with warnings denied.
- [CLI help](cli-help.log): actual binary exposes the documented arguments.
- [CLI rejection](cli-missing-journal.json): actual command rejected an empty
  target without creating a publication journal. Temporary files were removed.
- Formatting and `git diff --check` passed.

This is native local validation. No remote file transfer, deployed service repair,
hardware qualification, or production mutation occurred. The command requires a
usable local journal and a valid source for each needed unit. Automatic recovery
orchestration, lost-journal recovery, and the distributed failure campaign remain
outstanding. The command's successful result is row verification, not readiness.
