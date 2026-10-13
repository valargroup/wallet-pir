# Enhance canary broken by the monitor upgrade, 2026-10-13

The `pir-monitor-quality` installed at 03:15 UTC was built without the
`native-reinspiring` feature. Production Enhance serves native queries, so the
monitor's Enhance canary failed every probe at init (`init_failed`) from
03:17 UTC. Its family is active, so `canary_failures` on `public_pir` paged
#pir-alerts and repeated every 30 minutes for about nine hours. Enhance itself
was not affected.

- **12:23 UTC.** Reverted to `pir-monitor-quality.before-1868e5a6`; the canary
  passed at once. The txid probe was removed, because the old binary refuses it.
- **12:30 UTC.** A build from `23406014` plus `cdabca6d` without the feature
  failed the same way and was reverted within two minutes.
- **12:34 UTC.** Installed `23406014` plus `cdabca6d`, built with
  `--features native-reinspiring` (SHA-256 `776ca2af…c02e`), with the txid probe
  restored. The Enhance canary, `status`, `receiver`, `transparent` and `txid`
  all pass, with no active incident.

Every pir-monitor build must pass `--features native-reinspiring`.
