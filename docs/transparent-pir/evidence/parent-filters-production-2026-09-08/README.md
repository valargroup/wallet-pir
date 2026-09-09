# Production parent-filter deployment, 2026-09-08

Archive parent artifacts are publicly served at [archive-wide.json](https://enhance-pir.valargroup.dev/v1/filters/parents/archive-wide.json). The user explicitly authorized production deployment after accepting the measured 1,000-script overhead. This ships the evaluated K=8/M=100/P=6 archive bundle; recent traversal remains direct. Wallet client support is commit `5577b34`; callers must enable the manifest URL. No native wallet application release was performed.

## Deployment

The coordinator's existing Caddy service serves the 20 parent bodies and manifest from `/srv/transparent-parent-filters/public`, a symlink to `/srv/transparent-parent-filters/releases/archive-k8-m100-5577b34`. The source is the previously verified full-journal sweep. Staging checked all 160 child descriptors against the live archive and all body lengths/digests before activation. Public coverage was 3,476,743 during staging. Caddy configuration validation passed, the prior configuration was backed up, and Caddy was reloaded. No PIR worker, publisher or filter-service binary was restarted or replaced.

[stage.json](stage.json) and [activation.json](activation.json) record deployment digests. [public-artifacts.json](public-artifacts.json) records independent public HTTPS verification of all 20 bodies and agreement with the archive descriptors from both public origins. [post-deploy-health.txt](post-deploy-health.txt) records the exact client archive/binary hashes and active service checks; the private init endpoint also returned successfully.

## Production recovery validation

Both five-wallet runs completed with **5/5 exact recoveries** and compatible advancing publications. The same sampled scripts, accepted anchor 3,473,686 and recovery ranges were used in both variants. The preparation phase performed real HTTP recovery for the three recent wallets; the second variant reused these validated preparation seeds. Each measured wallet used its own cold SQLite filter/setup caches.

| Profile | Baseline download MiB | Parent download MiB | Baseline seconds | Parent seconds |
| --- | ---: | ---: | ---: | ---: |
| catch-up-1d | 0.306 | 0.306 | 0.490 | 0.345 |
| catch-up-30d | 0.551 | 0.551 | 0.598 | 0.534 |
| catch-up-7d | 0.306 | 0.306 | 0.453 | 0.283 |
| restore-old | 60.571 | 23.872 | 3.534 | 2.447 |
| unused | 60.423 | 13.635 | 3.207 | 1.335 |

The three recent profiles downloaded exactly the same number of bytes and made no parent discovery/body requests. Archive restore and unused-wallet traces contain real parent downloads and selective child requests. Full raw reports are [baseline.json](baseline.json) and [parents.json](parents.json); downloads are HTTP payload bytes, excluding TLS/socket overhead.

This is a bounded deployment smoke test: one minimum-event sampled wallet per profile, chosen deterministically by event count then original index. It is not the held-out 40-wave performance study, and the single timing observation per profile does not establish a latency distribution. The earlier baseline timeout and 1,000-script regression remain recorded in the [offline evaluation](../parent-filters-2026-09-08/README.md).

## Reproduce or roll back

[Deployment instructions](../../deployment.md) describe staging and routing. Raw configs, original sample indices, request traces, the source tar, client build log and the activation script are retained under `/srv/zakura/parent-filter-eval-20260908/`; the two canary runs are in `production-canary/`.

For this first rollout, withdraw the bundle by unlinking only `/srv/transparent-parent-filters/public` after checking it still targets this release. Parent-enabled wallets fall back to child filters when the manifest or a body is unavailable. To withdraw the route as well, remove the parent handler from the current Caddyfile, validate the candidate, and reload Caddy. The exact predecessor is `Caddyfile.before-parents` under the experiment root; restore the whole file only if no intervening routing changes occurred. Keep immutable release artifacts and evidence for diagnosis.
