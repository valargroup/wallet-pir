# Transparent quality monitoring moved to schema v11, 2026-10-09

The 2026-10-03 v11 cutover left two monitoring inputs on v10. This change moved
both. No shard server, publisher, router, load or alert mode was changed.
Approved by Roman for this session; the coordinator production lock was held
from 13:49:43 UTC through verification.

## What was wrong

- **Transparent canary.** The independent canary on `wallet-pir-monitor-01` used
  the 2026-09-29 v10 load fixture, so it returned `oracle_invalid` on every sample
  (129 failures, no successes). Its binary, built before v11, also bound every
  query to schema v10, so a new fixture alone would not have fixed it.
- **APM Transparent view.** pir-apm read the pre-v11 roster and the stopped v10
  load's `status.json`. Its load panel showed numbers frozen at 2026-10-03
  17:35:31 UTC, marked `fresh: false`, and the old roster still had archive-03
  owning shards 0–76 instead of 0–81.

## What changed

- **Canary rebuilt** from `854f5677` (main CI passed) with the Haswell baseline
  and fat-LTO `release` profile: SHA-256 `0c8651f3…7024`, highest glibc symbol 2.34
  (the monitor runs 2.39). It binds each query to its checked fixture schema.
- **Fixture.** The continuous v11 load's fixture, copied unchanged (`705770a6…`):
  178 tables, sealed shards 0–88, exported on 2026-10-01 from the initial v11
  publication that passed the cutover's chain-oracle gate. It contains row hashes
  of published plaintext only; no PIR answer was used to build it.
- **Pins.** `transparent-canary-pins.py` confirmed every fixture revision is still
  served sealed in the live map at height 3,511,820. It derived the anchor, block
  3,488,499 `00000000002a155a…86e1` (shard 88's last block), and checked that hash
  at the coordinator's node ([raw/pins.json](raw/pins.json)). The canary checks it
  again at the monitor's node before and after every query.
- **Configs.** `retarget-transparent-quality.py` ran read-only first
  ([monitor plan](raw/monitor-plan.txt), [APM plan](raw/apm-plan.txt)), then applied.
  - **Monitor host:** only the Transparent probe entry changed. The Status and
    receiver entries are byte-identical. `pir-monitor` restarted at 13:50:26.
  - **Coordinator:** APM's `roster` and `synthetic_status` point at
    `/opt/transparent-publisher/v11/roster.json` and
    `/srv/transparent-activity/canonical-load/v11/status.json`. The host sampler's
    roster block points at the same roster. `pir-apm` restarted at 13:50:47.
  - Results: [monitor](raw/monitor-apply.txt), [APM](raw/apm-apply.txt).

## Checks

- **Failure controls** on the monitor host with the new binary
  ([raw/negative-controls.json](raw/negative-controls.json)): a wrong fixture
  checksum and a wrong anchor hash both returned `oracle_invalid`. A fixture whose
  expected hashes were altered returned `answer_mismatch`. All three exited 1.
  The unaltered control passed.
- **Canary.** Every sample since the restart has passed
  ([raw/canary-watch.jsonl](raw/canary-watch.jsonl)). The open shadow incidents
  `service_canary_transparent_oracle` (opened at the cutover) and
  `service_canary_transparent_availability` closed after two healthy samples.
  Both were shadow-only and never sent to Slack.
- **APM.** After the restart every Transparent source was sampled within seconds
  without error, and the load panel is fresh: running, 300 exact queries per
  minute, no errors ([raw/apm-quality-summary.json](raw/apm-quality-summary.json),
  a summary of the public aggregate endpoint).
- **No side effects.** The Enhance canary stayed OK. APM had no active incidents
  after its restart.

## Not changed

- Alert modes: APM Enhance/Status `active`, quality and scaling families `shadow`;
  monitor Enhance canary `active`, service probes `shadow`. Promoting the service
  and quality families still needs their 24-hour shadow review.
- `/etc/pir-quality/qualified-workers.json` still names recent-08. It is the
  stopped v10 load's pin file, kept as v10 rollback state; nothing current reads it.
- The previous canary binary and v10 fixture remain in `/opt/pir-monitor/`.

## Rollback

Copy the backups in `/root/transparent-quality-v11-20261009/backup/` on each
host back to their original paths (the file name encodes the path), then restart
`pir-monitor` or `pir-apm` respectively.
