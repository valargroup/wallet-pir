# Publication alert investigation and corrections

## Findings and issue list

At 17:37 UTC on September 26, the first shadow window had 309 one-minute
samples with no collected canary failures, unknown inputs, monitoring-progress
failures, or delivery backlog. The incident database recorded 28 lag warnings,
four lag critical incidents, and one publication-failure warning; all recovered.
`before.json` retains the later investigation snapshot, including all incident
transitions and then-current publication metrics. Counts can differ after the
17:37 status check as observation continued.

| Priority | Issue and evidence | Resolution |
| --- | --- | --- |
| 1 | Continuous time behind tip was mistaken for stalled publication. Critical events fired with only two or three blocks outstanding, and successful advancement did not reset the rule. | Use time since successful advancement plus continuous time behind tip for stall alerts. Both must exceed 120/300 seconds. |
| 2 | A blocked flag bypassed the intended failure threshold. The only warning had one failure; the coordinator logged `canonical anchor changed during preparation` at 13:37:46 UTC and recovered. | Require blocked state or three consecutive failures continuously for 120s warning / 300s critical. Keep canonicality checks intact. |
| 3 | Replacing the old lag rule with advancement alone could miss a publisher that advances but accumulates backlog. | Add separate sustained backlog rules: 8 blocks/120s and 16 blocks/300s. These are initial operational guardrails, subject to shadow review. |
| 4 | An advancement-age rule alone would treat an idle chain's next block as an immediate stall. | Track continuous behind-tip time; start a new grace period on startup, catch-up, or observation gaps. |
| 5 | Publication coverage omitted anchor, blocked state and last-advancement timestamp; missing blocked data could look healthy. | Require valid publication inputs and preserve unknown states instead of inferring health. |
| 6 | Shadow-to-active promotion cleared active state before evaluating an unknown input, losing the existing incident. | Announce the existing active incident once while preserving its ID and state; require distinct healthy recovery samples. |
| 7 | Original observation files retained active keys but not the measurements needed to diagnose lag. | Expose structured publication measurements in status and the dashboard; retain them with chain state and active incident details in new evidence. |

The corrected rules do not prove that every old critical alert was false: the
old evidence lacks per-sample advancement timing. This uncertainty is why the
full shadow period restarts after deployment. A successful publication attempt
measured during investigation took 68.56 seconds, with zero pending commit/abort
age and zero consecutive failures. No serving-path or canonicality change is
needed to correct these alerting defects.

## Verification and deployment

Regression tests cover advancing while behind, true stalls, sustained backlog,
quiet-chain grace, stale/missing/invalid inputs, transient blocked retries,
persistent failure, and unknown state during promotion. Existing durable delivery
retry and incident persistence tests remain required. The native monitor is
rebuilt because it shares the incident engine with APM. Both binaries use the
explicit Haswell baseline; coordinator and router binaries are unchanged.

See `deployment.json` for deployed hashes, checks, and the new 24-hour window.
The old observation directory is retained. The new collector writes to a distinct
run directory for 72 hours. SQLite incident/outbox state is preserved. There is no
automatic promotion, and the 24-hour active gate remains pending after shadow
review.
