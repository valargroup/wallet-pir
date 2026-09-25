# Status PIR SSH deployment and live-source trial — 2026-09-25

The isolated Status service on `status-pir-p4000-ams1` was updated directly over
SSH. The prior binary (`91de3c9a2e7ef178163e7ca593ab7a5187c6820681a9b0523c8414bef302e871`)
was preserved as `/opt/status-pir/status-pir.pre-580e4c88`. The first candidate
was `580e4c88404cd5b3176d1973a04b9e299599a3c9eb14c05052a702f44f687d95`.
The final installed binary was
`7375041eec28d72c87ce1c3d7898314194b6295c2f501aa3bb6a8fe17ffa5384`;
the first candidate was preserved as `/opt/status-pir/status-pir.pre-7375041e`.
The unit remained the loopback-only **synthetic** service, with no public Status
route enabled. It was active with zero restarts after validation.

## P4000 validation

- Full 1,572,864-entry CUDA validation passed encrypted answers, full-hint
  differential checks, one-row update, evicted-session `409` retry, recovery
  `410` retry, worker-loss failure, stale-source failure, and epoch fencing.
- Initial index construction took 5.306 s and PIR preparation 11.758 s. The
  one-row update took 5.002 s for index construction plus 8.154 s preparation.
  These are isolated synthetic timings, within the revised 20 s budget.
- The deployed service answered 1,200/1,200 scheduled encrypted lookups at
  20 offered QPS over 60.031 s, with zero errors or skipped arrivals. Complete
  lookup p99 was 101.619 ms. The per-arrival record is in
  [synthetic-60s-requests.jsonl](synthetic-60s-requests.jsonl).
- The final binary passed the same full-size CUDA validation. Initial index and
  preparation took 5.407 s and 11.840 s; the one-row update took 5.319 s and
  8.305 s. After restart, it answered another 1,200/1,200 scheduled lookups
  at 20 offered QPS with zero errors or skipped arrivals and a 99.251 ms p99.
  The final record is
  [synthetic-final-60s-requests.jsonl](synthetic-final-60s-requests.jsonl).

## Canonical live-source trial

On the existing Enhance coordinator host, `observe-live` read a consistent
64-block canonical window and the complete observed mempool. A second sample
reused its durable block cache. A temporary, loopback-only `serve-live` unit
then prepared and published from the node's RPC. It was separate from the
deployed Enhance coordinator and ran its Status roles in one CPU process.

Four encrypted lookups and a later batch of 40 matched a transaction in a
canonical block; neither the txid nor query body was logged. Restarting the
trial advanced its durable recovery epoch from 1 to 2. During that restart,
mempool changes repeatedly invalidated candidates under the original rule.
The rule was revised so a still-canonical, complete candidate can be published
with its **original** observation time while a newer observation gets another
candidate. The revised trial started at epoch 3 and published its first
candidate in 6.507 s despite source changes during preparation. It answered
40/40 further live encrypted lookups correctly. Across the 35 recorded
publications in that short revised trial, the largest difference between the
published snapshot's observation time and queryable time was 6.507 s. The retained
[live-trial journal](live-trial-journal.txt) records the revised trial's source
and publication timestamps. The temporary trial unit was stopped afterward.

A further temporary build (`7375041eec28d72c87ce1c3d7898314194b6295c2f501aa3bb6a8fe17ffa5384`)
spaced unchanged-source session reaffirmations to five seconds while retaining
one-second source polling. Its restart used recovery epoch 4, answered 40/40
canonical encrypted lookups, and recorded five publications with at most
5.721 s from observation to queryable material. See the
[second trial journal](live-trial-5s-journal.txt). That unit was also stopped.
Most samples changed during this short trial, so it did not measure the
steady-state rate of unchanged-source reaffirmations.

## Production decision

**Blocked.** These checks do not include a live-source 20-QPS run on the P4000,
six hours of concurrent publication and lookups, a separate-process router and
worker with authenticated control acknowledgments, integration with the
deployed shared coordinator and HTTPS ingress, or a compatible release protocol
and wallet dependency pin. The pinned wallet protocol still enforces 10 seconds
while the agreed gate is 20 seconds. No public Status traffic was enabled.
