# Enhance v7 implementation and deployment checkpoint — September 24, 2026

This is an in-progress checkpoint requested before the production cutover finishes.
The tested v7 binary is installed for an isolated workload on the production hosts.
Canonical production service is stopped in the authorized downtime window. The
preserved v6 release and data are available for rollback. Do not interpret this
checkpoint as a completed deployment or hardware qualification.

`candidate.json` binds the Linux binary, build settings, source archive and wallet
source tree. Server functionality is built from `6809403`; later commits contain
test, documentation and operations updates. The external wallet checkout on Linux
has the exact Git tree recorded for the local wallet candidate.

## Completed functional validation

- Server library: 65 tests passed (`final-server-lib.log`).
- Native client/protocol: 14 passed (`final-native-client.log`).
- Full-size composition, confirmation, deep recovery and multi-boundary replay:
  passed (`full-lifecycle-final.log`).
- Seven standard HTTP cases passed across the initial run and focused reruns.
  The initial run retained two failures: an obsolete routing-revision assertion
  and a stale remote operations script. `repair-final.log` and
  `alternative-placement-final.log` record the successful corrections.
- Worker revocation persistence and monotonicity: passed (`revocation-restart.log`).
- Canonical-record and authenticated incoming/outgoing note recovery: passed
  (`final-record-authentication.log`). The initial obsolete revision assertion
  is preserved in `record-authentication-initial.log`.
- External wallet HTTP/SQLite and full lifecycle interoperability: four passed
  (`wallet-interop.log`). Wallet compatibility/vector and cover retry/cache tests
  are retained in the `enhance-v7-*.log` files.
- Operations tests: 95 passed, three skipped. Independent session-vector encoding
  and four focused-assessor tests passed. The two larger consolidation HTTP
  campaigns remain unrun in this focused validation scope.

## Work still running or pending at this checkpoint

The 30-minute physical workload started measurement at 13:32:40 UTC and requires
at least 30 publications. Sampling already shows reclaim pressure and swap activity
on the larger dataset, so it cannot establish the strict memory qualification.
Keep those observations in the final report even if every answer is correct.

Pending: finish and assess the workload; rehearse v6 rollback; start canonical v7;
verify exact public wallet/native answers, reject legacy framing, test live
failover/restart, and compare matched load with `v6-baseline.json`. Full six-hour
hardware qualification remains explicitly outstanding. CI waiting is skipped by
request. No capacity or throughput improvement is claimed.
