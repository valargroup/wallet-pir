# Durable routing outage audit

The post-reorg canary was stopped after the confirmed 23:14 control timeout
removed the only current recent replica. The route implementation requires
all archive owners and at least one recent replica; losing that quorum
installs an HTTP 503 catch-all. The previous observer could miss the interval
while awaiting other checks. The preserved run is invalidated, not accepted;
no earlier result or sample was rewritten and no fleet promotion occurred.

The source correction records a durable cumulative unavailable-event count
and an epoch in routing-availability.json. Recording precedes router
application so a crash or immediate recovery cannot erase an attempted
withdrawal. A failed withdrawal attempt conservatively invalidates the gate.
Warm reapplication preserves the epoch and counter; the availability flag is
restored only after successful router application. A separate cross-process
lock serializes the audit with application.

The observer requires valid evidence, starts its measured duration with the
baseline, checks again after slow worker reads and immediately before writing
success, and rejects missing/malformed/replaced/regressed evidence and any
counter change. It does not infer continuous network health from this audit;
ordinary public and private checks remain required. Tests exercise withdrawal
and recovery between polls, cancellation during router application, evidence
replacement, counter regression and unavailable initial routing.

The targeted operations discovery suite passed 88 tests. This source change
is not deployed. It requires a successful warm router application to initialize
the evidence, then a fresh matching full canary. Do not synthesize or reset an
audit record to rescue a previous run. The underlying worker scheduling delay
remains unresolved; closing this observation gap does not fix that delay.

Full `make check` passed: 588 Rust tests passed, 0 failed, 2 ignored,
with formatting, lint, operations, documentation and report checks.
