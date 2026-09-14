# Remaining work

Schema 7 is observable at the public origin as of September 14, 2026. The
[status record](status.md) and [performance guide](performance.md) describe what
that observation and the bounded load test establish. They do not close the
following deployment and qualification gaps.

- Reconcile the current binary SHA, host inventory, active data paths and release
  receipts. Determine which migration steps have already been completed before
  executing the [deployment](deployment.md) or [expansion](capacity-expansion.md) runbooks.
- Record intended wallet-client conformance and adoption, including schema
  validation, session refresh, authenticated recovery and mixed-transaction handling.
- Recover existing acceptance evidence or qualify the exact candidate on isolated
  c-4 workers for six hours and 300 publications. Cover memory/swap, exact answers,
  retained sessions, replica failure, online range-boundary append and publication lag.
- Assemble the combined acceptance receipt from that evidence and reconcile the
  required 24-hour production observation, legacy-worker retirement and expansion
  enablement prerequisites. A public baseline or short fixture report is insufficient.
- For future performance comparisons, retain matched workload, coverage, geometry,
  client environment and server identity. Add repeated measurements and isolated
  server benchmarks before drawing saturation or hardware-capacity conclusions.

If an operator explicitly waives qualification or initial observation, use the
[operator-acceptance receipt](capacity-expansion.md#explicit-operator-acceptance).
Record waived gates as waived. This documentation update grants no waiver.
