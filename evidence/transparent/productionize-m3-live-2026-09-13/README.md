# M3 deployed regression attempt — September 13, 2026

**FAIL: 6 of 11 cases passed; 5 stopped on transport/availability errors.**
M3 remains open. No event, UTXO, balance or history mismatch was reported; cases
that failed transport did not complete all required comparisons.

Subsequent work: the user supplied a validation wallet and birthday; its
[native recovery and resume comparison](../productionize-m3-private-wallet-2026-09-13/README.md)
is recorded separately. The [suite correction and new run](../productionize-m3-suite-fix-2026-09-13/README.md)
retain their own source and binary identity. The failures and missing-input notes
below describe this initial capture, not the current milestone checklist.

Execution: Codex for Roman, following his request to begin M3 validation.
The sequential test ran from the Mac against the default public origins; no
fleet service, routing, deployment or existing wallet database was changed.
Coordinator SSH checks timed out/reset, so current private fleet readiness and
server-side failure attribution remain unverified. This operator-access limit
does not erase the observed public-path failures.

## Source and method

PIR source `e98286bf345718b29e22ef81076cd22633405d83`, containing the corrected
sealed-prefix/tail pinning runner. Wallet source `7937d48df` in the existing
`wallet-libraries-m3-correctness` worktree; no wallet source changes.
[Provenance](provenance.json) binds runner, frozen fixture and rebuilt release
bridge hashes. Documentation changes in the PIR working tree were not compiled
into the runner. The fixture was not re-cut, weakened or replaced.

```sh
TRANSPARENT_REGRESSION_OUT=/tmp/transparent-m3-live-20260913/regression make transparent-regression
```

Make exited 2 after the runner reported five failed cases. The fixture supplies
independently reduced expected ledger state and accepted headers at its frozen
anchor, 3,473,686; it does not prove fresh node-consensus validation or indexer
completeness. Both public origins returned identical map bytes at preflight,
with served tail ending at 3,482,264. The runner's own pin observations are in
[report.json](report.json). All eleven cases were attempted sequentially with
the existing request and case deadlines.

## Results

| Case | Result | Failure |
|---|---|---|
| unused-p2pkh | Pass | — |
| unused-p2sh | Pass | — |
| small-active | Fail | HTTP 503 from `/v1/shards/init` at fresh-final |
| zero-balance | Fail | Transport error sending `/v1/shards/init` request |
| old-receive-recent-spend | Pass | — |
| offline-receive-spend | Pass | — |
| active-p2sh | Pass | — |
| reused-pages | Fail | Connection closed before directory-query response completed |
| multi-script-self-transfer | Fail | Connection closed before archive-manifest response completed |
| recent-birthday | Pass | — |
| coinbase | Fail | Connection reset during directory query at fresh-final |

Passing cases completed continuation checkpoints, close/reopen comparisons,
repeat and independent fresh-final recovery. For example, small-active completed
its continuation and repeat stages before failing fresh-final, but is still a
failed case. No failed case is removed from the denominator.

[Raw bundle](regression-raw.tar.gz) retains per-case reports and stdout/stderr
for these synthetic groupings of public scripts. The 839 MiB complete bundle,
including SQLite stores, remains locally at
`/tmp/transparent-m3-live-20260913/regression-complete-with-stores.tar.gz`;
the original stores also remain under that directory’s `regression/` tree. [JUnit](junit.xml)
and [command log](regression-command.log) retain the overall failure.
A follow-up public probe at 18:56:56 UTC returned HTTP 200 for init, map and
filters ([probe](availability-after-failure.json)); it establishes recovery at
that sample, not the cause or absence of further interruptions. Later cases
still encountered transport failures. There was no automatic rerun to obtain a
passing result and no retry/deadline policy was changed.

## Wallet preparation

The recovery bridge rebuilt successfully with default features (sending and
fixture-loopback disabled): `cargo build --locked --offline -p zakura_wallet_bridge --release`,
using the shared Cargo target directory. All **21 native binding tests passed**
through that exact library. The `shadow_compare` release example also built.
Build/test logs and bridge hash are included; these checks do not replace live
application validation or a new macOS app build.

## Outstanding work at this capture

1. Diagnose the public HTTP 503/disconnect/reset failures, qualify any correction,
   and run the complete deployed suite again into a new evidence directory.
   Coordinator access is currently unavailable from this session; no server-side
   cause is asserted. Observer-only bounded retries do not establish equivalent
   recovery in this reference client or the pinned wallet client.
2. Identify an isolated history-bearing validation wallet and birthday, then run
   the release-application shadow comparison against independent reconstruction
   at the same accepted anchor. No user wallet was opened or secret fetched.
3. Run the release-library kill/resume test against public services. Source review
   found that `kill_recovery_test.dart` infers a private query from
   `completion=sync-in-progress` plus `phase=idle`, waits two seconds and kills;
   this alone does not prove a request was in flight. It also checks synchronized
   resume without independently comparing the resumed ledger. Before claiming
   the gate, capture actual request activity and compare the resumed result with
   the same independent oracle; retain child cleanup on timeout/failure.

The application comparison and kill test were not run: a validation-wallet
profile/credential path and birthday were requested from Roman but have not
been supplied. Do not use an empty synthetic restore to close these gates.
