# M3 private-wallet recovery and interruption — September 13, 2026

**Passed within native-library scope:** a history-bearing wallet restored through
the release bridge against public services, matched independent block reduction,
was separately killed during its transparent sync phase, reopened interrupted,
resumed and matched independent reduction at the new anchor. **M3 remains open.**

Roman supplied a local mnemonic file and authorized yesterday as the wallet's
birthday. The file was read at runtime, not printed or copied into evidence;
it is ignored by Git and untracked, and its permissions were restricted to 0600.
Sending remained disabled. No funds were sent and no existing wallet was opened.
Execution: Codex for Roman. Sources and library hash: [provenance](provenance.json).

## Native recovery and independent comparison

Birthday 3,480,000 is September 11 19:30:08 UTC (13:30 Edmonton), independently
checked through lightwalletd GetTreeState. It precedes September 12, the day Roman
specified. This margin does not prove absence of history before the supplied
birthday. The recovery used the exact release library built from wallet
`7937d48df`, without sending or fixture-loopback features, in a new local shadow
profile. It completed at **3,482,317**, with zero pending pages and unresolved
spends. [Recovery status](recovery.json).

The [independent reducer](reconstruct.py) takes only watched scripts from the
wallet's derivation table and its target height/hash, never its recovered events
or balances. It downloads the complete requested public compact block range
with transparent inputs/outputs, checks heights and predecessor hashes, and
corroborates the final hash with a separate GetTreeState response. It matches
scripts and follows outpoints locally to reconstruct receives and spends,
including amounts and transaction/input indices. It does not issue plaintext
address/outpoint lookups. Its script set comes from wallet discovery, so this
is not an independent proof of gap discovery. It also relies on the block
service supplying complete transparent data; it is not a new completeness proof.

`shadow_compare` returned **equal**, including event content, UTXOs and balance,
at the same anchor. It opens the profile read-only and verifies file hashes
before and after. [Sanitized comparison](recovery-comparison.json) and
[block reconstruction summary](recovery-oracle-summary.json).

## Process interruption and exact resumed recovery

A separate new profile imported the same wallet through the same release
library. SIGKILL was sent after the child reported `phase=idle` and
`completion=sync-in-progress`, at scanned height 3,482,319. Reopening reported
**interrupted**, no committed anchor, and no completed coverage. Resume reached
**complete** at 3,482,322 in **50.853 seconds**. [Kill report](kill-report.txt).

The resumed profile was compared with a new independent block reconstruction
through its own anchor: **equal**, including balance and no unresolved spends.
The original recovery profile was additionally supplied as `--untouched` and
remained unchanged. [Resumed comparison](resume-comparison.json) and
[reconstruction summary](resume-oracle-summary.json).

This proves recovery after interruption in the sampled transparent phase, not
an HTTP request in flight at the instant of SIGKILL. The interrupted profile had
no completed transparent anchor, so this run does not establish survival of a
previously committed nonempty transparent ledger. Prior fixture evidence covers
other persistence boundaries; do not expand this live observation's scope.

## Harness and validation

The [harness patch](wallet-harness.patch) passes the mnemonic file path to the
child instead of the mnemonic in its environment, adds cleanup, preserves an
explicitly selected new profile for independent comparison, suppresses private
failure text, and labels phase-based interruption accurately. It removes the
arbitrary two-second delay after detecting sync, which could let a short run
finish before the kill. The [bounded recovery diagnostic](private_recovery_validation_test.dart)
prints only sanitized status and keeps its profile for comparison. Neither
changes production wallet code or the bridge binary. The final source also has
formatting and comment corrections after the successful run; the live run
predates those nonbehavioral edits. Dart analysis with `--fatal-infos` passes.

Run the diagnostic from the M3 bindings worktree with `ZAKURA_PRIVATE_VALIDATION=1`,
`ZAKURA_PHRASE_FILE` set to the operator's file, `ZAKURA_BIRTHDAY=3480000`,
`ZAKURA_BRIDGE_LIBRARY` set to the recorded release library, and new
`ZAKURA_VALIDATION_PROFILE` / `ZAKURA_VALIDATION_REPORT` paths. It uses
`fvm flutter test --tags private-validation test/private_recovery_validation_test.dart`.
The kill test uses `ZAKURA_LIVE=1 ZAKURA_KILL=1`, the same birthday/library/file,
the documented public origins, and new `ZAKURA_KILL_PROFILE` / `ZAKURA_LIVE_REPORT`
paths with `fvm flutter test --tags kill test/kill_recovery_test.dart`.

Only sanitized counts, heights, equality flags and digests are published.
Profiles, expected ledger details, block captures and raw private-run logs stay
under the private local directory named in provenance, outside the repository.
A local byte scan found no plaintext mnemonic in those profiles, logs or this
bundle. It is not a claim about memory zeroization or every possible encoding.

## Remaining gates

- The [original deployed suite](../productionize-m3-live-2026-09-13/README.md)
  failed on public-path availability. The [suite correction and rerun](../productionize-m3-suite-fix-2026-09-13/README.md)
  are separate evidence; this wallet trial does not replace public-fixture checks.
- The release macOS application screens were not driven here. The real library
  and Dart bindings were exercised; the application-level gate remains open.
- Capture direct request activity for the stronger in-flight kill claim and
  retain exact independent comparison. This phase-based rehearsal is useful
  live evidence, not a claim that the stricter condition was observed.
