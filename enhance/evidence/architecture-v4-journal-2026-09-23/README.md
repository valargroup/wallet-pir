# V4 durable expansion journal validation — September 23, 2026

The [journal](../../ops/scripts/expansion-journal.py) consumes live or captured
coordinator demand under a separate infrastructure writer lock. It freezes
operation inputs, persists Droplet identities, records apply intent before
external work, and requires reconciliation before retrying an interrupted apply.
The provider execution, orphan recovery, bootstrap, qualification receipt, and
registration adapters remain unfinished. No cloud operation was performed.

## Validation

[Seven crash-boundary tests](journal-tests.log) passed: pending request persistence
across restart/reorg, exclusive locking, ambiguous apply fencing, partial-resource
recovery, identity replacement/alias rejection, immutable inputs, invalid demand,
and lock release after a corrupt journal. These tests exercise journal contracts;
synthetic reconciliation evidence does not verify actual provider state.

[Operations and tooling checks](checks.log) passed: 48 Enhance operations tests,
3 filter tests, 4 parent-filter tests, and 17 release/tooling tests. Python compile
checks and `git diff --check` also passed.

A disposable process test used four workers and one coordinator. Two separate
journal CLI processes fetched live health and preserved the same operation
`successor-5-pair-2`. The [persisted journal](expansion-journal.json) and
[run manifest](manifest.json) retain that evidence. The runner then appended the
second fixture pair directly; this does not exercise provider execution or
qualified registration.

The [load report](load-c2.json) records 1,147 correct answers, zero errors or
incorrect answers, and p99 28.127 ms over ten seconds at concurrency two. Data
began at 67 synthetic records and grew while serving. All processes ran on one
macOS host; this is not full-size or off-host hardware qualification. The manifest
records binary hashes and dirty-checkout provenance.

The first launch selected an older `target/release` binary and failed before
serving because it lacked capacity CLI flags; its [startup log](stale-binary-startup.log)
is retained. The successful run followed a current `release-fast` build and used
`target/release-fast`. Every disposable process was cleaned up by the runner.
[Source hashes](source-sha256.json) identify the journal, test, and harness inputs.
