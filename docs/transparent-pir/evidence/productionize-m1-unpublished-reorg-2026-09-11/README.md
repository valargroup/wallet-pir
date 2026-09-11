# Unpublished reorg correction

The [failed writeback canary](../productionize-m1-incremental-writeback-2026-09-11/README.md)
withdrew valid older coverage while preparing a block replaced by a one-block
fork. The [baseline regression](baseline-regression.log) reproduces unconditional
withdrawal on a journal-only fork before any new publication is advertised.

The controller now requests retention only when the fork begins above its
current served endpoint and that endpoint is not already withdrawn. Under the
publication gate it increments the preparation epoch and persists the revocation
intent. The fleet must explicitly acknowledge retention; errors or older adapters
without the acknowledgment fail closed. Restart with an unfinished revocation
intent still withdraws conservatively.

Under the routing lock, the fleet checks that its active map matches the
controller's requested map; the recorded endpoint is below the fork and matches
the node; the currently routed set has an owner/recent quorum; and every routed
worker attests that warm map. Otherwise it withdraws before revocation. A rejected
retention hint causes all revisions to be rechecked for a potentially deeper
fork. Failed revocation of a retained routed worker also withdraws. The existing
durable routing audit is unchanged.

Explicit invalidation now always reaches the worker, even when all served and
retired revisions remain canonical: that cancels an orphaned candidate's epoch
without revoking the canonical predecessor. Regular staging retains the existing
conditional orphan-revocation behavior.

Regressions cover journal-only forks, a fork while preparation is paused,
continued public HTTP availability, rejection of the obsolete candidate, deep
served forks with immediate withdrawal before slow ancestor lookup, failed owner
revocation, and a stale retention hint after fleet activation. The existing
worker regression verifies that a fork above the predecessor invalidates a
prepared candidate's epoch. [All 101 operations tests](operations-tests.log) pass.
The controller concurrency regression also passes in the full workspace run;
complete workspace and Linux qualification results are recorded when finished.

The [source manifest](source-manifest.json) binds the staged Linux build at
`/opt/transparent-publisher-build/unpublished-reorg-20260911/` under
`transparent-m1-unpublished-reorg-build.service`. No corrected controller or
fleet script is deployed yet; the failed canary remains stopped. A new matching
full gate is required after deployment. This correction does not establish
resolution of the separate close-block freshness margin.

[Full make check](make-check.log) passed: 591 Rust tests, zero failures, two ignored, with formatting, Clippy, operations and documentation checks. The separate final 101-test operations run covers the final fleet changes. Linux qualification remains outstanding.
