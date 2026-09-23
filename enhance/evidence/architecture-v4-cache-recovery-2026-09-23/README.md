# Local compiled-cache recovery — September 23, 2026

Worker restart can rebuild missing or rejected compiled PIR cache units using
content-addressed durable rows. The reader validates complete padded length and
SHA-256 against the retained unit identity, bounds the read to expected size plus
one byte, and rejects invalid source ranges. Candidate preparation uses the same
reader. Committed metadata is not changed by recovery.

Validation:

- [Worker tests](worker-tests.log): all four passed. The new recovery test deletes
  the compiled cache, rejects corrupt padding and missing durable rows, restores
  valid rows, restarts successfully and compares actual PIR evaluation output.
  It also checks the durable journal is byte-identical and rejects invalid ranges.
- [HTTP regression](http-tests.log): exact-answer round trips, retention, failover
  and coordinator restart passed. This ran against the initial shared-reader
  change, before the final bounded-read and Clippy style refinements; the worker
  tests cover the final source.
- [Clippy](clippy.log): passed with warnings denied on the final library source.
- Formatting and `git diff --check` passed.

This is native local validation, not deployed Linux recovery or memory
qualification. Startup still fails if neither a valid compiled cache nor verified
durable rows are available. Peer/coordinator row restoration and recovery peak
memory/time measurement remain outstanding. No live infrastructure changed.
