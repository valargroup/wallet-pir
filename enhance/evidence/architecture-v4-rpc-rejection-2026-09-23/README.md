# RPC metadata rejection and recovery — September 23, 2026

The actual canonical-mode coordinator CLI integration now injects inconsistent
Ironwood tree metadata during a same-height reorg. The RPC reports a tree size of
three while its block contains two records and the rewound journal is empty.
The test waits for the actual `journal continuity mismatch` ingestion error,
then asserts that the complete previous manifest remains published and an exact
old-generation PIR query still succeeds. It does not infer rejection from elapsed
time or from a failed connection.

After restoring tree size two, publication completes without duplicate records.
The existing exact current/retained query and coordinator-restart checks then run.
The test passed in 29.77 seconds; test Clippy passed with warnings denied. This is
a test-only change, already included by the full-CI `v4_rpc` selection. Remote CI
has not been dispatched. No production source, credential, or service was changed.

The RPC test double and synthetic block envelopes are the same as the
[canonical RPC integration](../architecture-v4-canonical-rpc-2026-09-23/README.md).
This demonstrates rejection and recovery for one inconsistent-metadata case,
not live-chain inclusion, malicious-node robustness in general, full crash-phase
coverage, or production qualification.
