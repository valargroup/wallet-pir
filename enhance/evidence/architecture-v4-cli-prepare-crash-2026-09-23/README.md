# Actual coordinator crash during preparation — September 23, 2026

The canonical RPC integration test now holds a real worker HTTP prepare request
while the actual coordinator CLI runs in a separate process. It waits for the
request barrier and reads `controller-v4.json`, requiring persisted phase
`PREPARING`. The previous manifest must remain published. It then kills and waits
for that coordinator process, releases the old worker request and starts a new
coordinator against the same durable directory and live workers.

The restarted coordinator aborts/reconciles the interrupted attempt and publishes
the corrected reorg target. Assertions require a higher controller epoch and
attempt counter, exactly two records (no duplicate append), and exact current and
retained encrypted queries. A subsequent clean restart also preserves the same
manifest and resumes RPC polling. The bad-tree-metadata rejection checks added
previously remain part of this test.

The test passed in 39.13 seconds; test Clippy passed with warnings denied. This is
a test-only change already selected by full CI. The subprocess guard kills and
waits for owned coordinators on failure or completion; temporary data is removed.
No live chain, credentials, infrastructure or production services are used.

The RPC block envelopes remain synthetic. This validates one actual process-crash
boundary with journaled reservations, not every possible storage failure or
worker process crash. Other operation phases have existing journal/HTTP tests;
full deployed recovery and hardware qualification remain outstanding.
