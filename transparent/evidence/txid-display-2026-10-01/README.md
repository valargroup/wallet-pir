# Native transparent txid display qualification

The [machine-readable manifest](manifest.json), [native report](native-demo.json),
and [qualification output](qualification.log) record the exact clean source and
commands. This is a local server qualification, not a deployment or capacity run.
The [contract and reproducible command](../../docs/txid-display.md) define scope.

The confirmed-vector gate passed over four frozen mainnet blocks in two separate
publications: genesis and 347499–347501, with fifteen complete parent transactions.
It retrieved all fourteen transactions with transparent effects and checked their
ordered raw outputs and shared metadata against separately parsed facts. The
ordinary send has a 10,000-zatoshi fee and change to the input's script. Three
300-output confirmed transactions exercise private overflow. Genesis preserves
its 67-byte raw script while the supported history directory excludes it.

The same production HTTP endpoint served history directory/pages and txid
content. Wrong revision and table binding returned the expected refusal statuses;
missing txid was distinct from transport failure; an interrupted overflow returned
no complete record. Altered fee/output comparisons failed as intended. A separate
`--corrupt-oracle` process exited nonzero and produced no successful report.

Checkpoint interruption, immutable-sidecar contradictions, reorg/restart, missing
sidecars, large raw scripts, and malformed codec/fragment cases passed. A synthetic
native HTTP test decoded two segments for each display table and reached warm
readiness with all six runtime targets accounted for. Synthetic geometry tests
exercise fragment runs across segment boundaries; those tests are not confirmed
chain vectors.

The fixture holds 29,856 encoded display payload bytes across fourteen records.
Each small publication still allocates two 16 MiB display segments, because the
registered native geometry has 4,096 rows per table. This is minimum table
allocation, not worst-case space per transaction. Multiple entries/fragments can
share rows; the mainnet range's ten fragments occupy nine page rows.

The mainnet run added 167,867,504 bytes of prepared runtime-cache reservations
for display directory/pages. The genesis run prepared only the directory and
added 83,933,752 bytes. These counters exclude public setup, process/allocator
memory, transient validation and RSS peaks. Timing and byte measurements are
recorded in the report with their accounting boundaries. A single sequential
loopback run cannot establish a latency distribution, sustained throughput,
whole-wallet benefit or production headroom.

The earlier development attempts included a genesis history assertion failure
(the script is outside the profile), then a history paging assertion failure
(the newest two events are inline, older events are paged). Neither was an
accepted run. The corrected clean-source gate is the retained acceptance result.

Strict local Clippy also exposed inherited warnings in the metadata main
baseline (type complexity, sorting, loop/style and a test borrow). Those were not
silently counted as passing checks or folded into this server feature. The
[required repository fast check](fast-check.json) passed in 216.69 seconds.
Post-push main CI remains a separate result.

No client keys, encrypted query bodies, wallet secrets, public wallet lookup
fallback, production cutover, wallet-libraries changes or Vizor changes are part
of this evidence. SHA256SUMS covers the retained raw report, manifest and log.
