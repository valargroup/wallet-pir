# Deployed regression attempt, 2026-09-08

**Result: failed due to public HTTP 502 responses after five profiles passed.**
The full-chain publication matched the frozen fixture at the transparent-PIR
origin: 174 shards through height 3,473,686. The default Enhance filter origin
still served the old three-shard pilot. The run therefore explicitly used
`https://transparent-pir.valargroup.dev` for both retrieval and filters.

Five profiles passed every checkpoint, SQLite reopen comparison, repeated final
sync and fresh restore: unused P2PKH, unused P2SH, small active, zero balance,
and old receive/recent spend across archive and recent tiers. The offline case
passed its first two checkpoints, then received HTTP 502 fetching shard 90's
filter. The remaining five profiles received HTTP 502 fetching the map at
preflight. Overall, 33 sync calls completed; six of 11 cases failed. There was
no reported ledger mismatch, but the unexecuted checks remain unverified.

[report.json](report.json) and [junit.xml](junit.xml) preserve all case outcomes.
[manifest.json](manifest.json) records build, fixture, command and scope.
[outage-check.json](outage-check.json) confirms that both the transparent map
and init endpoints returned HTTP 502 after the run. The
[initial origin checks](default-origin-preflight/origin-checks.json) show the
publication disagreement before it began. Worker logs and the raw invocation
are retained here; SQLite stores remain under
`/tmp/transparent-regression-full-deployed-20260908` on the invoking machine.

This is partial deployed correctness evidence, not a successful release gate
or a capacity measurement. A fresh complete run remains necessary after service
availability is restored. No infrastructure configuration was changed.
