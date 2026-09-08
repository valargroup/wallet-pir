# Accepted-anchor regression preflight, 2026-09-08

**Result: blocked by publication mismatch. No deployed wallet recovery was run.**
The fixture requires the full 174-shard publication through 3,473,686. The filter
origin still served the three-shard pilot through 3,473,474, with digest
`581cb0f5fc2791dabd78b2021e8f862daf89b59218dbcf8a3155461baf70e525`.
A separate read of the retrieval origin also observed that pilot.

The runner refused the mismatch before opening any wallet or issuing a private
query. All 11 required cases are accounted for as unsuccessful in
[report.json](report.json) and [junit.xml](junit.xml). This verifies refusal and
reporting behavior; it does not establish correctness, latency or capacity of
the deployed full-chain service. Deployment remained with the other agent.

[manifest.json](manifest.json) records the command, source/build identity,
fixture pin and scope. [run.log](run.log) is the raw local invocation output.
The [frozen fixture](../../../../server/transparent-regression/fixtures/README.md)
contains 11 synthetic wallet profiles and 68 planned sync calls, with exact
journal expectations and arbitrary accepted checkpoint heights.

Local validation passed the workspace release suite, the wallet/store/runner
release tests, and an explicit fork-recovery test at an ancestor inside a shard.
Local tests are not a substitute for the blocked deployed gate. Re-run
`make transparent-regression` with a fresh output directory once both public
origins serve the pinned publication. Preserve this attempted run as evidence;
record the successful deployed run separately.
