# Combined full-CI server test selection — September 23, 2026

The exact Enhance server test selection currently configured in full CI passed
locally through `tools/ci/full-test.sh`, with `RUST_TEST_THREADS=1`, the locked
Cargo dependencies and release-fast profile. The wrapper compiled first, ran its
cache-reclaim helper, then executed tests. On macOS the helper correctly reported
zero advised files because `posix_fadvise` is unavailable.

Result: **94 passed, zero failed, 2 explicitly ignored**, across
13 test executables. The run includes 74 server-library tests, server
binary tests, operator payloads, record layout, unequal-shard evaluation, worker
migration, and all four bounded v4 integration suites. The v4 HTTP suite passed
seven tests; the two large full-size campaigns were explicitly ignored as configured.
The canonical CLI suite includes inconsistent RPC metadata rejection and real
coordinator kills before and after durable publication.

`command.txt` records the exact invocation, `server-tests.log` contains all terminal
suite results, and `source-sha256.json` binds the current server sources/tests and
CI configuration. These inputs were verified unchanged after the run. `runtime.json`
records the native toolchain/platform and actual coordinator executable hash.

This is a local reproduction of the server portion of full CI. It does not claim
that remote GitHub Actions ran, that every workflow job passed, or that Linux CI
runner memory capacity has been established. It does not replace the separate
full-size campaigns, native 8 GiB qualification, canonical-chain/wallet conformance,
clean Linux release build, deployment, or rollback rehearsal.
