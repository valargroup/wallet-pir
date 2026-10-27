# Transparent host build and recovery recipe

Use the selected checkout's [publisher documentation](../../../../docs/publisher.md),
[query-server documentation](../../../../docs/query-server.md), and
`tools/transparent/lib/binary_upgrade.py` for current request fields. Keep private
requests and inventories in ignored operator storage. Obtain fresh pins from the
running fleet; do not reuse a past rollout's request unchanged.

## Build the selected products

Run these examples in the pinned host checkout, with the existing compatible
build target and native build environment already selected:

```sh
cargo build --locked --release -p pir-publisher \
  --features enhance,status,transparent-history,transparent-detail,native-reinspiring,transparent-publisher/rocksdb-reader

cargo build --locked --release -p pir-query-server \
  --features transparent-history,transparent-detail
```

The publisher example includes the four products of the existing combined fleet;
derive the actual feature set from the installed roles. The query-server example
is for Transparent roles only. Preserve the publisher artifact before another
build and preserve the query artifact before feature-dependent checks. Host builds
are for diagnosis; deployable artifacts are the Release workflow's `build-<sha12>`
releases, which `pir-deploy` installs.

Read the installed configuration before choosing scope. If main includes an
unselected product's protocol migration, preserve that product through an
explicitly pinned, clean release composition when authorized. Record the actual
composition commit and its differences from main; do not describe it as an
unmodified main build or publish the composition as a PR reverting those products.

## Publisher, then History, then tx-detail

1. `pir-deploy --dry-run publisher <sha>`, then `pir-deploy publisher <sha>`. The
   installed configuration, product settings and wallet origins stay as they are;
   query roles keep serving.
2. Check `/.well-known/pir-service-discovery` at the configured publisher HTTPS
   origin. The service and compatible major version must be correct. This route
   does not authorize exposing management or readiness listeners publicly.
3. Use `transparent-binary-*` with separate `workers` requests for
   `transparent-history` and `transparent-display`. Capture the current controller
   and worker configuration/unit pins. Capture a baseline when required; use
   preflight with the actual binary and `--stage`, then deploy and check status.
   A plan with no binary can legitimately refuse an unstaged release.
4. Let each operation run its pinned encrypted-answer probe. Record History
   acceptance for each configured HTTPS origin and tx-detail acceptance at the
   existing `/v2/txid` origin. Retain the old binaries and original journals.

An autonomous History worker can report `mode: loaded-only` and `ready: true`
while warming or validating artifacts in the background. Tx-detail also exposes
background-validation progress. Use readiness and actual encrypted answers to
establish serving behavior; inspect `artifact_validation.failed`, withdrawals,
`prewarm_failed`, current publication identity, and restarts. Do not treat pending
background work alone as a reason to restart the process.

## Recover an interrupted publisher operation

Start with `pir-deploy status` and a short process/readiness check. A failed
`pirctl deploy` has already restored the previous release; `pir-deploy rollback
publisher` swaps back a release that started but misbehaves later.

If shutdown is stuck, record PID, invocation, executable digest, elapsed time, and
shutdown progress before acting. Do not use an external signal to get around the
deploy lease or invoke a rollback simply to force shutdown. A stopped checker and
its disk-capacity latch stay as they are through a deploy; repair the checker
through its own controlled operation.

## Debug slow publication and disk growth

Compare `cycle_seconds`, `runtime_preparation.seconds`, and the logged distribution
inventory time. Reusing many runtimes does not prove their files avoid repeated
hashing. Profile `publication::listed_digest/hash_listed` and
`SegmentSource::verify_into` when the measurements point there; preserve content
verification, file-replacement detection, cancellation, and restart behavior in
any reuse fix. Shared publisher Status hashing is a different product's cost.

History activation collects unused `staging-*` source trees and unreferenced
objects while protecting active, retained, and held sources. Measure actual free
space and collection logs afterward. Use the existing read-only
`tools/transparent/scripts/transparent-provider-reclaim-scan.py` for estimates
when needed. Its output is not a deletion manifest, and legacy caches outside
the provider's ownership must not be deleted based on their size alone.
