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
build and preserve the query artifact before feature-dependent checks. Assemble
publisher archives through `tools/ci/release.py` using its current help and clean
source identity; do not handcraft the archive manifest.

Read the installed configuration before choosing scope. If main includes an
unselected product's protocol migration, preserve that product through an
explicitly pinned, clean release composition when authorized. Record the actual
composition commit and its differences from main; do not describe it as an
unmodified main build or publish the composition as a PR reverting those products.

## Publisher, then History, then tx-detail

1. Prepare the supported `publisher-cutover/3` request. Preserve installed product
   settings and wallet origins. Plan/preflight with the publisher archive and the
   requested serving binary, then deploy the reviewed plan digest.
2. For a publisher-only update, `preserve_serving: true` permits separate source
   pins for existing serving roles. Every preserved serving role must assess as
   `skip` with no changes. Supply its existing matching serving binary, not the
   new Transparent-only query binary. The tool still journals an authorized
   discovery-route update and verifies the preserved products.
3. Check `/.well-known/pir-service-discovery` at the configured publisher HTTPS
   origin. The service and compatible major version must be correct. This route
   does not authorize exposing management or readiness listeners publicly.
4. Use `transparent-binary-*` with separate `workers` requests for
   `transparent-history` and `transparent-display`. Capture the current controller
   and worker configuration/unit pins. Capture a baseline when required; use
   preflight with the actual binary and `--stage`, then deploy and check status.
   A plan with no binary can legitimately refuse an unstaged release.
5. Let each operation run its pinned encrypted-answer probe. Record History
   acceptance for each configured HTTPS origin and tx-detail acceptance at the
   existing `/v2/txid` origin. Retain the old binaries and original journals.

An autonomous History worker can report `mode: loaded-only` and `ready: true`
while warming or validating artifacts in the background. Tx-detail also exposes
background-validation progress. Use readiness and actual encrypted answers to
establish serving behavior; inspect `artifact_validation.failed`, withdrawals,
`prewarm_failed`, current publication identity, and restarts. Do not treat pending
background work alone as a reason to restart the process.

## Recover an interrupted publisher operation

Start with `publisher-cutover-status` and a short process/readiness check.

| Observed state | Supported action |
| --- | --- |
| Recorded predecessor stop interrupted; predecessor now stopped; configuration and staged binary unchanged; checker stays stopped | `publisher-cutover-continue` checks that exact journal state and completes installation. |
| Candidate running; journal is `verifying` with no pending effect | `publisher-cutover-verify` finishes the current transaction without restarting the candidate. |
| Restoration is required | `publisher-cutover-rollback` restores recorded prior bytes. `publisher-cutover-reconcile` also restores; it is not an activation resume. |

Check the current command help and guards rather than broadening these cases.
If shutdown is stuck, record PID, invocation, executable digest, elapsed time, and
shutdown progress. Any bounded termination must be owned and journaled by the
deployment tool against that exact invocation. Do not use an external signal to
get around its production lock or invoke a rollback simply to force shutdown.

`resume_load: false` keeps an already-stopped checker stopped. The optional
`expected_disk_latch_sha256` permits only an exact, unchanged disk-only incident
during that stopped-checker publisher repair; it neither clears the latch nor
permits restarting the checker. Answer failures, memory incidents, changed
latches, and different failures still refuse. A later checker repair must use
the separate controlled operation and current worker pins.

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
