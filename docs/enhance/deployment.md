# Enhance PIR deployment

Production changes use `tools/scripts/wallet-pir-deploy.py` under the shared
production lock. Run plan and preflight first, pin exact executable/configuration
identities, and preserve a verified rollback baseline. Source examples and local
smoke tests do not establish the live deployment. Historical receipts remain in
[retained evidence](../../evidence/enhance/README.md).

The direct HTTPS cutover changes the publisher and all three serving roles as
one qualified operation. Intermediate PRs are not independent production modes.
There are no users requiring a compatibility window. Keep the prior executable,
configuration, canonical journal and recovery state for restoration, while
preserving every floor or invalidation observed after cutover.

## Node RPC policy

The current source reads the node through the server-only
`zakura-server-chain` adapter (`src/shared/pir-chain`). Enhance and Status read their RPC cookie when the
client is constructed and report HTTP 401 as an invalid cookie. Each request has
a 120-second total deadline, including connection establishment and reading the
body, with no separate connection timeout. Each whole response, including a
batch, is limited to 64 MiB even without Content-Length. A valid large batch can
exceed this deliberate cap and fail; the transport does not retry, split batches
or fall back to another endpoint. Batch results are returned in request order.
These are source policies; the deployed release is established by its transaction
and artifact receipt.

## Build and local validation

Use the pinned toolchain and the repository's affected checks. CPU/native tests
and deploy-profile artifacts are distinct. Build `pir-publisher` with `enhance`
and the selected reference/native format; build `pir-query-server` with `enhance`
and that same format, adding CUDA only for qualified GPU workers.

`pir-publisher enhance prepare-publication` prepares a complete publication from
a stopped canonical journal and an exact wallet manifest. Runtime publishing
uses canonical node RPC only. The isolated smoke checks supply a trusted RPC
fixture and fresh state directories. A canonical publisher rejects a directory
carrying the old `source-mode=synthetic-fixture` marker.

The small native fixtures cover independent record bytes, wallet decryption,
real HTTPS, process restart and journal repair. `tools/enhance/scripts/test-local.py`
runs two disposable worker processes, a router, ingress, TLS distribution and the
canonical publisher against a trusted RPC fixture containing public transactions
and synthetic block envelopes. Encrypted load starts after readiness while the
fixture chain continues advancing. The script retains
logs/reports outside Git and cannot establish consensus or hardware qualification.
Full profiles use explicit static consumer assignments;
there is no publisher-owned inventory expansion.

Select `--profile full-shard`, `five-shards`, `six-sealed` or `seven-sealed` for
explicit deployed-size local checks. These use repeated public transactions in
synthetic blocks, independent frozen record bytes at row/unit/shard boundaries,
and the ordinary canonical publisher and serving executables. Sealed profiles
include 1,000 confirmation blocks and then append into a successor. Use
`--ready-timeout` for local cold preparation and `--block-interval` to exercise
publication cadence; neither changes serving authority or production limits.
Reports retain executable hashes, observed publications and sampled process RSS.
These profiles do not replace Linux memory/swap, CUDA, real-chain or sustained
production qualification. Seven sealed shards remain opt-in until qualified.
The five-shard fixture can include sealed predecessors; it does not establish
the separate worst case of five simultaneously growing shards.

## Fresh deployment layout

Autonomous workers and routers check publisher service discovery at startup,
before every pass and before each on-demand authority refresh; see
[publisher compatibility](../query-server.md#publisher-compatibility). Forward
`/.well-known/pir-service-discovery` on the configured publisher HTTPS origin.

Use the root host examples in `tools/deploy/publisher/` and
`tools/deploy/query-server/`, and the Enhance role renderer. Publisher settings
contain only canonical RPC/cookie references, state, polling and private operator
and artifact listeners. They contain no worker, router or ingress destinations.

Each serving role requires `synchronization` settings:

- `publisher_origin`: the configured HTTPS origin, without path or credentials.
- `trusted_ca`: optional absolute path to a trusted CA file, readable in its sandbox.
- `download_budget_bytes` and `cache_limit_bytes`: explicit transport and disk budgets.
- Worker `shards`, router `workers`/optional `preferred`, or ingress `routers`:
  static responsibilities and private downstream endpoints. Each ingress router
  entry contains `control_origin` and `query_origin`.

Configuration checking parses real JSON and validates capabilities, limits,
origins and responsibilities without opening state, reading credentials/CA files
or loading a GPU driver. Runtime startup performs those checks before loading.
Keep control/evaluation listeners on the private network and process management
on loopback. Preserve the existing internal bearer-token credential references;
all private peers must use the same installed policy.

### Internal control token

`ENHANCE_INTERNAL_TOKEN` protects private evaluation, readiness and peer handover.
Read it from the installed credential environment, sourced from Infisical.
Never put its value in configuration, manifests, reports or commands. TLS
publication distribution is a separate read-only artifact/authority API.
Caddy exposes `/provider/enhance/v1/*` through its trusted HTTPS origin and sends
wallet `/v1/enhance/*` traffic to ingress. It must not expose publisher management
or private peer APIs.

### Query ingress uploads

Ingress retains bounded upload slots, per-client limits and a thirty-second body
deadline. It validates the complete binding before forwarding and never replays
an accepted or acceptance-unknown evaluation. Worker evaluation retains its two
permits and fixed request limits. Stale routing and incompatible sessions keep
the existing refusal behavior; a client refresh does not authorize a different
anchor on the wallet's behalf.

## Cutover checks

Qualify the complete path before requesting the production gate. The reviewable
plan must bind source/artifact digests, live topology, prior units/configuration,
private peer identities, exact independent answer checks and all enabled products
of the shared publisher. Retain resource and hardware evidence separately from
fast CI, including cold and incremental preparation, scratch/free disk, RSS,
full-size overlap, recovery and sustained encrypted answers.

Rollback must transfer recovery authority, not merely restore old files. Import
old recovery/domain floors, invalidations, lifecycle and the next reserved wallet
generation before the new publisher starts. Before restoring the previous
runtime, merge every new floor and invalidation back and require fresh preparation
under a strictly reserved recovery epoch. Keep the old consumer state separate
from the autonomous caches. A stale backup must never erase later invalidations.

Immediately before the first serving interruption, durably record the one-hour
window and arm a persistent transaction-owned watchdog that actually executes
rollback if the shell disappears. Retries do not reset the deadline. Under the
production lock, commit only after readiness, fresh advancement and independent
answers have passed for every affected enabled product. If they have not passed
within one hour, stop forward progress and begin rollback; restoration may finish
later. Verify all enabled products again after restoration. A saved deadline,
local test result or successful process restart alone does not meet this gate.

## Deploy CLI

Use the repository deployment CLI for plan, preflight, deploy, status and
rollback. Preserve its transaction journal and production-lock ownership. Do not
copy binaries, signal processes or change units by hand. The operator confirms
the concrete production plan after exact artifacts, full qualification and a
watchdog rehearsal are reviewable.

The `enhance-https-cutover/1` request preserves every other enabled product's
settings and names all existing Enhance consumers. It pins separate CPU and CUDA
executables, the HTTPS origin, preparation disk allowance and every independent
answer check. Its `enhance_https.progress` entries observe the actual published
heights: the Enhance and Status wallet manifests' `anchor_height`, Transparent
history's `public_height`, and transaction details' `published.tip`. Node tip
progress alone cannot satisfy acceptance.

Run `publisher-cutover-plan` and `publisher-cutover-preflight` with the checksummed
request and matching artifacts. `--serving-binary` supplies the CPU query host;
`--cuda-binary` supplies the matching CUDA query host when the inventory has GPU
workers. Both must have the publisher's source revision and reviewed executable
digests. The serving roles' existing configuration, units, executable identities
and public edge are captured for rollback before any interruption.

Production `publisher-cutover-deploy` runs as root on the existing coordinator
from its immutable, checksummed operations release. It installs two uniquely
named, transaction-owned systemd services: a forward runner and an independent
rollback watchdog enabled for boot. It returns their identities immediately;
`publisher-cutover-status` reports both the deployment journal and durable
supervision decision. This is not a completed deployment until that decision is
accepted and the journal is committed.

The watchdog records one deadline immediately before the first interruption.
Retries cannot reset it; a boot clock also prevents a backwards wall-clock change
from extending the hour. Acceptance requires readiness, fresh publication
advancement and independent encrypted answers for every enabled publisher
product before the deadline. At the deadline, or after loss of the forward
process, the watchdog fences further deployment, stops only that transaction's
verified runner invocation, and invokes the ordinary locked rollback. Failed
recovery retries use the same decision and retained inputs. They never terminate
another production-lock owner. Restoration can finish after the deadline; the
hour bounds the forward attempt.

While the publisher is stopped, the Rust `handoff-https-state` command preserves
generation reservations, domain recovery floors, revoked sessions and sealed
history. Rollback calls it with `--rollback` before starting the previous runtime.
It reserves one higher recovery epoch, invalidates retained sessions and keeps
newer floors on both sides; interrupted retries reuse that reservation. Runtime
and configuration rollback never restores an older safety-state backup. The
transaction's `publisher-cutover-rollback` command remains available after a
successful cutover. Retain its request, operations release, journal, old
executables and credential references until qualification is complete.

## Repairing historical fees

Publishers built before the [whole-transaction fee rule](architecture.md#record-metadata-and-fees)
published fee-absent records for Ironwood transactions with transparent, Sapling or
Orchard parts. The journal stores only encoded records, so installing and restarting
the fixed binary repairs new blocks only. History is repaired offline from chain and
then adopted by the publisher at startup. Neither step touches the publication safety state,
workers, routers or ingress.

1. Build the server with its source revision recorded in the receipt:
   `WALLET_PIR_SOURCE_REVISION=$(git rev-parse HEAD) cargo build --locked --release -p pir-publisher --features enhance,native-reinspiring --bin pir-publisher`.
2. While the publisher keeps serving, rebuild into the staged directory of its
   data directory:

   ```sh
   pir-publisher enhance rebuild-journal \
     --source <data-dir> --output <data-dir>/enhance-staged \
     --zakura-rpc-url <archive RPC> --zakura-cookie <cookie> \
     [--through-height H] [--concurrency 8] [--cache-outputs 250000]
   ```

   It reads `<data-dir>/enhance` without the publisher's journal lock, stops at
   the source tip (or `H`), checks every block hash with batched `getblockhash`,
   re-derives every non-empty block with the publisher's producer and the final
   Ironwood tree size with `getblock` verbosity 2. A regenerated record may differ
   from the old one only in flag bit 2 and bytes 645..653, and only where the old
   record had no fee. Any other difference, a hash or action-count mismatch, or an
   unresolvable spent output aborts the run. Choose `H` a few blocks below the tip
   if a reorganization near the tip is likely. An interrupted run resumes from the
   staged tip when started again with the same arguments. Progress is printed to
   stderr as JSON lines every 30 seconds.
3. On success the tool writes `enhance-staged/rebuild.json` last, after the staged
   journal is durable, and prints it. The receipt records the source revision and
   binary SHA-256, `H`, its block hash and tree size, SHA-256 digests of
   `records.bin` and `manifest.json`, the changed-record count and the count still
   without a fee (coinbase outputs). A completed output is never overwritten.
4. Restart through the locked deployment transaction. Before opening its journal it verifies the receipt
   digests, that the staged blocks (height, hash, first position, action count) are a
   prefix of the live journal, that every staged record differs from the live one
   only in the fee fields, and that no changed position lies in a sealed shard
   (`recovery.sealed` in `publication/publisher.json`). It then renames `enhance` to
   `enhance.before-rebuild-<receipt id>`, renames `enhance-staged` to `enhance` and
   syncs the data directory. A crash between the renames completes on the next start.
   It logs `adopted staged Enhance journal rebuild` with the receipt id, `H` and the
   changed count, then ingests `H+1` to the tip with the fixed producer. Repaired
   records reach wallets with the next publication, which follows the next block.
5. If any check fails, it logs `rejected staged Enhance journal rebuild` at error
   level with the reason, renames the staged directory to
   `enhance-staged.rejected-<unix time>`, and continues on the live journal. A
   staged directory without `rebuild.json` (a rebuild in progress) is left alone.

Adoption verification is one streaming pass over both journals: fixed-size chunks,
two reused buffers, the staged records digest computed during the comparison, and
no in-memory copy of either journal. It runs before the publisher opens its
control state and binds its HTTP listener, so the adopted journal is in place before
serving resumes; block catch-up and spent-output fetching stay in the poll loop after
the listener serves the restored snapshot. A restart without a completed staged
rebuild only checks for `enhance-staged/rebuild.json`. A locally measured adoption of
700,000 records in 50,000 blocks took 2.1 seconds with a warm page cache and grew peak
memory by under 1 MiB ([evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/mixed-fee-publication-2026-10-06/README.md));
a cold cache reads about 0.9 GB more slowly, and that time adds to the restart's
listener gap.

### Exact-record oracles during a repair

Two operator checks pin exact record bytes: the deploy exact-answer check, which runs
`enhance-pir-load-test --oracle` against a pinned oracle file, and the external
`pir-monitor` canary oracle (`PIR_MONITOR_ORACLE`, pinned by
`PIR_MONITOR_ORACLE_SHA256`; see [alerting](observability-alerting.md)). A record
whose fee is repaired no longer matches an oracle captured before the repair. For any
historical record repair:

Regenerate both oracles from canonical transactions decoded independently of the
publisher and pin their new digests in the reviewed maintenance transaction.
Do not use a served answer or the repaired journal as its oracle. The transaction
must verify the adopted journal and new publication before returning monitoring
to active mode; the HTTPS cutover does not permit skipping its exact checks.

Adoption refuses changes in sealed shards. A sealed-history repair needs a
separately reviewed recovery-fenced publication. Keep the preserved
`enhance.before-rebuild-<receipt id>` directory until verification finishes.
Runtime rollback retains current journal data and all recovery floors; undoing a
journal repair is a separate data-recovery operation.
