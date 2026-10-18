# First independent Transparent history provider

Status: approved for publication, remote implementation and PR preparation.
Roman reviews the resulting PRs and controls merging. This approval does not
authorize production deployment or new paid infrastructure.

## Outcome

An external operator can run one Transparent history provider with an embedded
router and recent worker. It pulls prepared publications from a configured
publisher over HTTPS, serves recent queries locally and forwards historical
requests to one fixed Valargroup archive upstream. An unchanged wallet-library
client uses that provider's complete history API for both kinds of request.
The current Vizor wallet must also remain compatible without an upgrade; a
passing library-client test alone does not satisfy this requirement.

The operator needs no indexer, local archival chain node, publisher filesystem,
fleet credential or SSH access to our infrastructure. A required canonicality
source may be remote; its existing validation and trust requirements remain.

This is an end-to-end slice of architecture phases 2–4, following the service
boundary refactor. It implements Transparent history only. It does not claim
support for display, Enhance, Status or Receiver.

The [architecture](../decentralized-query-serving.md),
[multi-server wallet specification](../spec/spec-multi-server-wallet.md),
[promotion specification](../spec/spec-shard-promotion.md) and existing
[Transparent contract](contract.md) govern behavior. A conflicting implementation
choice requires review; the worker must not silently weaken a contract.

Within those requirements, agents resolve ambiguity using the
[judgment and review policy](#judgment-calls-and-review) below instead of stopping
for routine design questions.

## Current Vizor compatibility requirement

The result MUST work with the current Vizor wallet without an application update,
wallet-library upgrade or repin, database migration, or new client-side protocol
behavior. Before implementation, identify and record the current application
version and artifact identity, its wallet-library revision and the protocol/profile
it uses. Keep that baseline fixed throughout qualification; do not substitute a
newer or rebuilt wallet to make the tests pass.

Preserve the wallet's existing initialization, discovery, setup, encrypted-query,
validation, retry and session-refresh behavior. Access must use an existing
supported endpoint configuration mechanism or server-side routing behind the
wallet's configured origin. Do not require new settings, a new endpoint-selection
UI, disabled TLS validation or a modified application binary. If the wallet cannot
select an independent endpoint today, document that constraint and validate
compatibility through its existing origin without claiming wallet-side provider
selection. Production routing or DNS changes still require separate approval.

Validate the unchanged Vizor application itself across recent and historical
recovery, continued synchronization, provider restart and publication handover.
Keep unrelated product paths used by that wallet working through their existing
services; history-only implementation does not authorize breaking display or
other existing calls. Failure against this baseline is a blocker for this effort,
not a reason to require a wallet upgrade or weaken its privacy/completeness checks.

## Reuse and boundaries

Reuse the existing product publisher, assignment planner, server backend,
prepared-runtime verification, activation and retained-request ownership. Use the
shared runtime and HTTP mechanisms from the preceding refactor when applicable.
Keep publication construction in the publisher and query execution in serving.
Add interfaces with their real producers and consumers in the owning PR.

Keep wallet paths, manifests, record layouts, cryptography, setup, shard identities
and refusal behavior unchanged. Distribution records belong to a separate
publisher/provider interface; wallets need not interpret them. Do not introduce a
universal publication format across products.

Lightweight mode requires compatible prepared runtime material and must refuse
preparation when it is missing or invalid. Existing deployments retain their
current preparation policy; this mode must not silently build locally to appear
ready. Persisted prepared files remain locally verified inputs, not authority to
serve arbitrary publications.

## PR stack

Implement bottom to top. Each PR has one outcome, working consumers and focused
tests. Later PRs target the preceding branch until it merges; update their bases
and rerun affected validation after integration. Use exact available upstream
heads rather than waiting for a human merge when implementation can proceed.

| PR | Outcome | Depends on |
|---|---|---|
| 1 | Fetch and verify a publisher's prepared recent publication over HTTPS | Finished service-boundary refactor |
| 2 | Follow updates and recover safely after interruption or restart | PR 1 |
| 3 | Serve the complete history API through an embedded router and recent worker | PR 2 |
| 4 | Give an external operator a reproducible standalone installation | PR 3 |
| 5 | Qualify continuous wallet recovery, updates and resource requirements | PR 4 |

### PR 1: distribute and bootstrap a prepared publication

Capture the current Vizor compatibility baseline and trace its required API and
endpoint configuration before finalizing the distribution/provider interface.

Add a versioned, read-only distribution interface to the existing publisher and
a provider-side bootstrap consumer. Reuse the current artifact formats and
verification machinery. The publisher exposes a small current-state record and
immutable, digest-bound publication metadata and artifacts. A finalized current
record must never reference unfinished or unavailable publisher artifacts.

The record identifies the network, exact publication and required runtime/profile,
recent holdings, necessary routing metadata, and applicable invalidation/recovery
state. The inventory distinguishes files needed locally from archive-only table
data; a recent provider must not download the archive's evaluation dataset.

Trust starts with the explicitly configured publisher HTTPS origin and existing
publication checks. Define the exact schema, authenticated-origin rules, replay
handling, canonicality inputs and invalidation ordering with this producer and
consumer. Download integrity alone cannot establish serving authority or chain
acceptance. Do not add a new signing or key-distribution system in this slice.

**Acceptance:** a clean provider directory obtains a real prepared recent
publication without SSH or publisher disk access and verifies it using the
production loader. Wrong network, incompatible runtime, corrupt or missing bytes,
unsafe artifact paths and incomplete publication fail before activation. Document
the precise trust and canonicality inputs; unresolved validity semantics block
this PR rather than being deferred until deployment.

### PR 2: reconcile, activate and recover

Add a local publication manager using request/response polling, conditional
requests, bounded transfer concurrency, timeouts, jitter and retry backoff. Fetch
only missing immutable files. After downtime, reconcile directly to current state;
do not require every intermediate revision or a delta base that no longer exists.

Stage, verify and load candidates while the active serving view remains available.
Recheck current state before activation and use the existing atomic activation
boundary. Retain admitted request handles and preserve revocation, canonicality
and freshness behavior. An unchanged response or failed poll must not extend
expired authority. Persist enough state to restart without exposing partial data.

**Acceptance:** real HTTP tests cover unchanged polling, missed revisions,
interrupted transfer, corrupt material, publisher outage, superseded candidates
and restart during staging and activation. Valid active service survives an
ordinary failed update; invalidated material is refused. Transfer counters prove
unchanged artifacts are reused. No implicit local runtime construction occurs.

### PR 3: embed the router and fixed archive route

Compose the recent worker and router in one provider process. Expose the complete
existing history API: initialization, map/filter discovery, manifests, setup and
encrypted queries. A configured provider endpoint must suffice for the wallet.
Keep routing and execution separable so the same components support decoupled
operation without requiring it for the default installation.

Serve eligible exact local holdings directly and forward archive requests to a
fixed Valargroup upstream supplied by trusted provider configuration. Preserve
encrypted framing, exact revision/session bindings, response validation,
admission, cancellation and existing retry restrictions. Prevent forwarding loops;
do not follow wallet-supplied destinations.

Changing published holdings must establish an eligible archive route before
retiring a promoted local shard. Exercise ownership changes of unchanged shards;
do not merge, split or repack them. Archive outages must leave independent valid
recent queries serviceable and make archive-dependent operations fail explicitly.

**Acceptance:** an unchanged wallet-library client completes both recent and
historical recovery through one real HTTP endpoint. Compare answers with the
existing independent oracle, and run a smoke test with the fixed current Vizor
application baseline. Test unavailable, incompatible and unready archives,
stale sessions, overload and disconnects. No missing range becomes an empty result
or public lookup. Verify route/worker handover and in-flight request ownership.

### PR 4: make standalone operation reproducible

Package the provider through existing release and role-rendering mechanisms,
preserving existing binary names. Supply an example configuration and a short
operator guide covering the approved publisher, fixed archive default, required
canonicality source, local state/cache, listener, resource budget and operator TLS.
Do not require our private inventory or controller credentials to render it.

Expose separate observations for synchronization, verified publication, runtime
readiness, active coverage and archive reachability. Explain how to bootstrap,
upgrade, restart, inspect an update failure and remove an installation. Keep
administration private and install only the requested product.

**Acceptance:** from a clean environment, another developer can start the provider
using documented artifacts and configuration and perform wallet queries without
editing Rust, using a source checkout at runtime or copying our deployment files.
Repeat the exercise across a process restart. This PR prepares installation
artifacts; it does not install them on production hosts.

### PR 5: qualify the complete slice

Exercise a publisher, two independent provider processes and an archive service
through real HTTP boundaries, with separate provider state directories. Each
provider independently supports an unchanged history client; no wallet failover
feature is needed to prove provider interchangeability.

Run the application-level compatibility cases with the fixed current Vizor build,
without changing its binary, dependencies or database schema. Record the exact
wallet and server artifacts and results. Library-only tests or a freshly rebuilt
wallet cannot replace this gate. Use isolated test data and endpoints; preserve
the user's installed wallet and real wallet databases.

Run bootstrap, continued publication updates, restore/rescan, publisher outage,
archive outage, restart, reorg invalidation and an ownership-only shard move under
continuous queries. Record exact source, artifacts, dataset, profiles, offered
load, errors and timeouts with the results. Reuse existing test tooling and keep
generated output out of Git except essential retained evidence.

Measure total host memory, including router, prepared data, query workspace,
candidate overlap, scratch and system overhead. Establish a supported configuration
that fits the 64 GB Recent Only host contract. Under matched supported load,
qualify update/handover interference against the existing 10% p99 requirement;
do not count rejected or timed-out requests as a latency improvement.

Qualify publisher distribution with 10 and then 100 logical HTTP consumers using
real artifact transfers and conditional polling. These are distribution clients,
not a requirement to provision 100 query hosts. Measure synchronized bootstrap
and ordinary updates separately; report publisher bandwidth and CPU alongside
serving interference. Publisher admission must remain bounded under overload.

**Acceptance:** publish the tested operating envelope and remaining limitations.
Use existing suitable hardware; shared-host timings are diagnostic and cannot
establish a strict p99 bound. If suitable isolated hardware is unavailable,
continue correctness work and report performance qualification as a named gate.
Do not purchase or provision infrastructure without separate approval. Passing
this stack does not authorize a production rollout.

## Start gate and coordination after approval

After Roman approves this plan, submit one persistent Codex coordinator to the
development hub. Do not submit it merely because this document exists. Preserve
the currently running repair and refactor agents and their work.

The coordinator first monitors the current repair and five-milestone refactor to
their actual outcomes. A task marked finished or integrated is insufficient:
verify its final agent exit, required checklist, preserved commits, reviewable PRs
and relevant check results. In particular, establish ownership and results for
runtime sharing, HTTP lifetimes and ingestion orchestration, plus any repair of
the preceding encapsulation change. Do not start duplicate implementations.

Start this PR stack once those agents have finished and a coherent, validated
base contains the changes it needs. The base may be an available unmerged stack
head; record its exact SHA and track repinning after merge. An unresolved defect
in a required boundary is a gate. Unrelated pending work need not hold the slice.

The coordinator owns progress and unblocking throughout:

- Verify real startup and fresh progress, not just assignment. Investigate stale
  checkpoints, model/authentication errors, failed checks and host refusal reasons.
  Never treat an unchanged base already on main as a failed agent's completion.
- Use existing approved Codex authentication, Medium effort for work and High at
  most for review. Do not fall back to the Claude subscription that hit its limit.
  A coordinator must have a supported hub control path; verify it before relying
  on that worker to submit or steer child tasks.
- Execute small child tasks with roughly 90-minute budgets and one reviewable
  outcome each. Split an oversized PR internally into checkpoints before a long
  unbounded run; retain the five outcomes and dependency order.
- Review each result, fix routine conflicts or failed checks within scope, keep
  the stack current and retain exact-head validation. Open focused follow-up PRs
  for confirmed issues in merged work, with the required repair references.
- Treat compatibility with the fixed current Vizor build as a hard acceptance
  gate. Resolve failures on the server side within scope; do not upgrade the
  wallet or quietly change the baseline.
- Keep the human status current at useful checkpoints. Use task-owned durable
  jobs and completion monitors for long checks. This is a persistent coordination
  task, not a heartbeat that repeatedly starts implementation from this chat.
- Resolve design questions within scope using the judgment and review policy
  below. Bring back only choices that would change an explicit requirement or
  authorization boundary, missing credentials, or repeated acceptance failures.
  Finish authorized independent work while a dependent step waits.

Roman reviews the PRs and controls merging. No auto-merge or production activation
is part of this approval. The final coordinator result must identify each PR,
its exact head, validation, integration state and any remaining qualification.

## Judgment calls and review

When the plan leaves an ambiguity or a question arises, the agent should use its
best judgment and continue within the approved scope. Prefer the smallest change
that satisfies the stated outcome and preserves the existing contracts. Routine
questions about implementation, component placement or behavior left open by the
plan are not reasons to wait for Roman when a reasonable reversible choice exists.

Make those choices easy to review and undo:

- Keep a judgment call localized and, where practical, in a focused commit that
  can be reverted without discarding unrelated work. Identify dependent PRs if
  reversing it would require changing them too.
- Reuse existing interfaces and configuration. Do not add speculative abstractions,
  compatibility layers or extra configuration switches merely to preserve every
  possible option.
- Avoid irreversible data changes and coupling an uncertain choice to publication
  identities, wallet behavior or other hard contracts. An easy code revert is not
  sufficient if it would leave incompatible state or already-published artifacts.
- Test the observable behavior and the failure cases affected by the assumption.
  Keep essential explanations at the owning code boundary, without adding a
  separate committed decision log or agent report.

Every implementation or follow-up PR in this effort MUST include a separate
**Judgment calls and assumptions** section in its description, alongside the
usual summary, breaking changes and validation sections. For each material choice,
document:

1. The ambiguity or unanswered question.
2. The chosen behavior or assumption, and why it best serves the approved goal.
3. The meaningful alternative considered and the tradeoff.
4. How to reverse the choice, including any state/artifact effects or dependent
   PRs; do not describe a change as reversible when those effects remain.
5. The validation supporting it and any uncertainty Roman should assess.

Keep this section concise and current with the final implementation, rather than
recounting the agent's work history. Include choices made by delegated agents.
When there are no material judgment calls, state **None** explicitly. The
coordinator verifies this section before presenting each PR for review, so Roman
can accept the choice or request its replacement as part of normal PR review.

This policy does not authorize changing explicit requirements, weakening privacy,
trust or correctness checks, upgrading the current Vizor baseline, spending on new
infrastructure, or taking destructive or production actions. If no reasonable
choice fits those boundaries, report the specific conflict and a recommended
resolution while continuing independent authorized work. Do not silently relax
the requirement to avoid a question.

## What this effort will not build

- Wallet-side provider discovery, automatic failover, ranking or query fan-out.
- Archive discovery, multiple archive-upstream selection or publisher switching.
- Subscriptions, streaming delivery, a DHT, gossip, payments or reputation.
- A new universal protocol, wallet schema or PIR primitive, or changes to existing
  wallet and publication persisted formats.
- Display, Enhance, Status or Receiver provider support.
- Automated promotion scheduling, new recent-window/cadence policy, shard
  re-cutting, merging, renumbering or repacking. Follow published holdings and test
  the handover mechanism without choosing the publisher's future policy.
- GPU qualification, production deployment, new paid infrastructure or a claim
  that source-level success proves a production performance guarantee.
