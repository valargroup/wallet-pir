# Transparent PIR documentation

Status updated 2026-09-28 UTC; milestone acceptance remains as recorded below. The accepted target is two small recent replicas, grown elastically under load, and two larger archive workers. M0–M2 are accepted, including the revised six-hour M1 fleet observation. M3 has fixture and native real-wallet evidence; application/lifecycle acceptance, whole-wallet measurements, capacity and release observation remain open. Read [status](status.md) for the current evidence and [remaining work](remaining-work.md) for the next actions.

## Reading order and authority

1. [Contract](contract.md): recovery, privacy, trust, coverage and failure requirements.
2. [Architecture](architecture.md): source-verified components and the target data flow.
3. [Deployment](deployment.md): the single source for accepted target parameters and operating procedures.
4. [Status](status.md): source-verified implementation and separately verified live state.
5. [Remaining work](remaining-work.md): ordered deliverables, dependencies and acceptance gates.
6. [Evidence](../evidence/README.md): measurement provenance and limitations.
7. [Wallet adapter contract](wallet-adapter.md): what a wallet supplies to, and may rely on from, the reference library.

Do not infer live state from a plan, source code, a workflow default or an old handoff. Do not infer target settings from benchmark fixtures. An unresolved conflict is recorded in status rather than resolved by selecting the most recent prose.

The contract governs intended behavior; code establishes implementation; deployment governs target configuration; dated operational evidence establishes live state. If these disagree, report the gap and update the owning document with the implementation change. Do not silently weaken the contract.

8. [Regression and conformance tests](testing.md): accepted-anchor recovery, fixed fixtures, and manual release validation.
9. [Elastic recent replicas](elastic-recent.md): inventory, membership, scaler and actuator formats for the automatically scaled recent tier.

10. [Txid display PIR](txid-display.md): opt-in server tables, native HTTP demo, the tiered proof of concept and subsequent wallet integration.

## Product boundaries

Active transparent recovery uses `transparent/crates/transparent-events`, `transparent/crates/transparent-filter`, `transparent/crates/transparent-shard`, `transparent/crates/transparent-wallet`, `transparent/crates/transparent-wallet-store`, `transparent/services/transparent-filter-server`, and `transparent/services/transparent-shard-server`.

Validation tools live under `transparent/tools/`: load tests, regression fixtures, and the
current-shard `transparent-measure` command used by the backfill workflow.
`enhance/crates/transparent-spend-pir` is preserved legacy source outside the active
workspace and CI. Enhance has its own [index](../../enhance/docs/README.md).
The excluded nullifier/witness [demos](../../demos/legacy-spendability/README.md)
remain an independent preserved workspace.

The [filter API](filter-api.md), [range envelope](filter-envelope.md), and [filter crate reference](../../transparent/crates/transparent-filter/README.md) remain active detailed references. Their formats have separate versioning from shard schema.

## Maintenance

Use measured, projected, proposed, implemented, and deployed precisely. Every new benchmark needs the evidence metadata; every deployment needs an observed status record. Keep exact target parameters only in deployment.md and link there elsewhere. Keep unchecked work only in remaining-work.md; status summarizes it by reference. Delete superseded documents and update inbound links instead of creating archive copies or forwarding stubs.

Retained raw evidence lives under `transparent/evidence/`; its bytes and historical
provenance are preserved. Superseded diagnostics and designs are recorded in the
[cleanup ledger](../../docs/cleanup-2026-09-14.md). Use Git history for those records;
they are not current instructions. Follow the [retention rules](../../evidence/README.md).

## Compact layout

Schema v10 is deployed; the [SSH cutover and public checks](../evidence/v10-cutover-2026-09-28/README.md) record the verified production state. See the
[architecture](architecture.md#private-record-layout),
[storage evidence](../evidence/compact-layout-2026-09-28/README.md) and
[qualification gates](remaining-work.md#schema-v10-qualification-and-republication).
The journal remains version 2.

## Experiments

The 2026-09-27 six-month architecture review and the entry-layout study that
followed it are closed. What they produced in source — choice tables, the native
ReinspiRING profile, schema v9's salted script tags, the v2 filter profile and
cross-shard request concurrency — is described in [architecture](architecture.md);
what remains unpublished or unmeasured is tracked in
[remaining work](remaining-work.md). Their raw measurements keep their own dated
evidence directories, and the review text itself is in Git history at `b693cfde`.

[Parent-filter evaluation](parent-filter-evaluation.md) documents the isolated hierarchy benchmark, recent-first selection rule, and explicit opt-in privacy change. The later [production artifact rollout](../evidence/parent-filters-production-2026-09-08/README.md) records HTTPS verification and the bounded production recovery canary.
