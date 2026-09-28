# Transparent PIR documentation

Status recorded 2026-09-13; documentation consolidated 2026-09-14. The accepted target is four small recent replicas and two larger archive workers. M0–M2 are accepted, including the revised six-hour M1 fleet observation. M3 has fixture and native real-wallet evidence; application/lifecycle acceptance, whole-wallet measurements, capacity and release observation remain open. Read [status](status.md) for the current evidence and [remaining work](remaining-work.md) for the next actions.

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

## Experiments

[Six-month architecture update](architecture_update.md) records the 2026-09-27
review recommendations, projected costs, privacy constraints, and evaluation
sequence for ordinary wallets waking after up to six months. It is a proposed
design, not a replacement for accepted deployment settings or measured results.

[Entry layout update](architecture_entry_update.md) records the current v7
directory and page record layouts and the proposed replacement of exact raw
scripts with 14-byte salted cryptographic tags. It is an unaccepted schema proposal,
not an implementation or deployment claim.

[Parent-filter evaluation](parent-filter-evaluation.md) documents the isolated hierarchy benchmark, recent-first selection rule, and explicit opt-in privacy change. The later [production artifact rollout](../evidence/parent-filters-production-2026-09-08/README.md) records HTTPS verification and the bounded production recovery canary.
