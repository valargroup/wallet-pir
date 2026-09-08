# Transparent PIR documentation

Updated 2026-09-07. The accepted target is four small recent replicas and two larger archive workers. This is an approved direction, not a claim of deployed fleet capacity.

## Reading order and authority

1. [Contract](contract.md): recovery, privacy, trust, coverage and failure requirements.
2. [Architecture](architecture.md): source-verified components and the target data flow.
3. [Deployment](deployment.md): the single source for accepted target parameters and operating procedures.
4. [Status](status.md): source-verified implementation and separately verified live state.
5. [Remaining work](remaining-work.md): ordered deliverables, dependencies and acceptance gates.
6. [Evidence](evidence/README.md): measurement provenance and limitations.

Do not infer live state from a plan, source code, a workflow default or an old handoff. Do not infer target settings from benchmark fixtures. An unresolved conflict is recorded in status rather than resolved by selecting the most recent prose.

The contract governs intended behavior; code establishes implementation; deployment governs target configuration; dated operational evidence establishes live state. If these disagree, report the gap and update the owning document with the implementation change. Do not silently weaken the contract.

## Product boundaries

Active transparent recovery uses `pir/transparent-events`, `pir/transparent-filter`, `pir/transparent-shard`, `pir/transparent-wallet`, `pir/transparent-wallet-store`, `server/transparent-filter-server`, and `server/transparent-shard-server`.

`pir/transparent-history`, `server/transparent-history-server`, `server/transparent-measure`, and the older Python retrieval/navigation/reuse tools are research backends. Some remain workspace members and test inputs; they do not define deployed geometry or fleet sizing. `pir/transparent-spend` is the separate retained outpoint protocol. Enhance PIR has its own [architecture](../architecture.md) and [runbook](../enhance-pir-deploy.md). The excluded nullifier/witness [demos](../../demos/legacy-spendability/README.md) are historical.

The [filter API](../transparent_filter_api.md), [range envelope](../transparent_filter_envelope.md), and [filter crate reference](../../pir/transparent-filter/README.md) remain active detailed references. Their formats have separate versioning from shard schema.

## Maintenance

Use measured, projected, proposed, implemented, and deployed precisely. Every new benchmark needs the evidence metadata; every deployment needs an observed status record. Keep exact target parameters only in deployment.md and link there elsewhere. Keep unchecked work only in remaining-work.md; status summarizes it by reference. Delete superseded documents and update inbound links instead of creating archive copies or forwarding stubs.

Superseded transparent PIR documents and their old paths have been deleted. Raw evidence stays at its original paths. Use version control history for former designs and operational narratives; those historical texts are not current instructions.
