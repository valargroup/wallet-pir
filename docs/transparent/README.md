# Transparent PIR documentation

Start shared operations with the [fleet operator guide](../operations.md).
The [October 13 retirement record](../../evidence/transparent/v1-sunset-2026-10-13/README.md)
records `/v2/` schema-v12 history and txid-4k8 as the remaining wallet API, with
v1 serving stopped and the journal writer handed to v12. Earlier milestone and
elastic-fleet acceptance records remain historical evidence, not the current
fleet inventory. Read [status](status.md) for observed deployment records and
[remaining work](remaining-work.md) for qualification and follow-up.

## Reading order and authority

1. [Contract](contract.md): recovery, privacy, trust, coverage and failure requirements.
2. [Architecture](architecture.md): source-verified components and the target data flow.
3. [Deployment](deployment.md): the single source for accepted target parameters and operating procedures.
4. [Status](status.md): source-verified implementation and separately verified live state.
5. [Remaining work](remaining-work.md): ordered deliverables, dependencies and acceptance gates.
6. [Evidence](../../evidence/transparent/README.md): measurement provenance and limitations.
7. [Wallet adapter contract](wallet-adapter.md): what a wallet supplies to, and may rely on from, the reference library.

Do not infer live state from a plan, source code, a workflow default or an old handoff. Do not infer target settings from benchmark fixtures. An unresolved conflict is recorded in status rather than resolved by selecting the most recent prose.

The contract governs intended behavior; code establishes implementation; deployment governs target configuration; dated operational evidence establishes live state. If these disagree, report the gap and update the owning document with the implementation change. Do not silently weaken the contract.

8. [Regression and conformance tests](testing.md): accepted-anchor recovery, fixed fixtures, and manual release validation.
9. [Elastic recent replicas](elastic-recent.md): inventory, membership, scaler and actuator formats for the automatically scaled recent tier.

10. [Txid display PIR](../txid/txid-display.md): opt-in server tables, native HTTP demo, the tiered proof of concept and subsequent wallet integration.

11. [Txid sizing and anonymity findings](../txid/transparent-txid-sizing-and-anonymity-findings.md): whole-range probability sizing bounds, codec proposals and joint routing limits.

12. [Txid display shipped runtimes plan](../txid/txid-display-shipped-runtimes-plan.md): handoff design for building the recent display runtimes on the coordinator and loading them on the worker, with tests, benchmarks, deploy order and acceptance targets.

13. [First independent history provider plan](recent-provider-plan.md): approved PR stack for HTTPS publication synchronization, embedded recent serving and a fixed Valargroup archive upstream.

14. [Prepared history distribution and standalone provider](prepared-distribution.md): HTTPS trust/replay rules, release artifacts, provider configuration and operator lifecycle.

## Product boundaries

Active transparent recovery uses `wallet-libraries/zakura/transparent-events`, `wallet-libraries/zakura/transparent-filter`, `wallet-libraries/zakura/transparent-shard`, `wallet-libraries/zakura/transparent-txid-client`, `wallet-libraries/zakura/pir-transparent` (`engine` and `engine_store`), `src/shared/pir-publisher/transparent`, and `src/transparent/transparent-query-server`.

Both services consume the shared package
`src/transparent/transparent-shared`; the compact-block server is a
separate benchmark baseline in `tools/transparent`.

Validation tools live in the `transparent-tools` package under `tools/transparent/`: load tests, regression fixtures, and the
current-shard `transparent-measure` command used by the backfill workflow.
Enhance has its own [index](../enhance/README.md).

The [filter API](filter-api.md), [range envelope](filter-envelope.md), and [filter crate reference](https://github.com/zakura-core/wallet-libraries/blob/main/zakura/transparent-filter/README.md) remain active detailed references. Their formats have separate versioning from shard schema.

## Maintenance

Use measured, projected, proposed, implemented, and deployed precisely. Every new benchmark needs the evidence metadata; every deployment needs an observed status record. Keep exact target parameters only in deployment.md and link there elsewhere. Keep unchecked work only in remaining-work.md; status summarizes it by reference. Delete superseded documents and update inbound links instead of creating archive copies or forwarding stubs.

Retained raw evidence lives under `evidence/transparent/`; its bytes and historical
provenance are preserved. Superseded diagnostics and designs are recorded in the
[historical cleanup ledger](https://github.com/valargroup/wallet-pir/blob/3765536ee363af7646cc70d8c11ea4da27818ef5/docs/cleanup-2026-09-14.md). Use Git history for those records;
they are not current instructions. Follow the [retention rules](../../evidence/README.md).

## Compact layout

Schema v10 is deployed; the [SSH cutover and public checks](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/v10-cutover-2026-09-28/README.md) record the verified production state. See the
[architecture](architecture.md#private-record-layout),
[storage evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/compact-layout-2026-09-28/README.md) and
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

[Parent-filter evaluation](parent-filter-evaluation.md) documents the isolated hierarchy benchmark, recent-first selection rule, and explicit opt-in privacy change. The later [production artifact rollout](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/parent-filters-production-2026-09-08/README.md) records HTTPS verification and the bounded production recovery canary.
