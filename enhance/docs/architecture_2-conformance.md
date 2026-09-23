# Architecture 2 conformance audit

Status: **deployed; six-sealed campaign building; qualification incomplete**.
This maps [architecture 2](architecture_2.md) to implementation and observed tests.
It is not a release certificate. Source and binary hashes bound each evidence set.

The user explicitly replaced isolated-host provisioning with direct deployment
and testing on the existing production hosts, authorized stopping legacy services,
and requested CI bypass where possible and a clean merge into `main`. No new
hosts were created. This changes deployment order, not the meaning of a passing
qualification result.

## Requirement evidence

| Area | Authoritative evidence | Remaining verification |
|---|---|---|
| Geometry and PIR equivalence | Protocol/runtime tests, unequal-unit versus monolithic evaluation, and [94-test server regression](../evidence/architecture-v4-ci-selection-2026-09-23/README.md). | Tie later functional changes to fresh relevant regression results. |
| Loan/return, reorg and retained sessions | [Current full-size HTTP tests](../evidence/architecture-v4-worker-disk-2026-09-23/README.md); canonical CLI tests and explicit expired-session refresh. | Active transition campaign completed; qualify uninterrupted resource traces and delayed reclamation. |
| Placement and admission | Count-based placement, whole-shard memory relocation, destination pair quorum and reservation-failure tests. | Measure actual accepted active/sealed assignments and calibrate overhead. |
| Durable recovery | Per-phase journals, fencing, commit/abort outboxes, offline row repair, actual CLI crashes during prepare and after durable commit. [Production failover/restart](../evidence/architecture-v4-production-2026-09-23/README.md) adds 329 exact answers without errors. | Deployed missing-row repair passed: unchanged journal and 218 exact answers through repaired replica. Legacy rollback/return also passed (9 legacy and 443 v4 exact answers). Full-size recovery and delayed reclamation remain. |
| Canonical ingestion and record packing | Live RPC catch-up and replicated publication after explicit 29-to-33 packing migration. The raw 565,944-record journal hash was unchanged. | Broader independent chain extraction/inclusion checks, distinct from comparing against the legacy journal. |
| Client and note behavior | Real encrypted queries, retained sessions, row/unit boundary oracles, canonical transaction fixtures and authenticated note recovery tests. | No downstream application/SDK integration claim is made. |
| Memory and resource limits | Measured production host RAM and reserve, explicit cgroup limits, accurate RSS/kernel/cgroup/PSI/swap sampling. Active campaign completed; worker 1 has a preserved sampling gap. | Complete sustained active/sealed campaigns, guard/overhead calibration, reclaim, disk and load acceptance. |
| Load and overload | Four production cases: public HTTPS, private concurrency 1/2, open-loop 2 QPS; 1,870 correct measured answers and zero errors. | Full-size capacity/overload acceptance. Short live-dataset tests are not maximum-placement evidence. |
| Infrastructure and capacity demand | Durable request journal, guarded Terraform/provider reconciliation, bootstrap and inventory tests. Existing hosts are reused and legacy autoscaling is disabled. | Live automatic expansion remains unverified and is intentionally not enabled under the no-new-hosts instruction. |
| Artifact and deployment | [Clean full-LTO Linux candidate](../evidence/architecture-v4-clean-candidate-2026-09-23/README.md), checksums, direct SSH deployment, public canonical serving and production tests. | Finish qualification. Canonical restoration, four load cases and legacy rollback/return passed. Remote CI was explicitly bypassed. |
| Main branch | Implementation merged and pushed at `436dcc7`. | Commit subsequent test-driven fixes and evidence. |

## Current operational state

The [production runbook](../ops/deploy/v4-production.md) identifies services,
addresses, preserved directories and restoration order. Canonical v4 serving
is restored with both replicas. The active workload completed six hours and
320 publications; four post-restoration load cases passed with 1,893 exact answers.

Worker 1 has a measurement gap from 06:51:47 to 09:50:33 UTC on September 23.
The failed sampler restart and failed initial automatic restoration are recorded
in the production evidence. Neither the gap nor missing observations are treated
as passing measurements. The six-sealed campaign is now building on the c-4 pair with coordinator-hosted
helper replicas. Canonical serving is temporarily stopped for that campaign,
with automatic restoration supervised. See production evidence `sealed-start/`.

## Scope and acceptance rules

The architecture permits exceptional reorgs to relocate into qualified capacity
or block publication. A general multi-shard rearrangement solver is optional.
Automatic peer transfer is optional; recoverable immutable artifacts and a safe
repair procedure are required.

Retain six-hour/300-publication campaign evidence from the implementation plan.
The architecture requires sustained qualification beyond retention, off-worker
traffic and measured transition overlap. A short smoke run, a provisioned host,
a successful bootstrap or an assessor exit code does not grant qualification.

An operator-authorized early cutover is recorded separately from qualification.
Any coordinator-hosted helper replicas used to honor the no-new-hosts instruction
must be identified as test support and must not be reported as qualified physical
8 GiB replicas. Preserve evidence of errors and resource limits rather than
substituting smaller workloads for an explicit full-size requirement.
