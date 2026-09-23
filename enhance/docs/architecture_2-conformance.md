# Architecture 2 conformance audit

Status: **deployed; sustained qualification and final restoration in progress**.
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
| Loan/return, reorg and retained sessions | [Current full-size HTTP tests](../evidence/architecture-v4-worker-disk-2026-09-23/README.md); canonical CLI tests and explicit expired-session refresh. | Finish native full-size transition/reclaim campaign. |
| Placement and admission | Count-based placement, whole-shard memory relocation, destination pair quorum and reservation-failure tests. | Measure actual accepted active/sealed assignments and calibrate overhead. |
| Durable recovery | Per-phase journals, fencing, commit/abort outboxes, offline row repair, actual CLI crashes during prepare and after durable commit. [Production failover/restart](../evidence/architecture-v4-production-2026-09-23/README.md) adds 329 exact answers without errors. | Deployed repair and legacy rollback rehearsal; full-size recovery and delayed reclamation. |
| Canonical ingestion and record packing | Live RPC catch-up and replicated publication after explicit 29-to-33 packing migration. The raw 565,944-record journal hash was unchanged. | Broader independent chain extraction/inclusion checks, distinct from comparing against the legacy journal. |
| Client and note behavior | Real encrypted queries, retained sessions, row/unit boundary oracles, canonical transaction fixtures and authenticated note recovery tests. | No downstream application/SDK integration claim is made. |
| Memory and resource limits | Measured production host RAM and reserve, explicit cgroup limits, accurate RSS/kernel/cgroup/PSI/swap sampling. One-second active-campaign sampling is live. | Complete sustained active/sealed campaigns, guard/overhead calibration, reclaim, disk and load acceptance. |
| Load and overload | Four production cases: public HTTPS, private concurrency 1/2, open-loop 2 QPS; 1,870 correct measured answers and zero errors. | Full-size capacity/overload acceptance. Short live-dataset tests are not maximum-placement evidence. |
| Infrastructure and capacity demand | Durable request journal, guarded Terraform/provider reconciliation, bootstrap and inventory tests. Existing hosts are reused and legacy autoscaling is disabled. | Live automatic expansion remains unverified and is intentionally not enabled under the no-new-hosts instruction. |
| Artifact and deployment | [Clean full-LTO Linux candidate](../evidence/architecture-v4-clean-candidate-2026-09-23/README.md), checksums, direct SSH deployment, public canonical serving and production tests. | Finish sustained testing, restore canonical serving, rehearse rollback and record final state. Remote CI was explicitly bypassed. |
| Main branch | Final cleanup and merge requested by the user. | Record the pushed commit and any later test-driven fixes. |

## Current operational state

The [production runbook](../ops/deploy/v4-production.md) identifies services,
addresses, preserved directories and restoration order. The public canonical
coordinator is temporarily stopped for the full-size active fixture campaign.
Fixtures listen only on loopback 8280. Actual production c-4 workers run the new
binary with fresh test state; canonical state is preserved separately.

`enhance-pir-v4-active-campaign.service` on the coordinator is the authoritative
live workload handle. Worker sampling uses `enhance-pir-v4-active-sampling.service`.
Inspect these handles and `exercise.json` before deciding whether a campaign
finished, failed or needs recovery. A timeout while observing a live job is not
permission to restart it.

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
