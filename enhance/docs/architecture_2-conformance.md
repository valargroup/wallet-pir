# Architecture 2 conformance and release audit

Status: **incomplete; not qualified or deployed**. This audit separates the
requirements in [architecture 2](architecture_2.md) from optional automation and
planner improvements. It is a map of evidence and remaining work, not a release
certificate. Evidence applies to the source hashes recorded in each report.

The [current combined server CI selection](../evidence/architecture-v4-ci-selection-2026-09-23/README.md)
passed locally: 94 tests, zero failures, and two explicitly ignored full-size
campaigns across 13 executables. This includes all four bounded v4 integration
suites. It is not a remote CI result or native Linux qualification.
The [current Linux candidate](../evidence/architecture-v4-current-linux-2026-09-23/README.md)
also compiled and passed emulated publication/load checks after the last placement
correction. It remains a dirty-source development artifact.

## Scope decisions

The architecture's exceptional-recovery table explicitly permits a reorg to
relocate into qualified capacity **or block publication**. It does not require a
general multi-shard rearrangement solver. The implemented whole-shard fallback
and safe blocking path must be tested, but adding an exhaustive placement solver
is an optional improvement, not a prerequisite invented by this plan. Qualification
of every accepted assignment remains mandatory.

Offline row repair is implemented. Automatic peer transfer can improve recovery,
but the specification requires recoverable artifacts and safe references; it does
not prescribe automatic remote transfer as the only acceptable repair mechanism.
The deployed repair/restart procedure still needs rehearsal and evidence.

The six-hour/300-publication campaigns and 24-hour cutover observation are gates
of the implementation/deployment plan. The architecture itself requires sustained
qualification beyond retention with concurrent off-host traffic and measured
transition overlap. Retain those campaign gates; a short test does not replace them.

## Evidence and gaps

| Requirement area | Available evidence | Completion gap |
|---|---|---|
| Geometry and PIR equivalence | Protocol/runtime tests, including unequal-unit vs monolithic evaluation; [library regression](../evidence/architecture-v4-memory-relocation-2026-09-23/library.log) | Tie final qualified artifacts to a complete regression run; no hardware qualification follows from these unit tests. |
| Carve-out, return, reorg and retained sessions | [Full-size HTTP campaigns](../evidence/architecture-v4-placement-fullsize-2026-09-23/README.md) verify boundaries, old queries and anchor-change rejection. | These precede the last reorg/admission correction. Final combined regression and deployed transition/restart rehearsal remain. |
| Exceptional active moves | [Four-worker HTTP relocation](../evidence/architecture-v4-memory-relocation-2026-09-23/README.md) checks memory refusal, pair replication and current/retained queries. | Refusal is injected, not measured on qualified hardware. Multi-shard optimization is optional; preserving service when blocking is required. |
| Reorg destination admission | [Composition regression](../evidence/architecture-v4-reorg-memory-placement-2026-09-23/README.md) checks count-planned moves, pair quorum and spare placement. | Uses full-size metadata and controlled admission endpoints; Current native combined regression and emulated Linux artifacts pass; clean release provenance and measured hardware admission remain open. |
| Durable publication and recovery | HTTP abort/commit/restart, stale-decision and offline-peer campaigns; phase-journal tests; [actual coordinator kill during preparation](../evidence/architecture-v4-cli-prepare-crash-2026-09-23/README.md) and [kill after durable commit](../evidence/architecture-v4-cli-commit-crash-2026-09-23/README.md), with distinct abort/retry versus committed-decision recovery assertions. | Complete distributed per-phase process-crash and deployed recovery evidence, including delayed query reclamation. |
| Canonical ingestion | [Actual CLI RPC integration](../evidence/architecture-v4-canonical-rpc-2026-09-23/README.md) checks canonical ingestion, journal rewind, exact PIR records and restart against a local RPC test double. [Inconsistent metadata rejection](../evidence/architecture-v4-rpc-rejection-2026-09-23/README.md) preserves the published manifest and retained service before recovery. | Synthetic block envelopes do not prove live-chain compatibility or inclusion. Validate selected real-chain data and independent wallet conformance. |
| Record correctness and wallet authentication | [Canonical transaction byte oracle](../evidence/architecture-v4-canonical-record-2026-09-23/README.md) and [synthetic note recovery](../evidence/architecture-v4-note-recovery-2026-09-23/README.md). | Not snapshot-wide independent extraction, chain inclusion proof, or downstream wallet validation. |
| Provisioning and expansion | Durable demand, guarded Terraform adapter, membership recovery, bootstrap and inventory reconciliation tests. | Confirm target project/hardware, exercise live provider recovery and bootstrap, bind qualification to registration, and measure readiness lead time. |
| Memory limits and performance | Admission model, sampler, observer, workload driver and [campaign assessment](../evidence/architecture-v4-campaign-assessment-2026-09-23/README.md). | Actual 8 GiB assignments, six-hour campaigns, off-host traffic, transient peaks, 512 MiB guard, overhead calibration, reclaim and swap evidence are missing. The assessor deliberately never issues qualification. |
| Load and overload | Native characterization and short emulated Linux exact-answer runs. | Full capacity/open-loop/soak acceptance on qualified native hardware; the earlier open-loop sweep had HTTP 429 even at its lowest offered rate. |
| Deployment and rollback | Candidate packaging, private worker service/bootstrap and isolated infrastructure tools. | Clean current release artifact, live parallel-origin deployment, rollback rehearsal, wallet conformance, cutover and observation are missing. No production deployment is established. |

## Execution order

1. Complete remaining local recovery tests; the bounded canonical RPC integration
   test is now implemented.
   Run final combined regressions after functional edits; avoid rebuilding a
   release artifact after every isolated test-only change.
2. Produce a clean, identified Linux candidate and validate its runtime and
   installation. Existing dirty-source binaries are development evidence only.
3. With the target project and hardware confirmed, provision the isolated fleet
   and generator, exercise provider/bootstrap recovery, and collect host limits.
4. Run full-size active/sealed, transition/reorg/reclaim and offered-load campaigns.
   Review measured memory and latency evidence; do not equate an assessor exit
   code with qualification or automatically register an unqualified pair.
5. Validate independent canonical oracles and downstream wallet behavior, rehearse
   rollback, then execute the parallel-origin rollout and observation gates.

The infrastructure target question remains unanswered. This blocks live
provisioning, not the independent local implementation and testing above. Do not
infer target selection or authorization from automatic goal continuation.


The [deployment input record](../ops/deploy/v4-target-selection.json) identifies
the proposed initial pair and unresolved target inputs. It is deliberately not
an executable provisioning policy, an approval, or a qualification receipt. The
provisioning consumer handles expansion after an established first pair; initial
bootstrap and remote state must be established separately and verified before
using that consumer. Do not feed placeholder or inferred identifiers into it.
