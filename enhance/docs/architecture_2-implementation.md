# Architecture 2 implementation, testing and deployment

## Current state

The implementation is deployed directly to the existing production hosts using
clean Linux candidate `9718a6dcf9801385f69f31bb71f02efd261914f0`. Public canonical
queries, native load, single-replica failover and coordinator restart checks
passed. Full-size sustained qualification is **in progress, not passed**.

The user explicitly authorized direct SSH deployment, stopping the old Enhance
services, bypassing CI where possible, and creating no new hosts. This replaces
the earlier proposal for an isolated fleet and a new c-16 generator. Production
cutover before qualification is an operator decision, not qualification evidence.

The current full-size active campaign temporarily occupies both production c-4
workers. Canonical serving is stopped during that test; synthetic fixture traffic
is confined to coordinator loopback port 8280. Restore canonical serving after
the capacity and recovery campaigns. The [production runbook](../ops/deploy/v4-production.md)
records identities, preserved state and restoration commands.

## Implementation delivered

- Schema 10 and protocol `ironwood-enhance-pir-v4`, explicit public domains,
  fixed 32K shards, 4K loan/return boundaries and adaptive 2K/4K/8K units.
- Content-bound immutable runtimes and sessions; five retained generations plus
  candidate preparation. Queries pin the runtime they use through reclamation.
- Durable publication, worker fencing, atomic journals, commit/abort outboxes,
  restart recovery, quarantine and replica-aware routing. Ordinary publications
  can use one healthy replica; new groups and shard moves require both replicas.
- Whole-shard placement, consolidation, reorg handling, admission-driven
  relocation or safe publication blocking, and durable capacity demand.
- Private worker HTTP, canonical RPC ingestion, source-anchor revalidation,
  exact record queries, lazy client sessions and expired-session refresh.
- Runtime telemetry, explicit memory reservations and bounded concurrency,
  disk-backed recovery artifacts, and offline repair from verified peer rows.
- Exact-answer closed/open-loop load generation, full-size workload drivers,
  worker sampling and campaign assessment. The assessor never grants qualification.
- Optional guarded infrastructure expansion, durable operation journaling,
  candidate packaging and bootstrap. Provisioning is disabled for this deployment
  because the user requested no new hosts.
- Explicit offline migration of the legacy 29-record-per-row journal to 33-record
  packing, preserving record bytes and block entries. Serving artifacts are rebuilt.

## Validation evidence

| Scope | Evidence and result |
|---|---|
| Current server regression | [94 passing tests](../evidence/architecture-v4-ci-selection-2026-09-23/README.md), including actual CLI canonical-RPC recovery; two full-size cases are intentionally excluded from that bounded selection. |
| Current full-size correctness | [Two passing full-size HTTP cases](../evidence/architecture-v4-worker-disk-2026-09-23/README.md): consolidation/retry and loan/return with retained queries and reorgs. |
| Record and note behavior | [Canonical record oracle](../evidence/architecture-v4-canonical-record-2026-09-23/) and [authenticated note recovery](../evidence/architecture-v4-note-recovery-2026-09-23/). These do not claim downstream application conformance. |
| Clean release artifact | [Full-LTO Linux build and bundle verification](../evidence/architecture-v4-clean-candidate-2026-09-23/README.md); 1,176 exact emulated load answers, zero errors, three publications. Local provenance, not remote CI. |
| Production migration and serving | [Direct production evidence](../evidence/architecture-v4-production-2026-09-23/README.md): 565,944 migrated records with unchanged SHA-256; live RPC catch-up and replicated publication. |
| Native production load | Public HTTPS, private concurrency 1/2 and open-loop 2 QPS: 1,870 exact measured answers, zero errors. These use the current live dataset, not maximum placement. |
| Native recovery | One-replica-offline and coordinator restart: 329 additional exact measured answers, zero errors. |
| Operational tools | 115 tests passed with three environment-specific skips; 17 CI-tool tests passed. Journal migration, source locks, record continuity and explicit-port sampling have targeted tests. |
| Sustained capacity | Active-profile six-hour campaign and one-second worker samples are running. No passing result yet; sealed-role, full-load and final restoration remain. |

The current Rust runtime sources are byte-identical to the clean deployed
candidate; the production evidence records that comparison. Deployment fixes so
far concern journal migration and sampling, not cryptographic runtime behavior.
Raw historical evidence remains under `enhance/evidence/architecture-v4-*`.
A result applies only to the source and binary hashes recorded with it.

## Remaining execution plan

1. Finish the full-size active campaign on the actual 8 GiB workers: at least
   six hours and 300 publications, including loan/return, growth, rewind, exact
   current/retained queries and concurrent off-host traffic.
2. Exercise six-sealed placement, delayed reclamation, replica recovery and
   rollback on existing resources. Any helper processes on the coordinator must
   be identified as test support; they do not count as qualified 8 GiB workers.
3. Review cgroup peaks/events, swap, PSI, process RSS/kernel accounting, disk
   growth and query/publication latencies. Preserve the 512 MiB resident guard;
   tune the overhead/admission model only from measured evidence and rerun
   affected campaigns after functional changes.
4. Run offered-load/overload characterization on the full-size assignments.
   Record rejected/unstarted work and scheduled latency; an allowed-error
   characterization run is not a zero-error acceptance result.
5. Restore canonical worker and coordinator state, verify live source anchors,
   public queries and both replicas, and rehearse rollback to the preserved
   legacy installation. Record final service status and remaining limitations.
6. Clean and merge the tested implementation into `main`, preserving artifact
   provenance. CI may be skipped as explicitly requested; local and production
   validation evidence must remain available. Commit subsequent test-driven fixes.

## Interpretation and limits

The architecture permits exceptional reorgs to relocate into qualified capacity
**or block publication**. An exhaustive placement solver is not required.
Automatic peer transfer is optional; safe offline row repair and its deployed
rehearsal remain part of recovery validation.

The 32 GiB initial worker free-space floor is based on sampled full-size worker
footprints (14.392 GiB peak), with additional native disk monitoring required.
The separate 64 GiB recommendation applies to the local multi-worker test host.

Capacity policy is not a hardware certificate. Neither a successful bootstrap,
short smoke test, green unit suite nor an assessor exit code proves sustained
qualification or downstream wallet conformance. Track these claims separately.
