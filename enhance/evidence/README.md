# Enhance PIR evidence

The [qualification page](../docs/qualification.md) states what the retained
runs support and what remains open. This index points to decision evidence;
the [catalog](catalog.md) lists every retained run. Results are bound to their
recorded source and binary hashes. Historical `v4` names identify the captured
release and must not be read as current source paths.

## Current protocol and release inputs

- [Optional CUDA worker validation on P4000](cuda-p4000-2026-09-24/README.md) — GPU integration correctness and composed-domain matrix benchmark; not fleet qualification.

- [V7 SSH deployment, wallet interop and focused validation](immutable-v7-2026-09-24/README.md) — functional checks passed; memory/swap and short-run p99 qualification gates remain open.

- [Q48 precision qualification](p16-q48-2026-09-23/README.md)
- [Schema-11/v5 wallet interoperability (q46 predecessor)](schema11-suffix-2026-09-23/README.md)
- [Canonical record oracle](architecture-v4-canonical-record-2026-09-23/README.md)
  and [authenticated note recovery](architecture-v4-note-recovery-2026-09-23/README.md)
- [Clean release candidate](architecture-v4-clean-candidate-2026-09-23/README.md)
  and [integrated regression](architecture-v4-integrated-regression-2026-09-23/README.md)

## Production and qualification

- [Packing-router memory limits under 4/7/8 GiB caps](packing-budget-2026-09-24/README.md)

- [Adaptive batching on isolated 8 GiB Linux workers](batching-linux-2026-09-24/README.md)
  and [earlier local batching comparison](batching-local-2026-09-24/README.md)
- [Isolated worker expansion at the forecast threshold](architecture-v4-live-threshold-2026-09-23/README.md)
- [Protocol-v6 direct SSH cutover and exact-answer checks](protocol-v6-production-2026-09-23/README.md)
- [Direct production deployment and active campaign](architecture-v4-production-2026-09-23/README.md)
- [Sealed cold-preparation failure and retry](architecture-v4-sealed-deadline-2026-09-23/README.md)
- [Full-size correctness and disk sizing](architecture-v4-worker-disk-2026-09-23/README.md)
- [Historical worker capacity](schema9-worker-capacity-2026-09-22/REPORT.md)
  and [public baseline](public-baseline-2026-09-14/README.md)

Keep production decisions, unique failures, qualification inputs, and their raw
provenance. The [cleanup ledger](../../docs/cleanup-2026-09-23.md) records
superseded diagnostic bundles removed after reference review. Retained raw files
are immutable; new corrections belong in a new note or run. Follow the
[shared evidence requirements](../../evidence/README.md) for future captures.
