# Plan 1: isolate Status packing preparation

## Objective and gates

Remove competition between live generation preparation and admitted query response
packing. Preserve the 20-second freshness deadline, authenticated private topology,
immutable generations, durable commit ordering, and fail-closed recovery. Public
Status remains disabled until the existing availability and qualification gates pass.

## Implementation sequence

1. Measure the upstream packing artifact geometry before choosing placement. Compact
   v2 has three CRS blocks; upstream documents approximately 100.6 MB of gadget digits
   per block. Moving preparation requires fetching 96 MiB of hints to the coordinator
   and transferring approximately 288 MiB of prepared matrices back per full update.
   Sparse row updates do not imply sparse changes to NTT packing matrices.
2. Immediate mitigation: run router preparation on a dedicated, bounded Rayon pool,
   separate from the global query packing pool. `STATUS_PREPARATION_THREADS`
   defaults to 1 and accepts only 1–4; online `RAYON_NUM_THREADS` remains independent.
   Keep preparation serial at the
   generation level; retain unchanged-block reuse. Measure preparation wall time and
   source age under load. Bound CPU usage at the Status service level so preparation
   cannot consume every core needed by queries.
3. Coordinator placement is a follow-up conditional on measured transfer and import
   time fitting the same freshness budget. Its artifact must be versioned, fixed
   shape, little endian, canonical, and bound to protocol/revision, geometry,
   network/setup, original manifest, hint digest, and complete artifact digest. Import
   rejects wrong shape, noncanonical residues, truncation, trailing bytes, and replay.
   Publication must retain prepare/commit/activate ordering. Never extend deadlines
   or relabel observations to make a slow transfer pass.
4. Test dedicated-pool execution and concurrent isolation; run full encrypted
   distributed validation, then production private 20-QPS live updates. Compare
   router preparation/packing durations, rejection origin, latency and source age.

## Trust and rollout

The coordinator already controls all source rows and publication authority; moving
public preprocessing there would not add a source trust assumption. SSH remains the
transport authentication boundary. This immediate change introduces no new wire
format, cryptographic primitive, or trusted setup. Roll back the binary if the
independent preparation pool increases publication age beyond the existing gate.
