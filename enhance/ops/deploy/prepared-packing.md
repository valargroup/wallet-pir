# Coordinator-prepared packing artifacts

The coordinator prepares the collapse matrices, decomposition digits, top key
images and automorphism tables once per public packing material. The packing
router downloads, verifies and decodes that state on a dedicated loading thread.
Public matrices are read directly from immutable read-only mappings; compact
permutation tables remain heap-owned. It does not call packing preprocessing.
Worker evaluation and the wallet wire
protocol are unchanged.

The private serving control version is 2. Coordinator, ingress and packing
router must be upgraded together. Workers do not need a restart for this change.
Preserve current placement settings, including any GPU pool policy, when changing
the three service executables.

## Artifact contract

`<control>/prepared-packing-v1` contains `.bin` artifacts and `.bin.json`
descriptors. Names bind the format domain, canonical parameter identity and
public packing digest. Descriptors carry format version, exact byte length and
SHA-256. Snapshot metadata retains these descriptors alongside the original
raw hints. Writes use temporary files, fsync and rename before publication.

The outer encoding is a little-endian canonical row count, the fixed-length
public packing bytes, and the `inspiring::prepared` version-1 stream. The stream
contains a parameter header, collapse/decomposition matrices, top key images and
automorphism permutations. Dimensions are derived from supported parameters;
wire values never determine allocation sizes. Decoding rejects incompatible
parameters, noncanonical residues, invalid permutations, truncation and trailing
bytes. The codec is pinned to ipir-sp commit
`66b05ac59139897674489a5ea6461118772ee1c0`. The mapping update reads the same
v1 encoding; existing prepared artifacts do not need rebuilding.

These are public, database-derived values, not client keys or requests. The
coordinator and its private control network are trusted. A digest detects
corruption and binds the downloaded bytes to the control descriptor; it is not
an independent authentication mechanism against a compromised coordinator.
Existing private-network access restrictions remain required.

The router reserves preparation memory before download, serializes loads, checks
the full digest before decoding, and checks the resulting session reference.
Failed loads preserve the active view. Its disk cache retains active and proposed
view artifacts; coordinator retention follows retained snapshots. Raw hints stay
available for rollback.

## Migration and rollout

1. Qualify an isolated router with `QUALIFY_STEADY=1`,
   `QUALIFY_PUBLICATIONS=30`, and `QUALIFY_RECORDS=600000` in the `packing_http`
   integration test. Supply the external router's control/query URLs and the
   fixture artifact listener via the `QUALIFY_ROUTER_CONTROL`,
   `QUALIFY_ROUTER_QUERY` and `QUALIFY_ARTIFACT_BIND` environment variables.
   Use the production 7-GiB memory limit, zero swap, two router CPU cores,
   six-object limit and four-request admission limit.
2. Rehearse migration on a copy of the controller and all retained snapshot,
   hint and public files. Check unchanged manifests, sessions, routes, recovery
   state and domain keys. Opening the controller store advances its fencing
   epoch; this is expected. A second migration should reuse valid artifacts.
3. Build using Rust 1.91 and `-C target-cpu=skylake-avx512`. Stage immutable,
   checksummed release binaries. Hold `/run/lock/wallet-pir-production.lock`
   throughout cutover. Save the latest service definitions and Caddy config.
4. Pause public query admission and review load; let pending publication settle.
   Stop coordinator, ingress and router. Back up the stopped control directory.
   Run the new server's `prepare-packing-artifacts --data-dir
   /srv/enhance-pir-v7/canonical/control`. This argument is the **control
   directory**, not the canonical data root. The command takes the sole
   controller lock and refuses an unresolved operation.
5. Replace only service executable paths, preserving all arguments and limits.
   Start router and ingress, then coordinator. Wait for both serving roles to
   become ready, current required worker replicas and zero coordinator resident
   packing objects before restoring Caddy query admission.
6. Verify exact wallet answers, publication progress, new metrics and resource
   limits under 2 queries/s and 1 init/s for at least 30 minutes and ten new
   material publications. Require packing p99 below one second in every complete
   five-minute window, zero unexpected errors, zero incorrect answers and zero
   OOM events. Record measured resident assignments; the configured limit alone
   is not evidence of reaching that limit.

Rollback stops query admission and all three serving roles, restores their
previous executable definitions together and restarts them against current
control state and retained raw hints. Keep all publication and recovery history;
never restore an old control backup after publication resumes. This rollback
does not change placement architecture or worker binaries.

## Operational checks

`enhance_packing_preparations_total` must remain zero on the packing router.
The coordinator exposes that counter and preparation duration. The router also
exposes artifact download bytes/time, load count/time, disk cache hits and load
failures under `enhance_packing_router_artifact_*`.

The existing query-stage definitions are unchanged: packing covers response
packing after worker evaluation, worker covers matrix-vector multiplication,
and total excludes request-body reading and response transmission.


## Mapped-reader update

The mapping update preserves control version 2 and the artifact format. When
upgrading from the first prepared-artifact release, only the packing router
needs restarting; coordinator backfill is unnecessary. Preserve the existing
four-request admission setting and the host/service memory limits.

The router verifies the exact length and SHA-256 of the same file descriptor it
maps, marks that inode read-only, and validates parameters, canonical residues,
permutations and prefix/session binding. Every cache writer uses a new temporary
inode followed by rename; GC only unlinks. Operators must never truncate or edit
a mapped artifact inode in place. Root and already-open writable descriptors
are outside this trusted-filesystem contract.

A persistent loading thread reuses its allocator arena. Queries retain only
their selected packing object, not the entire retained view. Packing's existing
fused arithmetic reads digit slices directly from the mapping and reuses small,
bounded, cleared scratch buffers on each Rayon thread. Retired mappings are
released when their final in-flight query reference disappears.

The router derives its budget from its cgroup limit with 1,536 MiB reserved for
requests/runtime. Resident working-set charges remain 768 MiB per object;
mapped loading adds 128 MiB temporarily, while coordinator preprocessing keeps
its separate 1,024-MiB temporary charge. Six resident objects plus a replacement
fit the seven-GiB service budget. Before loading, physical headroom is also
checked, excluding clean inactive file pages that the kernel can reclaim.
Insufficient headroom defers publication loading; query concurrency is unchanged.

Qualification must measure anonymous versus file-backed memory, allocator live
and free bytes, publication progress and packing latency with concurrent queries.
Do not infer physical safety from the logical charge or object count alone.
