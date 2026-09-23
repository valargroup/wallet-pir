# Protocol-v6 production SSH cutover — September 23, 2026

The user authorized a direct SSH deployment, bypassing CI, and confirmed that
v6/q48 wallet clients were ready. The deployed source is commit
`afdb4b6d70d6947fa60688a3cf4df21fe5eeae69`, schema 11, protocol
`ironwood-enhance-pir-v6`. A later `main` commit pinned `ipir-sp` rc.2; this
deployment predates that dependency change. This is a deployment record, not a
hardware qualification certificate.

## Build and installation

The exact commit was archived locally and built natively on the existing
production coordinator with Rust 1.91, four build jobs, `CFLAGS=-mpclmul`,
`CXXFLAGS=-mpclmul`, and `RUSTFLAGS="-Dwarnings -C target-cpu=x86-64-v3"`.
All 2,295 archived files matched [the source hash manifest](source-sha256.txt)
before release assembly. The flat release archive SHA-256 was
`cafdc330f166c17aa3213978bb1f18067cfe0cf6c55c1c609abac74318ea0996`.
The repository's release extractor accepted it; its [candidate metadata](candidate.json)
and [file checksums](release-SHA256SUMS.txt) were verified on the coordinator and both
existing workers. No new hosts or Terraform changes were made.

V6 started on separate worker port 8091 and coordinator loopback port 8082.
The schema-11 canonical journal has the same version-7, 653-byte, 33-record
layout as v5. To avoid a full RPC re-ingest, the v5 manifest was captured first,
then its append-only records copied to a fresh v6 journal directory. The copied
committed prefix was compared byte for byte against the source: 583,440 records
and 65,496 block entries. Controller, worker, hint, and cache state were built
fresh. Both shadow workers published generation 1 before public cutover.

The public coordinator switched under `/run/lock/enhance-production.lock` to
the unversioned `enhance-pir-coordinator.service`; Caddy continued to proxy
port 8080. Each worker then moved, one at a time, to the unversioned
`enhance-pir-worker.service`. [Final host state](host-state.json) records active
and enabled services, matching binary hashes, the public manifest, and free
disk after cleanup.

## Exact-answer checks

The oracle used nine journal records including positions 32/33, 270335/270336,
540671/540672, and the final covered record. It checks PIR against ingestion
output, not an independent chain extractor. Each run retains its manifest,
oracle, command, raw log, and JSON report.

| Run | Measured correct | Wrong | Request errors | p99 |
|---|---:|---:|---:|---:|
| [Shadow, two-way](shadow-20260923-1703/load.json) | 149 | 0 | 0 | 269.311 ms |
| [First public](public-20260923-1706/load.json) | 197 | 0 | 3 | 379.647 ms |
| [Public retry](public-retry-20260923-1708/load.json) | 238 | 0 | 0 | 153.599 ms |
| [Public during separate eight-way load](public-final-20260923-1711/load.json) | 118 | 0 | 133 | 206.719 ms |
| [Final public, uncontended](public-uncontended-20260923-1713/load.json) | 234 | 0 | 0 | 154.111 ms |

The first public run overlapped the next canonical publication and saw two 429s
and one 502. A separate five-minute eight-way private load started at 17:07:13
UTC and overlapped the later failed public check. Its [report](competing-c8-private-load.json)
recorded 5,298 correct answers, no wrong answers, and 14,013 HTTP 429 responses.
The coordinator admits two concurrent queries and each worker admits two
evaluations; excess requests are rejected immediately. This is a measured
capacity limit, and the no-error two-way results do not qualify sustained
high-concurrency service.

## Cleanup and rollback

The old v5 coordinator and workers are stopped and disabled. Their service
units, release `2b76bcdfadebd564ffdcad0e460901ac0cd91bcc`, and separate
`/srv/enhance-pir-v11` data remain on all three hosts for a 24-hour rollback
window, until at least September 24, 2026 at 17:05 UTC. The old binary must
never open v6 state. Schema-8/10 worker caches, older release builds, temporary
shadow units, and build staging were removed. Historical validation samples
cited by prior evidence remain on the hosts.

A one-time systemd timer on each host is scheduled for September 24 at 17:05
UTC. Its [guarded cleanup script](retention-cleanup.py), [service](retention-cleanup.service),
and [timer](retention-cleanup.timer) were copied verbatim into this record.
Each host's dry check passed and showed the same deadline. At execution, the
script requires the exact v6 binary hash, active current service, stopped old
service, and healthy local v6 protocol before deleting the v5 canonical or
worker state, release, and old unit. It preserves historical validation files.
If any check fails, the rollback copy remains and the failed service needs
operator review; the timer does not force deletion.

The cutover did not run full CI or the sustained hardware campaigns. Production
wallet operation, memory under longer load, and the two-query admission limit
still require observation before claiming qualification.
