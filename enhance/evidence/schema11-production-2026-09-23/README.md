# Schema-11 production cutover — September 23, 2026

Production serves revision `2b76bcdfadebd564ffdcad0e460901ac0cd91bcc`, schema 11,
`ironwood-enhance-pir-v5`, 653-byte records and 33 records per row. The configured
policy remains six sealed shards, five total in active/lending groups. Seven
sealed is implemented but has not passed the native sustained hardware gate.

The user authorized committing to main and direct SSH production deployment.
No hosts were created. Existing Caddy, DNS, public routes and binary/service names
remain in use. Workers use private TCP 8291; the DigitalOcean firewall permits it
only from the coordinator tag. That rule is mirrored in Terraform; no Terraform
apply was performed. No private SSH key was copied to a host.

## Build and data provenance

The release was built natively on the coordinator with Rust 1.91, four build jobs,
`CFLAGS=-mpclmul`, `CXXFLAGS=-mpclmul`, and
`RUSTFLAGS="-Dwarnings -C target-cpu=x86-64-v3"`. The native source snapshot's 527
files were checked against [fingerprints](native-build-inputs.json). Only the
production deployment document and firewall HCL changed between that snapshot
and the committed source; compiled inputs are identical. The archive was
assembled from a clean checkout, extracted with the repository verifier, and
its SHA256SUMS verified on every host. See [release identity](release-identity.json)
and [native build result](native-build.txt).

The new canonical journal was rebuilt from the local trusted RPC into
`/srv/enhance-pir-v11/canonical`, starting at activation. Workers built fresh
schema-11 rows and artifacts in `/srv/enhance-pir-v11/worker`. No 737-byte journal,
publication, worker state or artifact was reinterpreted or migrated.

The old schema-10 sealed-retry campaign was interrupted before six hours. Its
supervisor restored canonical serving and passed its four validation cases before
this cutover. Its evidence and cached data remain historical and unqualified.

## Verification

| Check | Result |
|---|---|
| Private pre-cutover exact queries | 416/416, zero errors, p99 329.727 ms |
| Public post-cutover exact queries | 468/468, zero errors, p99 154.367 ms |
| Public test coverage | 580,762 records; anchor 3,493,574 |
| Replicas after restart | Two ready; no blocked operation |
| Services | Enabled and running; no automatic restarts at collection |
| Worker cgroup peak after service switch | 1,354,403,840 / 1,334,411,264 bytes |
| Worker memory.high/max/OOM events | Zero on both workers |

Both query tests run for 30 measured seconds at concurrency two, after warmup.
The generator runs on the coordinator; the public case uses the real HTTPS
endpoint but is not a geographically remote latency test. These are smoke tests,
not six-hour qualification or dense seven-shard residency measurements. Worker
peaks cover the restarted production service; the earlier shadow preparation
sample peaked at 1,938,575,360 bytes on worker 1. Memory is workload-dependent.

[Private report](shadow/load.json) and [public report](public/load.json) retain
warmup and measured errors. The oracle reads nine positions from the fresh
653-byte journal, including 32/33, 270335/270336, 540671/540672 and the final
covered record. This verifies PIR against ingestion output; it is not independent
transaction extraction. Independent canonical extraction, incoming/outgoing note
authentication, malformed-format rejection, restart/reorg behavior and pinned
wallet batch application passed in the
[implementation evidence](../schema11-placement-2026-09-23/README.md).

Reproduce the smoke check on the coordinator with a new output label:

```sh
python3 validate.py followup https://enhance-pir.valargroup.dev /opt/enhance-pir-v4/releases/2b76bcdfadebd564ffdcad0e460901ac0cd91bcc/enhance-pir-load-test
```

The script refuses an existing output directory. Full GitHub CI was still queued
at deployment; no claim of a completed full CI run is made. Local affected tests,
Clippy and documentation/operations checks passed before cutover.

## Cutover and rollback

The existing services were stopped, their units backed up, and restarted with the
new release/data paths. This exercised recovery from the persisted shadow
publication. Both replicas and the public manifest recovered successfully. The
temporary shadow units were removed and the production lock released.

Original units are preserved on each host under
`/var/backups/enhance-pir-schema11/2b76bcdfadebd564ffdcad0e460901ac0cd91bcc/`.
Original data under `/srv/enhance-pir-v4` and revision
`b1863b1d270df52d213d7dd389e5b8bf96bca224` remain intact. To roll back, acquire the
production lock, stop the new coordinator, stop both workers, restore the saved
`enhance-pir-v4-worker.service` units on both workers and the saved
`enhance-pir-v4-coordinator.service` on the coordinator, and daemon-reload.
Start workers first, then the coordinator; verify the original protocol and exact
queries before releasing the lock. The saved units restore worker port 8091 and
original data paths. Never run an old binary against schema-11 data.

Current worker free disk is about 15–16 GB with all historical caches retained.
That is sufficient for the present publication, not evidence of capacity for a
future seven-shard qualification fixture. Plan its disk budget separately.
