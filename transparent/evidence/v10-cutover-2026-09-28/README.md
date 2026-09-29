# Schema v10 production cutover, 2026-09-28

V10 was deployed directly over SSH at the user's explicit request, without
waiting for long-running CI. Activation and public verification completed at
**22:17:04 UTC**. Runtime source is
`8e69ea75b1e0071e3b978b0c78cc0487377f9e82`; the three operations overlays are from
`4905d3a30f63b096e514ab6851a95d4b9b0e80e0`. Exact executable hashes, build flags,
operator helpers, commands and failed attempts are retained in this bundle.

## Capacity and publication

The [full publication](publication/README.md) contains the same 353,795,624 events
through height 3,499,341 as v9: **86 shards instead of 139**, and
**31,457,280,000 instead of 51,388,612,608 allocated table bytes**. This is
**38.79% less space / 63.36% more history capacity** at equal allocated plaintext
table storage for that history. It excludes journal, public metadata, prepared
caches and replicas; it is not measured wallet throughput or a retention promise.
Every emitted shard matches the census. Every manifest, table and filter was
verified, and eight selected shards rebuilt byte for byte.

All 172 actual-table native correctness certificates pass the configured policy.
Seven archive page segments are below 128 bits, with minimum 96, above the
previously accepted 83-bit archive-page floor. These are conditional analytic
decryption-failure bounds, not lattice security levels. Publication took
80 min 23 s and peaked at 4.45 GiB RSS. This is not a controlled paired CPU
comparison with the previous v9 build.

## Deployment and recovery checks

All six workers were privately prewarmed while v9 served. The four recent replicas
finished in 151–186 s; archive-01 in 1,232 s. Archive-02's first one-thread attempt
was deliberately stopped because its projected finish exceeded the private
deadline. The retry used three low-priority threads, restored 41 saved runtimes,
built the remaining 35, and completed in 567 s. Both attempts are preserved.
No prewarm build or cache-write failure occurred.

The first cutover attempt, 21:59:03–21:59:18, failed while copying ephemeral SSH
Unix sockets into the rollback state. It changed no worker binary and restarted
the v9 publisher automatically. The corrected helper excludes sockets and copies
durable files; a local Unix-socket/file copy check passed. The incomplete backups
remain in separate `attempt1` locations on the hosts.

The successful maintenance interval ran from stopping the old publisher at
**22:01:18** to completed public verification at **22:17:04** (15 min 46 s).
The shard origin began exposing the static v10 map before the filter origin
returned; this interval is not continuous full service. The fixed fleet was
ready at 22:07:13; continuous-publication controls and shadow verification
completed at 22:16:52; activation completed at 22:17:03.

[Public verification](deployment/public-check.json) checked both origins, all six
warm workers with binary SHA-256
`0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b`,
85 unchanged sealed entries, all 170 corresponding public setup digests, and
canonical public height 3,499,627 against the local node. The two initial tail
certificates passed during publication; the moving recent tail is covered by the
geometry's data-independent 128-bit correctness bound. The publisher continued
advancing after activation. The post-load check repeated these checks at canonical
height 3,499,638; public operator routes remained 404.
The [completion check](deployment/completion-status.json) at 22:43:27 UTC found
all six workers warm, both origins identical and the publisher serving height
3,499,641. The four coordinator services had no automatic restarts. Temporary
monitors were stopped and the v9 rollback root remained present.

The [public regression](regression/report.json) passed **11/11 cases and 68/68
checkpoints**. It issued **2,319 logical HTTP requests and 2,319 attempts**, with
zero failed requests, failed attempts or recovered retries. The v10 frozen
fixture was installed only after this pass. The 22 SQLite databases remain on
the bench host; [their checksums and paths](load/sqlite-artifacts.json) are retained
without committing 464 MiB of largely repeated cached public artifacts.

The [one-client smoke](load/smoke.json) completed **124/124 exact synthetic range
syncs**, zero failures. The [four-client, nine-class bounded run](load/README.md) finished with **47 attempts,
46 completed and exact, zero failed, and one query-budget incomplete**, in
710.182 s. Eight classes used less logical payload than v9; one-day catch-up
used 17.96% more. Minimum sampled available memory was 73.84%, maximum reported
freshness 23.249 s, with no OOM, automatic restarts or cache-write failures. One
independent init probe saw a transient 503 during publication and recovered at
the next five-second sample; both map endpoints stayed consistent. The
harness counts unresolved-spends-only synthetic ranges as covered while the wallet
still withholds its anchor; the independent regression verifies actual ledger
and checkpoint expectations.

## Rollback and limits

The unchanged version-2 journal is `/srv/zakura/transparent-event-data-v2`.
The new initial set lives physically at
`/srv/transparent-data-v10/publications/initial`. The logical publication root
`/srv/zakura/transparent-publications` points to its parent. An actual systemd
`ProtectSystem=strict` hard-link probe passed, preserving sealed-byte sharing
between revisions. Every relocated file was hashed and fsynced first. The final
sealed-file check found five hard links; publication and journal filesystems
retained 31.94% and 22.77% available disk space respectively.

V9 is retained at `/srv/zakura/transparent-publications-v9-8e69ea75` and
`/opt/transparent-publisher/state-v9-8e69ea75`, with coordinator rollback material
under `/opt/transparent-v10-8e69ea75/rollback-v9`. Worker v9 publication roots,
active records, binaries and units are retained separately. V10 uses
`/srv/transparent-pir/runtime-cache-v10`; the former default cache remains for v9.
[The rollback helper](deployment/rollback-v9.py) describes the restore sequence.
A post-cutover rollback was not exercised; saved material alone is not a complete
recovery rehearsal. V7 rollback material had already been retired before this work.

Variable parsing, local outpoint context and byte-aware packing add CPU and
validation complexity. Greedy placement can miss a feasible packing, and savings
depend on the event mix. Schema v9 clients fail closed on v10. This work qualifies
the wallet-pir reference client and SQLite store; downstream application upgrade
and lifecycle qualification are separate. A sustained capacity envelope, a
controlled paired CPU/latency study and broader recovery exercises remain open.

[SHA256SUMS](SHA256SUMS) covers every retained file in this bundle except itself,
including the compressed raw observation traces and nested checksum files.
