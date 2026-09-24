# Serving worker no-swap alignment

Status: **planned, not applied**. At 2026-09-24 08:00 UTC, the two serving
workers still had effective `MemoryHigh=7516192768`,
`MemoryMax=7609516032`, and `MemorySwapMax=2147479552`. Their running binary
was the corrected release `216b9993cc3ae4e0f1820d60c6b8f03d104b5e46`.
The isolated no-swap active campaign is running; the sealed campaign has not
started. Do not use either as passing evidence before its measured six hours,
300 publications, and complete direct worker traces are assessed.

Only after the current public load has finished and its raw reports, freshness
trace, and both serving-worker traces are copied and verified, and after the
isolated no-swap hardware campaigns pass, align the serving pair as follows:

1. Preserve each worker's current unit, `50-candidate-5270482.conf` and
   `60-candidate-216b999.conf` drop-ins, current sampling policy, release
   binary, and worker state. Confirm the coordinator reports two published
   replicas, no ingestion error and no publication block.
2. On worker 01 only, add a new systemd drop-in
   `/etc/systemd/system/enhance-pir-worker.service.d/70-noswap-c4.conf` with
   `[Service]` and `MemorySwapMax=0`. Reload systemd and restart the worker.
   Verify the exact binary and private listen address, active service, cgroup
   `memory.swap.max=0` and `memory.swap.current=0`, private health, and restored
   two-replica coverage before touching worker 02. A restart invalidates its
   old one-second trace for a subsequent load window.
3. Apply the same new drop-in and checks to worker 02. If either step fails,
   remove only that host's `70-noswap-c4.conf`, reload and restart it, and
   verify correct serving and two replicas before deciding how to proceed.
   Do not roll both workers together.
4. Install new root-only direct sampling policies that name the effective
   zero-swap limit. Start new one-second worker samplers before a **new full**
   public HTTPS qualification, with a new coordinator freshness observer and
   external load output directory. Recheck checksum identity and APM after
   each worker roll. The earlier load at a 2 GB swap allowance is historical
   evidence and does not qualify the changed configuration.

The code template now sets the same zero-swap and hard memory limits, but
editing the repository does not change a running unit. The production rollout
and affected requalification remain open until the above checks and fresh
full-window reports are recorded.
