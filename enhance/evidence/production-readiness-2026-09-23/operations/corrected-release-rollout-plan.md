# Corrected v6 release rollout and repeat qualification

Status: corrected release serving on the coordinator and both original workers;
fresh observation and the full public load are running. Four isolated c-4
campaign workers are provisioned, idle and smoke checked under a separate
Terraform state key.
The superseded public load on revision
`527048217f8cb83d27c838e94dbaac21ac25837d` was deliberately stopped after
about 4½ hours of soak; its parent process is terminal, its samplers are
stopped, and its traces have been copied and assessed. The old run has no
six-hour soak report or burst. Its post-load chain-derived public check passed
16 exact answers. Full CI passed on the corrected server source before the
serving switch, and the fleet passed a fresh chain-derived 10/10 public check.

## Artifact and invariants

- Corrected source revision: `216b9993cc3ae4e0f1820d60c6b8f03d104b5e46`.
  The [build record](release-build-216b999.json) pins all file and archive
  checksums. The server binary SHA-256 is
  `7be19ca82108c2046d571ce6b901dd348d2e43435510a74257d530a49b309fe1`.
- Operator archive:
  `/tmp/wallet-pir-readiness/release-216b999.tar.gz`, SHA-256
  `4c24c30c0adbd19779671ee731adc09bad3ea77b941ca51fb01e235bcd26a3ac`.
  Extract under a new release directory named by the **full** revision on all
  three existing hosts; verify `SHA256SUMS` there. The top directory and server
  binary must be executable by worker service user `enhance-pir-v4`.
- Keep the old release, `50-candidate-5270482.conf` drop-ins, canonical data,
  worker inventories, and disabled cleanup timers intact. Install a new
  `60-candidate-216b999.conf` per host; removing only this new drop-in restores
  the previous effective `ExecStart`. Do not change the worker memory limits,
  private listen addresses, port 8091, data directory, or write sandbox.

## After the current load

1. Completed: the load parent is terminal. Its three completed stage reports,
   oracle identity, partial soak manifest, full coordinator freshness trace
   through a five-minute tail, and both complete worker traces were preserved.
   The same-height reorg finding remains visible. The planned burst did not run.
2. Completed: read-only node RPC verified the final canonical hash at disputed
   height 3,494,062. The chain oracle extracted records from a published
   anchor, and the pinned wallet client queried them through public HTTPS from
   the external client host. Its first attempt rejected a moved generation; a
   fresh extraction passed 16/16 exact answers. The oracle and client reports
   were preserved.
3. Completed: each host has the checksum-verified corrected release in a new
   `/opt/enhance-pir-v6/releases/<full revision>/` directory. The corrected
   binary's `--help` and all archive file hashes passed on all three production
   hosts. The prior release was retained.
4. Completed: switched worker 01, verified its private health and exact `ExecStart`, then
   switch worker 02 with the same checks. Require the coordinator to report
   two published replicas and no ingestion or publication error after each
   switch. Restart the coordinator on the corrected release last; verify its
   public health, advancing anchor, two replicas, APM reachability and a
   chain-derived 10/10 exact-answer smoke check. Roll back the new drop-in on any host
   that fails, and stop the rollout if correct serving is not restored.
5. Started: saved the old observation traces before changing policies. Staged the current
   `sample-loop.py` and `sample-worker.py` together on both workers, and the
   current `observe-freshness.py` on the coordinator. Use **new** root-only
   direct policies bound to revision `216b999`, its server binary and manifest
   SHA-256, each worker's private address and the unchanged effective cgroup
   limits. Keep the old policy file for rollback. Start new one-second worker
   samplers and a ten-second freshness observer in new output directories for
   at least 12 hours; successful first samples and service supervision were
   verified before starting the full load.
6. Running: generated a fresh chain-derived oracle from a published anchor and started the
   full 30-minute 1/2/4 QPS stages, six-hour 4 QPS soak and five-minute burst
   from the external client with the corrected load-driver binary. The load
   report, sampler policies, fleet binaries and node oracle must identify this
   single candidate. Assess full immutable worker traces and the complete
   freshness window, including a five-minute tail. Preserve all raw failures.

The corrected deployment and second run still do not replace the separate
active/six-sealed physical campaigns, release-level wallet recovery, numerical
review, fault/alert rehearsal, or 24-hour opt-in observation. Do not admit
wallets until every gate is explicitly reviewed and passed.
