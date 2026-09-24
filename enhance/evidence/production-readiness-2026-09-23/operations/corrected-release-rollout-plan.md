# Corrected v6 release rollout and repeat qualification

Status: prepared, **not dispatched**. The six-hour public load still runs on
revision `527048217f8cb83d27c838e94dbaac21ac25837d`. Do not change a
serving binary, sampler, worker policy, or node workload until its five-minute
burst has finished and the parent `qualify-public.py` process is terminal.

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

1. Verify the load parent is terminal and preserve its immutable stage reports,
   oracle identity, full coordinator freshness trace, and both complete worker
   traces. Assess the full measured stages, burst, resource windows and
   five-minute freshness tail. Keep the same-height reorg finding visible.
2. Read-only node RPC: verify the final canonical hash at height 3,494,062,
   the disputed height. Run the chain oracle on a published anchor, then run
   the pinned wallet client through public HTTPS from the external client host.
   Any wrong answer is a stop condition. Preserve the oracle and public-client
   reports before the fleet switch.
3. On each host, stage the checksum-verified corrected release in a new
   `/opt/enhance-pir-v6/releases/<full revision>/` directory. Verify the
   binary's `--help` on the coordinator CPU and its exact SHA-256 everywhere.
   Do not overwrite a prior release.
4. Switch worker 01, verify its private health and exact `ExecStart`, then
   switch worker 02 with the same checks. Require the coordinator to report
   two published replicas and no ingestion or publication error after each
   switch. Restart the coordinator on the corrected release last; verify its
   public health, advancing anchor, two replicas, APM reachability and one
   chain-derived exact public answer. Roll back the new drop-in on any host
   that fails, and stop the rollout if correct serving is not restored.
5. Save the old observation traces before changing policies. Stage the current
   `sample-loop.py` and `sample-worker.py` together on both workers, and the
   current `observe-freshness.py` on the coordinator. Use **new** root-only
   direct policies bound to revision `216b999`, its server binary and manifest
   SHA-256, each worker's private address and the unchanged effective cgroup
   limits. Keep the old policy file for rollback. Start new one-second worker
   samplers and a ten-second freshness observer in new output directories for
   at least 12 hours; verify successful first samples and service supervision
   before starting any load.
6. Generate a fresh chain-derived oracle from a published anchor and run the
   full 30-minute 1/2/4 QPS stages, six-hour 4 QPS soak and five-minute burst
   from the external client with the corrected load-driver binary. The load
   report, sampler policies, fleet binaries and node oracle must identify this
   single candidate. Assess full immutable worker traces and the complete
   freshness window, including a five-minute tail. Preserve all raw failures.

The corrected deployment and second run still do not replace the separate
active/six-sealed physical campaigns, release-level wallet recovery, numerical
review, fault/alert rehearsal, or 24-hour opt-in observation. Do not admit
wallets until every gate is explicitly reviewed and passed.
