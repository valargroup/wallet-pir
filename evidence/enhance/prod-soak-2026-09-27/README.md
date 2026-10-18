# Production soak and fixes — 2026-09-27

Joint 20 QPS load against production Enhance (public HTTPS, 28-record oracle)
and Status (router query origin, canonical mined-answer oracle), with APM and
the external monitor sampled every 30 s. Each finding below was fixed on `main`
and deployed during the run unless marked open. Earlier runs (`b`–`f`) were
stopped or superseded as fixes landed; `g` (19:34–01:37 UTC, 36 ten-minute
batches) is the retained run. A clean follow-up run (`h`) started at 01:38.

## Soak g totals

| | Offered | Correct | Incorrect | Failed | Unstarted |
| --- | ---: | ---: | ---: | ---: | ---: |
| Enhance | 432,000 | 432,000 | 0 | 0 | 0 |
| Status | 432,000 | 424,810 | 0 | 6,756 | 434 |

Enhance per-batch p99 was 145–163 ms. Most Status failures came from eight
deliberate role restarts during deployments and one hypervisor pause (01:11).
The last batch, with every fix deployed, was 12,000/12,000 at p50 40 ms and
p99 78 ms. `raw/soak-g-batches.jsonl` has every batch.

## Soak h: all fixes, no changes during the run

Run `h` (2026-09-28 01:38–07:41 UTC, 36 batches) ran with every fix below
deployed and no deployments during the run.

| | Offered | Correct | Incorrect | Failed | Unstarted |
| --- | ---: | ---: | ---: | ---: | ---: |
| Enhance | 432,000 | 432,000 | 0 | 0 | 0 |
| Status | 432,000 | 431,772 | 0 | 228 | 0 |

Per-batch p99 was 129–163 ms for Enhance and 68–100 ms for Status. 35 of 36
batches were perfect on both products; all 228 Status failures are the
unattended-upgrade restart in batch 26 (finding 14).
`raw/soak-h-batches.jsonl` has every batch.

## Findings and fixes

1. **Status worker OOM on the 4-vCPU host.** The worker exceeded its 6 GiB
   limit after about 50 minutes (17:44, about 20 s outage) and the host CPU
   saturated at block arrivals. The droplet was resized to `s-8vcpu-16gb`,
   CPU/RAM only (`be67316e`); worker memory then stayed at 1.6–2.8 GiB.
2. **Preparation competed with queries.** Preparation threads now run at nice
   10 in their own pool (`148335c2`).
3. **Large copies and frees under the role locks.** The worker artifact and
   router public routes copied ~100 MB under the state lock, and replaced
   generations were freed under the state and views locks (`ef511fc1`,
   `cd3e9f02`).
4. **fsync on a runtime thread.** A thread dump during a freeze showed a
   Tokio worker in `D` state in `jbd2_log_wait_commit` persisting the fence.
   Fence persistence moved to the blocking pool (`04b56da9`): whole-process
   freezes went from 23 in 47 minutes to none in 36 minutes.
5. **fsync under the state lock.** Activation now persists its fence under a
   separate control mutex, so queries keep serving during the fsync
   (`852d6b5d`).
6. **Admission too tight for sub-second pauses.** Serving roles now queue 32
   requests for up to 1 s instead of 8 for 250 ms (`b7783e94`).
7. **Freshness margin.** Worker and router prepare serially, so worst-case
   staleness is about twice the pipeline time. At 11–12 s the 20 s limit was
   crossed (151 failures in one batch). Six preparation threads (`41795cf3`)
   reduced router source age to about 8–10 s. The margin remains narrow.
8. **APM paging on promotion.** Promotion paged "FIRED · unavailable" when
   input was unknown after a restart, and an incident that recovered in
   shadow never closed in Slack (`148335c2`; also deployed to `pir-monitor`).
9. **Enhance serial replica preparation.** Replicas prepared one after
   another; they now prepare concurrently, reducing a publication attempt from
   58–60 s to about 41 s (`92845ebc`).
10. **Enhance init 503 at activation.** Init now waits up to 2 s for packing
    routers to activate a new generation (`92845ebc`).
11. **Enhance publication metrics after restart.** The last-advancement time
    is restored from the newest snapshot, so a restart no longer raises
    `coverage_publication_metrics` (`208c75a9`).
12. **Tunnel recovery after a VM pause.** At 01:11:19 the droplet's
    clocksource watchdog marked the TSC unstable: the hypervisor paused the
    shared-vCPU VM, both tunnels timed out, and the droplet's sshd held the
    dead session's 8495 listener for 90 s. `status-control` sessions now use
    `ClientAliveInterval 2`/`ClientAliveCountMax 3` and the tunnels tolerate
    8 s (`852d6b5d`).
14. **Unattended upgrades restarted production services.** In run `h` at
    06:09:35 on 2026-09-28, `unattended-upgrades` installed `libc6` on the
    Status droplet and `needrestart` (Ubuntu mode, no restart mode configured,
    so automatic) restarted both roles, paging `status_init_5xx`. The
    coordinator, Enhance workers and router, GPU mirror and monitor were due
    for the same upgrade within 30 minutes. Every production host now has
    `$nrconf{restart} = 'l'` (list only), and the cloud-init templates install
    it. Upgrades still apply; services pick them up on their next deploy.
13. **jemalloc experiment, reverted.** Preloading jemalloc reduced freezes
    but slowed preparation, breached freshness and paged once (22:17). It was
    removed at 22:48.

## Open

- The shared-vCPU droplet can pause (finding 12). A dedicated-CPU droplet
  would remove this at additional cost.
- Status freshness margin (finding 7) and the gradual growth of router
  preparation time over the night are not explained.
- Enhance publication still has a sequential second phase after concurrent
  preparation; the optional GPU mirror is effectively part of the quorum
  because its failure fails publication.
- Status recovery epochs advance once per failed fencing attempt during a long
  role outage (13 to 84 during the resize).
- `status::source::tests::compact_wire_preserves_stsobs01_checkpoint_bytes`
  failed once in a full run and passed on rerun.

## Pages sent during testing

Real Slack pages caused by this work: the 17:44 OOM (`status_init_5xx`, whose
recovery happened in shadow and was never delivered before finding 8), the
16:05 and 17:51 promotion FIRED/RECOVERED pairs, the 18:12 role restart, the
22:17 jemalloc freshness breach, the 17:08 `publication_lag_warning`, and the
01:11 VM-pause outage (`coverage_status_*`, `status_init_5xx`,
`status_query_5xx`).

## Limits

One client host, one 28-record Enhance oracle and a mined-answer-only Status
oracle; no multi-client or deep-window Status answer checks. This is operating
evidence, not the six-hour qualification gate.
