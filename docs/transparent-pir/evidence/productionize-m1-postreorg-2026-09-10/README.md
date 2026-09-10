# M1 reorg-related withdrawal and fresh gate

Observed 2026-09-10. Worker source d8f5217 and operations eb116e9 are unchanged.
The preceding collection/forwarding canary failed at **22:46:54 UTC** with
HTTP 503 after 3,941.405 seconds, 51 blocks, 57,670 exact queries and 36 retries.
It is not accepted. No fleet promotion occurred. The complete terminal run is
preserved in failed-run.tar.gz, with its result extracted separately.

## Failure evidence

Local probes in worker-window.json have 40 samples per endpoint over the
failure window, maximum 5.10 ms Unix status and 7.95 ms HTTP readiness. The
previous multi-second worker pauses were not present in this interval.
Controller logs show rejection of a noncanonical target and invalidation
during preparation, followed by rebuilt coverage. The rejected candidate at
height 3,479,007 named hash
00000000003e2f17b69fe9db99c4d57a7e033da9fbe02b85d4a7f791e853ae27;
a read-only node query after failure returned
00000000002c70371a4a21fdcb9d9f8e67f69ffad62e2885a27102442010fd31
for that height. The last sampled worker tip at 3,479,006 remained canonical.
Both public origins recovered to identical height 3,479,008 and hash
00000000006a326f095f45ead14fd205592c94f81b1ecc1d88a14b86d4d7a8e9.

This supports reorg-related public withdrawal as the failure trigger, rather
than a simultaneous local worker status stall. It does not close the separate
scheduling-pause investigation. The monitor did not emit chain_reorganized
before its HTTP fetch failed, so its terminal classification remains HTTP 503.
No availability or freshness check was weakened to accept this run.

## Fresh full gate

After preserving the terminal run and checking recovered public coverage,
transparent-m1-collection-postreorg-rollout.service started a fresh observation
at **22:50:58.892 UTC**, node height 3,479,009. It reuses the verified installed
worker without a maintenance restart. At 22:51:23 UTC, MainPID 2521008 was active
in canary_observation. Output root:
/opt/transparent-publisher-build/collection-20260910/postreorg-rollout.

Worker binary SHA-256 remains
fee415d93ad15dbd18612777c9d05b065ed71f5214d1bb86aefe6bb9b3fec3c5.
The new run requires both six hours and 300 new blocks, then the gated fleet
upgrade and full 24-hour observation. Old samples do not count. M1 remains open.
