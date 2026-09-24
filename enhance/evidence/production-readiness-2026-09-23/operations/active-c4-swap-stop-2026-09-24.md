# First temporary c-4 active campaign: swap stop

The first active campaign on temporary workers `enhance-pir-v4-g01-r1/r2`
began at 2026-09-24 03:51:12 UTC. Its measured phase began after the initial
build at about 03:59 UTC. Both physical workers ran corrected server revision
`216b9993cc3ae4e0f1820d60c6b8f03d104b5e46` in separate profile data
directories, with one-second direct sampling from before exercise start.
The coordinator exercise ran in its own data directory and listener, separate
from canonical serving. The public 1 QPS stage completed successfully during
this overlap.

The worker units had `MemoryHigh=7516192768`, `MemoryMax=7609516032`, and
`MemorySwapMax=2147479552`, matching the serving pair. The first nonzero
worker-cgroup swap appeared 1,105.374 seconds after sampler start on replica 1
(2,232,320 bytes), then at 1,266.446 seconds on replica 2 (81,387,520 bytes).
The maximum observed worker swap was 2,232,320 and 81,764,352 bytes. Both
events occurred during the measured phase. Neither worker had an OOM, restart
or sampling error. The first 1,435 one-second samples on each host were
preserved; the sampling manifests ended `interrupted` after the operator stop.

The exercise was deliberately stopped after 12 publications because the pilot
hardware gate requires zero worker swap. Its `exercise.json` still reads
`running` because the exercise does not finalize that report on SIGTERM;
it must be treated as failed operator-stopped evidence, never as a completed
campaign. The two worker services were then stopped. Their isolated data and
all raw traces remain intact on the temporary hosts. Local immutable copies
are under `/tmp/wallet-pir-readiness/active-failed-216b999/`; trace SHA-256
values are `edcb91f92bb1bcabfcfcc9889a84d431d4759ab82aa4f7b7046780674d42dc69`
and `7eb46bb46cff507c0caaf864e15537b806f14788712c8e77b2ef09f9145d3ab3`.
The partial publication trace SHA-256 is
`c80054862217452706b7c32f68585a416957000279ce4dfeaca0a23a8c9a4f2c`.

At first swap, replica 2 was near the 7 GiB soft memory limit with about
2.18 GB of inactive file cache and 5.19 GB anonymous memory. This supports
testing a no-swap cgroup policy on a fresh isolated run, but does not prove
it will pass or qualify the currently deployed production limits. The
production serving pair and public load were not restarted or reconfigured.
