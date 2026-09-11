# Publication I/O canary — 2026-09-11

The [source correction](../productionize-m1-publication-io-2026-09-10/README.md)
`043c051` passed full local checks and Linux regressions. Three runs on the
existing Amsterdam generator passed before deployment: 174 exact queries,
maximum worker visibility 8.502 seconds against the unchanged 14-second
qualification budget, minimum modeled host headroom 25.694%, no OOM events.
Raw reports and external-client evidence are in the
[qualification archive](m1-publication-io-qualification.tar.gz). These isolated
runs do not establish sustained live-host acceptance.

The routing audit was installed under both existing public maintenance guards,
then both origins were verified canonical and reopened. No timeout or acceptance
threshold was relaxed. The initial durable epoch was created through actual
routing, not a synthesized acceptance record. The deployment script is adjacent.

The supervised canary upgrade then installed recent-01 worker binary
`6b97509b20dc21e67e39512676dbc896963411b00a7df7e76727d1305a1d705d`.
Its warm/private-query verification passed and public service reopened. The
other five worker binaries were not promoted. The matching fleet script digest
is `12612e9d2e75bf322f90fc9c32f70f7c283c51fbe4016d851a9a35fb64b9d339`;
configuration `9ac6e0eae0a98feb48645698520981a8bf8862fccd92ae9d293e22866bcdffc2`;
roster `f81674cd250e596aead154e579ab24d5c9b3367f088823c94cf0444112ae4db9`.
The unchanged shard-control was rebuilt from this source and has SHA-256
`c46543dd79ed66dfff72fd846b852fb84961532f97809068c200cd3a022a47dd`.

The new loaded gate started **2026-09-11 00:01:23 UTC**, node height 3,479,059,
under `transparent-m1-publication-io-rollout.service`, PID 2594122. Its output
is `/opt/transparent-publisher-build/publication-io-20260910/rollout` on the
coordinator. The [00:02:55 capture](m1-publication-io-start.tar.gz) proves that
supervisor was active in `canary_observation`, with the selected warm binary.
The routing audit baseline had epoch `b1a6acc302ca4bf88b6532837e3c5435`, counter
3 and availability true; the counter was unchanged at capture. Those initial
withdrawals preceded the acceptance baseline and include guarded deployment.

A separate six-hour, two-thread probe runs on recent-01 as
`transparent-m1-publication-io-probe.service` (observed PID 679661, worker
PID 679049). It reads local Unix status and HTTP readiness every 0.5 seconds,
recording individual request durations in `/run/tp-publication-io-probe/`.
It does not inspect every thread or run BPF; its source is adjacent. These
small diagnostic requests and tmpfs logs coexist with measured load and do not
replace the observer or establish continuous availability between samples.

**This is deployment and start evidence, not a passing M1 gate.** Both six hours
and 300 new blocks must pass before the supervised full-fleet batch and separate
24-hour all-worker observation. Earlier failed and invalidated samples do not
count. The earlier multi-second timeout has not yet been conclusively explained;
this gate tests whether the confirmed publication-I/O fix resolves its recurrence.
