# Enhance v7 implementation and SSH deployment — September 24, 2026

Protocol v7 is deployed at https://enhance-pir.valargroup.dev with two ready worker
replicas. All three enabled systemd services run source `2a83c21` from
`/opt/enhance-pir-v7/releases/2a83c21`, server SHA-256 `3f79969da5900d249afacadafa13883989d9954b6a131f4007574a7ab0e41075`.
The canonical state is under `/srv/enhance-pir-v7`. The preserved v6 binaries,
state and service drop-ins remain available for rollback. No Terraform was run.

The implementation is merged into wallet-pir main. The compatible wallet is
[wallet-libraries PR #31](https://github.com/zakura-core/wallet-libraries/pull/31),
commit `b84852bc7`. CI waiting was skipped as requested. This deployment is
**not hardware-qualified**: the larger-dataset memory gate and short-run latency
comparison failed, and full six-hour qualification remains outstanding.

## Provenance and functional validation

`candidate.json` identifies the initial `6809403`/`c4e031d…` binary used for the
physical workload and most functional tests. `deployment.json` identifies the
final `2a83c21`/`3f79969…` binary. The only runtime change between those binaries
corrects health's placement revision field; query and publication behavior is
unchanged. Final server library tests passed all 65 cases.
`deployment-verification.json` records enabled services, actual running binary
hashes, canonical data paths and private health on all three hosts.

- Native client/protocol: 14 tests passed (`final-native-client.log`).
- Full-size composition, confirmation, deep recovery and a single replayed block
  crossing two fixed boundaries: passed (`full-lifecycle-final.log`).
- Seven standard HTTP cases passed across the initial suite and focused reruns;
  a further durable revocation restart/monotonicity case passed. Initial failures
  and fixes are retained: obsolete routing-revision assertions and a stale
  remote operations script. There is no remaining functional failure.
- Canonical-record and authenticated incoming/outgoing note recovery passed
  (`final-record-authentication.log`).
- External wallet HTTP/SQLite and full lifecycle interoperability: four passed.
  The Linux wallet checkout tree matches the candidate's
  `013cf94c9ad4a6706a0abc1d68faa75620ec73e8` tree exactly.
- Wallet compatibility, frozen identity vectors, cover retries and cache reuse
  passed. The independent Python encoder verifies 531 canonical session bytes.
- Final operations suite: 95 passed, three skipped (`final-ops.log`). The focused
  assessor's four checks passed. Two larger consolidation HTTP campaigns remain
  unrun in this focused scope.

## Physical workload and memory gate

The workload completed 1871.8 seconds, 35
publications, 24,506 correct background answers,
zero query errors, 1,424 exact boundary/retention
probes, and 31 expired-client refreshes. Background p99 was
367.359 ms. It exercised composition growth, tail removal,
frontier expansion and rewind with four sealed shards plus an active frontier.

Independent two-second sampling had no errors or gaps above 2.01 seconds during
the measured window. No host hit its hard cgroup limit or OOM. Worker peak RSS
was 5.52/5.47 GiB; RSS plus sampled kernel memory remained below the 6.5 GiB guard.
However, both workers incurred reclaim pressure and cgroup swap growth, peaking
at approximately 124/104 MiB. The strict focused assessment therefore **failed**;
see `focused-assessment.json`. Do not convert correct query counts into a
capacity certificate or increase placement/admission limits based on this run.

`focused-workload.tar.gz` contains the publication trace and final metrics. The
three `*-samples.jsonl.gz` files contain complete raw host traces. To reproduce
the assessment, decompress those three non-`canonical-` sample files and run
`enhance/tools/v7-qualification/assess.py` with `exercise.json`, both worker files,
the coordinator file and an output path. Exit 1 is the expected retained result.

A separate canonical one-shard observation is in `canonical-memory.json` and the
`canonical-*-samples.jsonl.gz` files. Worker RSS stayed near 1.01 GiB, with no
worker cgroup swap or memory-pressure events. Worker 1's host swap-out counter
advanced by 23 pages; this is not an uninterrupted zero-host-swap qualification.

## Public endpoint and recovery checks

The v6 rollback rehearsal restarted the preserved release and returned 202 correct
public answers with zero errors (`rollback-rehearsal.json`). Switching to canonical
v7 then produced two ready replicas. The final release was restarted again from
that v7 state and checked through public HTTPS.

The actual wallet client matched 28 independently extracted canonical records,
accepted routing refresh and completed a cover round on both the initial and final
v7 releases. `public-anchor.json`/`final-public-anchor.json` bind anchors verified
against both the local canonical journal and independent zakurad `getblockhash`.
An earlier run correctly rejected an anchor that changed during harness compilation;
its log is retained. No server-supplied anchor was silently accepted.

Legacy `EPQ4` is rejected with 400/`invalid_request`; stale `EPQ7` routing returns
409/`stale_routing` (`public-negative.json`). With one worker deliberately stopped,
151 public answers were correct with zero errors (`live-failover.json`). Both
workers were restored before final measurement.

## Performance observations and open gates

All loads use public HTTPS from the coordinator host and an independent canonical
oracle. They exclude geographically remote network latency. Eight closed-loop
clients were used; the same seeded old-row oracle was used for comparison. The
live chain advanced, so these are not frozen identical-data benchmarks.

| Run | Duration | Correct QPS | p99 | Incorrect / errors |
|---|---:|---:|---:|---:|
| v6 baseline | 30.2 s | 27.206 | 340.223 ms | 0 / 0 |
| v7 first short run | 31.0 s | 30.005 | 713.727 ms | 0 / 0 |
| v7 second short run | 30.1 s | 31.642 | 534.015 ms | 0 / 0 |
| final v7 longer run | 120.2 s | 33.029 | 430.335 ms | 0 / 0 |

The first two short v7 runs exceeded the proposed 10% p99 regression gate despite
higher throughput. The longer run also exceeded that p99 gate. It records a broader sample, not a
passed regression gate or an established throughput improvement claim. Remaining work is
worker memory/swap calibration, latency during publication and routing refresh,
the supported block-burst envelope, and uninterrupted six-hour active/sealed
hardware qualification. Seven sealed shards remain opt-in and unqualified.
