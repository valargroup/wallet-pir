# Private live Status checkpoint — 2026-09-25

Public Status remains disabled. Neither short campaign passed the availability
gate, and the required six-hour joint live-publication/20-QPS run has not run.

| Campaign | Offered | Correct | Failed | Unstarted | Successful p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before packing reuse | 1,200 | 196 | 969 | 35 | 2,627.38 ms |
| Packing reuse | 1,200 | 867 | 333 | 0 | 452.69 ms |

Each campaign offered 20 complete lookups per second for 60 seconds against the
private live controller with independent P4000 worker/router processes. These
are mined-answer smoke measurements, not complete publication qualification.
Successful-request latency excludes failed requests.

The packing-reuse campaign used P4000 binary SHA-256
`63796b754cb6bb8df0b16ef0285cdecf163892eb56a012c71459de35e8978885`
and coordinator binary SHA-256
`49f97bd851068bf50eaa52b4d7b7278f743dc5f160c2e49f6e309f7ba4369a0c`.

Investigation found metadata-refresh backpressure causing unnecessary recovery.
The source checkpoint includes subsequent fixes for retained request pins,
metadata-only refresh admission, heartbeat authority validation, role restart
detection, and retrying preparation failures before durable commit. Those latest
fixes passed 25 local Status tests but have not been deployed or retested live.

Remaining gates include complete observation/supersession evidence, independent
block and mempool publication checks, the six-hour load run, fault recovery,
restricted HTTPS rehearsal, protocol review, live monitoring integration, and
compatibility validation before moving the controller into the deployed Enhance
process. See `../../docs/status_distributed.md` and
`../../docs/status_qualification.md`.
