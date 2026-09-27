# Native Status public endpoint activation — 2026-09-27

The production native Status controller became publicly reachable through
`https://enhance-pir.valargroup.dev/v1/status/` on 2026-09-27. The public
listener is dedicated loopback port `8489`; the private controller listener
remains on loopback port `8480` and is not proxied by Caddy.

The deployment enabled `public_enabled` in
`/etc/status-pir-control/controller-native.json`, added
`--public-listen 127.0.0.1:8489` to the active controller command, and routed
only `/v1/status/*` from Caddy to that listener. The deployed Status binary was
not replaced. Its protocol is `status-pir-v3-native-two-mask-m29`.

The controller restarted into recovery epoch 8 and first exposed generation
26005. An external HTTPS manifest request returned HTTP 200 with the native v3
protocol. A 60-second live smoke then sent initialization, public-material, and
encrypted query traffic through the public HTTPS origin at 20 QPS:

- 1,200 offered, 1,200 correct, zero failed, zero unstarted;
- 46 ms scheduled-to-completed p99;
- canonical mined-answer comparison against the coordinator's local node;
- same-origin query path through Caddy and the dedicated public listener.

This smoke establishes reachability and encrypted mined-answer correctness. As
the retained summary states, it is not the complete production qualification
gate and does not establish mempool publication completeness or negative-answer
trustworthiness.

The pre-cutover host files are retained at
`coordinator:/root/status-public-20260927T141812Z`. The raw smoke report remains
at
`coordinator:/var/lib/status-pir-controller-native/public-smoke-20260927T141850Z`.
The report's immutable request and summary files are copied into this evidence
directory; their digests and deployment provenance are recorded in
`manifest.json`.
