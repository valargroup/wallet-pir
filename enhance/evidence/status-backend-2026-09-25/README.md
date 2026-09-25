# Synthetic Status PIR backend and APM validation — 2026-09-25

The isolated Status backend was built and exercised on a Paperspace P4000 in
AMS1. Its coordinator ingress, packing router, and CUDA worker run in one
process on that host. The public PIR APM dashboard has separate Enhance and
Status panes and one deployment topology. The Status source is a deterministic
synthetic fixture; there is no live chain or wallet integration.

## Final deployment

| Component | Identity |
| --- | --- |
| Status host | `status-pir-p4000-ams1` · P4000, 8 GiB VRAM |
| Status binary and unit | `/opt/status-pir/status-pir` · `status-pir.service` |
| Status protocol | `status-pir-v1-synthetic-q48` |
| Status binary SHA-256 | `91de3c9a2e7ef178163e7ca593ab7a5187c6820681a9b0523c8414bef302e871` |
| APM binary | `/opt/enhance-pir/releases/apm-status-d07ab140/pir-apm` |
| APM binary SHA-256 | `d07ab140ff0029d5e0c64cc087c07ff25134fed1f0627e09b811609f4bf9776f` |

The final deployed encrypted probe returned **40/40 correct answers**, with
no errors. Both public APM panes rendered without the retired label and with
exactly one deployment topology. The Status pane displayed “Synthetic Status
service.” Status and APM units were active. The private monitoring tunnel
forwarded only between loopback listeners; an eight-second tunnel outage
showed a Status scrape failure while Enhance APM remained available.

## Backend measurements

The retained [CUDA validation](validate-4.jsonl) covers all four observations,
a mempool-to-mined transition, full-hint differential checks, request and
response binding, malformed txids, fresh randomness, absent payload routes,
worker loss, and recovery-epoch fencing. Its final result is `passed: true`.
The synthetic fixture has 1,572,864 txids in 8,192 rows of 256 slots. The
one-row update rebuilt one 2,048-row unit and reused three.

| Preparation phase | Initial snapshot | One-row update |
| --- | ---: | ---: |
| Index reconstruction | 5.354 s | 5.341 s |
| Database and hint preparation | 6.661 s | 2.871 s |
| Packing preparation | 5.682 s | 5.577 s |
| PIR preparation, excluding index | 12.343 s | 8.448 s |

The update required about **13.789 seconds** including index reconstruction.
The five-second publication target remains unmet. The synthetic source has no
live node ingestion, durable recovery authority, or concurrent publication
qualification.

The retained [fixed-source load result](load-20qps.jsonl) records 1,200/1,200
correct encrypted lookups at 20 offered QPS over 60.03 seconds, with no errors
or skipped arrivals. Each lookup included initialization, public-material
download, client setup, encrypted query, and answer validation. p50/p95/p99
were 82.40/91.45/96.72 ms on the same host. The [GPU sample](gpu-load-20qps.csv)
peaked at 309 MiB during that run; sampling does not bound instantaneous use.

Status protocol and index tests, the focused telemetry test, all 76 APM tests,
formatting, and Clippy with warnings denied passed. The protocol revision and
synthetic fixture txid domain changed during the final naming update, so the
retained CUDA and load files record the preceding revision; the 40/40 deployed
probe validates the final revision. The APM test suite covers separate panes,
shared topology, and stale Status presentation after a scrape failure.
