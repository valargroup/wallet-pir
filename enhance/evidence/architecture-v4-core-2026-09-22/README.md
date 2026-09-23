# V4 core validation — September 22, 2026

This is local functional and load evidence, **not production or 8 GiB hardware
qualification**. Two separate worker processes and one coordinator ran on the
same shared macOS ARM64 host as the load driver. The positional synthetic fixture
started at 67 records and appended one record per publication. Other builds and
tests shared the host. Each measured stage lasted 15 seconds after a two-second
warmup; timings are not a stable capacity estimate.

[Run manifest](manifest.json) records the base Git revision, dirty state, binary
SHA-256 identities and publication progress. [Source hashes](source-sha256.json)
identify the relevant local source snapshot. The command files beside each report
record the exact invocation. No production services or cloud resources changed.

## Load results

| Stage | Correct answers | Errors | Correct QPS | p99 ms | Scheduled p99 ms |
| --- | ---: | --- | ---: | ---: | ---: |
| [Concurrency 1](load-c1.json) | 782 | 0 | 52.11 | 62.815 | 62.815 |
| [Concurrency 2](load-c2.json) | 1270 | 0 | 84.54 | 78.655 | 78.655 |
| [Concurrency 4](load-c4.json) | 1379 | 369 http_429 | 90.81 | 61.535 | 61.535 |
| [Concurrency 8](load-c8.json) | 1475 | 1201 http_429, 1 transport | 98.09 | 63.871 | 63.871 |
| [Open loop 50%](open-loop-0.5.json) | 611 | 23 http_429 | 40.75 | 104.831 | 105.855 |
| [Open loop 75%](open-loop-0.75.json) | 903 | 48 http_429 | 60.20 | 75.967 | 78.847 |
| [Open loop 100%](open-loop-1.0.json) | 1234 | 34 http_429 | 82.11 | 59.519 | 61.951 |
| [Open loop 125%](open-loop-1.25.json) | 1540 | 45 http_429 | 102.62 | 47.679 | 52.927 |

Every successful response was checked against the exact positional record oracle.
All stages recorded zero incorrect answers and zero unstarted arrivals. The
open-loop offered rates were 50%, 75%, 100% and 125% of the immediately preceding
concurrency-two throughput. Every open-loop stage returned some HTTP 429s;
concurrency eight also recorded one transport error. Its underlying cause was
not captured, so it remains unresolved. Latency includes completed failed attempts
as well as successful responses; fast rejections can lower the percentiles, so
errors must be assessed alongside the latency figures.

The characterization commands allow errors (`--max-error-rate 1`). Exit code zero
therefore does **not** demonstrate the zero-error release criterion. The small
fixture, short duration and shared host do not establish full-size throughput,
publication-delay limits, resident memory safety or sustainable fleet capacity.

## Correctness checks

- [Final Rust tests](final-tests.log): 83 passing tests across the selected library,
  binary and HTTP targets; the large lifecycle case was excluded from this command
  and run explicitly below. The distributed HTTP test checks exact encrypted
  answers, retained/expired sessions, refresh, failover, coordinator restart and
  a failed worker commit notification after the durable coordinator decision.
- [Full lifecycle test](full-lifecycle.log): passed the real 32K shard boundary,
  4K suffix loan, atomic return and old-session query checks over HTTP. Its two
  worker listeners share one test process; it is not a host-budget test.
- [Clippy](clippy.log): all workspace targets/features passed with warnings denied.
  The full repository `make check` also passed before the final recovery changes;
  the final Rust suite and clippy above cover the subsequent code snapshot.

The [implementation status](../../docs/architecture_2-implementation.md) lists
remaining implementation, hardware qualification and deployment work.
