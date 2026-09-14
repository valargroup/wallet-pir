# Previously reported Enhance production performance

The [pre-cleanup README](https://github.com/valargroup/enhance-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/README.md)
reported the figures below without a run date, source identity, or committed raw
query log. Retained for context only; these figures do not qualify the c-4 target
or establish present capacity. The traffic calculation describes the reported geometry.

A five-minute, eight-worker production test completed 6,512 encrypted queries
with no errors at 21.71 requests/second. End-to-end latency was 322.6 ms p50,
483.3 ms p95, and 1.75 s p99.

At the current 32,768-row by 4,096-column configuration, each query uploads
258,056 bytes (252.0 KiB) and downloads 10,256 bytes (10.0 KiB), or 262.0 KiB
combined. At 21.71 requests/second, that is 5.60 MB/s upload plus 0.22 MB/s
download (46.6 Mbit/s combined), excluding HTTP and TLS overhead.
