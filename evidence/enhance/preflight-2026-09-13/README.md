# Enhance isolated c-4 preflight — September 13, 2026

Consolidated from the [recorded expansion status](https://github.com/valargroup/enhance-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/enhance-autoscaling.md).
This is a reported result, not a new run. No raw qualification bundle was committed
with that status; the report alone cannot reproduce or establish full acceptance.

The isolated short hardware preflight at revision
`2338dcdac3ea71838c7388b49a19fe360784cbbb` passed with 16 shards, 12
publications, 1,316 exact-answer queries including retained sessions, and a
303-ms paired-client p99. Peak cgroup memory was 5.51/5.62 GiB, with zero swap
and OOM events. Cold preparation took 53.5 seconds; the longest subsequent
publication took 8.2 seconds. This preliminary run does not qualify another
revision or replace the six-hour qualification, online expansion rehearsal,
or 24-hour production observation.
