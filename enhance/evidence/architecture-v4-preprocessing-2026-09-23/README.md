# Preprocessing benchmark validation

The standalone `v4-preprocess` example compiled in release mode and completed a
local aarch64 macOS smoke run. It measured 2K/4K/8K units and 4K/8K/16K/32K query
domains, verified 16 encrypted exact answers, and reloaded/reused artifacts without
canonical rereads. It does not change the deployed runtime.

`local-smoke.jsonl` is validation of the tool, not production performance data.
One repetition on a developer machine is not a stable latency estimate. Native
c-4 runs remain pending while the six-sealed campaign occupies the workers.
See the example README for exact timing boundaries and caveats. Initial fixture
failure and corrected source identity are recorded in `provenance.json`.
