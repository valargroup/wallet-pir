# Preprocessing phase benchmark

Build and run outside a qualification workload:

```sh
cargo build --locked --release -p enhance-pir-server --example preprocess
./target/release/examples/preprocess --output /new/disposable/directory --repetitions 3
```

The output directory must not exist. `timings.jsonl` is flushed after every case;
a final `complete` entry indicates successful completion. Failure leaves partial
results and disposable artifacts for diagnosis. Successful cases remove only
their own temporary artifacts. The binary does not contact or modify services.

Measurements use production code, P16Q48 and the schema-11 row layout:

- Full 32K public query setup construction.
- 2K/4K/8K unit construction (row encoding plus offline PIR precomputation),
  persistence, and artifact reload, timed separately. Fixture generation, content
  hashing and public setup are outside the unit-construction timer.
- 4K/8K/16K/32K query domains: plan generation, Engine preparation, hint assembly,
  packing setup, in-memory reuse and artifact reload. Engine preparation includes
  setup, synthetic row generation, unit construction and persistence.
- Four encrypted exact-answer queries at each query-domain size. Their
  `Packing::pack` calls are timed individually after evaluation and reported as
  `pack_samples_ms`, with query and response sizes. This includes repeated
  query validation and packing-key deserialization, but excludes evaluation,
  body reception, network transfer and decoding. Reuse/reload cases must
  succeed without invoking the canonical record reader.

The 32K case uses 32K allocated rows with one record of padding. At exactly 32K
populated rows the lifecycle creates two query shards through lending, so that
state would not isolate a single query domain. This benchmark does not exercise
that transition; the full-size lifecycle campaign covers it.

These are elapsed wall times, not CPU time. Artifact reload may use OS page cache;
it is not a cold-disk measurement. There is no network, replicated publication,
concurrent query traffic or resident-memory certification. Run repeatedly on the
actual target hardware to characterize it; local macOS timings do not establish
production c-4 performance. Preserve source revision, compiler/build flags,
binary digest, host identity, and concurrent workload details with reports.
