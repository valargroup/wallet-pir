# Transparent PIR tools and research

Start at [the current transparent PIR index](../docs/transparent-pir/README.md).
The active shard tools are Rust binaries: `shard-census` and `shard-publish` in
`server/transparent-filter-server`, and `shard-residency` / `shard-scaling` in
`server/transparent-shard-server`. Read their current CLI help before use.

The Python `transparent_pir_*` collection/evaluation/replay tools and the
`transparent-pir-reuse-bench` package preserve earlier studies. In particular,
incremental, HTTP, reuse and navigation results use research layouts/backends;
they do not specify the active shard schema, deployment geometry or capacity.
Filter build/validate/measure and cross-check tools retain their format-specific
purpose; consult the active filter reference and matching tests.

Reproduce an old run only with its recorded source, backend, journal range and
parameters. Record new results using the [evidence metadata](../docs/transparent-pir/evidence/README.md).
