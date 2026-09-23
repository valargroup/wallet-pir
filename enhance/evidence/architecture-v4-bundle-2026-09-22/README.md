# V4 candidate bundle validation — September 22, 2026

Full CI is now configured to build and upload a separate
`enhance-pir-v4-candidate-<sha>` artifact. It contains the v4 coordinator/worker,
Rust CLI, load driver, disposable process runner, and isolated launch example.
The existing production deploy workflow still selects the legacy artifact.
Candidate metadata is explicitly `unqualified`; checksummed packaging is not a
qualification receipt. The remote Linux CI workflow has not been executed here.

## Local validation

Native macOS binaries were built with `release-fast`, assembled into the new
candidate kind, and extracted through the strict inventory/checksum verifier.
The included runner was then executed from the extracted directory outside any
Git checkout. Four worker processes and a coordinator performed the synthetic
forecast/registration scenario and a concurrency-two exact-answer run.

[The load report](load-c2.json) records 1,708 correct answers, zero incorrect
answers, zero errors, and p99 31.215 ms over approximately 15 seconds. The fixture
starts at 67 records and appends during serving. This is a short local smoke run,
not full-size capacity, per-worker hardware, or off-host qualification.

[Candidate metadata](candidate.json) records `source_dirty: true`. The base commit
therefore does not identify the complete modified source. The [run manifest](manifest.json)
records binary digests and preserves candidate provenance outside Git. The
[bundle record](bundle.json) identifies the native archive and its SHA-256 digest;
[SHA256SUMS](SHA256SUMS) is the verified archive inventory. Native macOS binaries
cannot be deployed as the planned Linux release.

## Checks

- [Release/CI tooling tests](release-tests.log): 17 passed, including candidate
  round-trip, legacy-kind rejection, and rejection of a candidate changed to
  claim passing qualification even when its checksums are recomputed.
- [Enhance ops tests](ops-tests.log): 36 passed, including runner provenance
  outside Git and rejection of invalid candidate metadata without Git fallback.
- [Build](build.log): all three native optimized binaries built successfully.
- [Actionlint](actionlint.log): modified full-CI workflow passed; its empty log
  indicates no diagnostics. Formatting and Markdown link checks passed too.

The bundle's launch instructions and example inventory are independently
checksummed. [Source hashes](source-sha256.json) identify the packaging changes.
No remote workflow was dispatched, no production service changed, and no cloud
resource was provisioned. The remaining [deployment gates](../../docs/architecture_2-implementation.md)
still apply.
