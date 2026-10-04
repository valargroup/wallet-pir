# Changed-native activity candidate `c3c66b9b`, 2026-10-04

This directory holds evidence for candidate
`c3c66b9b51a5e2f4a6ac9261a941a397d119e920` and the operations path that
qualifies it. Everything here was produced on the development hub.

None of it is a native, hardware, fleet or production qualification:

- No production host was accessed.
- No artifact was staged, no candidate gate was run, and nothing was deployed.

The earlier activity record is
[activity-metadata-2026-09-30](../activity-metadata-2026-09-30/README.md); its
12ce and 80c94f32 identities stay historical.

## Files

| File | What it records |
| --- | --- |
| [build-tools.py](build-tools.py) | Driver that built only the 13 activity tools absent from the exact-head CI release bundles. It works from an immutable `git archive` export with Rust 1.97.1, `--locked`, fat-LTO `release` and `x86-64-v3` plus `pclmulqdq`. |
| [manifest.json](manifest.json) | Sanitized result of that build: source and tree, toolchain, Cargo input digests, stages and flag checks. It also records each artifact's SHA-256, ELF and maximum glibc, the argument-handling probe, CI run 37173250956's roles and the retained archive digest `d3a8f60a...`. The binaries, logs and archive stay outside Git. |
| [focused.log](focused.log) | Run on the working tree committed with this record. Focused operations suites on the candidate preparation change: 25 new candidate tests, the touched input, product and routing suites, the schema runner, host and baseline suites, and the deploy wrapper tests. All passed. |
| [mutations.log](mutations.log) | Nineteen single-guard mutations of the new checks. Each made `test_activity_candidate.py` fail, and each source was restored. |

## How the pieces fit

The candidate's 18 artifacts are the five CI serving roles plus these 13 tools.
`transparent/ops/lib/activity_candidate.py` pins them:

- **CI roles.** `transparent-filter-server`, `transparent-shard-server`,
  `transparent-publish-controller`, `shard-control` and `shard-assign`, from run
  37173250956.
- **Supplemental tools.** Pinned from `manifest.json` and the archive digest.

Before the reported release pins were committed, the supplemental reader was run
read-only against the retained archive. It accepted all 13 tools, their ABI and
the build result. The candidate provenance digest is `bc109c95...`. The release
`[profile.release]` at `c3c66b9b` is `lto = "fat"`, `codegen-units = 1`, and its
Cargo manifest, lock and toolchain digests equal the manifest's.

Deployment documents the [preparation and qualification
path](../../docs/deployment.md#changed-native-activity-candidate-c3c66b9b) that
root runs. Root supplies the actual candidate certificate, oracle,
artifact-verification and CI reports from retained raw results. This directory
contains no passing gate report and none may be inferred from it.

## Limitations

- **Loadability only.** The glibc 2.39 ceiling comes from the five production
  hosts' read-only ABI report. It bounds loadability only.
- **Fixture source evidence.** Fixture archives stand in for real ones, and the
  tests use fixture artifact digests. The real pins are checked against
  `manifest.json` and the prompted CI role digests.
- **Open gates.** Installed-setup agreement, warm serving, canonical recovery,
  rollback timing, freshness, sustained capacity and lifecycle all remain open,
  with unchanged floors and deadlines.
