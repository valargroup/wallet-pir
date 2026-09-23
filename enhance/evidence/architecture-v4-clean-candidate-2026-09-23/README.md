# Clean local v4 release candidate

Candidate source commit: `9718a6dcf9801385f69f31bb71f02efd261914f0`.
Local branch: `codex/architecture-v4-candidate-20260923`.

Created with a separate Git index and detached worktree; the original main
workspace and index remain unchanged. No branch was pushed. The candidate is
unqualified and has no remote CI provenance.

The clean checkout passed 111 operations tests (three environment-specific skips),
17 CI-tool tests, and `cargo fmt --all -- --check`. The offline Ubuntu 24.04 x86-64
Rust 1.91.0 release build passed in 11m33s. It used full LTO, one codegen unit,
one build job, and the CI artifact CPU flags. Build inputs and logs are retained.

The archive was assembled from that clean checkout, extracted by the release
verifier, and accepted by the packaged bootstrap verifier with its independently
computed manifest digest. `candidate.json` records archive, manifest and worker
binary checksums. A validation import initially created `__pycache__`, which the
strict verifier correctly rejected; that generated cache was removed and the
verification passed with Python bytecode generation disabled. No release payload
was edited to obtain acceptance.

The extracted Linux binaries passed two 90-second real HTTP/PIR load steps under
QEMU 10.2.3: 407 correct answers at concurrency 1 and 769 at concurrency 2,
zero wrong answers/errors, plus 24 correct warmup answers with zero errors.
The coordinator advanced generation 1 to 4 during traffic (three publications).
The raw runner manifest hashes the emulator wrappers and cannot discover Git
revision from them; `bundle-SHA256SUMS`, the retained wrappers, and candidate
metadata bind the actual extracted ELF binaries to the clean source revision.

This is an emulated small-fixture correctness/load smoke test, not a native c-4
performance result or six-hour qualification. Both task-owned execution containers
were stopped after their processes completed. No cloud resources were created.

The durable local archive is beneath
`.local-pir/v4-candidates/9718a6dcf9801385f69f31bb71f02efd261914f0/` (Git-ignored).
Deployment still needs the qualification coordinator choice, isolated state
initialization and writer credential boundary, guarded provisioning, native
active/sealed campaigns, wallet conformance and rollout/rollback validation.
