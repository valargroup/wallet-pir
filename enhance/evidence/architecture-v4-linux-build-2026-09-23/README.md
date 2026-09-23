# Linux x86-64 build and emulator diagnosis — September 23, 2026

The frozen source compiled all three release-fast Linux x86-64 binaries successfully
with Rust 1.91.0 and the CI x86-64-v3 flags. Compilation took 7 minutes 49 seconds.
`build.log`, `source-sha256.json`, and `binaries.sha256` bind the inputs and outputs.
The source is an uncommitted candidate; these are not clean release artifacts.

The original build script then exited 139 while running server `--help` under
Docker Desktop's default local x86 emulation. All three binaries reproduced that
failure, including when copied off the bind mount. A temporary signal handler
localized the server crash to Clap help/usage generation. GDB register inspection
was unavailable under that emulator; its output is not a valid debugger trace.

A minimal Clap 4.6.0 program reproduced the failure with x86-64-v3 and with AVX/AVX2
disabled. The same source passed with baseline x86-64 and with BMI2 disabled.
Ubuntu 22.04's older QEMU also failed. The unchanged v3 reproducer and server help
both pass under the native ARM64 QEMU 10.2.3 executable extracted from the locally
cached `tonistiigi/binfmt` image. This points to emulator compatibility; it does
not prove correctness on native x86 hardware. No production CPU flags were changed.
The binfmt build expects an explicit application argv[0] after the ELF path when
invoked directly. Final successful help logs include that argument.

A real two-worker/coordinator HTTP smoke and exact-answer concurrency 1/2 load
campaign passed under QEMU 10.2.3: 151 measured correct answers, zero wrong answers
and zero errors, plus 36 correct warmup answers. Each concurrency step lasted
10 seconds. Generation 1 served through both replicas; generation 2 was still
preparing when the harness stopped, so this run does not establish completed
concurrent publication, retention, or failover. Reports are in `linux-qemu-smoke/`. Its
wrapper scripts are diagnostic launchers; hashes in the smoke manifest identify
those wrappers, while `binaries.sha256` identifies the actual ELF programs.
The harness was updated to permit a missing Git executable, reporting unavailable
checkout provenance. The frozen Rust build remains unchanged.

No credentials or host home directory were mounted. No global binfmt registration
was changed. Containers and toolchains are retained for diagnosis. Emulation cannot
qualify production throughput, latency, 8 GiB memory limits, six-hour campaigns,
or live deployment. The candidate remains unqualified.
