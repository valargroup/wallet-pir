# Current-source Linux candidate validation — September 23, 2026

All three Linux x86-64-v3 binaries rebuilt successfully in 29.81 seconds from a
frozen 1,313-file source snapshot containing the final reorg/admission correction.
The source inputs were verified unchanged after the runtime campaign. The build
used Rust 1.91.0, the existing lockfile, offline dependencies, release-fast and the
original x86-64-v3 flags. No production CPU flags or global emulator registration
changed. Actual ELF hashes are in `binaries.sha256`.

The updated server help and real two-worker/coordinator publication/load campaign
passed under QEMU 10.2.3. Two 90-second steps at concurrency 1 and 2 returned
**1212 correct measured answers**, with zero wrong answers or request errors,
plus 36 correct warmup answers and no warmup errors. Generation
1 advanced to 4, completing 3 publications against a required minimum of two.
Raw reports and manifests are in `campaign/`.

`runtime.json` identifies the retained ARM64 emulator environment; `qemu-wrappers/`
contains its diagnostic launchers. Campaign manifest binary hashes identify those
launchers, not ELF bytes. This run does not exercise actual memory-pressure
relocation; the current native regression tests cover that behavior. Emulation
cannot qualify native hardware throughput, latency, memory limits, or six-hour
capacity. Diagnostic containers are stopped after collection.

This remains a dirty-source development candidate, not a clean release artifact,
a qualification receipt or a deployment. The unresolved project/VPC, coordinator
and worker-profile selection still prevents live provisioning under the supplied
infrastructure instructions. No credentials or cloud resources were used.
