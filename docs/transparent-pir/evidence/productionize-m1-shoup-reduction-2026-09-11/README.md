# M1 single-CRT NTT reduction candidate

The worker spends substantial CPU time reducing precomputed-root products with
general 128-bit division. The candidate uses the existing quotient's residual
bound to replace that final remainder with one conditional subtraction, protected
by `subtle::Choice` and `ConditionallySelectable`. It changes no ring parameters,
root ordering, protocol or wire format. The [arithmetic argument](arithmetic-argument.md)
states the range proof and references the corresponding SEAL operation.

Final dependency source is `584fee3893b1d2d17b5d774c83bd801fb70d5ace`, published on
spiral-rs's `m1/shoup-reduction` branch. The downstream ipir-sp pin is
`accc424e879d8da425fa620aad80f0f2c4e0defd`, on its matching candidate branch.
The [final patch](choice-candidate.patch) and [frozen division oracle](alt_division_reference.rs)
record the implementation and independent regression baseline.

## Profile and isolated measurements

The [preceding diagnostics](../productionize-m1-collection-timing-2026-09-11/README.md)
favored two build slots and found no unchanged tables in ten tail revisions.
A subsequent Amsterdam profile of the unchanged `3d13da6` worker recorded
51.97% of sampled CPU time in `__umodti3`. [Raw perf data and run traces](baseline-cpu-profile.tar.gz),
[symbol totals](baseline-cpu-symbols.txt), [caller stacks](baseline-remainder-callers.txt)
and [provenance](baseline-provenance.json) are retained. Some stacks unwind
incompletely. This profile includes cold warmup, queries and burst publication;
it does not attribute every remainder operation to steady-state NTT work.
The profiled run completed with 89 exact queries; its 9.640-second maximum
worker visibility includes instrumentation overhead and is not a baseline for
performance acceptance. Raw remote data remains at
`/opt/transparent-timing-slots-20260911/cpu-profile` on Amsterdam.

All comparisons below use the frozen canonical fixture, two build slots, four
worker CPUs, separate query clients and the same x86-64-v3 compiler flags.

| Implementation | Per-run maximum worker visibility, seconds | Decision |
|---|---|---|
| Unchanged baseline | 8.805, 8.046, 9.123 | Comparison baseline |
| Ordinary conditional selection | 6.121, 5.573, 6.319 | Rejected: introduced data-dependent jumps |
| General `ConstantTimeLess` | 10.086 | Rejected: slower than baseline |
| Native comparison into `Choice` | 5.332, 5.955, 5.895 | Advance to release qualification |

Raw [first-candidate runs](rejected-selection-qualification.tar.gz),
[general-comparison run](subtle-qualification.tar.gz) and
[final-candidate runs](choice-qualification.tar.gz) include reports, manifests,
client traces and logs. The final runs completed with 47, 53 and 55 exact queries,
client load overlapping the burst, persistence complete, no kernel OOM events,
and modeled headroom above 25%. These are isolated-worker measurements: the
controller's serial publication queue and live host headroom remain outside
this result. They do not satisfy the live M1 freshness gate.

## Arithmetic and target-code verification

The first candidate's [assembly](rejected-selection-assembly.txt) introduced
coefficient-dependent jumps and was rejected before deployment, despite faster
timing. The [general comparison patch](subtle-candidate.patch) removed those
jumps but generated substantial comparison work. Its
[forward](subtle-forward.asm) and [inverse](subtle-inverse.asm) assembly are
preserved along with the slower measurement.

The final experimental binary SHA256 is
`8d930654377a715627682864499f916228079ea8687d39534bda56fc0e7960d3`.
Its inspected [forward](choice-forward.asm) and [inverse](choice-inverse.asm)
reductions use `setb`, one fixed-address `subtle::black_box` call and bit-mask
selection, without new coefficient-dependent jumps. The black-box body is a
fixed stack byte store/load and return. This is target-specific inspection of
the changed operations, not a constant-time audit of the complete library.
Final release assembly must be checked again.

The [71-test local library suite](local-choice-library-tests.log) passed,
including wide-remainder boundary/random equivalence, forward and independent
inverse comparisons with the frozen division implementation, and direct
negacyclic convolution. The final experimental Linux build executed three
focused reduction tests and one live prepare/activate/invalidate integration
test; their logs are included in the final-candidate archive. The pinned
[ipir-sp release workspace suite](ipir-workspace-tests.log) passed 163 tests,
zero failures and one ignored. Build procedures and dependency manifests for
each candidate remain in this directory.

## Final release and live canary

Root source `2c4a2507523a39e76aef8a6db3076c40d54026ae` consistently pins both
dependency revisions. The [initial root check](initial-pin-check-failure.log)
caught an old direct spiral-rs pin and was corrected. The subsequent
[full make check](make-check.log) passed: 591 Rust tests, zero failures, two
ignored, plus formatting, Clippy, operations, report and documentation checks.
The lockfile changes only the four intended git source identities.

The [release build procedure](build-release.py) verified a
[321-file source manifest](release-source-manifest.json), with every file
subsequently [verified against the final commit](release-source-commit.json).
The [release verification archive](release-verification.tar.gz) contains source
binding, binary digests, build flags, target assembly review and executed Linux
suites: 36 library tests and 18 integration tests passed, with two integration
tests ignored. Final [forward](release-forward.asm) and
[inverse](release-inverse.asm) reduction selection was inspected again: no new
coefficient-dependent jumps, with the same fixed-address barrier and masks.

Final worker SHA256:
`cc6dabbd03ea2b1bccbe2d92d547e10bb9433e1d96215b7cd240c03dab9c8ec9`.
The [guarded start procedure](start-rollout.py) required successful build/tests,
matching source/artifact hashes, assembly approval for this exact binary,
unchanged predecessor identities and no other rollout supervisor.

The [single-worker upgrade](canary-upgrade.tar.gz) passed and public service reopened. The fresh loaded
canary began at approximately 08:35:38 UTC under
`transparent-m1-shoup-rollout.service`, root
`/opt/transparent-publisher-build/shoup-release-20260911/rollout` on the
coordinator. The [08:37:02 checkpoint](initial-canary-checkpoint.json) confirms
active PID 3286669 in canary observation: 3 new blocks, 1,179 exact queries,
zero retries/mismatches, maximum public visibility 12.655 seconds, replica
visibility 12.658 seconds and minimum sampled available memory 28.811%.
Routing availability baseline is 16 and the service is available. These are
initial samples, not acceptance. The [status helper](canary-status.py) reads the
current supervisor and its evidence without altering the run.

The other five workers remain gated on this canary. M1 still requires both
six hours and 300 new blocks, then the matching fleet upgrade and 24-hour fleet
observation. No prior diagnostic, failed or interrupted run time counts.
