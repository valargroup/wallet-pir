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

## Final release qualification in progress

The root workspace now consistently pins the two published dependency commits.
The [initial root check](initial-pin-check-failure.log) caught one old direct
spiral-rs pin, causing type mismatches; it was corrected. The lockfile changes
only the four intended git source identities, and
`cargo metadata --locked --offline` validates the resulting graph. The fresh
[full make check](make-check.log) passed: 591 Rust tests, zero failures, two
ignored, plus formatting, Clippy, operations, reports and documentation checks.
The failed initial check is not counted as validation.

The [release build](build-release.py) runs on the coordinator under
`transparent-m1-shoup-release-build.service`, root
`/opt/transparent-publisher-build/shoup-release-20260911`. It verifies the
[321-file source manifest](release-source-manifest.json), uses committed git
dependencies rather than a path patch, executes the worker library and default
integration suites, and builds the worker, control tool and query client.
Before deployment, every source file must also match the final root commit,
all checks must pass, and final release assembly must be inspected.

No new worker has been deployed and no full acceptance canary is running.
M1 remains open under the unchanged six-hour/300-block canary, gated fleet
upgrade and 24-hour fleet observation requirements.
