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


The [quarter-hour checkpoint](quarter-hour-checkpoint.json), captured at
08:50:59 UTC after 921 seconds, confirms the same active supervisor and source:
10 new blocks, 13,569 exact queries, seven retries and zero mismatches. Maximum
public visibility is 15.655 seconds, replica visibility 15.658 seconds, minimum
sampled available memory 26.604%, and routing withdrawals remain at baseline 16.
No cache write failures were recorded. This is partial observation, not M1
acceptance; no source, binary or configuration was changed during this interval.


The [half-hour checkpoint](half-hour-checkpoint.json), captured at 09:06:04 UTC
after 1,827 seconds, confirms the same active supervisor: 21 new blocks,
27,170 exact queries, 14 retries and zero mismatches. Maximum public visibility
remains 15.655 seconds, replica visibility 15.658 seconds and minimum sampled
available memory 26.604%. Routing withdrawals remain at baseline 16; no cache
write failures are recorded. This is partial observation, not M1 acceptance.

## Terminal canary failure: 09:13:59 UTC

The [complete failed canary](failed-canary.tar.gz) stopped after 2,301.911
seconds and 26 visible blocks: block 3479507 exceeded the 30-second public
freshness budget at 30.084 seconds. Clients completed 34,183 exact queries,
19 retries and zero mismatches. Minimum available host memory was 26.604%.
No elapsed time from this run counts toward M1; all-five expansion remains gated.

The [controller journal](failure-controller.log) identifies a 25.032-second
recent-01 activation timeout for candidate `9475606e…`. The fleet withdrew
routing after losing activation quorum, then recovered through recent-04 at
09:14:22.434 UTC with height 3479508 and recorded freshness 52.468 seconds.
Thus the observation's 30.084-second age was not the eventual delay.

The [worker journal](failure-worker.log) records candidate loading 1.049 seconds
and warming 3.614 seconds, completed at 09:13:37.438. A later collection at
09:14:54.769 waited 9.613 seconds for the runtime disk lock. The
[host check](failure-host.txt) confirms the same PID 792204, start 08:34:45 UTC
and zero restarts; historical sar returned no samples for the requested window.
The [control-session journal](failure-sessions.log) contains no events for this
window. None of these proves which operation stalled activation.

Investigation sequence:

1. Preserve the terminal report, raw queries, samples and journals (above).
2. Capture blocked worker thread kernel stacks, disk counters and I/O pressure
   during normal publication using the [bounded sampler](activation-sampler.py).
   `transparent-m1-activation-sampler.service` on recent-01 runs for 15 minutes
   with a 16-minute systemd ceiling; it changes no worker configuration.
3. Correlate any stalls with activation, persistence and cache writes. Add
   focused stage instrumentation only if the capture cannot distinguish them.
4. Correct the demonstrated blocking path while retaining durable publication,
   restart/reorg correctness and verified activation quorum. Reproduce the
   failure under controlled load and run the relevant regressions and root checks.
5. Qualify the resulting release, then start a fresh matching six-hour/300-block
   canary before fleet expansion and the separate 24-hour observation.

No timeout or acceptance requirement has been relaxed.

### Loaded diagnostic reproduces durable-record blocking

`transparent-m1-activation-diagnostic.service` ran the same two exact-query
clients on the installed release with a diagnostic 600-second duration, zero
minimum blocks and unchanged freshness budgets. It failed at 09:23:29 UTC
(232.853 seconds) on block 3479518 age 30.190 seconds. It grants no acceptance
credit. The [controller journal](activation-diagnostic-controller.log) records
an activation timeout of 25.105 seconds at 09:23:35 and routing withdrawal,
then recovery through height 3479519 at 09:23:53 with 74.275-second freshness.
A reorg interrupted the preceding candidate; it must be distinguished from the
subsequent durable-record stall.

The additional [file-descriptor sampler](activation-fd-sampler.py), bounded to
600 seconds with an 11-minute systemd ceiling, identifies blocked flush files.
Its [partial capture](activation-fd-partial.ndjson) shows `active.tmp` and a
runtime-cache `.partial` file waiting on file writeback at 09:23:11–13, and
both waiting on journal commit at 09:23:57–58. The sampling gap is preserved;
it does not prove uninterrupted blocking throughout the gap. This directly
identifies publication persistence in the failing path, but does not by itself
establish why the filesystem stalled or prove a particular correction.
The original kernel/disk sampler and this supplementary sampler continue to
their bounded deadlines; preserve complete captures on termination.

### Bounded online-discard experiment: 09:26:58 UTC

Both runtime cache and publication record resolve to `/dev/vda1`, ext4 root
with `discard,commit=30`. Disk counters during the reproduced failure show
outstanding requests without completions and large accumulated write/flush
latencies. Online discard is a hypothesis, not an established cause.
[Kernel documentation](https://www.kernel.org/doc/html/v6.6/admin-guide/ext4.html)
defines discard as issuing TRIM when blocks are freed;
[util-linux documentation](https://kernel.googlesource.com/pub/scm/utils/util-linux/util-linux/+/refs/heads/master/sys-utils/fstrim.8.adoc)
notes that unqueued trim can penalize other disk operations.

The [guarded trial](nodiscard-trial.py) temporarily remounted recent-01 root
with `nodiscard`, retaining the other observed options, at 09:26:58 UTC.
[Before/after output](nodiscard-trial.log) records the change. It changes no
file flush, durability barrier, worker binary or persistent fstab.
`transparent-m1-discard-restore.timer` was installed first and restores
`discard` 30 minutes later, approximately 09:56:58 UTC. Do not leave a full
acceptance run spanning that change.

`transparent-m1-nodiscard-diagnostic.service` runs a separate 900-second
two-client diagnostic with unchanged 30/60-second budgets, zero minimum
blocks, and a 17-minute process ceiling. Output is
`/opt/transparent-publisher-build/shoup-release-20260911/nodiscard-diagnostic`.
This is neither a canary restart nor acceptance credit. Assess its result and
retain the raw captures before deciding on persistent configuration changes.

Both samplers subsequently terminated successfully at their original deadlines.
Their [complete scripts and raw captures](activation-samplers-complete.tar.gz)
are preserved, along with the [complete failed loaded diagnostic](activation-diagnostic-complete.tar.gz).
The file-path sampler contains 546 samples spanning 09:21:50–09:31:50 UTC.
Of 260 samples before the remount, 68 contain blocked operations; of 286 after,
four do. Workload and block-arrival differences prevent attributing that
comparison solely to discard. The original disk sampler spans the failure
and remount as well; the mount-change timestamp must be used when analyzing it.
The separate nodiscard diagnostic remains in progress at this checkpoint.

### Online-discard diagnostic completed successfully

The [complete nodiscard diagnostic](nodiscard-diagnostic-complete.tar.gz)
passed at 09:42:02 UTC after 900.878 seconds and nine new blocks, with 13,682
exact queries, three retries, maximum public freshness 16.509 seconds, replica
freshness 14.444 seconds, and minimum available host memory 26.955%. The
process terminated successfully. No restart/OOM/routing withdrawal was recorded.
This small live comparison warrants advancing the mount configuration to
properly managed qualification; it does not prove discard was the sole cause
or satisfy the full acceptance gate.

Next: implement a guarded persistent mount-policy helper and read-only
verification, cover option preservation and rollback, integrate it into the
existing staged worker rollout and observation, run required checks, and apply
the qualified policy on the canary before a fresh six-hour/300-block run.
Do not modify publication fsync or relax any gate. The temporary restoration
timer remains scheduled for approximately 09:56:58 UTC; let it restore the
original mount unless a qualified persistent transition explicitly supersedes it.

### Managed storage policy: source qualification

The worker installer now accepts `storage_nodiscard`, stages the helper with a
read-only preflight, and installs an enabled worker prestart. The helper only
supports the observed shared writable ext4 root, preserves other mount options,
and verifies its effect. The observer checks mount state, loaded prestart and
helper hash; the full-fleet gate binds the same helper identity. Rollback saves
the original discard option and a restoration helper before installation, so
restoration does not depend on successful installation of the new helper.
It does not edit fstab, fsync calls, barriers or scheduled fstrim.

[Full make check](storage-policy-make-check.log) completed successfully with
591 Rust tests passed, zero failed and two ignored, plus formatting, Clippy,
operations/report/docs checks. The [final 110 operations tests](storage-policy-ops-tests.log)
include the subsequent rollback-helper correction and installation tests.
The implementation remains undeployed at this source checkpoint.
