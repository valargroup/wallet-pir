# Second review

Scope: upstream ipir-sp `3c9e82b52b8b481869b005cea0191c7bd24f162f` against
`223626f`, and its transparent runtime integration. Required by the shared
ai-runbook reviewer rule for changes involving key material (here fixed public
mask images).

Verdict: approved, conditional on the application `make check` and existing
wrong-shard/wrong-table negative query tests passing. No algorithmic defect or
secret reuse identified. The initial request for a frozen known-answer vector
was resolved before approval.

The reference keys have fixed public top rows and zero body rows. The new path
uses the same library decomposition, multiplication, and addition; preserves
left/right cascade and final kh ordering; skips only zero-body work and duplicate
public image creation. No additional persistent cache, client secret, uploaded
key body, randomness, or database-dependent result is shared. API docs require
images constructed from identical parameters and left unmodified. Shape
validation alone is not a provenance guarantee; this remains a trusted local API.

The known-answer test freezes SHA-256 of c1 followed by every digit polynomial,
NTT words encoded as LE u64, from the original general builder. Differential tests
include production parameters, zero and modulus-boundary CRS, multiple gadget
widths and degrees, and malformed shapes. Existing conservative memory
reservations remain unchanged. This review does not approve a live rollout.

## Parallel-cascade and buffered-write follow-up

Reviewed upstream `61dc83e7410ff13ccfdd9ad1e830b711bd9080ed` and the application
snapshot writer. Approved conditional on tests: c1 halves share no mutable state,
the only reference carry is an identically zero body, and joined digits retain
left/right/final ordering including degree 2. Temporary concurrent scratch is
bounded by existing admission reservations. The 8 KiB writer preserves LE bytes,
checksum input, error propagation, flushing, sync and rename. Boundary and short
writer tests supplement the existing restore/corruption tests.

## Validation closure

Final application `make check` exited 0: 579 Rust tests passed, zero failed,
with two intentional manual benchmark tests ignored. This includes wrong-shard,
wrong-table, truncated/oversized query rejection, byte-format boundary tests,
restore equivalence and corrupt-cache fallback. Ops, documentation/report checks,
formatting and warning-free clippy passed. The final upstream inspiring release
suite, including its frozen vector and production differential tests, passed.
The conditional code-review requirements are satisfied; rollout remains gated by
performance and availability evidence.
