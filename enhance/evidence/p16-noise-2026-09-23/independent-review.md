# Independent review

A separate review agent, explicitly authorized by the user, performed a read-only
code-and-mathematics review of the extractor, verifier, pinned backend, and
priority evidence. This is not an external cryptographic audit.

**Verdict:** approve the analysis as a conditional fixed-snapshot sufficient
bound; do not claim production clearance.

The reviewer reproduced the `2^-216` and `2^-185` priority bounds and found no
arithmetic flaw in grouped reuse, the finite-CDF MGF argument, exact Chernoff
optimization, or the union over both tails of all 12,288 output coefficients.
Six verifier tests and the exhaustive tiny-ring basis-vector test passed.

The finite-CDF argument uses inclusive thresholds and fallback-to-zero exactly
as the backend does. Bounding the Taylor remainder at `|u|=1/4`, scaling its
higher-order terms by `16u²`, and applying `log(1+x) <= x` yields the stated local
MGF constant. The rational optimizer maximizes that quadratic exponent on the
admissible interval; `ln(2) < 0.694` is conservative.

Remaining production blockers:

1. Dense 16K/32K cases exhaust the sufficient deterministic budget. Their
   observed successful queries cannot establish the requested rare-failure bound.
2. The retained deployed public material uses the previous decomposition, and
   candidate schema-11 projections do not certify those old setup pairs.
3. Full campaign coverage was still in progress at review time.
4. The standalone verifier trusts extraction and expected identity supplied by
   the operator. Digest syntax is not authentication. Replacing an input digest,
   or fabricating internally consistent weight moments, can still yield a
   numerical pass. A production gate would need trusted expected identities and
   trusted/recomputed extraction; this tool is not such a gate.

The reviewer found no justified simple replacement of deterministic rounding by
independent uniform rounding: rows share a secret. Conditioning on the secret
makes fresh input errors independent but does not remove potentially aligned
conditional rounding means. Even discarding input noise entirely, dense 16K
query rounding alone nearly consumes the entire decoding threshold, before
response rounding. A stronger reviewed argument or separately qualified profile
is required.

The reviewer also inspected the fast extractor's two-transform derivation and
found it algebraically correct. It requires the caller's digit-range checks and
the centered-lift bound. Its approval was conditional on a full-degree comparison
with the original extractor. That comparison subsequently passed for all moments
and action checks; the measured reference/fast times were 14.177/3.292 seconds.
The reference method and explicit full-degree regression remain in the tool.

The full-degree comparison also passed on native AVX512 Linux (27.262/3.593
seconds). End-to-end accelerated 8K maximum and generation-69 runs matched all
deterministic inputs and grouped moments. The reviewer subsequently checked
fixture caching and confirmed these checks resolve the implementation-equivalence
condition; no further correctness concern was found. The cross-platform
comparison now rejects unequal block counts before comparing moments.
