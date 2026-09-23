# Workload host identity — September 23, 2026

The synthetic workload records the SHA-256 of the raw Linux machine-ID file,
matching the worker sampler. Missing or malformed identity is explicitly null.
The campaign assessor rejects missing generator identity or a generator identity
matching any worker; sharing the observer host is permitted.

Validation:

- [Rust tests](rust-tests.log): workload tests check identity byte compatibility,
  missing/malformed identity, full profile assignment cycles and exact probes.
- [Python v4 tests](python-tests.log): 72 tests ran, 70 passed with two Linux-only
  skips on macOS. Includes rejection of missing and co-located generator identity.

No deployed workload was run for this change. Machine IDs are not authenticated
attestations, and cross-host clock alignment remains unproven. All assessment
results still retain `qualification: unqualified`. Earlier recorded workloads
without host identity cannot pass the strengthened assessment.
