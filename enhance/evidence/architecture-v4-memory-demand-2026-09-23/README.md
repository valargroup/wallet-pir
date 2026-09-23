# Reservation memory-limit demand — September 23, 2026

Worker budget refusal has a distinct private HTTP 507 response. When reservation
returns that response, the coordinator persists the earlier record boundary and
observed fleet size, immediately emits at most one next-pair request, and retains
the normal publication failure/reconciliation rules. Other unavailable responses
do not take this path. Four groups cannot request a fifth pair.

[Five capacity tests](capacity-tests.log) passed. The new test verifies earlier
memory demand before count-based demand, repeated-refusal deduplication,
serialization/restart persistence, removal of the old limit from forecasting once
a larger inventory is registered, and the four-group ceiling.

[The HTTP test](http-tests.log) passed in 7.70 seconds: a middleware-injected 507
on the alternate destination's reservation leaves publication uncommitted and
emits demand for pair three. Clearing the refusal and reconciling allows a real
publication and exact encrypted queries through the alternative pair. This is
fault injection, not an actual exhausted worker or hardware-memory measurement.

[Clippy](clippy.log) passed for the library and HTTP test with warnings denied.
Formatting and diff checks passed. The admission model remains unqualified;
no provisioner was invoked, workers registered, or live infrastructure changed.
