# Plan 2: bounded Status admission

Keep four executing requests per serving role; retain the coordinator’s existing sixteen forwarding slots. Serving roles admit at most thirty-two additional waiting requests, with a one-second maximum admission wait; the coordinator keeps eight waiting and 250 ms. Reject excess arrivals immediately with HTTP 429; do not retry internally. Count queue delay against a five-second role request deadline. The router's executing permit includes worker I/O and packing. Blocking CPU work retains its permit even when its caller times out or disconnects.

Validate the session before waiting and recheck freshness and authority after admission and before returning. Queueing does not renew observation timestamps or authority leases. Bound coordinator forwarding and worker evaluation using the same policy; downstream failures remain explicit.

Export aggregate per-role queue-full, queue-timeout and request-deadline rejection counters, active/waiting gauges, and wait/execution histograms. Labels contain only fixed role/reason names, never transaction or session identifiers.

Test queue capacity, cancellation cleanup, timeout followed by recovery, blocking-task permit retention, and freshness revocation while waiting. Start at four executing/eight waiting/250 ms, then measure paced 20 QPS and deliberate bursts under live publication. A queue absorbs short bursts; it does not satisfy a sustained capacity deficit. Require zero lost/failed arrivals and end-to-end p99 below one second before accepting these defaults.

## Forwarding follow-up from production measurement

The initial deployment removed observed admission rejections in its first smoke,
but still lost offered arrivals and exceeded the latency gate. Coordinator permit
hold times were much longer than router and worker hold times. Inspection found
that each request built a new HTTP client and disabled idle connection pooling,
opening new TCP/SSH channels for every lookup. Retain one serving client per role,
with four idle connections per host, 15-second idle expiry, one-second connect
timeout, and the existing five-second request timeout. The separate publication
client and all authority checks remain in force. Measure the change; this finding
alone does not establish the cause of every tail-latency incident.

Record cumulative completion times for init, independent oracle, session fetch,
client preparation, query round trip, and decoding in the load harness. Continue
to count scheduling delay and every unstarted arrival in qualification. Test real
connection reuse and close accepted connections in synthetic worker-loss tests.

The pooled smoke still measured a 1,625 ms query-round-trip p99 while every
successful router permit hold was below 200 ms. Test independent authenticated
query transport: install `status-query-tunnel.service` and set the controller's
optional `query_router_origin` to `http://127.0.0.1:8492`. The existing router
origin continues carrying publication and heartbeat traffic. This preserves the
loopback and SSH authentication boundaries while separating TCP congestion for
queries and control. Compare all arrivals, latency, and role rejection reasons;
do not infer success solely from typical latency improving.


The 2026-09-27 CPU-host soak failed the original four/eight/250 ms defaults: at 20 QPS the worker or router occasionally paused for 0.2-0.9 s and each pause became a burst of HTTP 429s (up to 81 of 12,000 per batch). The serving roles now allow thirty-two waiting requests and a one-second wait, which absorbs those pauses within the p99 budget; the cause of the pauses remains open.
