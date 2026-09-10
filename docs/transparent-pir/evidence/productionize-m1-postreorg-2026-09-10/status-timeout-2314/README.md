# Confirmed post-reorg worker status timeout

Observed 2026-09-10 at 23:14 UTC. The post-reorg canary supervisor remained
active, PID 2521008, when checked at 23:14:35. This does not make this window
healthy or establish acceptance.

The HTTP readiness probe beginning 23:14:05.408 timed out after 4.005 seconds.
Unix status beginning 05.538 completed after 3.923 seconds. Reconciler logs
record both status attempts timing out, membership_status_failed, and
recent-01 becoming ineligible at 23:14:08. Eligibility returned at 23:14:10.
Thus the local pause affected the real control plane even though the canary's
periodic checks had not terminated its run. Logs establish membership removal;
these files alone do not establish the response to every public request.

The selected raw probe window spans 23:14:03 inclusive to 23:14:12 exclusive.
Threads 649050 and 649051 were repeatedly runnable without recorded scheduling
progress while 664138 waited for writeback. Unlike earlier windows, the
pressure sampler itself has a gap between 23:14:04.886 and 09.463. Do not
interpret missing pressure samples as proof of continuous CPU behavior.
The runnable-stack sampler had already completed its scheduled hour, so it
has no records in this window. The longer thread/status probe was still active.

The root cause remains unresolved. Acceptance must be assessed against this
known membership failure, not inferred solely from an active supervisor or a
later success result. No worker configuration or timeout was changed here.
