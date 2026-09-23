# Sealed campaign cold-preparation deadline failure

The first sealed campaign failed before any measured publications. The report
records a send error on worker 2's `/internal/v4/prepare` request. The coordinator
used a shared 180-second HTTP timeout. One-second samples show worker 2 busy for
approximately 186 seconds in its final preparation interval; the send failure
occurred just before that interval ended. This supports a preparation-deadline
failure. The original error string does not expose the reqwest source chain, so
it alone does not establish the cause.

Both worker traces recorded no OOM, hard-limit or swap events. Sampled resident
plus kernel peaks were 5,046,218,752 and 4,926,734,336 bytes. These are initial
preparation measurements, not six-sealed placement or six-hour qualification.

The supervisor observed the terminal failed service, stopped the helper processes
and samplers, restored canonical worker state and restarted canonical serving.
Public and private restoration checks run separately; included partial reports
must not be mistaken for completion of all cases.

The fix gives only preparation requests a bounded 600-second deadline, in both
ordinary publication and committed-candidate recovery. Existing default control
and explicit query deadlines remain unchanged. The regression test demonstrates
that a slow response exceeds the client's default deadline but succeeds through
the preparation request path. Server library regression: 75 tests passed. The
new binary still requires deployment and a fresh sealed campaign.
