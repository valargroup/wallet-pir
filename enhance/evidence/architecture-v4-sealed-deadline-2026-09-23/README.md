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
All four public/private restoration cases completed successfully; their
reports and completion marker are included.

The fix gives only preparation requests a bounded 600-second deadline, in both
ordinary publication and committed-candidate recovery. Existing default control
and explicit query deadlines remain unchanged. The regression test demonstrates
that a slow response exceeds the client's default deadline but succeeds through
the preparation request path. Server library regression: 75 tests passed. The
new binary has now been deployed as described below.

## Fixed candidate deployment and new campaign

Revision `b1863b1d270df52d213d7dd389e5b8bf96bca224` was built in the local Linux
container with locked offline dependencies and the full release profile. All
65 selected Rust source/Cargo inputs matched the committed checkout. Unchanged
CLI/load-test binaries were reused from the earlier verified clean build. The
bundle was assembled and independently extracted with the release verifier, then
checksummed and its native CLI executed on the coordinator and both workers.
`identity.json` and `build-inputs.json` record provenance; this is not remote CI.

Services and worker sampling receipts now name this revision and its binary hash.
Canonical public load after deployment passed 460 measured and 48 warmup answers,
zero wrong answers/errors, with 234.623 ms measured p99 at concurrency two.

A fresh six-sealed campaign is running as
`enhance-pir-v4-sealed-retry-campaign.service`, initially `building`. Its directory
is `/srv/enhance-pir-v4/validation/sealed-retry`, and worker samplers write
`/srv/enhance-pir-v4/validation/sealed-retry-samples`. It requests 21,600 measured
seconds and 300 publications. Canonical serving is temporarily stopped again,
with state preserved in `worker.canonical`; the supervisor restores it afterward.
Coordinator-hosted helper replicas remain test support, not qualified c-4 hosts.
The failed synthetic worker directories were removed only after retaining their
measurement evidence, to restore at least 32 GiB free disk before this new run.
No passing sealed qualification result exists yet.
