# M1 forwarded-status diagnosis — 2026-09-10

This is diagnostic evidence, not acceptance or a deployed control-path change.
The prior turn qualified worker `d8f5217`; the live recent-01 worker remains
`f2f351c` with operations `85d76e8`.

## New discriminating observation

At 21:00:05.56 UTC, coordinator control status, an SSH no-op and remote HTTP
readiness each took approximately 0.77–0.79 seconds. During that interval,
worker-local Unix status at 21:00:05.625 and 21:00:06.127 completed in 1.93 ms and
1.36 ms. Worker-local HTTP readiness took 704 ms. The captured thread samples
and these complete raw probe rows are in `forward-capture/`.

This event demonstrates delay outside the Unix status handler. It does not
identify the cause of the earlier multi-second canary timeout. Candidate causes
include remote helper startup, SSH channel handling and host I/O or scheduling;
filesystem work in HTTP readiness is a separate possible source of latency.
Do not equate the collection regression with this event or claim all stalls
share a cause.

## Bounded comparison

Two root-only Unix forwards carry status directly to the existing worker control
socket, without spawning `shard-control` for each request. The comparison uses:

- A separate owned SSH process, `transparent-m1-forward-trial.service`, for
  30 minutes; `/run/transparent-m1-forward/status.sock`.
- A dynamic forward through the already-running production SSH master,
  `transparent-m1-owned-forward-trial-v2.service`, for 20 minutes. Its `t-forward`
  socket is in the master's existing private `control_dir`. This controls for
  the separate connection in the first trial.

Both local directories are private and socket mode 0600 is checked. Requests
are read-only, bounded to four seconds, use newline JSON with the existing
1 MiB response limit, and open one stream per request. The existing production
control code, routing and status budgets are unchanged. The same-master trial
cancels its own forward on normal completion; it does not stop the master.
If that probe is forcibly stopped, cancel its `-L` mapping with `ssh -O cancel`
using the same master arguments before removing its local socket.

The initial same-master attempt used `/run/transparent-m1-forward/owned.sock`;
the master's service correctly refused the write because that directory is
read-only in its mount namespace. No forward was created. The v2 trial uses
its already-authorized private directory. The original and corrected scripts
are both preserved; no service hardening was relaxed.

At the 21:11:54 UTC capture, both corrected trials were active:

| Path | Samples | Errors | Median | Maximum |
|---|---|---|---|---|
| Separate SSH connection | 740 | 0 | 2.29 ms | 163.54 ms |
| Existing SSH master | 121 | 0 | 2.08 ms | 7.81 ms |

These are initial samples, not a matched tail-latency result. Compare the
forwarding records against simultaneous legacy `stall-probe.ndjson` events
before attributing an improvement. Full outputs remain under
`/opt/transparent-publisher-build/sessions-20260910/` on the coordinator.
The original diagnostic load, local status probes and worker thread probe
continue independently.

## Prepared implementation

The opt-in `status_socket_forwarding` flag requires `control_sessions`. It adds
one root-only Unix forward per owned master and sends only status over it.
Mutation handling is unchanged. Newline framing, the 1 MiB response limit and
existing 1 s / 1.5 s status attempts remain bounded; there is no helper fallback.
Cancelled requests close their own stream. Reconnection removes refused stale
sockets and refuses to overwrite ordinary files, symlinks or live listeners.
The flag is disabled by default and has not been enabled in production.

All 86 transparent operations tests passed, including new framing, rejection,
timeout, cancellation and stale-socket tests. `make check` passed all 588 Rust
tests with zero failures and two manual benchmarks ignored, plus the repository
formatting, Clippy, operations, documentation and report checks.

`integration.py` tested the actual new module in a separate private namespace
on the coordinator. Its socket was mode 0600; startup took 1.00 s, a forced exit
of its own SSH master recovered in 1.34 s, and supervisor shutdown closed the
forward. The production master was not restarted. `integration.json` binds
these results to the exact script SHA-256. Tight consecutive requests in this
integration took 1.3–45.4 ms; this is distinct from the spaced diagnostic samples.

## Remaining diagnosis and acceptance

Later samples show shared delays too: at 21:16:09, legacy status took 0.88 s,
a forwarded request beginning later took 0.43 s, and worker-local Unix status
also took about 0.41 s. Therefore forwarding does not eliminate every observed
pause. Thread records show a snapshot writer in dirty-page throttling and fsync.
The probes themselves write to the worker disk, so logging and scheduling delay
must be separated from socket duration before assigning causality. The 21:16
window has been inspected remotely; its complete raw capture remains pending.
Do not claim this optional transport is the sole M1 root-cause fix.

Continue the bounded comparison, preserve final results, and remove disk logging
from the latency probes. Then qualify the selected live configuration and run
the matching deployment and full M1 acceptance gates. The six-hour AND 300-block
canary and 24-hour fleet observation remain open.
