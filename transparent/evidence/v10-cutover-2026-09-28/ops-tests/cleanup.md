# Follow-up: fixed-fleet cache cleanup

The non-cutover fixed-fleet cleanup path also used a hardcoded default runtime
cache. It now reads the installed worker unit's directory, just as preparation
and rendering use the configured directory. This prevents a later deployment
from pruning an old schema's cache while leaving the active schema's cache
uncollected. The v10 cutover itself continues to defer fixed-fleet pruning.

All 11 fleet tests passed. The added test executes the remote cleanup shell
with a synthetic unit and a mocked privileged command, then verifies that the
actual prune invocation names the v10 cache. Shell syntax and whitespace checks
also passed. [Raw test log](cleanup-passed.log).
