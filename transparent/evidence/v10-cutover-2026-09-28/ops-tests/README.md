# Schema-cutover cache isolation

The live worker's `Collect` operation prunes cache entries absent from its
active, prepared and retired v10 revisions. The running v9 collector likewise
does not retain newly prepared v10 entries. Disabling fixed-deploy pruning does
not protect either side from live collection.

The fleet deploy now accepts `TRANSPARENT_RUNTIME_CACHE_DIR` and preserves an
installed cache directory when the override is absent. Prepared-subset disk
headroom is checked on that directory's actual filesystem. This is an operations
change; the qualified Rust binary source remains `8e69ea75`.

The v10 rollout uses `/srv/transparent-pir/runtime-cache-v10` and the separate
static-set parent `/srv/transparent-pir/sets-v10`. Before the v10 publisher starts,
the old worker publication roots and active/revocation records are retained
outside the new collector's namespace. Rollback restores the old namespace and
records together with its binaries, units and routing.

Validation: 10 fleet tests, 9 worker-upgrade tests and 29 publication tests passed
using `python3 -m unittest discover -s transparent/ops/tests -p <file>`, for
`test_transparent_fleet.py`, `test_transparent_worker_upgrade.py` and
`test_transparent_publication.py`. Both changed shell files also passed `bash -n`.
The checks cover an explicit schema cache, later preservation, malformed paths,
actual rendered units, worker upgrade preservation and publication/rollback
behavior. Live staging and activation are separate validation steps.

The retained first attempt failed an existing Caddy compatibility test because
it scanned a source comment saying `health_fails` must not be used. The test now
excludes comment lines while still checking executable strings and the rendered
fixture. Its direct-script entry point was moved after the test class so direct
execution also includes that check. No Caddy configuration was changed.
