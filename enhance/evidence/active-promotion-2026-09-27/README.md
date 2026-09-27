# Active alerting promotion — September 27, 2026

The user explicitly requested promotion from shadow to real mode now. APM and
the external monitor were promoted with their existing binaries, credentials,
and incident/outbox databases retained. No serving service was restarted.

This is an **early promotion**, not a completed shadow qualification. At review,
the latest Status-rule window contained roughly 21 minutes of observations,
with no active incidents and transient startup/init/connectivity failures that
had recovered. The previous 24-hour gate was not completed. This exception is
recorded in `promotion.json`; the user was informed before the mode changes.

- APM switched at 07:01 UTC (11:01 Dubai).
- External monitor switched at 07:02 UTC (11:02 Dubai). Its environment file
  overrides the systemd Environment directive, so both mode settings were updated.
- Both public status endpoints subsequently reported `shadow=false`, healthy
  progress, no active incidents or unknown checks, and empty delivery queues.
- One labeled TEST FIRED / TEST RECOVERED pair per service was queued through
  the real durable outbox. Both queues drained and recorded fresh successful
  Slack delivery timestamps. Human receipt confirmation was requested separately;
  it had not yet been received when this record was written.
- Active mode enables durable rule notifications and disables legacy APM sending
  to avoid duplicate notifications. Warning/critical firing, recovery and configured
  critical reminders now use the durable delivery path.

The active observer started **07:04:40 UTC / 11:04 Dubai on September 27** and
runs for 72 hours as `pir-observability-active-20260927.service` on the monitor.
It writes `/var/lib/pir-monitor/active-evidence-20260927/`. Earliest 24-hour active
review is **September 28 at 07:04 UTC / 11:04 Dubai**. This record does not claim
that active qualification is complete. The earlier shadow evidence is preserved.

Rollback configuration copies remain mode 0600 under
`/root/alert-promotion-20260927T0701/` on the corresponding hosts. Restore the
APM drop-in; on the monitor restore the unit and the environment-file mode.
Reload systemd and restart only the respective monitoring service. Preserve the
SQLite databases and their WAL files. These operational settings are intentionally
not changes to the repository's default-shadow deployment templates.
