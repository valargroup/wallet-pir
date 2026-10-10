# One line per minute of the 5 QPS load's trailing-60s counts; exits on latch or after N minutes.
n=${1:-5}
for i in $(seq 1 $n); do
  sleep 60
  jq -r '"\(.utc[11:19]) \(.mode) exact=\(.trailing_60s.exact) errors=\(.trailing_60s.errors) missed=\(.trailing_60s.missed_slots) p99=\(.trailing_60s.http_p99_seconds) fresh=\(.publisher.freshness_seconds|floor) reasons=\(.reasons)"' /srv/transparent-activity/canonical-load/v11/status.json
  test -e /srv/transparent-activity/canonical-load/v11/latched.json && { echo LATCHED; exit 1; }
done
