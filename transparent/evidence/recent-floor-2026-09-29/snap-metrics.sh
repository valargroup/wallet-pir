#!/bin/bash
# Snapshot worker counters used by the gate analysis (read-only).
for h in "$@"; do echo "== $h"; curl -s --max-time 3 http://$h:8093/metrics | grep -E "^transparent_shard_(queries_total|query_slot_busy_microseconds_total|query_slots|queue_rejections_total|overloads_total|deadline_exceeded_total|query_errors_total)" | sed -E "s/\{[^}]*\}//"; done
