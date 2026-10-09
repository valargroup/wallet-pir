# Follow archive-03's service journal (filtered) into a local log for the restart window.
. /root/deploy-a317455e/env.sh
$W root@$A03 'journalctl -u transparent-shard-server -f -n 0 -o short-iso | grep --line-buffered -E "Stopping|Stopped|Started|loaded shard set|prewarm|serving|listen|cold|build|restor|cache|error|ERROR|WARN|panic"' >> $DEP/archive-journal.log 2>&1
