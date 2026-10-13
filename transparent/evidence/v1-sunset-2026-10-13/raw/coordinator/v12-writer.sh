#!/bin/bash
set -u
D=/etc/systemd/system/transparent-v12-publish-controller.service.d; P=/opt/transparent-publisher/v12/controller.json
exec 9>/run/lock/wallet-pir-production.lock; flock -n 9 || { echo "production lock held"; exit 1; }
cat > $D/journal-writer.conf <<EOF
# Since 2026-10-13 the v12 controller writes the shared journal (follow_journal false);
# the v11 controller that wrote it is retired.
[Unit]
Description=Continuous transparent PIR v12 publication (writes the shared journal)
[Service]
ReadOnlyPaths=
ReadOnlyPaths=/opt/transparent-publisher/credentials
ReadWritePaths=/srv/transparent-activity/full-v3/journal
EOF
systemctl daemon-reload
python3 -c "import json,os;p=\"$P\";c=json.load(open(p));c[\"follow_journal\"]=False;open(p+\".next\",\"w\").write(json.dumps(c,indent=1)+\"\n\");os.replace(p+\".next\",p)"
systemctl restart transparent-v12-publish-controller
ok=0
for i in $(seq 1 12); do
  sleep 5
  s=$(curl -s -m5 http://10.142.0.3:8096/v1/status)
  if echo "$s" | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get(\"phase\")==\"serving\" and not d.get(\"ingest_error\") else 1)" 2>/dev/null && fuser -s /srv/transparent-activity/full-v3/journal/writer.lock; then ok=1; break; fi
done
if [ $ok = 1 ]; then echo WRITER-OK; echo "$s" | head -c 300; echo
else echo WRITER-FAILED-REVERTING; journalctl -u transparent-v12-publish-controller -n 8 --no-pager -o cat
  cp -a /root/v1-sunset-20261013/v12-controller.json $P; systemctl restart transparent-v12-publish-controller; fi
