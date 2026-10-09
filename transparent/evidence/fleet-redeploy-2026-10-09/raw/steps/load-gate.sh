. /root/deploy-a317455e/env.sh
python3 -c "import json;l=json.load(open('$LOAD/status.json'));print(l['mode'],l['utc'],l['started_unix'],l['counts'],l['trailing_60s'],'incidents',l['incidents'],l['reasons'])"
test ! -e $LOAD/latched.json && echo no-latch
systemctl is-active transparent-5qps-continuous
