# Read-only fleet health: membership, controller, load, worker readiness.
. /root/deploy-06a972db/env.sh
python3 $DEP/steps/membership.py
curl -s 127.0.0.1:8094/v1/status | python3 -c 'import json,sys;s=json.load(sys.stdin);print("controller",s["phase"],s["public_height"],s["node_height"],s["freshness_seconds"])'
echo "load $(systemctl is-active transparent-5qps-continuous) scaler $(systemctl is-active transparent-fleet-scaler) $(test -e $LOAD/latched.json && echo LATCHED || echo not-latched)"
python3 -c 'import json;s=json.load(open("/srv/transparent-activity/canonical-load/v11/status.json"));print("load-status",s["mode"],s["utc"],{k:s[k] for k in s if k in ("trailing_minute","window","last_minute")})' 2>&1 | cut -c1-400
for h in $R01 $R02 $A03; do curl -s -m5 http://$h:8093/v1/ready | python3 -c 'import json,sys;r=json.load(sys.stdin);print(sys.argv[1],r["binary_sha256"][:12],r["ready"],r["mode"],r["warm_runtimes"],r["target_runtimes"],r["map_sha256"][:12],"prewarm_failed",r.get("prewarm_failed"))' $h; done
