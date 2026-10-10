# Runs under the production lock. Swap the controller binary, record source_sha (D5), restart.
set -eu
. /root/deploy-9d2cbda0/env.sh
systemctl stop transparent-5qps-continuous; systemctl is-active transparent-5qps-continuous || true
test "$(sha256sum < /usr/local/bin/transparent-publish-controller | cut -d' ' -f1)" = "$OLDC" || { echo "live controller binary is not OLDC; refusing"; exit 1; }
test ! -e $DEP/transparent-publish-controller.before
cp -p /usr/local/bin/transparent-publish-controller $DEP/transparent-publish-controller.before
cp -p /opt/transparent-publisher/v11/controller.json $DEP/controller.json.before
curl -sf https://transparent-pir.valargroup.dev/v1/shards > $DEP/map-before-controller.json
sha256sum $DEP/transparent-publish-controller.before $DEP/controller.json.before $DEP/map-before-controller.json
install -m 0755 $R/binaries/transparent-publish-controller /usr/local/bin/transparent-publish-controller.next
mv /usr/local/bin/transparent-publish-controller.next /usr/local/bin/transparent-publish-controller
sha256sum /usr/local/bin/transparent-publish-controller
python3 - $NEW_SHA <<'PY'
import json, os, shutil, sys
p = '/opt/transparent-publisher/v11/controller.json'; c = json.load(open(p)); old = c['source_sha']; c['source_sha'] = sys.argv[1]
t = p + '.tmp'; open(t, 'w').write(json.dumps(c, indent=2, sort_keys=True) + '\n'); shutil.copymode(p, t); os.replace(t, p)
print('source_sha', old, '->', sys.argv[1])
PY
diff $DEP/controller.json.before /opt/transparent-publisher/v11/controller.json || true
date -u +%s.%N > $DEP/controller-restart-start; cat $DEP/controller-restart-start
systemctl restart transparent-publish-controller
date -u +%s.%N > $DEP/controller-restart-returned; cat $DEP/controller-restart-returned
