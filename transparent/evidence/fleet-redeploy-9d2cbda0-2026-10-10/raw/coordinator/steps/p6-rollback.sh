# Restore the previous controller binary and controller.json saved by p6-swap-inner.sh, then restart.
set -eu
. /root/deploy-9d2cbda0/env.sh
install -m 0755 $DEP/transparent-publish-controller.before /usr/local/bin/transparent-publish-controller.next
mv /usr/local/bin/transparent-publish-controller.next /usr/local/bin/transparent-publish-controller
test "$(sha256sum < /usr/local/bin/transparent-publish-controller | cut -d" " -f1)" = "$OLDC"
cp -p $DEP/controller.json.before /opt/transparent-publisher/v11/controller.json
systemctl restart transparent-publish-controller
