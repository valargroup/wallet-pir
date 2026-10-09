# Phase 6 rollback (runbook): restore the 12ce1291 controller and controller.json.
set -eu
. /root/deploy-a317455e/env.sh
install -m 0755 /srv/transparent-activity/build/evidence/release-12ce12918446eaa56e2d766ec2f43d82c531abb9/artifacts/transparent-publish-controller /usr/local/bin/transparent-publish-controller.next
mv /usr/local/bin/transparent-publish-controller.next /usr/local/bin/transparent-publish-controller
test "$(sha256sum < /usr/local/bin/transparent-publish-controller | cut -d' ' -f1)" = "$OLDC"
cp -p $DEP/controller.json.before /opt/transparent-publisher/v11/controller.json
systemctl restart transparent-publish-controller
