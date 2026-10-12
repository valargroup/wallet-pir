#!/bin/bash
# Deploy the v12 publish controller built at 322c4f07: init is answered through a
# publication switch instead of 503. v12 only; v11's units are not touched.
# Other v12 binaries stay at releases/fb90519b.
set -euo pipefail
R=/opt/transparent-publisher/v12/releases/322c4f07
B=/srv/wallet-pir-build-322c4f07/target/release/transparent-publish-controller
D=/etc/systemd/system/transparent-v12-publish-controller.service.d
exec 9>/run/lock/wallet-pir-production.lock
flock -w 120 9
date -u
install -d "$R/bin"
install -m 0755 "$B" "$R/bin/transparent-publish-controller"
(cd "$R/bin" && sha256sum transparent-publish-controller > ../SHA256SUMS)
cat "$R/SHA256SUMS"
install -d "$D"
printf '[Service]\nExecStart=\nExecStart=%s/bin/transparent-publish-controller --config /opt/transparent-publisher/v12/controller.json\n' "$R" > "$D/release.conf"
systemctl daemon-reload
systemctl restart transparent-v12-publish-controller
sleep 5
systemctl show transparent-v12-publish-controller -p ActiveState -p MainPID -p ExecMainStartTimestamp -p ExecStart | cut -c1-200
