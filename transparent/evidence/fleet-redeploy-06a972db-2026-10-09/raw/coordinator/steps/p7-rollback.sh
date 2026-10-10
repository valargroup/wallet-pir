# Phase 7 rollback (runbook): restore archive-03's previous binary, shard-control and unit (another ~5.5 min outage).
. /root/deploy-06a972db/env.sh
id=transparent-pir-archive-03; h=$A03
$W root@$h "set -eu; B=$OUT/$id; test -f \$B/worker.service
 install -m755 \$B/transparent-shard-server /usr/local/bin/transparent-shard-server.rollback
 mv /usr/local/bin/transparent-shard-server.rollback /usr/local/bin/transparent-shard-server
 install -m755 \$B/shard-control /usr/local/bin/shard-control
 install -m644 \$B/worker.service /etc/systemd/system/transparent-shard-server.service
 systemctl daemon-reload; systemctl restart transparent-shard-server"
