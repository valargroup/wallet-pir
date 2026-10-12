#!/bin/bash
# Render the two v12 worker units for the v12 host.
set -euo pipefail
M=1261e06fb7aee008b41bebc3fe53c7007f1ff5d84bb4a7cd56065e7f43fa611f
DIR=/srv/transparent-pir/v12/publications/$M
unit() { # name id port cache_bytes cache_disk memhigh memmax
cat <<UNIT
[Unit]
Description=Transparent PIR v12 shard worker ($1)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
RuntimeDirectory=transparent-pir
RuntimeDirectoryPreserve=yes
ExecStart=/opt/transparent-pir/v12/bin/transparent-shard-server --listen 0.0.0.0:$3 --shard-dir $DIR --cache-bytes $4 --query-slots 2 --assignment $DIR/assignment.json --worker-id $2 --prune-excess --runtime-cache-dir /srv/transparent-pir/v12/runtime-cache-$1 --runtime-cache-max-bytes $5 --control-socket /run/transparent-pir/v12-$1.sock --active-record /opt/transparent-publisher/v12/$1-active.json --build-slots 1
Restart=on-failure
RestartSec=10
LimitNOFILE=1048576
Environment=RUST_LOG=info
MemoryHigh=$6
MemoryMax=$7
MemorySwapMax=0

[Install]
WantedBy=multi-user.target
UNIT
}
unit archive transparent-pir-v12-archive 8093 47244640256 94489280512 46G 48G > /root/v12-units/transparent-v12-archive.service
unit recent transparent-pir-v12-recent 8193 6442450944 12884901888 7G 8G > /root/v12-units/transparent-v12-recent.service
