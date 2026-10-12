#!/bin/bash
# Build the initial v12 publication from the reflinked journal snapshot.
set -euo pipefail
B=/opt/transparent-publisher/v12/releases/fb90519b/bin
nice -n 15 ionice -c3 /usr/bin/time -v -o /opt/transparent-publisher/v12/deploy-20261012/s1.time $B/shard-publish \
  --data-dir /srv/transparent-activity/full-v12/journal-snapshot \
  --output /srv/transparent-activity/full-v12/publications/initial \
  --archive-geometry decoupled-32k-32k --recent-geometry decoupled-2k-8k \
  --recent-from 3076108 --directory-choice all --range-profile zcash-transparent-range-v2 \
  --zakura-cookie /root/.cache/zakura/.cookie \
  --record /opt/transparent-publisher/v12/deploy-20261012/s1-record.json --source-sha fb90519b
echo S1_DONE
