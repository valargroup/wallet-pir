#!/usr/bin/env bash
# Copy the two read-only journal windows for the decoupled-pages simulation from
# the coordinator to the development hub. Run from Roman's Mac, which can reach
# both hosts:
#
#   bash transparent/evidence/decoupled-pages-prototype-2026-10-11/fetch-slices.sh
#
# On the coordinator this only reads /srv/transparent-activity/full-v3/journal
# (below its committed checkpoint and 1,000 blocks below its tip, without the
# writer lock), writes /srv/claude-decoupled-slices and /root/decoupled-slice.py,
# and removes both once the hub copy is verified. It takes no production lock
# (/run/lock/wallet-pir-production.lock is never opened), and starts, stops,
# publishes or reconfigures nothing. The slicer and the copy run under
# nice -n 19 and ionice -c3.
#
# The copy goes straight from the coordinator to the hub with your forwarded
# SSH agent. Set RELAY=1 to stream it through this Mac instead.
set -euo pipefail

COORDINATOR=${COORDINATOR:-root@167.99.42.60}
HUB=${HUB:-ai-dev@157.245.239.19}
JOURNAL=/srv/transparent-activity/full-v3/journal
STAGE=/srv/claude-decoupled-slices
SLICER=/root/decoupled-slice.py
DEST=/mnt/roman-dev-2-data/ai-dev/evidence/decoupled-pages-journal
WINDOWS="--window recent:3262749:3473686 --window archive:200000:419999"
# About 14 GB of events plus block records and sidecars, with margin.
NEED_BYTES=$((20 * 1000 * 1000 * 1000))
HERE=$(cd "$(dirname "$0")" && pwd)

echo "== preflight"
ssh "$COORDINATOR" "test -f $JOURNAL/meta.json && test ! -e $STAGE && test ! -e $SLICER" \
  || { echo "journal missing, or $STAGE / $SLICER already exists on the coordinator" >&2; exit 1; }
ssh "$HUB" "test ! -e $DEST" \
  || { echo "$DEST already exists on the hub; move it aside first" >&2; exit 1; }
read -r avail size < <(ssh "$COORDINATOR" "df -B1 --output=avail,size /srv | tail -1")
echo "coordinator /srv: $avail bytes free of $size"
# Keep 20% of the filesystem free after staging, as the slicer also checks.
if (( avail - NEED_BYTES < size / 5 )); then
  echo "staging ~$NEED_BYTES bytes would leave less than 20% of /srv free; not starting" >&2
  exit 75
fi
read -r hub_avail < <(ssh "$HUB" "df -B1 --output=avail /mnt/roman-dev-2-data | tail -1")
echo "hub: $hub_avail bytes free"
(( hub_avail > NEED_BYTES * 2 )) || { echo "not enough space on the hub" >&2; exit 75; }

echo "== slice on the coordinator"
ssh "$COORDINATOR" "cat > $SLICER" < "$HERE/slice-journal.py"
ssh "$COORDINATOR" "nice -n 19 ionice -c3 python3 $SLICER --journal $JOURNAL --out $STAGE $WINDOWS"

echo "== copy to the hub"
ssh "$HUB" "mkdir -p $DEST"
if [ -n "${RELAY:-}" ]; then
  ssh "$COORDINATOR" "nice -n 19 ionice -c3 tar -C $STAGE -cf - ." | ssh "$HUB" "tar -C $DEST -xf -"
else
  ssh -A "$COORDINATOR" "nice -n 19 ionice -c3 rsync -a --partial \
    -e 'ssh -o StrictHostKeyChecking=accept-new' $STAGE/ $HUB:$DEST/"
fi

echo "== verify on the hub"
# Each window's SHA256SUMS was written by the slicer on the coordinator.
ssh "$HUB" "cd $DEST && for w in recent archive; do (cd \$w && sha256sum -c SHA256SUMS && \
  echo \"\$w sidecars: \$(find . -path './display-*' -type f | wc -l)\"); done && sha256sum */*.bin */meta.json provenance.json"

echo "== remove the staging copy from the coordinator"
ssh "$COORDINATOR" "rm -rf $STAGE $SLICER"
echo "done: slices at $HUB:$DEST"
