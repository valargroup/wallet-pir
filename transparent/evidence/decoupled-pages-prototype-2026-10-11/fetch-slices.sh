#!/usr/bin/env bash
# Stream the two read-only journal windows for the decoupled-pages simulation
# from the coordinator to the development hub, through this Mac. Run from
# Roman's Mac, which can reach both hosts:
#
#   bash transparent/evidence/decoupled-pages-prototype-2026-10-11/fetch-slices.sh
#
# The coordinator only reads, under nice -n 19 ionice -c3: meta.json,
# checkpoint.bin, the windows' blocks.bin records, each window's exact
# events.bin byte range, and the committed display sidecars of the windows'
# blocks (tar to stdout). It writes nothing there, not even a helper: both
# Python helpers travel on ssh stdin. No production lock is taken, and nothing
# is started, stopped, published or reconfigured. Agent forwarding is off.
#
# Every byte lands under DEST on the hub, where slice-hub.py rebases offsets,
# writes each window's checkpoint/meta/SHA256SUMS and provenance.json, and
# verifies the result. Windows must end 1,000 blocks below the committed tip,
# and the source block records are read again at the end and must not change.
set -euo pipefail

COORDINATOR=${COORDINATOR:-root@167.99.42.60}
HUB=${HUB:-ai-dev@157.245.239.19}
JOURNAL=${JOURNAL:-/srv/transparent-activity/full-v3/journal}
DEST=${DEST:-/mnt/roman-dev-2-data/ai-dev/evidence/decoupled-pages-journal}
WINDOWS=${WINDOWS:-"recent:3262749:3473686 archive:200000:419999"}
MARGIN=1000
# The design's disk budget for the slice.
BUDGET=$((60 * 1000 * 1000 * 1000))
RECORD=48
HERE=$(cd "$(dirname "$0")" && pwd)
REVISION=$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)
QUIET="nice -n 19 ionice -c3"

coord() { ssh -a -x -o BatchMode=yes "$COORDINATOR" "$@"; }
hub() { ssh -a -x -o BatchMode=yes "$HUB" "$@"; }
hub_py() { hub "python3 - $*" < "$HERE/slice-hub.py"; }
# Read bytes [skip, skip+count) of a coordinator journal file.
coord_range() {
  coord "$QUIET dd if=$JOURNAL/$1 iflag=skip_bytes,count_bytes skip=$2 count=$3 bs=4M status=none" < /dev/null
}
# Size of a file on the hub.
hub_size() { hub "stat -c %s $1" < /dev/null; }

echo "== preflight"
coord "test -r $JOURNAL/meta.json && test -r $JOURNAL/checkpoint.bin" \
  || { echo "journal not readable on the coordinator" >&2; exit 1; }
hub "test ! -e $DEST" \
  || { echo "$DEST already exists on the hub; move it aside first" >&2; exit 1; }
read -r hub_avail < <(hub "df -B1 --output=avail /mnt/roman-dev-2-data | tail -1")
echo "hub: $hub_avail bytes free"
(( hub_avail > BUDGET * 2 )) || { echo "less than twice the ${BUDGET}-byte budget free on the hub" >&2; exit 75; }

echo "== meta and committed checkpoint"
hub "mkdir -p $DEST/source"
coord "$QUIET cat $JOURNAL/meta.json" | hub "cat > $DEST/source/meta.json"
coord "$QUIET cat $JOURNAL/checkpoint.bin" | hub "cat > $DEST/source/checkpoint.bin"
window_args=""
for w in $WINDOWS; do window_args="$window_args --window $w"; done
plan=$(hub_py plan "$DEST" --margin "$MARGIN" $window_args)
echo "$plan"

echo "== block records"
while read -r name index count <&3; do
  hub "mkdir -p $DEST/$name"
  coord_range blocks.bin $((index * RECORD)) $((count * RECORD)) | hub "cat > $DEST/source/$name.records"
done 3<<< "$plan"
ranges=$(hub_py ranges "$DEST")
echo "$ranges"

echo "== sizes"
need=0
sidecar_counts=""
while read -r name index count <&3; do
  read -r begin length < <(echo "$ranges" | awk -v n="$name" '$1 == n { print $2, $3 }')
  read -r files bytes < <(coord "cd $JOURNAL && $QUIET python3 - size $index $((count - 1))" \
    < "$HERE/coordinator-sidecars.py")
  [ -n "${bytes:-}" ] || { echo "$name: could not size the sidecars" >&2; exit 1; }
  echo "$name: events $length B, $files sidecars $bytes B"
  need=$((need + length + bytes + count * RECORD))
  sidecar_counts="$sidecar_counts --sidecars $name=$files"
done 3<<< "$plan"
echo "total: $need B (budget $BUDGET B)"
(( need <= BUDGET )) || { echo "the slice exceeds the disk budget; not copying" >&2; exit 75; }
(( hub_avail > need * 2 )) || { echo "not enough space on the hub" >&2; exit 75; }

echo "== events and sidecars"
while read -r name index count <&3; do
  read -r begin length < <(echo "$ranges" | awk -v n="$name" '$1 == n { print $2, $3 }')
  echo "$name: events.bin [$begin, $((begin + length)))"
  coord_range events.bin "$begin" "$length" | hub "cat > $DEST/$name/events.bin"
  got=$(hub_size "$DEST/$name/events.bin")
  [ "$got" = "$length" ] || { echo "$name: events.bin is $got B, expected $length" >&2; exit 1; }
  echo "$name: sidecars"
  coord "cd $JOURNAL && $QUIET python3 - list $index $((count - 1)) | $QUIET tar -cf - --null -T -" \
    < "$HERE/coordinator-sidecars.py" \
    | hub "tar -C $DEST/$name --no-same-owner -xf -"
done 3<<< "$plan"

echo "== source block records unchanged"
while read -r name index count <&3; do
  before=$(hub "sha256sum $DEST/source/$name.records" | cut -d' ' -f1)
  after=$(coord_range blocks.bin $((index * RECORD)) $((count * RECORD)) | shasum -a 256 | cut -d' ' -f1)
  [ "$before" = "$after" ] || { echo "$name: source block records changed during the copy" >&2; exit 1; }
  echo "$name: $after"
done 3<<< "$plan"

echo "== assemble and verify on the hub"
hub_py finish "$DEST" --source "$COORDINATOR:$JOURNAL" --revision "$REVISION" $sidecar_counts
hub "cd $DEST && for w in \$(ls -d */ | grep -v source/); do \
  (cd \$w && sha256sum -c --quiet SHA256SUMS && sha256sum -c --quiet SIDECARS.SHA256SUMS && echo \"\$w ok\"); done"
echo "done: slices at $HUB:$DEST"
