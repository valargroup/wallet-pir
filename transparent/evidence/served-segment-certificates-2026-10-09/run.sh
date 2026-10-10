#!/usr/bin/env bash
# Certify every served Transparent segment (history v11 and txid display v2) at
# 49-bit nearest and 44-bit dithered. Runs on the bench host; reads segment
# bytes from the coordinator under ionice/nice. Needs an SSH agent (ssh -A).
set -uo pipefail
C=root@167.99.42.60
H=/srv/transparent-activity/full-v11/publications
X=/srv/txid-display-genesis/v2/root
TOOL=/root/wallet-pir-06a972db/target/release-fast/examples/native_certificate
CERT=/root/ipir-sp-d76e61a/reinspiring/tools/security/certify_native.py
OUT=${OUT:-/root/served-certs-06a972db}
mkdir -p "$OUT/inputs" "$OUT/reports"; cd "$OUT"
CM=(-o BatchMode=yes -o ControlMaster=auto -o ControlPath=/tmp/served-certs-cm -o ControlPersist=600)
r() { ssh -n "${CM[@]}" "$C" "$@"; }
date -u +%FT%TZ >started
r "cat $H/active.json" >inputs/history-active.json
r "cat $X/active.json" >inputs/txid-active.json
HD=$(jq -r .directory inputs/history-active.json)
XD=$(jq -r .directory inputs/txid-active.json)
r "cat $HD/shards.json" >inputs/history-shards.json
r "cat $HD/publication.json" >inputs/history-publication.json
r "cat $XD/txid-shards.json" >inputs/txid-shards.json
python3 - >inputs/plan.tsv <<'EOF'
# Unsealed tips first: their candidate directories are pruned within minutes.
import json
rows = []
for s in json.load(open('inputs/history-shards.json'))['shards']:
    for t, n in (('directory', s['directory_segments']), ('pages', s['page_segments'])):
        rows += [(s['sealed'], 'history', s['shard_id'], s['geometry'], t, i, s['manifest_digest'], f'{t}.{i}.bin') for i in range(n)]
for s in json.load(open('inputs/txid-shards.json'))['shards']:
    assert s['n_buckets'] == 1
    rows += [(s['sealed'], 'txid', s['shard_id'], s['geometry'], 'txdirectory', i, s['manifest_digest'], f'directory-0.{i}.bin') for i in range(s['directory_segments'][0])]
for x in sorted(rows, key=lambda x: x[0]):
    print('\t'.join(map(str, x[1:])))
EOF
: >failures.tsv
while IFS=$'\t' read -r prod shard geom table i d file; do
  if [ "$prod" = history ]; then dirs="$HD/$d $H/initial/$d"; else dirs="$X/sealed/$d $XD/$d $X/recent/$d"; fi
  get() { r "for p in $dirs; do [ -f \$p/$file ] && exec ionice -c3 nice -n19 cat \$p/$1; done; exit 3"; }
  if ! get manifest.json >/dev/shm/m.json || ! get "$file" >/dev/shm/seg.bin; then
    printf '%s\t%s\t%s\t%s\tfetch\n' "$prod" "$shard" "$table" "$d" >>failures.tsv; continue
  fi
  if ! python3 - "$d" "$prod" "$table" "$i" <<'EOF'
import hashlib, json, sys
d, prod, t, i = sys.argv[1:]
raw = open('/dev/shm/m.json', 'rb').read(); m = json.loads(raw)
assert hashlib.sha256(raw).hexdigest() == d, 'manifest digest'
segs = m['buckets'][0]['directory_segments'] if prod == 'txid' else m[{'directory': 'directory_segments', 'pages': 'page_segments'}[t]]
h = hashlib.sha256(); f = open('/dev/shm/seg.bin', 'rb')
for b in iter(lambda: f.read(1 << 24), b''): h.update(b)
assert h.hexdigest() == segs[int(i)]['sha256'], 'segment bytes'
EOF
  then printf '%s\t%s\t%s\t%s\tverify\n' "$prod" "$shard" "$table" "$d" >>failures.tsv; continue; fi
  for q in nearest dithered; do
    n=reports/$prod-s$shard-$table.$i-${d:0:12}-$q
    /usr/bin/time -f "%e s %M KiB $n" "$TOOL" segment --geometry "$geom" --table "$table" --rows-bin /dev/shm/seg.bin --query-rounding $q >"$n.report.json" 2>>native.log </dev/null \
      || printf '%s\t%s\t%s\t%s\treport-%s\n' "$prod" "$shard" "$table" "$d" "$q" >>failures.tsv
  done
  rm -f /dev/shm/seg.bin /dev/shm/m.json
done <inputs/plan.tsv
ls reports/*.report.json | xargs -P8 -n1 sh -c 'python3 '"$CERT"' "$0" >"${0%.report.json}.certificate.json" 2>"${0%.report.json}.certify.err" || echo "$0" >>certify-failures.txt'
date -u +%FT%TZ >finished
echo "done: $(ls reports/*.certificate.json | wc -l) certificates, $(wc -l <failures.tsv) segment failures"
