#!/bin/bash
# Read-only page-cache warmer for the txid-4k8 bootstrap: reads every block sidecar
# in height order, starting 40% in (where the bootstrap first pass is), then wraps.
N=/srv/txid-display-genesis/journal-v3-snap-4k8
python3 - <<PY | xargs -0 -P 32 -n 256 cat >/dev/null 2>&1
import struct, sys
n = "$N"
ev, bl = struct.unpack("<QQ", open(n + "/checkpoint.bin", "rb").read())
raw = open(n + "/blocks.bin", "rb").read(bl)
names = [raw[i:i+32][::-1].hex() for i in range(0, len(raw), 48)]
cut = int(len(names) * 0.40)
out = sys.stdout.buffer
for name in names[cut:] + names[:cut]:
    out.write(("%s/display-v2x/%s.bin\0" % (n, name)).encode())
PY
echo PREFETCH_DONE
