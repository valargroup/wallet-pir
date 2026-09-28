#!/usr/bin/env python3
"""Compare two v2 event journals over the heights the smaller one covers.

Usage: journal_cmp.py BIG SMALL
Every height SMALL commits must be in BIG with the same block hash, event count
and event bytes. Exits non-zero on the first difference.
"""
import json, struct, sys

def load(d):
    meta = json.load(open(f"{d}/meta.json"))
    assert meta["version"] == 2, (d, meta)
    ev_len, bl_len = struct.unpack("<QQ", open(f"{d}/checkpoint.bin", "rb").read())
    raw = open(f"{d}/blocks.bin", "rb").read()[:bl_len]
    recs = [struct.unpack("<32sQQ", raw[i:i + 48]) for i in range(0, len(raw), 48)]
    return meta, recs, ev_len

def block_bytes(d, recs, ev_len, i, f):
    start = recs[i][1]
    end = recs[i + 1][1] if i + 1 < len(recs) else ev_len
    f.seek(start)
    return f.read(end - start)

big, small = sys.argv[1], sys.argv[2]
bm, br, bl = load(big)
sm, sr, sl = load(small)
assert bm["genesis_hash"] == sm["genesis_hash"]
events = 0
with open(f"{big}/events.bin", "rb") as bf, open(f"{small}/events.bin", "rb") as sf:
    for j in range(len(sr)):
        h = sm["start_height"] + j
        i = h - bm["start_height"]
        if not (0 <= i < len(br)):
            sys.exit(f"height {h} not covered by {big}")
        if br[i][0] != sr[j][0] or br[i][2] != sr[j][2]:
            sys.exit(f"height {h}: hash or count differs")
        if block_bytes(big, br, bl, i, bf) != block_bytes(small, sr, sl, j, sf):
            sys.exit(f"height {h}: event bytes differ")
        events += sr[j][2]
print(json.dumps({"small": small, "from": sm["start_height"],
                  "through": sm["start_height"] + len(sr) - 1,
                  "blocks": len(sr), "events": events, "identical": True}))
