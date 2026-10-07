#!/usr/bin/env python3
"""Independent Enhance record oracle built only from the node's verbose JSON RPC.

It does not use zakura-chain or any Enhance code. For each Ironwood action it
derives the 653-byte record (encCiphertext[52:580] | cv | outCiphertext | flags |
expiry u32le | fee u64le) and the whole-transaction fee from prevout values,
transparent outputs and every pool's value balance. Positions come from the
journal manifest (block first_position + action index) and are cross-checked by
action counts.

Usage:
  record_oracle.py compare --journal DIR [--heights A-B ...] [--sample N --seed S] [--json-out F]
  record_oracle.py oracle-file --journal DIR --positions-from OLD.json --out NEW.json
"""
import argparse, bisect, hashlib, json, os, random, struct, sys, urllib.request, base64

RPC = os.environ.get("ORACLE_RPC_URL", "http://127.0.0.1:8232")
COOKIE = os.environ.get("ORACLE_RPC_COOKIE", "/root/.cache/zakura/.cookie")
R = 653
MAX_MONEY = 21_000_000 * 100_000_000


def _auth():
    with open(COOKIE) as f:
        return "Basic " + base64.b64encode(f.read().strip().encode()).decode()


AUTH = None


def rpc_batch(calls):
    global AUTH
    AUTH = AUTH or _auth()
    body = json.dumps([{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
                       for i, (m, p) in enumerate(calls)]).encode()
    req = urllib.request.Request(RPC, body, {"content-type": "application/json", "authorization": AUTH})
    out = json.load(urllib.request.urlopen(req, timeout=120))
    by_id = {r["id"]: r for r in out}
    res = []
    for i in range(len(calls)):
        r = by_id[i]
        if r.get("error"):
            raise RuntimeError(f"{calls[i]}: {r['error']}")
        res.append(r["result"])
    return res


class Prevouts:
    def __init__(self):
        self.cache = {}

    def values(self, txids):
        need = [t for t in dict.fromkeys(txids) if t not in self.cache]
        for i in range(0, len(need), 128):
            chunk = need[i:i + 128]
            for t, tx in zip(chunk, rpc_batch([("getrawtransaction", [t, 1]) for t in chunk])):
                assert tx["txid"] == t
                self.cache[t] = [o["valueZat"] for o in tx["vout"]]
        return self.cache


def load_journal(d):
    m = json.load(open(os.path.join(d, "manifest.json")))
    blocks = {b["height"]: b for b in m["blocks"]}
    return m, blocks, open(os.path.join(d, "records.bin"), "rb")


def read_record(f, pos):
    f.seek(pos * R)
    b = f.read(R)
    assert len(b) == R
    return b


def block_records(height, prev):
    """Return (block_hash, [(record_bytes, fee_or_None, coinbase)]) for one height."""
    (blk,) = rpc_batch([("getblock", [str(height), 2])])
    txs = [t for t in blk["tx"] if (t.get("ironwood") or {}).get("actions")]
    spent = [v["txid"] for t in txs for v in t["vin"] if "coinbase" not in v]
    vals = prev.values(spent) if spent else {}
    out = []
    for t in txs:
        iw = t["ironwood"]
        coinbase = any("coinbase" in v for v in t["vin"])
        if coinbase:
            fee = None
        else:
            seen = set()
            vin = 0
            for v in t["vin"]:
                key = (v["txid"], v["vout"])
                assert key not in seen, "duplicate input"
                seen.add(key)
                vin += vals[v["txid"]][v["vout"]]
            vout = sum(o["valueZat"] for o in t["vout"])
            js = sum(j.get("vpub_newZat", 0) - j.get("vpub_oldZat", 0) for j in t.get("vjoinsplit", []))
            fee = (vin - vout + js + t.get("valueBalanceZat", 0)
                   + (t.get("orchard") or {}).get("valueBalanceZat", 0) + iw["valueBalanceZat"])
            assert 0 <= fee <= MAX_MONEY, f"invalid fee {fee} in {t['txid']}"
        flags = (1 if t["vin"] else 0) | (2 if t["vout"] else 0) | (4 if fee is not None else 0)
        expiry = t.get("expiryheight", 0) or 0
        for a in iw["actions"]:
            enc = bytes.fromhex(a["encCiphertext"])
            assert len(enc) == 580
            rec = (enc[52:] + bytes.fromhex(a["cv"]) + bytes.fromhex(a["outCiphertext"]) + bytes([flags])
                   + struct.pack("<I", expiry) + struct.pack("<Q", fee or 0))
            assert len(rec) == R
            out.append((rec, fee, coinbase))
    return blk["hash"], out


def diff_kind(expected, actual):
    if expected == actual:
        return "equal"
    e, a = bytearray(expected), bytearray(actual)
    e[640] &= ~4; a[640] &= ~4
    e[645:653] = a[645:653] = b"\0" * 8
    return "fee-only" if e == a else "non-fee"


def compare(args):
    m, blocks, f = load_journal(args.journal)
    heights = set()
    for spec in args.heights or []:
        a, b = map(int, spec.split("-"))
        heights.update(h for h in range(a, b + 1) if h in blocks)
    if args.absent_blocks:
        starts = sorted((b["first_position"], h) for h, b in blocks.items() if b["action_count"])
        keys = [k for k, _ in starts]
        for pos in range(m["tree_size"]):
            if pos % 50000 == 0:
                f.seek(pos * R); buf = f.read(R * 50000)
            if not buf[(pos % 50000) * R + 640] & 4:
                heights.add(starts[bisect.bisect_right(keys, pos) - 1][1])
    if args.sample:
        nonempty = sorted(h for h, b in blocks.items() if b["action_count"])
        heights.update(random.Random(args.seed).sample(nonempty, min(args.sample, len(nonempty))))
    prev = Prevouts()
    stats = {"blocks": 0, "records": 0, "equal": 0, "fee_only": 0, "non_fee": 0,
             "with_fee": 0, "coinbase_absent": 0, "mixed_with_fee": 0, "bad": []}
    for h in sorted(heights):
        jb = blocks[h]
        bhash, recs = block_records(h, prev)
        assert bhash == jb["hash"], f"hash mismatch at {h}"
        assert len(recs) == jb["action_count"], f"action count mismatch at {h}"
        stats["blocks"] += 1
        for i, (rec, fee, cb) in enumerate(recs):
            pos = jb["first_position"] + i
            k = diff_kind(rec, read_record(f, pos))
            stats["records"] += 1
            stats[{"equal": "equal", "fee-only": "fee_only", "non-fee": "non_fee"}[k]] += 1
            stats["with_fee"] += fee is not None
            stats["coinbase_absent"] += cb
            stats["mixed_with_fee"] += fee is not None and rec[640] & 3 != 0
            if k != "equal" and len(stats["bad"]) < 20:
                stats["bad"].append({"height": h, "position": pos, "kind": k})
    stats["journal_tree_size"] = m["tree_size"]
    print(json.dumps(stats, indent=1))
    if args.json_out:
        json.dump(stats, open(args.json_out, "w"), indent=1)
    stats["absent_not_coinbase"] = stats["records"] - stats["with_fee"] - stats["coinbase_absent"]
    sys.exit(0 if stats["fee_only"] == stats["non_fee"] == stats["absent_not_coinbase"] == 0 else 1)


def oracle_file(args):
    m, blocks, f = load_journal(args.journal)
    old = json.load(open(args.positions_from))
    starts = sorted((b["first_position"], h) for h, b in blocks.items() if b["action_count"])
    keys = [s for s, _ in starts]
    prev = Prevouts()
    records, report = [], []
    for r in old["records"]:
        pos = r["position"]
        _, h = starts[bisect.bisect_right(keys, pos) - 1]
        _, recs = block_records(h, prev)
        rec = recs[pos - blocks[h]["first_position"]][0]
        oldb = bytes.fromhex(r["record_hex"])
        kind = diff_kind(rec, oldb)
        assert kind != "non-fee", f"non-fee difference at {pos}"
        assert rec == read_record(f, pos), f"journal differs from oracle at {pos}"
        records.append({"position": pos, "record_hex": rec.hex()})
        report.append({"position": pos, "height": h, "change": kind})
    new = dict(old, records=records)
    data = json.dumps(new, separators=(",", ":")).encode()
    with open(args.out, "wb") as o:
        o.write(data)
    print(json.dumps({"sha256": hashlib.sha256(data).hexdigest(), "changed": sum(x["change"] != "equal" for x in report),
                      "records": len(report)}, indent=1))


p = argparse.ArgumentParser()
s = p.add_subparsers(dest="cmd", required=True)
c = s.add_parser("compare"); c.add_argument("--journal", required=True); c.add_argument("--heights", nargs="*")
c.add_argument("--sample", type=int, default=0); c.add_argument("--absent-blocks", action="store_true"); c.add_argument("--seed", type=int, default=20261006); c.add_argument("--json-out")
o = s.add_parser("oracle-file"); o.add_argument("--journal", required=True); o.add_argument("--positions-from", required=True); o.add_argument("--out", required=True)
a = p.parse_args()
compare(a) if a.cmd == "compare" else oracle_file(a)
