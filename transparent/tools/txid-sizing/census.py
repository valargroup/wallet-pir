#!/usr/bin/env python3
"""Resumable offline canonical census. No RPC, credentials, or history-index input.

Raw NDJSON and its independently supplied receipt must be immutable. SQLite is
the single writer: one block, its UTXO mutations, identities and aggregates commit
atomically. Interrupted blocks replay; a changed export/receipt/worker refuses
resume. Source consensus acceptance is a separate receipt/oracle gate.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import shutil
import sqlite3
import subprocess

import analyze as a

GENESIS = "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"
PARSER = "af944f5194ef2e9921bc96af017629450375013c"
BASE = "7d46f7618b94f2a889dcfebd5b3dc9229bd98b5a"
STATE = "txid-census-sqlite-v1"
THRESHOLDS = (128, 192, 256, 384, 512, 768, 1024)
MAX_LINE = 8_100_000  # canonical block bound, hex, and small JSON envelope


def encoded(v):
    return json.dumps(v, sort_keys=True, separators=(",", ":"))


def checksum(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_value(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def validate_receipt(r):
    if r["schema"] != "txid-canonical-export-receipt-v1" or r["network"] != "mainnet":
        raise ValueError("canonical mainnet export receipt required")
    if r["genesis_hash"] != GENESIS or r["parser_sha"] != PARSER or r["metadata_source_sha"] != BASE:
        raise ValueError("genesis/parser/metadata pin mismatch")
    for field in ("node_sha", "export_sha256", "anchor_hash"):
        n = 40 if field == "node_sha" else 64
        if len(r[field]) != n or any(c not in "0123456789abcdef" for c in r[field]):
            raise ValueError("missing complete source pin: " + field)
    if not isinstance(r["anchor_height"], int) or isinstance(r["anchor_height"], bool) or r["anchor_height"] < 0:
        raise ValueError("anchor height")
    if r["block_count"] != r["anchor_height"] + 1:
        raise ValueError("expected genesis-through-anchor height count")
    if r["source_consensus_verified"] is not True:
        raise ValueError("source consensus receipt required")
    oracle = r["independent_oracle"]
    if not oracle.get("provenance") or oracle["anchor_hash"] != r["anchor_hash"] or oracle["block_count"] != r["block_count"]:
        raise ValueError("independent chain oracle anchor/count mismatch")
    for key in ("transactions", "eligible", "shielded_only", "outputs"):
        if not isinstance(oracle[key], int) or isinstance(oracle[key], bool) or oracle[key] < 0:
            raise ValueError("independent inventory count: " + key)
    if oracle["transactions"] != oracle["eligible"] + oracle["shielded_only"]:
        raise ValueError("oracle eligibility denominator mismatch")


class Worker:
    def __init__(self, binary):
        self.process = subprocess.Popen([str(binary), "--stream"], stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, text=True)

    def call(self, v):
        self.process.stdin.write(encoded(v) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline(MAX_LINE * 4)
        if not line or not line.endswith("\n"):
            raise ValueError("canonical worker terminated or oversized response")
        result = json.loads(line)
        if "error" in result:
            raise ValueError("canonical worker: " + result["error"])
        return result

    def close(self):
        self.process.stdin.close()
        self.process.stdout.close()
        if self.process.wait() != 0:
            raise ValueError("canonical worker exit")


def open_state(path, identity):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA cache_size=-32768")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS blocks (height INTEGER PRIMARY KEY, hash TEXT UNIQUE,
            raw_sha256 TEXT, end_offset INTEGER);
        CREATE TABLE IF NOT EXISTS txids (txid TEXT PRIMARY KEY, height INTEGER, eligible INTEGER);
        CREATE TABLE IF NOT EXISTS utxos (key TEXT PRIMARY KEY, canonical_hex TEXT);
        CREATE TABLE IF NOT EXISTS histogram (codec TEXT, era TEXT, coinbase INTEGER,
            size INTEGER, count INTEGER, PRIMARY KEY(codec,era,coinbase,size));
        CREATE TABLE IF NOT EXISTS totals (key TEXT PRIMARY KEY, value INTEGER);
        CREATE TABLE IF NOT EXISTS observations (txid TEXT PRIMARY KEY, height INTEGER,
            coinbase INTEGER, input_count INTEGER, outputs INTEGER, fee_state TEXT,
            display_size INTEGER, compressed_size INTEGER, common_size INTEGER,
            missing INTEGER, script_set_digest TEXT);
    """)
    pin = db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
    if pin and pin[0] != encoded(identity):
        db.close()
        raise ValueError("source/receipt/worker/state identity changed; start a new state")
    if not pin:
        db.execute("INSERT INTO meta VALUES ('identity',?)", (encoded(identity),))
        db.commit()
    if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("checkpoint integrity failure")
    return db


def increment(db, key, count=1):
    db.execute("INSERT INTO totals VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value", (key, count))


def apply_block(db, raw, parsed, end_offset, receipt):
    last = db.execute("SELECT height,hash FROM blocks ORDER BY height DESC LIMIT 1").fetchone()
    expected = last[0] + 1 if last else 0
    if raw["height"] != expected or parsed["height"] != expected:
        raise ValueError("missing/duplicate height")
    if expected > receipt["anchor_height"]:
        raise ValueError("block beyond fixed anchor")
    if parsed["hash"] != raw["hash"] or (last and parsed["parent"] != last[1]):
        raise ValueError("source hash/parent mismatch or reorg")
    if not last and (parsed["hash"] != GENESIS or parsed["parent"] != "0" * 64):
        raise ValueError("genesis hash/parent mismatch")
    if expected == receipt["anchor_height"] and parsed["hash"] != receipt["anchor_hash"]:
        raise ValueError("fixed anchor mismatch")
    with db:
        for tx in parsed["transactions"]:
            try:
                db.execute("INSERT INTO txids VALUES (?,?,?)", (tx["txid_internal"], expected, tx["eligible"]))
            except sqlite3.IntegrityError as e:
                raise ValueError("duplicate transaction identity") from e
            increment(db, "transactions")
            for key in tx["inputs"]:
                db.execute("DELETE FROM utxos WHERE key=?", (key,))
            for output in tx["utxos"]:
                db.execute("INSERT INTO utxos VALUES (?,?)", (output["key"], output["canonical_hex"]))
            if not tx["eligible"]:
                increment(db, "shielded_only")
                continue
            r = tx["record"]
            if r["txid_internal"] != tx["txid_internal"] or r["height"] != expected:
                raise ValueError("display transaction identity mismatch")
            if len(r["outputs"]) != len(tx["utxos"]) or r["input_count"] != len(tx["inputs"]):
                raise ValueError("canonical eligible inventory mismatch")
            if not r["coinbase"] and not r["input_count"] and not r["outputs"]:
                raise ValueError("display-ineligible record")
            if a.payload(r).hex() != r["display_v1_hex"]:
                raise ValueError("shared canonical encoder byte mismatch")
            missing = len(r["missing_prevouts"])
            if bool(missing) != (r["fee"] == "unknown"):
                raise ValueError("unknown fee/missingness contradiction")
            increment(db, "eligible")
            increment(db, "outputs", len(r["outputs"]))
            increment(db, "missing_prevouts", missing)
            increment(db, "missing_prevout_transactions", int(missing > 0))
            increment(db, "coinbase" if r["coinbase"] else "noncoinbase")
            increment(db, "input_only", int(bool(r["input_count"]) and not r["outputs"]))
            increment(db, "external_unshielding", int(not r["coinbase"] and not r["input_count"] and bool(r["outputs"])))
            increment(db, "mixed_pool", int(bool(r["input_count"]) and r["shielded_components"]))
            increment(db, "raw_escape_outputs", sum(a.script_encode(bytes.fromhex(o["script"]))[0] == 0 for o in r["outputs"]))
            fee_state = "exact_zero" if r["fee"] == 0 else "exact_nonzero" if isinstance(r["fee"], int) else r["fee"]
            increment(db, "fee_" + fee_state)
            sizes = {}
            for codec in a.CODECS:
                size = len(a.payload(r, codec))
                sizes[codec] = size
                db.execute("INSERT INTO histogram VALUES (?,?,?,?,1) ON CONFLICT(codec,era,coinbase,size) DO UPDATE SET count=count+1",
                           (codec, a.era(expected), int(r["coinbase"]), size))
            # Public output scripts only. This equality digest is a conservative
            # modeled history correlation, not a captured wallet transcript.
            scripts = sorted(set(o["script"] for o in r["outputs"]))
            db.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                r["txid_internal"], expected, r["coinbase"], r["input_count"], len(r["outputs"]), fee_state,
                sizes["display-v1"], sizes["script-compression"], sizes["common-counts"], missing, hash_value(scripts)))
        db.execute("INSERT INTO blocks VALUES (?,?,?,?)", (expected, parsed["hash"], raw["raw_sha256"], end_offset))


def rank(hist, proportion):
    target = max(1, math.ceil(sum(hist.values()) * proportion))
    total = 0
    for size, count in sorted(hist.items()):
        total += count
        if total >= target:
            return size
    return None


def summarize_hist(hist, codec):
    n = sum(hist.values())
    frontiers = {str(p): rank(hist, p / 100) for p in (85, 90, 95, 99)}
    thresholds = sorted(set(THRESHOLDS) | {v for v in frontiers.values() if v is not None})
    choices = []
    for t in thresholds:
        inline = sum(count for size, count in hist.items() if size <= t)
        fragments = sum(len(a.fragments(size, codec != "display-v1")) * count for size, count in hist.items() if size > t)
        choices.append({"threshold": t, "inline": inline, "overflow": n - inline,
                        "strictly_over_80_percent": 5 * inline > 4 * n,
                        "fragments": fragments, "cost_kind": "projected_fragment_count_only"})
    return {"transactions": n, "payload_bytes": sum(s * c for s, c in hist.items()),
            "histogram": dict(sorted(hist.items())), "percentiles": {str(p): rank(hist, p / 100) for p in (1, 5, 50, 85, 90, 95, 99)},
            "frontiers": frontiers, "thresholds": choices}


def report(db, receipt):
    totals = dict(db.execute("SELECT key,value FROM totals"))
    count = db.execute("SELECT count(*) FROM blocks").fetchone()[0]
    complete = count == receipt["block_count"]
    oracle = receipt["independent_oracle"]
    matched = complete and all(totals.get(k, 0) == oracle[k] for k in ("transactions", "eligible", "shielded_only", "outputs"))
    if complete and not matched:
        raise ValueError("independent eligible/chain inventory oracle mismatch")
    codecs = {}
    strata = {}
    for codec in a.CODECS:
        hist = Counter()
        for size, n in db.execute("SELECT size,sum(count) FROM histogram WHERE codec=? GROUP BY size", (codec,)):
            hist[size] += n
        codecs[codec] = summarize_hist(hist, codec)
        for era, coinbase in db.execute("SELECT DISTINCT era,coinbase FROM histogram WHERE codec=?", (codec,)):
            h = dict(db.execute("SELECT size,count FROM histogram WHERE codec=? AND era=? AND coinbase=?", (codec, era, coinbase)))
            strata[f"{codec}/{era}/{'coinbase' if coinbase else 'noncoinbase'}"] = summarize_hist(h, codec)
    return {"schema": STATE, "qualification": "UNQUALIFIED", "full_chain_census_ran": complete,
            "coverage_verified": matched, "exact_fee_qualified": matched and totals.get("missing_prevouts", 0) == 0,
            "receipt_sha256": hash_value(receipt), "blocks": count, "totals": totals, "codecs": codecs,
            "strata": strata, "selected_threshold": None,
            "packing_native_joint_anonymity_qualified": False,
            "limitations": "No native latency/RSS or full-scale sorted packing measured; separate replay gate required."}


def scan(source, receipt, state, worker, worker_identity, stop_after=None, reserve_bytes=1024**3):
    validate_receipt(receipt)
    if checksum(source) != receipt["export_sha256"]:
        raise ValueError("immutable export checksum mismatch")
    state = Path(state)
    if shutil.disk_usage(state.parent).free < reserve_bytes:
        raise ValueError("insufficient checkpoint disk reserve")
    identity = {"format": STATE, "receipt": hash_value(receipt), "worker": worker_identity,
                "scanner": checksum(__file__), "analysis": checksum(a.__file__)}
    db = open_state(state, identity)
    try:
        last = db.execute("SELECT end_offset FROM blocks ORDER BY height DESC LIMIT 1").fetchone()
        processed = 0
        with Path(source).open("rb") as f:
            f.seek(last[0] if last else 0)
            while True:
                line = f.readline(MAX_LINE + 1)
                if not line:
                    break
                if len(line) > MAX_LINE or not line.endswith(b"\n"):
                    raise ValueError("oversized/incomplete canonical export line")
                raw = json.loads(line)
                if hashlib.sha256(bytes.fromhex(raw["raw_hex"])).hexdigest() != raw["raw_sha256"]:
                    raise ValueError("canonical raw checksum mismatch")
                request = {"op": "inspect", "height": raw["height"], "raw_hex": raw["raw_hex"]}
                inspected = worker.call(request)
                prevouts = {}
                for tx in inspected["transactions"]:
                    for key in tx["inputs"]:
                        value = db.execute("SELECT canonical_hex FROM utxos WHERE key=?", (key,)).fetchone()
                        if value:
                            prevouts[key] = value[0]
                request.update(op="extract", prevouts=prevouts)
                parsed = worker.call(request)
                if (parsed["hash"], parsed["parent"], [t["txid_internal"] for t in parsed["transactions"]]) != (
                    inspected["hash"], inspected["parent"], [t["txid_internal"] for t in inspected["transactions"]]):
                    raise ValueError("worker source consistency mismatch")
                apply_block(db, raw, parsed, f.tell(), receipt)
                processed += 1
                if stop_after is not None and processed >= stop_after:
                    break
        result = report(db, receipt)
        if stop_after is None and not result["coverage_verified"]:
            raise ValueError("missing canonical heights before fixed anchor")
        result["checkpoint"] = {"format": STATE, "last": db.execute("SELECT height,hash,end_offset FROM blocks ORDER BY height DESC LIMIT 1").fetchone(),
                                "identity": identity, "restart": "atomic block commit; reorg/source changes require a new state"}
    finally:
        db.close()
    result["checkpoint"]["sha256"] = checksum(state)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source", type=Path); p.add_argument("receipt", type=Path)
    p.add_argument("state", type=Path); p.add_argument("output", type=Path)
    p.add_argument("--worker", type=Path, required=True)
    p.add_argument("--stop-after", type=int)
    p.add_argument("--reserve-bytes", type=int, default=1024**3)
    args = p.parse_args()
    worker = Worker(args.worker)
    try:
        result = scan(args.source, json.loads(args.receipt.read_bytes()), args.state, worker,
                      checksum(args.worker), args.stop_after, args.reserve_bytes)
        args.output.write_text(encoded(result) + "\n")
    finally:
        worker.close()


if __name__ == "__main__":
    main()
