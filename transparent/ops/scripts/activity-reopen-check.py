#!/usr/bin/env python3
"""Check retained reference stores using independently decoded event arithmetic.

The fixture digest proves retrieval equality; the separately retained chain oracle
must establish extraction equality. This checker never imports the Rust ledger.
"""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path


def integer(raw, offset, maximum):
    value = 0
    start = offset
    for shift in range(0, 70, 7):
        byte = raw[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            assert offset == start + 1 or byte != 0, "noncanonical integer"
            assert value <= maximum, "integer overflow"
            return value, offset
    raise ValueError("unterminated integer")


def decode(raw):
    assert 89 <= len(raw) <= 103
    kind = raw[0] & 1
    coinbase = bool(raw[0] & 2)
    assert raw[0] & ~3 == 0 and not (kind and coinbase)
    flags = raw[87]
    assert flags & 32 and flags & ~56 == 0
    count, used = integer(raw, 88, 2**32 - 1)
    fee = {"state": "not-applicable" if coinbase else "unknown"}
    if flags & 8:
        value, used = integer(raw, used, 21_000_000 * 100_000_000)
        fee = {"state": "exact", "value": value}
    assert used == len(raw)
    assert not coinbase or count == 0 and fee["state"] == "not-applicable"
    return dict(kind=kind, height=int.from_bytes(raw[3:7], "little"),
                position=int.from_bytes(raw[1:3], "little"),
                value=int.from_bytes(raw[7:15], "little"), txid=raw[15:47].hex(),
                index=int.from_bytes(raw[47:51], "little"), parent=raw[51:83].hex(),
                parent_index=int.from_bytes(raw[83:87], "little"),
                fee=fee, input_count=count, shielded=bool(flags & 16), raw=raw)


def check(record, fixtures):
    connection = sqlite3.connect(f"file:{record['database']}?mode=ro", uri=True)
    scripts = list(connection.execute("SELECT hex(script), required_from FROM scripts"))
    selection = frozenset(script.lower() for script, _ in scripts)
    candidates = [f for f in fixtures if frozenset(f["scripts"]) == selection
                  and all(start == f["required_from"] for _, start in scripts)]
    assert len(candidates) == 1, "ambiguous/missing fixture"
    fixture = candidates[0]
    events = [decode(bytes.fromhex(e["bytes"])) for e in record["events"]]
    events.sort(key=lambda e: (e["height"], e["position"], e["kind"], e["txid"],
                               e["index"], e["parent"], e["parent_index"]))
    unique = {e["raw"]: e for e in events}
    assert len(unique) == len(events), "duplicate recovered event"
    digest = hashlib.sha256(b"".join(e["raw"] for e in events)).hexdigest()
    assert digest == fixture["expected_digest"] and len(events) == fixture["journal_events"]
    assert record["schema"] == "transparent-shard-v11" and record["pending"] == 0
    receives = {(e["txid"], e["index"]): e["value"] for e in events if not e["kind"]}
    grouped = {}
    for event in events:
        key = event["txid"]
        group = grouped.setdefault(key, dict(txid=key, height=event["height"], received=0,
                       spent=0, owned_inputs=0, unresolved_inputs=0, fee=event["fee"],
                       input_count=event["input_count"], shielded=event["shielded"]))
        assert all(group[k] == event[k] for k in ["height", "fee", "input_count", "shielded"])
        if not event["kind"]:
            group["received"] += event["value"]
        else:
            group["owned_inputs"] += 1
            value = receives.get((event["parent"], event["parent_index"]))
            if value is None:
                group["unresolved_inputs"] += 1
            else:
                group["spent"] += value
    for group in grouped.values():
        complete = True
        for script, start in scripts:
            through = start
            for first, last in connection.execute(
                    "SELECT start_height,end_height FROM coverage WHERE hex(script)=? ORDER BY start_height", (script,)):
                if first <= through:
                    through = max(through, last + 1)
            complete &= through > group["height"]
        group["owned_effects_complete"] = complete
        group["net"] = str(group["received"] - group["spent"])
        payment = None
        if (complete and not group["shielded"] and not group["unresolved_inputs"]
                and group["owned_inputs"] > 0 and group["owned_inputs"] == group["input_count"]
                and group["fee"]["state"] == "exact"):
            amount = group["spent"] - group["received"] - group["fee"]["value"]
            if amount >= 0:
                payment = amount
        group["aggregate_payment"] = payment
    assert sorted(grouped.values(), key=lambda x: x["txid"]) == sorted(record["summaries"], key=lambda x: x["txid"])
    connection.close()
    return dict(database=record["database"], profile=fixture["class"], events=len(events),
                transactions=len(grouped), unresolved=sum(g["unresolved_inputs"] for g in grouped.values()),
                exact_payments=sum(g["aggregate_payment"] is not None for g in grouped.values()),
                digest=digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--reopened", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    records = [json.loads(line) for line in args.reopened.read_text().splitlines()]
    assert records, "no recovered stores"
    checked = [check(record, json.loads(args.sample.read_text())["clients"]) for record in records]
    report = dict(status="passed", stores=len(checked), observations=checked,
                  checker_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  sample_sha256=hashlib.sha256(args.sample.read_bytes()).hexdigest(),
                  reopened_sha256=hashlib.sha256(args.reopened.read_bytes()).hexdigest(),
                  limitations=["Bounded synthetic public-script wallets; unresolved historical input values remain partial.",
                               "Retrieval fixture equality is separate from the retained independent chain extraction oracle."])
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": "passed", "stores": len(checked)}))


if __name__ == "__main__":
    main()
