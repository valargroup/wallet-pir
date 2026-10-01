#!/usr/bin/env python3
"""Deterministic offline byte/packing and observable-intersection analysis.

Consumes complete canonical outputs and shared metadata, never history events.
Only display-v1/128 is implemented; other codecs/thresholds are projections.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path

ROW = 4096
FRAGMENT = 4050
THRESHOLDS = (64, 96, 128, 160, 192, 256, 384, 512, 768, 1024, 2048, 3072, 4044)
CODECS = ("display-v1", "manifest-version", "script-compression", "common-counts")
MAX_MONEY = 21_000_000 * 100_000_000


def uleb(n):
    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise ValueError("unsigned integer required")
    out = bytearray()
    while n >= 128:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def script_encode(raw):
    """Exact byte templates; uncompressed P2PK stays uncompressed."""
    if len(raw) == 25 and raw[:3] == bytes.fromhex("76a914") and raw[-2:] == bytes.fromhex("88ac"):
        return b"\x01" + raw[3:23]
    if len(raw) == 23 and raw[:2] == bytes.fromhex("a914") and raw[-1:] == b"\x87":
        return b"\x02" + raw[2:22]
    if len(raw) == 35 and raw[0] == 33 and raw[1] in (2, 3) and raw[-1] == 172:
        return b"\x03" + raw[1:34]
    if len(raw) == 67 and raw[:2] == b"\x41\x04" and raw[-1] == 172:
        return b"\x04" + raw[1:66]
    return b"\x00" + uleb(len(raw)) + raw


def script_decode(encoded):
    tag = encoded[0]
    if tag == 1 and len(encoded) == 21:
        return bytes.fromhex("76a914") + encoded[1:] + bytes.fromhex("88ac")
    if tag == 2 and len(encoded) == 21:
        return bytes.fromhex("a914") + encoded[1:] + b"\x87"
    if tag == 3 and len(encoded) == 34 and encoded[1] in (2, 3):
        return b"\x21" + encoded[1:] + b"\xac"
    if tag == 4 and len(encoded) == 66 and encoded[1] == 4:
        return b"\x41" + encoded[1:] + b"\xac"
    if tag == 0:
        n = at = shift = 0
        for byte in encoded[1:]:
            at += 1
            n |= (byte & 127) << shift
            if byte < 128:
                prefix = encoded[1:1 + at]
                raw = encoded[1 + at:]
                if prefix == uleb(n) and len(raw) == n:
                    return raw
                break
            shift += 7
    raise ValueError("invalid compressed script")


def payload(record, codec="display-v1"):
    # Fee is supplied by the Rust canonical extractor. This code sizes/encodes
    # shared metadata; it never infers or calculates a transaction fee.
    fee = record["fee"]
    ni, no = record["input_count"], len(record["outputs"])
    cb, shielded = record["coinbase"], record["shielded_components"]
    if fee not in ("unknown", "not_applicable") and (not isinstance(fee, int) or not 0 <= fee <= MAX_MONEY):
        raise ValueError("fee state")
    if cb != (fee == "not_applicable") or (cb and ni != 0):
        raise ValueError("shared metadata contradiction")
    flags = 32 | int(cb) | (16 if shielded else 0) | (8 if isinstance(fee, int) else 0)
    fee_bytes = uleb(fee) if isinstance(fee, int) else b""
    if codec == "common-counts":
        header = bytes([flags, (min(ni, 15) << 4) | min(no, 15)])
        header += (uleb(ni) if ni >= 15 else b"") + (uleb(no) if no >= 15 else b"") + fee_bytes
    else:
        header = (b"\x01" if codec == "display-v1" else b"") + bytes([flags]) + uleb(ni) + fee_bytes + uleb(no)
    if codec not in CODECS:
        raise ValueError("codec")
    result = bytearray(header)
    total = 0
    for output in record["outputs"]:
        value = output["value"]
        if not isinstance(value, int) or not 0 <= value <= MAX_MONEY:
            raise ValueError("output value")
        total += value
        raw = bytes.fromhex(output["script"])
        if len(raw) > 2_000_000 or total > MAX_MONEY:
            raise ValueError("output bounds")
        result += uleb(value)
        result += script_encode(raw) if codec in ("script-compression", "common-counts") else uleb(len(raw)) + raw
    if len(result) > 4_000_000:
        raise ValueError("record bound")
    return bytes(result)


def percentiles(values):
    values = sorted(values)
    if not values:
        return {k: None for k in ("min", "p01", "p05", "p50", "p85", "p90", "p95", "p99", "p999", "max")}
    out = {"min": values[0], "max": values[-1]}
    for label, p in (("p01", .01), ("p05", .05), ("p50", .5), ("p85", .85), ("p90", .9), ("p95", .95), ("p99", .99), ("p999", .999)):
        out[label] = values[max(0, math.ceil(p * len(values)) - 1)]
    return out


def choices(txid, rows, bucket=0):
    return [int.from_bytes(hashlib.sha256(b"transparent-txid-display-v1/directory/" + bytes([c]) + bucket.to_bytes(8, "little") + bytes.fromhex(txid)).digest()[:8], "little") % rows for c in (0, 1)]


def fragments(size, compact=False):
    if not compact:
        return [(offset, min(FRAGMENT, size - offset), 40) for offset in range(0, size, FRAGMENT)]
    result = []
    offset = 0
    while offset < size:
        header = 32 + len(uleb(offset)) + len(uleb(size))
        chunk = min(ROW - 4 - 2 - header, size - offset)
        result.append((offset, chunk, header))
        offset += chunk
    return result


def packing(records, codec, threshold, directory_rows=4096, page_rows=4096):
    """Reproduce sorted two-choice directory/next-fit overflow packing.

    Changed thresholds use the same packer as a counterfactual; compact codecs
    additionally use the explicitly proposed compact envelope and fragments.
    """
    if threshold > ROW - 4 - 2 - 46 or min(directory_rows, page_rows) < 1:
        raise ValueError("invalid packing geometry/inline threshold")
    compact = codec != "display-v1"
    dirs = [[4] * directory_rows]
    pages = [4]
    directory_entries = overflow_entries = payload_bytes = inline = frag_count = page_requests = 0
    output_count = lookup_requests = 0
    sizes = []
    per_record = []
    for r in sorted(records, key=lambda r: r["txid_internal"]):
        size = len(payload(r, codec)); sizes.append(size); payload_bytes += size
        output_count += len(r["outputs"])
        nfrags = nrows = 0
        if size <= threshold:
            inline += 1
            entry = 32 + 1 + len(uleb(size)) + size if compact else 46 + size
        else:
            first = None
            for offset, chunk, header in fragments(size, compact):
                extent = 2 + header + chunk
                if pages[-1] + extent > ROW:
                    pages.append(4)
                if first is None:
                    first = len(pages) - 1
                pages[-1] += extent
                overflow_entries += extent
                nfrags += 1
            nrows = min(len(pages) - first, page_rows)
            # first_page is one based, just like the implemented envelope.
            entry = 32 + 1 + len(uleb(size)) + len(uleb(first + 1)) + len(uleb(nfrags)) if compact else 46
            frag_count += nfrags; page_requests += nrows
        directory_entries += 2 + entry
        a, b = choices(r["txid_internal"], directory_rows)
        lookup_requests += len({a, b})
        placed = False
        for segment in dirs:
            preferred = a if segment[a] <= segment[b] else b
            for row in dict.fromkeys((preferred, a, b)):
                if segment[row] + 2 + entry <= ROW:
                    segment[row] += 2 + entry; placed = True; break
            if placed:
                break
        if not placed:
            segment = [4] * directory_rows; segment[a] += 2 + entry; dirs.append(segment)
        per_record.append({"txid":r["txid_internal"], "size":size, "fragments":nfrags, "page_rows_requested":nrows})
    occupied_dirs = sum(v > 4 for s in dirs for v in s)
    occupied_pages = sum(v > 4 for v in pages)
    ds, ps = len(dirs), max(1, math.ceil(len(pages) / page_rows))
    db, pb = ds * directory_rows * ROW, ps * page_rows * ROW
    # Exactly the native runtime upper-bound formula at 4096-byte rows.
    fixed = 14848 + 16 + 32 + 2 * 2048 * 8 + 8 + 2048 * 2048 * 2 * 8
    reserved = db + ds * fixed + (pb + ps * fixed if occupied_pages else 0)
    average_overflow_rows = page_requests / len(records) if records else 0
    return {"threshold":threshold,"codec":codec,"implemented":codec == "display-v1" and threshold == 128,
        "transactions":len(records),"outputs":output_count,"inline":inline,"overflow":len(records)-inline,
        "coverage":inline / len(records) if records else None,"payload_bytes":payload_bytes,
        "directory_entry_bytes":directory_entries,"overflow_entry_bytes":overflow_entries,
        "fragments":frag_count,"overflow_page_rows_requested_sum":page_requests,
        "occupied_directory_rows":occupied_dirs,"occupied_page_rows":occupied_pages,
        "directory_segments":ds,"page_segments":ps,"directory_allocated_bytes":db,"pages_allocated_bytes":pb,
        "occupied_bytes":directory_entries + overflow_entries + 4 * (occupied_dirs + occupied_pages),
        "packing_slack_in_occupied_rows":ROW*(occupied_dirs+occupied_pages)-directory_entries-overflow_entries-4*(occupied_dirs+occupied_pages),
        "allocation_slack_bytes":db+pb-directory_entries-overflow_entries-4*(occupied_dirs+occupied_pages),
        "native_reservation_bytes":reserved,
        "encrypted_requests_per_tx_uniform":lookup_requests / len(records) + average_overflow_rows if records else 0,
        "response_segment_evaluations_per_tx_uniform":lookup_requests / len(records) * ds + average_overflow_rows * ps if records else 0,
        "http_requests_per_tx_uniform":lookup_requests / len(records) + average_overflow_rows if records else 0,
        "overflow_rows_per_tx_uniform":average_overflow_rows,"sizes":percentiles(sizes),"per_record":per_record}


def era(height):
    for end, name in ((347500,"Sprout"),(419200,"Overwinter"),(653600,"Sapling"),(903000,"Blossom"),(1046400,"Heartwood"),(1687104,"Canopy")):
        if height < end:
            return name
    return "NU5+"


def classes(rows, fields):
    """Intersection: count distinct real txids only, never rows/fragments."""
    groups = defaultdict(set)
    for row in rows:
        if not row.get("real", True):
            continue
        groups[tuple(row[f] for f in fields)].add(row["txid"])
    populations = [len(ids) for ids in groups.values()]
    tx_weighted = [len(ids) for ids in groups.values() for _ in ids]
    return {"classes":len(groups),"class_weighted":percentiles(populations),
        "transaction_weighted":percentiles(tx_weighted),
        "classes_below_policy":{str(floor):sum(n < floor for n in populations) for floor in (1000, 10000)},
        "transactions_below_policy":{str(floor):sum(n for n in populations if n < floor) for floor in (1000,10000)}}


def routing(records, pack, lookup="temporal", overflow="global", padding=None, buckets=4,
            revision_width=None, timing_width=None, lookup_segments=1, overflow_segments=1):
    per = {p["txid"]:p for p in pack["per_record"]}
    result = []
    for r in records:
        txid = r["txid_internal"]; p = per[txid]
        hashed = int.from_bytes(hashlib.sha256(b"txid-sizing/lookup/"+bytes.fromhex(txid)).digest()[:8],"little") % buckets
        overflow_hash = int.from_bytes(hashlib.sha256(b"txid-sizing/overflow/"+bytes.fromhex(txid)).digest()[:8],"little") % buckets
        l = r["height"] // 50000 if lookup == "temporal" else (r["height"] // 1_000_000 if lookup == "coarse" else (hashed if lookup == "hash" else 0))
        # Overflow route exists only when requested, except cover includes inline.
        actual = p["page_rows_requested"]
        requested = actual if padding is None else max(padding, actual)
        o = (overflow_hash if overflow == "hash" else (r["height"] // 1_000_000 if overflow == "broad" else 0)) if requested else "none"
        # The server observes row requests, not decoded fragment count. Under
        # this contiguous layout they coincide; model it through request count.
        result.append({"txid":txid,"lookup":l,"overflow":o,
            "revision":r["height"] // revision_width if revision_width else "frozen",
            "segments":(lookup_segments, overflow_segments if requested else 0),
            "query_count":2+requested,"fragment_request_count":requested,
            "timing":r["height"] // timing_width if timing_width else "unmodeled"})
    return classes(result,("lookup","overflow","revision","segments","query_count","fragment_request_count","timing"))


def negative_controls():
    # 20000 ordinary txids and FIVE tail txids in one narrow public range.
    rows = [{"txid":str(i),"lookup":0,"overflow":0,"revision":0,"segments":1,"query_count":2,"fragment_request_count":0,"timing":0} for i in range(20000)]
    rows += [{"txid":str(i),"lookup":1,"overflow":0,"revision":0,"segments":1,"query_count":5,"fragment_request_count":3,"timing":0} for i in range(20000,20005)]
    fields = ("lookup","overflow","revision","segments","query_count","fragment_request_count","timing")
    unpadded = classes(rows,fields)
    # Global overflow and repeated/dummy retrieval do not add candidates.
    repeated = classes(rows + rows[-5:] * 20, fields)
    padded = [dict(r, lookup=0,query_count=5,fragment_request_count=3) for r in rows]
    fixed = classes(padded,fields)
    refresh = [dict(r,revision=int(r["txid"])>=20000) for r in padded]
    timed = [dict(r,timing=int(r["txid"])>=20000) for r in padded]
    segmented = [dict(r,segments=2 if int(r["txid"])>=20000 else 1) for r in padded]
    dummy_rows = rows + [dict(rows[-1],txid="dummy-"+str(i),real=False) for i in range(10000)]
    # Each marginal route has 10000 real txids, while two intersections have 5.
    independent = [dict(rows[0],txid=str(i),lookup=int(i>=10000),
        overflow=int(5<=i<10000 or i>=19995)) for i in range(20000)]
    return {"qualification":"synthetic negative controls only","global_overflow_narrow_lookup":unpadded,
        "repeated_fragments":repeated,"global_lookup_count_cover":fixed,
        "new_revision_tail":classes(refresh,fields),"timing_tail":classes(timed,fields),
        "segment_tail":classes(segmented,fields),"dummy_and_empty_rows":classes(dummy_rows,fields),
        "independent_routes_joint":classes(independent,fields),
        "independent_routes_lookup_marginal":classes(independent,("lookup",)),
        "independent_routes_overflow_marginal":classes(independent,("overflow",))}


def analyze(data):
    records = data["records"]
    ids = [r["txid_internal"] for r in records]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate transaction identity")
    for r in records:
        if payload(r).hex() != r["display_v1_hex"]:
            raise ValueError("canonical display-v1 byte mismatch")
    result = {"schema":"txid-sizing-analysis-v1","qualification":"UNQUALIFIED",
        "population_basis":"availability-selected upstream vectors with missing-parent exclusions; no full-chain inference",
        "transactions":len(records),"outputs":sum(len(r["outputs"]) for r in records),
        "eligible_in_selected_blocks":sum(b["eligible"] for b in data["blocks"]),
        "excluded_missing_prevouts":len(data["excluded"]),"blocks":len(data["blocks"]),
        "complete_display_blocks":sum(b["complete_display_block"] for b in data["blocks"]),
        "shielded_only_in_selected_blocks":data["shielded_only_transactions"],"codecs":{},"routing":{}}
    for codec in CODECS:
        sizes = [len(payload(r,codec)) for r in records]
        targets = sorted({percentiles(sizes)[key] for key in ("p85","p90","p95","p99","p999")}) if sizes else []
        cutoffs = sorted(set(THRESHOLDS) | {s for s in targets if s <= 3072})
        packs = [packing(records,codec,t) for t in cutoffs]
        strata = {}
        for key in sorted({era(r["height"]) for r in records}):
            strata[key] = {"transactions":sum(era(r["height"])==key for r in records),"sizes":percentiles([len(payload(r,codec)) for r in records if era(r["height"])==key])}
        strata["coinbase"] = {"transactions":sum(r["coinbase"] for r in records),"sizes":percentiles([len(payload(r,codec)) for r in records if r["coinbase"]])}
        strata["non_coinbase"] = {"transactions":sum(not r["coinbase"] for r in records),"sizes":percentiles([len(payload(r,codec)) for r in records if not r["coinbase"]])}
        coverage_targets = {}
        for target in (.85,.90,.95,.99,.999):
            s = sorted(sizes)[math.ceil(target*len(sizes))-1] if sizes else None
            coverage_targets[str(target)] = {"sample_threshold":s,"actual_sample_coverage":sum(x<=s for x in sizes)/len(sizes) if sizes else None,
                "measured_full_chain_threshold":None,"binomial_confidence_bound":None}
        result["codecs"][codec] = {"sizes":percentiles(sizes),"strata":strata,"coverage_targets":coverage_targets,
            "thresholds":[{**{k:v for k,v in p.items() if k!="per_record"},
                "selected_blocks_missingness_bounds":[p["inline"]/result["eligible_in_selected_blocks"],
                    (p["inline"]+result["excluded_missing_prevouts"])/result["eligible_in_selected_blocks"]]} for p in packs]}
    for threshold in (128,192,256,384,512,768,1024):
        pack = packing(records,"display-v1",threshold)
        scenarios = {}
        for lookup, overflow in (("temporal","global"),("coarse","global"),("hash","global"),("global","global"),("hash","broad"),("hash","hash")):
            name = lookup+"/"+overflow
            scenarios[name] = {"unpadded":routing(records,pack,lookup,overflow),
                "cover_1":routing(records,pack,lookup,overflow,padding=1),
                "cover_3":routing(records,pack,lookup,overflow,padding=3),
                "cover_3_refresh_100_blocks":routing(records,pack,lookup,overflow,padding=3,revision_width=100),
                "cover_3_timing_10_blocks":routing(records,pack,lookup,overflow,padding=3,timing_width=10)}
        result["routing"][str(threshold)] = scenarios
    result["negative_controls"] = negative_controls()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input",type=Path); parser.add_argument("output",type=Path)
    args = parser.parse_args()
    data = json.loads(args.input.read_bytes())
    report = analyze(data)
    report["input_sha256"] = hashlib.sha256(args.input.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(report,sort_keys=True,separators=(",",":"))+"\n")
    print(json.dumps({k:report[k] for k in ("qualification","transactions","outputs","blocks","excluded_missing_prevouts")}))


if __name__ == "__main__":
    main()
