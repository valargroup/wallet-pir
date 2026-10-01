#!/usr/bin/env python3
"""Raw acquisition preflight, not a parsed census or population sample.

Retain each fetched raw block once outside Git. Measure the authorized sequential
getblockhash/getblock(0) path before spending a multi-day turn on extraction.
The projection excludes parsing/UTXO/aggregation, so it is optimistic.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import census


def projection(blocks, seconds, remaining):
    if blocks <= 0 or seconds <= 0:
        raise ValueError("positive block count and measurement duration required")
    rate = blocks / seconds
    return dict(blocks_per_second=rate, remaining_blocks=remaining,
                projected_remaining_hours=remaining / rate / 3600,
                limit_hours=72, exceeds_limit=remaining / rate > 72 * 3600)


def measure(root, gateway, status, duration=180):
    root = Path(root).resolve()
    repo = Path(__file__).resolve().parents[3]
    if root.is_relative_to(repo):
        raise ValueError("raw inputs must remain outside repository")
    root.mkdir(parents=True, exist_ok=True)
    rawdir = root / "raw"
    rawdir.mkdir(exist_ok=True)
    def call(method, params):
        response = gateway(method, params)
        if response.get("error") is not None:
            raise RuntimeError("gateway RPC error: " + json.dumps(response["error"], sort_keys=True))
        return response["result"]
    if call("getblockhash", [census.ANCHOR_HEIGHT]) != census.ANCHOR_HASH:
        raise RuntimeError("canonical anchor changed")
    tip = call("getblockcount", [])
    manifest = root / "raw-manifest.jsonl"
    pins = []
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            pin = json.loads(line)
            if pin["height"] != len(pins):
                raise ValueError("nonsequential raw checkpoint")
            path = rawdir / f'{pin["height"]}.bin'
            if census.digest(path) != pin["raw_sha256"] or path.stat().st_size != pin["raw_bytes"]:
                raise ValueError("raw checkpoint checksum mismatch")
            pins.append(pin)
    first = len(pins)
    start = time.monotonic()
    started = datetime.now(timezone.utc).isoformat()
    last_status = start
    with manifest.open("a") as journal:
        while time.monotonic() - start < duration:
            height = len(pins)
            if height > census.ANCHOR_HEIGHT:
                break
            block_hash = call("getblockhash", [height])
            raw = bytes.fromhex(call("getblock", [block_hash, 0]))
            path = rawdir / f"{height}.bin"
            # An orphan file from an interrupted write can only be reused intact.
            if path.exists():
                if path.read_bytes() != raw:
                    raise ValueError("orphan raw checkpoint differs")
            else:
                temp = path.with_suffix(".tmp")
                temp.write_bytes(raw)
                temp.replace(path)
            pin = dict(height=height, hash=block_hash, raw_bytes=len(raw),
                       raw_sha256=hashlib.sha256(raw).hexdigest())
            journal.write(json.dumps(pin, separators=(",", ":")) + "\n")
            journal.flush()
            pins.append(pin)
            now = time.monotonic()
            if now - last_status >= 15:
                measured = projection(len(pins)-first, now-start, census.ANCHOR_HEIGHT+1-len(pins))
                census.atomic_json(status, dict(summary=f'Raw throughput preflight: {len(pins)} blocks retained, 0 canonically scanned; {measured["blocks_per_second"]:.3f} blocks/s; optimistic ETA {measured["projected_remaining_hours"]:.1f}h',
                    eta=f'{measured["projected_remaining_hours"]:.1f}h', needs_you="", state="working"))
                last_status = now
    elapsed = time.monotonic() - start
    measured = projection(len(pins)-first, elapsed, census.ANCHOR_HEIGHT+1-len(pins))
    if call("getblockhash", [census.ANCHOR_HEIGHT]) != census.ANCHOR_HASH:
        raise RuntimeError("canonical anchor changed after throughput measurement")
    receipt = dict(schema="txid-census-throughput-preflight-v1", started_utc=started,
        source="pir-census@167.99.42.60 read-only production zakurad gateway",
        anchor_height=census.ANCHOR_HEIGHT, anchor_hash=census.ANCHOR_HASH,
        tip_at_start=tip, fetched_blocks=len(pins), measured_blocks=len(pins)-first,
        first_measured_height=first, last_fetched_height=len(pins)-1,
        canonical_scanned_blocks=0, eligibility_inventory="not yet parsed",
        rpc_calls_measured=2*(len(pins)-first), seconds=elapsed, **measured,
        raw_manifest_sha256=census.digest(manifest),
        raw_bytes=sum(p["raw_bytes"] for p in pins),
        transport="one persistent SSH session, sequential requests, 40 requests/s maximum",
        measurement="sequential getblockhash(height), getblock(hash,0), retain raw bytes; excludes parser, UTXO and aggregates",
        qualification="THROUGHPUT ONLY; no full-chain findings or candidate counts")
    census.atomic_json(root / "throughput.json", receipt)
    need = (f'Measured {measured["blocks_per_second"]:.3f} blocks/s over {elapsed:.1f}s; '
            f'optimistic remaining census {measured["projected_remaining_hours"]:.1f}h exceeds 72h. '
            'Roman must authorize a longer duration or provide an approved faster acquisition method.') if measured["exceeds_limit"] else ""
    census.atomic_json(status, dict(summary=f'Raw throughput measured at {measured["blocks_per_second"]:.3f} blocks/s; {len(pins)} raw blocks retained, 0 canonically scanned',
        eta="" if need else f'{measured["projected_remaining_hours"]:.1f}h', needs_you=need, state="blocked" if need else "working"))
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=180)
    args = parser.parse_args()
    try:
        with census.Gateway() as gateway:
            result = measure(args.checkpoint, gateway, args.status, args.seconds)
        print(json.dumps(result, sort_keys=True))
    except Exception as error:
        census.atomic_json(args.status, dict(summary="Full-chain throughput preflight blocked", eta="", needs_you=str(error), state="blocked"))
        raise
