"""Measure transparent-history retrieval over real HTTP against a live service.

The same workloads and the same independent ledger oracle as
`transparent_pir_incremental_run.py`; the difference is the transport. Every
byte reported here crossed a socket, where the existing evidence charges the
application payload of an in-process round trip and excludes HTTP framing,
retries and TLS.

Requires `transparent-history-server` already serving the generation this script
builds. The generation is content addressed, so a service serving a different
one is detected rather than silently measured.
"""

import argparse
import hashlib
import json
import platform
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from transparent_pir_http_transport import HttpPirTransport
from transparent_pir_incremental import (
    build_generation,
    load_sample,
    new_state,
    save,
    source_events,
    sync,
)
from transparent_pir_incremental_run import oracle, verify

#: Ordinary-retrieval bytes for the same block interval, from
#: docs/transparent-pir-evaluation/reuse4/baseline.json. The comparison is only
#: meaningful against the identical interval.
ORDINARY_FULL_DAY = 2_888_097


def run(args):
    metadata, blocks = load_sample(args.sample)
    accepted = {
        blocks[0]["height"] - 1: blocks[0]["prev_hash"],
        **{b["height"]: b["hash"] for b in blocks},
    }
    folder = build_generation(
        metadata["network"], blocks, args.output / "generations", page_bytes=args.page_bytes
    )
    events = source_events(blocks)
    ranked = sorted(events, key=lambda s: (len(events[s]), s))
    absent = [
        "76a914"
        + hashlib.sha256(f"incremental-absent-{i}".encode()).digest()[:20].hex()
        + "88ac"
        for i in range(100)
    ]
    if any(s in events for s in absent):
        raise ValueError("unexpected absent-fixture collision")
    start = blocks[0]["height"] - 1
    cases = [
        ("unused_100", absent, start),
        ("sparse_1", [ranked[len(ranked) // 2]], start),
        ("median_10", ranked[len(ranked) // 2 : len(ranked) // 2 + 10], start),
        ("large_1", [ranked[-1]], start),
    ]

    result = {
        "classification": "integrated client over real HTTP against a live service; "
        "not a mobile device, not TLS, single host loopback",
        "network": metadata["network"],
        "start": start + 1,
        "end": blocks[-1]["height"],
        "blocks": len(blocks),
        "generation": folder.name,
        "service": args.base_url,
        "machine": platform.platform(),
        "page_bytes": args.page_bytes,
        "ordinary_retrieval_bytes_same_interval": ORDINARY_FULL_DAY,
        "pad_to": args.pad_to,
        "trust": "complete indexer, accepted chain supplied by wallet",
        "runs": [],
    }

    for name, scripts, checkpoint in cases:
        out = args.output / name
        out.mkdir(parents=True, exist_ok=True)
        initial, expected, utxos = oracle(blocks, scripts, checkpoint)
        state = new_state(
            metadata["network"], scripts, checkpoint, accepted[checkpoint], initial
        )
        path = out / "state.json"
        save(path, state)
        transport = HttpPirTransport(args.base_url, args.client_binary, pad_to=args.pad_to)

        begin = time.perf_counter()
        state = sync(path, folder, accepted, transport)
        elapsed = time.perf_counter() - begin
        # Exact ledger equality against the independent oracle. A byte count for
        # a sync that recovered the wrong history is not a result.
        verify(state, expected, utxos, blocks[-1]["height"])

        cost = state["cost"]
        total = sum(
            cost[k]
            for k in (
                "public_download_bytes",
                "upload_bytes",
                "response_bytes",
                "setup_download_bytes",
            )
        )
        result["runs"].append(
            {
                "workload": name,
                "scripts": len(scripts),
                "checkpoint": checkpoint,
                "verified_events": len(state["events"]),
                "queries": cost["queries"],
                "padding_queries": sum(c[2] - len(c[1]) for c in transport.calls),
                "public_download_bytes": cost["public_download_bytes"],
                "setup_download_bytes": cost["setup_download_bytes"],
                "upload_bytes": cost["upload_bytes"],
                "response_bytes": cost["response_bytes"],
                "total_bytes": total,
                "ratio_to_ordinary": total / ORDINARY_FULL_DAY,
                "wall_seconds": elapsed,
            }
        )
        print(
            f"{name}: {cost['queries']} queries, {total} bytes, "
            f"{total / ORDINARY_FULL_DAY:.2f}x ordinary, "
            f"{len(state['events'])} events verified",
            flush=True,
        )

    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sample", type=Path, required=True)
    p.add_argument("--base-url", required=True)
    p.add_argument("--client-binary", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--page-bytes", type=int, default=17920)
    p.add_argument(
        "--pad-to",
        type=int,
        default=None,
        help="fixed queries per batch; omit to issue exactly the selections made",
    )
    run(p.parse_args())


if __name__ == "__main__":
    main()
