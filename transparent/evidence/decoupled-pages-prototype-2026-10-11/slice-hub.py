#!/usr/bin/env python3
"""Hub side of fetch-slices.sh: plan, check and assemble standalone journal windows.

fetch-slices.sh streams raw bytes from the coordinator's v3 event journal into
DEST on the hub and runs this file there (on ssh stdin) between the streams.
Nothing here touches the coordinator. Modes, in the order the script runs them:

- `plan DEST --window NAME:FIRST:LAST ... [--margin N]`: reads the streamed
  `source/meta.json` and `source/checkpoint.bin`, checks every window lies at
  least N blocks below the committed tip, and prints per window
  `NAME RECORD_INDEX RECORD_COUNT`. RECORD_COUNT includes the record of the
  block after the window, whose offset ends the window's events.
- `ranges DEST`: reads each streamed `source/NAME.records` and prints
  `NAME EVENTS_BEGIN EVENTS_LENGTH` (byte range of events.bin).
- `finish DEST --sidecars NAME=COUNT ... --revision SHA`: checks the streamed
  `NAME/events.bin` length and the sidecars, then writes each window's
  `blocks.bin` (offsets rebased to 0), `checkpoint.bin`, `meta.json`,
  `SHA256SUMS`, `SIDECARS.SHA256SUMS`, and the top-level `provenance.json`.

Each window becomes a journal directory that `EventStore::open_existing` reads.
`source/` keeps the streamed meta, checkpoint and block records unmodified.
"""
import argparse
import datetime
import hashlib
import json
import os
import struct
import sys

RECORD = 48
CHUNK = 64 << 20
SIDECAR_DIRS = ("display-v1", "display-v2x")


def fail(message):
    sys.exit(f"slice-hub: {message}")


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def load_source(dest):
    meta = json.load(open(os.path.join(dest, "source", "meta.json")))
    if meta.get("version") != 3:
        fail(f"journal version {meta.get('version')}, expected 3")
    raw = open(os.path.join(dest, "source", "checkpoint.bin"), "rb").read()
    if len(raw) != 16:
        fail("checkpoint is not 16 bytes")
    events_len, blocks_len = struct.unpack("<QQ", raw)
    if blocks_len % RECORD:
        fail("checkpoint blocks length is not whole records")
    covered = meta["start_height"] + blocks_len // RECORD - 1
    return meta, events_len, blocks_len, covered


def plan_path(dest):
    return os.path.join(dest, "source", "plan.json")


def plan(args):
    meta, events_len, blocks_len, covered = load_source(args.dest)
    start = meta["start_height"]
    windows = []
    for text in args.window:
        name, first, last = text.split(":")
        first, last = int(first), int(last)
        if not (start <= first <= last <= covered - args.margin):
            fail(f"window {text} is outside {start}-{covered - args.margin}")
        windows.append({"name": name, "heights": [first, last],
                        "record_index": first - start,
                        # The next block's record ends the window; the margin keeps it committed.
                        "record_count": last - first + 2})
    with open(plan_path(args.dest), "w") as f:
        json.dump({"source_checkpoint": [events_len, blocks_len], "source_covered": covered,
                   "margin": args.margin, "windows": windows}, f, indent=2)
    print(f"# source heights {start}-{covered}, events {events_len} B", file=sys.stderr)
    for w in windows:
        print(w["name"], w["record_index"], w["record_count"])


def records_of(dest, window):
    raw = open(os.path.join(dest, "source", f"{window['name']}.records"), "rb").read()
    if len(raw) != window["record_count"] * RECORD:
        fail(f"{window['name']}: {len(raw)} record bytes, expected {window['record_count'] * RECORD}")
    return raw


def offsets(raw):
    return [struct.unpack_from("<Q", raw, at + 32)[0] for at in range(0, len(raw), RECORD)]


def ranges(args):
    state = json.load(open(plan_path(args.dest)))
    events_len = state["source_checkpoint"][0]
    for w in state["windows"]:
        offs = offsets(records_of(args.dest, w))
        if any(b < a for a, b in zip(offs, offs[1:])) or offs[-1] > events_len:
            fail(f"{w['name']}: block offsets are not ordered below the checkpoint")
        w["events_bin_bytes"] = [offs[0], offs[-1]]
        print(w["name"], offs[0], offs[-1] - offs[0])
    with open(plan_path(args.dest), "w") as f:
        json.dump(state, f, indent=2)


def finish(args):
    meta, _, _, _ = load_source(args.dest)
    state = json.load(open(plan_path(args.dest)))
    expected = dict(item.split("=") for item in args.sidecars)
    provenance = {
        "source": args.source, "source_meta": meta,
        "source_checkpoint": state["source_checkpoint"],
        "source_covered": state["source_covered"], "margin": state["margin"],
        "method": "streamed read-only from the coordinator through the operator's Mac "
                  "by fetch-slices.sh; nothing was written on the coordinator",
        "tool_revision": args.revision,
        "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "windows": []}
    for w in state["windows"]:
        name = w["name"]
        target = os.path.join(args.dest, name)
        raw = records_of(args.dest, w)
        begin, end = w["events_bin_bytes"]
        events = os.path.join(target, "events.bin")
        if os.path.getsize(events) != end - begin:
            fail(f"{name}: events.bin is {os.path.getsize(events)} B, expected {end - begin}")
        records = raw[:-RECORD]
        hashes = set()
        rebased = bytearray(records)
        for at in range(0, len(records), RECORD):
            offset = struct.unpack_from("<Q", records, at + 32)[0]
            struct.pack_into("<Q", rebased, at + 32, offset - begin)
            hashes.add(records[at:at + 32][::-1].hex() + ".bin")

        # Every streamed sidecar must belong to a block of this window, and the
        # count must match what the coordinator listed.
        sidecar_sums = []
        sidecar_bytes = 0
        for sub in SIDECAR_DIRS:
            folder = os.path.join(target, sub)
            if not os.path.isdir(folder):
                continue
            for file in sorted(os.listdir(folder)):
                if file not in hashes:
                    fail(f"{name}: sidecar {sub}/{file} is not a block of the window")
                path = os.path.join(folder, file)
                sidecar_bytes += os.path.getsize(path)
                sidecar_sums.append(f"{sha256_file(path)}  {sub}/{file}\n")
        others = set(os.listdir(target)) - {"events.bin", *SIDECAR_DIRS}
        if others:
            fail(f"{name}: unexpected files {sorted(others)}")
        if len(sidecar_sums) != int(expected[name]):
            fail(f"{name}: {len(sidecar_sums)} sidecars, coordinator listed {expected[name]}")

        with open(os.path.join(target, "blocks.bin"), "wb") as f:
            f.write(rebased)
        with open(os.path.join(target, "checkpoint.bin"), "wb") as f:
            f.write(struct.pack("<QQ", end - begin, len(rebased)))
        first, last = w["heights"]
        with open(os.path.join(target, "meta.json"), "w") as f:
            json.dump({"version": 3, "genesis_hash": meta["genesis_hash"],
                       "start_height": first}, f, indent=2)
        with open(os.path.join(target, "SIDECARS.SHA256SUMS"), "w") as f:
            f.writelines(sidecar_sums)
        sums = {file: sha256_file(os.path.join(target, file))
                for file in ("events.bin", "blocks.bin", "checkpoint.bin", "meta.json",
                             "SIDECARS.SHA256SUMS")}
        with open(os.path.join(target, "SHA256SUMS"), "w") as f:
            for file, digest in sums.items():
                f.write(f"{digest}  {file}\n")
        entry = {"name": name, "heights": [first, last],
                 "anchor": records[-RECORD:-RECORD + 32][::-1].hex(),
                 "events_bin_bytes": [begin, end], "block_records": len(records) // RECORD,
                 "event_count": sum(struct.unpack_from("<Q", records, at + 40)[0]
                                    for at in range(0, len(records), RECORD)),
                 "sidecars": len(sidecar_sums), "sidecar_bytes": sidecar_bytes,
                 "source_records_sha256": hashlib.sha256(raw).hexdigest(),
                 "sha256": sums}
        provenance["windows"].append(entry)
        print(json.dumps(entry), flush=True)
    with open(os.path.join(args.dest, "provenance.json"), "w") as f:
        json.dump(provenance, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_subparsers(dest="mode", required=True)
    p = modes.add_parser("plan")
    p.add_argument("dest")
    p.add_argument("--window", action="append", required=True,
                   help="NAME:FIRST:LAST, heights inclusive")
    p.add_argument("--margin", type=int, default=1000,
                   help="blocks every window must end below the committed tip")
    p.set_defaults(run=plan)
    p = modes.add_parser("ranges")
    p.add_argument("dest")
    p.set_defaults(run=ranges)
    p = modes.add_parser("finish")
    p.add_argument("dest")
    p.add_argument("--sidecars", action="append", required=True, help="NAME=COUNT")
    p.add_argument("--source", required=True)
    p.add_argument("--revision", required=True)
    p.set_defaults(run=finish)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
