#!/usr/bin/env python3
"""Copy read-only height windows of the v3 event journal into standalone journals.

Run on the coordinator under nice/ionice. Reads the source journal without its
writer lock and writes nothing next to it. Each window becomes its own journal
directory that `EventStore::open_existing` reads:

- `events.bin`: the window's bytes, copied verbatim;
- `blocks.bin`: the window's 48-byte block records, offsets rebased to 0;
- `checkpoint.bin`: the copied lengths;
- `meta.json`: the source's version and genesis, `start_height` = window start;
- `display-v1/`, `display-v2x/`: every committed sidecar of a block in the
  window. The native reader treats a missing sidecar as "no oversized events",
  so a copy without them would silently drop events.

The source is only ever read below its committed checkpoint, and every window
must end at least `--margin` blocks below the committed tip, so an append or a
shallow reorganisation cannot move the copied bytes. The block records are read
again after copying and must be unchanged.

Usage:
  slice-journal.py --journal DIR --out DIR --window NAME:FIRST:LAST [--window ...]
"""
import argparse
import hashlib
import json
import os
import shutil
import struct
import sys

RECORD = 48
CHUNK = 64 << 20
SIDECAR_DIRS = ("display-v1", "display-v2x")


def read_checkpoint(journal):
    raw = open(os.path.join(journal, "checkpoint.bin"), "rb").read()
    if len(raw) != 16:
        sys.exit("checkpoint is not 16 bytes")
    return struct.unpack("<QQ", raw)


def read_records(journal, first_index, count):
    with open(os.path.join(journal, "blocks.bin"), "rb") as f:
        f.seek(first_index * RECORD)
        raw = f.read(count * RECORD)
    if len(raw) != count * RECORD:
        sys.exit("blocks.bin is shorter than requested")
    return raw


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--window", action="append", required=True,
                        help="NAME:FIRST:LAST, heights inclusive")
    parser.add_argument("--margin", type=int, default=1000,
                        help="blocks every window must end below the committed tip")
    parser.add_argument("--free-floor", type=float, default=0.20,
                        help="fraction of the output filesystem that must stay free")
    args = parser.parse_args()

    meta = json.load(open(os.path.join(args.journal, "meta.json")))
    if meta.get("version") != 3:
        sys.exit(f"journal version {meta.get('version')}, expected 3")
    start = meta["start_height"]
    events_len, blocks_len = read_checkpoint(args.journal)
    if blocks_len % RECORD:
        sys.exit("checkpoint blocks length is not whole records")
    covered = start + blocks_len // RECORD - 1

    windows = []
    for text in args.window:
        name, first, last = text.split(":")
        first, last = int(first), int(last)
        if not (start <= first <= last <= covered - args.margin):
            sys.exit(f"window {text} is outside {start}-{covered - args.margin}")
        windows.append((name, first, last))

    # Disk check before any byte is written.
    need = 0
    plans = []
    for name, first, last in windows:
        count = last - first + 1
        records = read_records(args.journal, first - start, count)
        begin = struct.unpack_from("<Q", records, 32)[0]
        if last + 1 <= covered:
            end = struct.unpack_from("<Q", read_records(args.journal, last + 1 - start, 1), 32)[0]
        else:
            end = events_len
        plans.append((name, first, last, records, begin, end))
        need += end - begin + len(records)
    os.makedirs(args.out, exist_ok=True)
    usage = shutil.disk_usage(args.out)
    if usage.free - need < usage.total * args.free_floor:
        sys.exit(f"copying {need} bytes would leave less than {args.free_floor:.0%} free")
    print(f"source {args.journal}: heights {start}-{covered}, events {events_len} B; copying {need} B",
          flush=True)

    provenance = {"source": os.path.abspath(args.journal), "source_meta": meta,
                  "source_checkpoint": [events_len, blocks_len], "source_covered": covered,
                  "windows": []}
    for name, first, last, records, begin, end in plans:
        target = os.path.join(args.out, name)
        if os.path.exists(target):
            sys.exit(f"{target} exists; refusing to overwrite")
        os.makedirs(target)
        with open(os.path.join(args.journal, "events.bin"), "rb") as src, \
                open(os.path.join(target, "events.bin"), "wb") as dst:
            src.seek(begin)
            left = end - begin
            while left:
                chunk = src.read(min(CHUNK, left))
                if not chunk:
                    sys.exit("events.bin ended early")
                dst.write(chunk)
                left -= len(chunk)
        rebased = bytearray(records)
        sidecars = 0
        for i in range(len(records) // RECORD):
            at = i * RECORD
            offset = struct.unpack_from("<Q", records, at + 32)[0]
            struct.pack_into("<Q", rebased, at + 32, offset - begin)
            display = records[at:at + 32][::-1].hex() + ".bin"
            for sub in SIDECAR_DIRS:
                path = os.path.join(args.journal, sub, display)
                if os.path.exists(path):
                    os.makedirs(os.path.join(target, sub), exist_ok=True)
                    shutil.copyfile(path, os.path.join(target, sub, display))
                    sidecars += 1
        with open(os.path.join(target, "blocks.bin"), "wb") as f:
            f.write(rebased)
        with open(os.path.join(target, "checkpoint.bin"), "wb") as f:
            f.write(struct.pack("<QQ", end - begin, len(rebased)))
        with open(os.path.join(target, "meta.json"), "w") as f:
            json.dump({"version": 3, "genesis_hash": meta["genesis_hash"],
                       "start_height": first}, f, indent=2)
        # The source must not have moved under the copy.
        if read_records(args.journal, first - start, last - first + 1) != records:
            sys.exit(f"{name}: source block records changed during the copy")
        anchor = records[-RECORD:-RECORD + 32][::-1].hex()
        sums = {f: sha256_file(os.path.join(target, f))
                for f in ("events.bin", "blocks.bin", "checkpoint.bin", "meta.json")}
        with open(os.path.join(target, "SHA256SUMS"), "w") as f:
            for file, digest in sums.items():
                f.write(f"{digest}  {file}\n")
        entry = {"name": name, "heights": [first, last], "anchor": anchor,
                 "events_bin_bytes": [begin, end], "block_records": len(records) // RECORD,
                 "sidecars": sidecars, "sha256": sums}
        provenance["windows"].append(entry)
        print(json.dumps(entry), flush=True)
    with open(os.path.join(args.out, "provenance.json"), "w") as f:
        json.dump(provenance, f, indent=2)


if __name__ == "__main__":
    main()
