#!/usr/bin/env python3
"""List a window's committed display sidecars on the coordinator. Read-only.

fetch-slices.sh passes this file on ssh stdin; it is never written on the
coordinator. Run from the journal directory:

  python3 - size  RECORD_INDEX RECORD_COUNT   # prints "COUNT BYTES"
  python3 - list  RECORD_INDEX RECORD_COUNT   # NUL-separated paths for tar -T

Reads the window's 48-byte records from blocks.bin (`block_hash[32] | offset |
event_count`, below the committed checkpoint by fetch-slices.sh's plan) and
names each existing `display-v1/<hash>.bin` and `display-v2x/<hash>.bin`, the
display hash being the record's internal bytes reversed. Only sidecars of
committed blocks are named; reorged sidecars stay behind.
"""
import os
import sys

RECORD = 48
SIDECAR_DIRS = ("display-v1", "display-v2x")


def main():
    mode, index, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    with open("blocks.bin", "rb") as f:
        f.seek(index * RECORD)
        raw = f.read(count * RECORD)
    if len(raw) != count * RECORD:
        sys.exit("blocks.bin is shorter than requested")
    files = total = 0
    out = sys.stdout.buffer
    for at in range(0, len(raw), RECORD):
        name = raw[at:at + 32][::-1].hex() + ".bin"
        for sub in SIDECAR_DIRS:
            path = os.path.join(sub, name)
            try:
                size = os.stat(path).st_size
            except FileNotFoundError:
                continue
            files += 1
            total += size
            if mode == "list":
                out.write(path.encode() + b"\0")
    if mode == "size":
        print(files, total)
    elif mode != "list":
        sys.exit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
