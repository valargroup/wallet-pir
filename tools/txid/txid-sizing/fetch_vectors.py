#!/usr/bin/env python3
"""Retrieve exact public vector files or verify a local immutable source."""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request


def verify_bytes(data, expected):
    if hashlib.sha256(data).hexdigest()!=expected:
        raise ValueError("source checksum mismatch")


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("pins",type=Path);p.add_argument("directory",type=Path)
    p.add_argument("--fetch",action="store_true")
    args=p.parse_args();pins=json.loads(args.pins.read_bytes())
    if args.fetch: args.directory.mkdir(parents=True,exist_ok=True)
    for b in pins["blocks"]:
        target=args.directory/b["file"]
        if args.fetch and not target.exists():
            data=urllib.request.urlopen(pins["base_url"]+b["file"],timeout=30).read()
            verify_bytes(data,b["text_sha256"]);target.write_bytes(data)
        verify_bytes(target.read_bytes(),b["text_sha256"])
    names={p.name for p in args.directory.glob("block-main-*.txt") if "bad" not in p.name}
    if names!={b["file"] for b in pins["blocks"]}:
        raise ValueError("source selection differs from pinned inventory")
    print("verified", len(pins["blocks"]), "immutable public block vectors")


if __name__ == "__main__": main()
