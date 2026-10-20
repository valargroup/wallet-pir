#!/usr/bin/env python3
"""Read-only independent Status publication/resource capture; never grants authority."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from status.lib.publication_capture import capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True, help='Unified publisher configuration, credentials remain in files')
    parser.add_argument('--origin', default='http://127.0.0.1:8480')
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=21600)
    parser.add_argument('--unit', default='pir-publisher.service')
    parser.add_argument('--retained-source', type=Path)
    parser.add_argument('--retained-source-sha256')
    args = parser.parse_args()
    if bool(args.retained_source) != bool(args.retained_source_sha256):
        parser.error('retained source and its checksum must be supplied together')
    if not 1 <= args.seconds <= 21660:
        parser.error('seconds must be in 1..=21660')
    config = json.loads(args.config.read_text())['status']['settings']
    result = capture(config, args.origin, args.report_dir, args.seconds, args.unit,
                     retained_source=args.retained_source, retained_sha256=args.retained_source_sha256)
    print(json.dumps(result))
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
