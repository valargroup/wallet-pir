#!/usr/bin/env python3
"""Assemble inert local v2 cutover requests after all candidate gates pass."""
import argparse
from pathlib import Path
import sys
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[3]/'ops/lib'), str(HERE.parents[1]/'lib')]
from activity_candidate_inputs import assemble, value, write_requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--input-sha256', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    requests = assemble(value({'path': str(Path(args.input).absolute()), 'sha256': args.input_sha256}))
    write_requests(args.out, requests)
    print(args.out)


if __name__ == '__main__':
    main()
