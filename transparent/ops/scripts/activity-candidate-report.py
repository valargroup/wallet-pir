#!/usr/bin/env python3
"""Produce a local immutable report from retained candidate qualification bytes."""
import argparse
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[3]/'ops/lib'))
spec = importlib.util.spec_from_file_location('candidate_report', HERE.parents[1]/'lib/activity_candidate_reports.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('gate', choices=('artifact-verification', 'native-certificates'))
    parser.add_argument('--input', required=True, help='local checksum-bound raw evidence references')
    parser.add_argument('--input-sha256', required=True)
    parser.add_argument('--out', required=True, help='new local report path; never overwritten')
    args = parser.parse_args()
    inputs = M.value({'path': str(Path(args.input).absolute()), 'sha256': args.input_sha256})
    if args.gate == 'artifact-verification':
        M.require(isinstance(inputs, dict) and set(inputs) == {'mapping', 'execution'}, 'invalid artifact inputs')
        report = M.artifact_report(inputs['mapping'], inputs['execution'])
    else:
        M.require(isinstance(inputs, dict) and set(inputs) == {'mapping', 'manifests', 'executions', 'certifier'},
                  'invalid certificate inputs')
        report = M.certificate_report(inputs['mapping'], inputs['manifests'], inputs['executions'], inputs['certifier'])
    M.write_report(args.out, report)
    print(args.out)


if __name__ == '__main__':
    main()
