#!/usr/bin/env python3
"""Summarize archived observations without averaging percentiles or hiding errors."""
import collections
import json
import pathlib
import re
import tarfile

root = pathlib.Path(__file__).resolve().parent
groups = collections.defaultdict(list)
with tarfile.open(root / "raw/production-loads.tar.gz") as archive:
    for member in archive:
        if not member.isfile():
            continue
        if re.search(r"/load-\d+\.json$", member.name):
            report = json.load(archive.extractfile(member))
            groups[member.name.split('/')[0]].append(report)
        elif member.name == "mapped-rollout-4d14feb/load.log":
            # This run's complete reports were also printed verbatim to its log.
            log = archive.extractfile(member).read().decode()
            decoder = json.JSONDecoder()
            for match in re.finditer(r'^\{', log, re.MULTILINE):
                try:
                    report, _ = decoder.raw_decode(log[match.start():])
                    if report.get('protocol'):
                        groups[member.name.split('/')[0]].append(report)
                except json.JSONDecodeError:
                    pass

summary = {}
for run, reports in sorted(groups.items()):
    errors = collections.Counter()
    for report in reports:
        errors.update(report['errors'])
    measured = [r for r in reports if r['completed'] > 0]
    summary[run] = {
        'completed_reports': len(reports),
        'queries': sum(r['completed'] for r in reports),
        'correct': sum(r['succeeded'] for r in reports),
        'incorrect': sum(r['incorrect_answers'] for r in reports),
        'unstarted_arrivals': sum(r['unstarted_arrivals'] for r in reports),
        'errors': dict(errors),
        'per_report_p99_ms_range': [min(r['p99_ms'] for r in measured), max(r['p99_ms'] for r in measured)],
        'per_report_scheduled_p99_ms_range': [min(r['scheduled_p99_ms'] for r in measured), max(r['scheduled_p99_ms'] for r in measured)],
    }
print(json.dumps(summary, indent=2, sort_keys=True))
