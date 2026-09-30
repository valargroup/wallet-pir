#!/usr/bin/env python3
"""Read Actions timing evidence without downloading logs or touching production.

Use --run for stage/queue timings, or --workflow and --limit for completed-run
p95. Historical populations must be separated by warm/cold and deployment mode
before claiming the fast/routine latency targets.
"""
import argparse
from datetime import datetime, timezone
import json
import math
import os
import subprocess
import urllib.request


def api(path):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if token:
        request = urllib.request.Request(
            os.environ.get('GITHUB_API_URL', 'https://api.github.com') + '/' + path,
            headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28'})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    return json.loads(subprocess.check_output(['gh', 'api', path]))


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def elapsed(start, end):
    return round((timestamp(end) - timestamp(start)).total_seconds(), 3)


def percentile95(values):
    return sorted(values)[math.ceil(len(values) * .95) - 1] if values else None


def report(repo, run_id):
    run = api(f'repos/{repo}/actions/runs/{run_id}')
    attempt = run.get('run_attempt', 1)
    jobs = api(f'repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100')['jobs']
    # GitHub preserves created_at across reruns; run_started_at is reset when
    # a new attempt is requested, including its wait for a runner.
    started = run['run_started_at'] if attempt > 1 else run['created_at']
    now = datetime.now(timezone.utc).isoformat()
    result = {'run': run_id, 'attempt': attempt, 'timing_start': started, 'sha': run['head_sha'], 'status': run['status'], 'jobs': []}
    for job in jobs:
        if not job.get('started_at'):
            continue
        reused = timestamp(job['started_at']) < timestamp(started)
        result['jobs'].append({
            'reused_from_previous_attempt': reused,
            'name': job['name'], 'runner': job.get('runner_name'),
            'dispatch_to_job_start_seconds': None if reused else elapsed(started, job['started_at']),
            'job_seconds': elapsed(job['started_at'], job.get('completed_at') or now),
            'steps': [{'name': s['name'], 'seconds': elapsed(s['started_at'], s['completed_at']), 'conclusion': s['conclusion']}
                      for s in job['steps'] if s.get('started_at') and s.get('completed_at') and s['status'] == 'completed'],
        })
    # completed_at is more precise than run.updated_at, which can change later.
    ends = [j['completed_at'] for j in jobs if j.get('completed_at')]
    result['dispatch_seconds'] = max(0, elapsed(started, max(ends) if run['status'] == 'completed' and ends else now))
    result['total_dispatch_seconds'] = elapsed(run['created_at'], max(ends) if run['status'] == 'completed' and ends else now)
    result['complete'] = run['status'] == 'completed'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY', 'valargroup/wallet-pir'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--run')
    group.add_argument('--workflow')
    parser.add_argument('--limit', type=int, default=20)
    args = parser.parse_args()
    if args.run:
        result = report(args.repo, args.run)
    else:
        runs = api(f'repos/{args.repo}/actions/workflows/{args.workflow}/runs?status=completed&per_page={args.limit}')['workflow_runs']
        rows = [report(args.repo, r['id']) for r in runs if r['conclusion'] == 'success']
        result = {'workflow': args.workflow, 'successful_runs': len(rows),
                  'p95_dispatch_seconds': percentile95([r['dispatch_seconds'] for r in rows]), 'runs': rows}
    print(json.dumps(result, indent=2))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('\n## Actions timing evidence\n\nMeasured through the reporting step; final cleanup is excluded.\n\n```json\n' + json.dumps(result, indent=2) + '\n```\n')


if __name__ == '__main__':
    main()
