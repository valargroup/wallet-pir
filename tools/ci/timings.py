#!/usr/bin/env python3
"""Read Actions timing evidence without downloading logs or touching production.

Use --run for stage/queue timings, or --workflow and --limit for completed-run
p95. Historical populations must be separated by warm/cold and deployment mode
before claiming the fast/routine latency targets.

dispatch_to_job_start is not runner queue time: it includes waiting for `needs`
jobs, concurrency groups and environments. Step phases come from the jobs API;
the Rust setup step includes hosted cache restore. --cache-records reads each
job's log for the sanitized CI_CACHE_* records, which split setup, restore,
compilation and execution and count fresh/rebuilt Cargo units.
"""
import argparse
from datetime import datetime, timezone
import json
import math
import os
import subprocess
import urllib.error
import urllib.request

DEFINITIONS = {
    'dispatch_to_job_start_seconds': 'current attempt start to job start; includes needs, concurrency and environment waits, not only runner queue',
    'phases.setup_seconds': 'job setup, checkout, comparison fetch, selection and Rust setup including hosted cache restore',
    'phases.work_seconds': 'remaining steps (compilation and execution; see cache_records for the split)',
    'phases.post_seconds': 'post steps, including hosted cache save',
    'phases.report_seconds': 'reporting steps',
}
SETUP = ('Set up job', 'Run actions/checkout', 'Run ./.github/actions/rust-setup', 'Run dtolnay/',
         'Run Swatinem/', 'Fetch comparison revision', 'Select affected', 'Install ', 'Identify ', 'Restore ',
         'Record restored', 'Run hashicorp/setup-terraform')


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


class _DropAuthOnRedirect(urllib.request.HTTPRedirectHandler):
    """Job logs redirect to presigned storage, which must not receive the token."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None:
            new.remove_header('Authorization')
        return new


def job_log(repo, job_id):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    path = f'repos/{repo}/actions/jobs/{job_id}/logs'
    try:
        if token:
            request = urllib.request.Request(
                os.environ.get('GITHUB_API_URL', 'https://api.github.com') + '/' + path,
                headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json'})
            with urllib.request.build_opener(_DropAuthOnRedirect).open(request, timeout=60) as response:
                return response.read().decode(errors='replace')
        return subprocess.check_output(['gh', 'api', path], text=True, stderr=subprocess.DEVNULL)
    except (urllib.error.URLError, subprocess.CalledProcessError, OSError):
        return None


def cache_records(log):
    """Sanitized CI_CACHE_* and CI_STAGE_REPORT records from a job log."""
    records = []
    for line in (log or '').splitlines():
        for tag in ('CI_CACHE_IDENTITY', 'CI_CACHE_RESTORE', 'CI_CACHE_REPORT', 'CI_STAGE_REPORT'):
            marker = line.find(tag + ' {')
            if marker >= 0:
                try:
                    records.append({'type': tag, **json.loads(line[marker + len(tag) + 1:])})
                except json.JSONDecodeError:
                    pass
    return records


def phase(name):
    if name.startswith(SETUP):
        return 'setup_seconds'
    if name.startswith('Post ') or name == 'Complete job':
        return 'post_seconds'
    if name.startswith('Report '):
        return 'report_seconds'
    return 'work_seconds'


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def elapsed(start, end):
    return round((timestamp(end) - timestamp(start)).total_seconds(), 3)


def percentile95(values):
    return sorted(values)[math.ceil(len(values) * .95) - 1] if len(values) >= 20 else None


def report(repo, run_id, records=False):
    run = api(f'repos/{repo}/actions/runs/{run_id}')
    attempt = run.get('run_attempt', 1)
    jobs = api(f'repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100')['jobs']
    # GitHub preserves created_at across reruns; run_started_at is reset when
    # a new attempt is requested, including its wait for a runner.
    started = run['run_started_at'] if attempt > 1 else run['created_at']
    now = datetime.now(timezone.utc).isoformat()
    result = {'run': run_id, 'attempt': attempt, 'definitions': DEFINITIONS, 'timing_start': started, 'sha': run['head_sha'], 'status': run['status'], 'jobs': []}
    for job in jobs:
        if not job.get('started_at'):
            continue
        reused = timestamp(job['started_at']) < timestamp(started)
        steps = [s for s in job['steps'] if s.get('started_at') and s.get('completed_at') and s['status'] == 'completed']
        phases = {key: 0.0 for key in ('setup_seconds', 'work_seconds', 'post_seconds', 'report_seconds')}
        for step in steps:
            phases[phase(step['name'])] = round(phases[phase(step['name'])] + elapsed(step['started_at'], step['completed_at']), 3)
        result['jobs'].append({
            'reused_from_previous_attempt': reused,
            'name': job['name'], 'runner': job.get('runner_name'),
            'dispatch_to_job_start_seconds': None if reused else elapsed(started, job['started_at']),
            'job_seconds': elapsed(job['started_at'], job.get('completed_at') or now),
            'phases': phases,
            'steps': [{'name': s['name'], 'seconds': elapsed(s['started_at'], s['completed_at']), 'conclusion': s['conclusion']}
                      for s in steps],
        })
        if records and job.get('id') and not reused:
            log = job_log(repo, job['id'])
            result['jobs'][-1]['cache_records'] = cache_records(log) if log is not None else None
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
    parser.add_argument('--cache-records', action='store_true', help='read job logs for sanitized cache/stage records')
    args = parser.parse_args()
    if args.run:
        result = report(args.repo, args.run, args.cache_records)
    else:
        runs = api(f'repos/{args.repo}/actions/workflows/{args.workflow}/runs?status=completed&per_page={args.limit}')['workflow_runs']
        rows = [report(args.repo, r['id'], args.cache_records) for r in runs if r['conclusion'] == 'success']
        result = {'workflow': args.workflow, 'successful_runs': len(rows),
                  'p95_dispatch_seconds': percentile95([r['dispatch_seconds'] for r in rows]), 'runs': rows}
    print(json.dumps(result, indent=2))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('\n## Actions timing evidence\n\nMeasured through the reporting step; final cleanup is excluded.\n\n```json\n' + json.dumps(result, indent=2) + '\n```\n')


if __name__ == '__main__':
    main()
