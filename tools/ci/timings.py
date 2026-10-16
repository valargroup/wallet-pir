#!/usr/bin/env python3
"""Read Actions timing evidence without downloading logs or touching production.

Use --run for stage/queue timings, or --workflow and --limit for completed-run
p95. Historical populations must be separated by warm/cold and deployment mode
before claiming the fast/routine latency targets.

dispatch_to_job_start is not runner queue time: it includes waiting for `needs`
jobs, concurrency groups and environments. Step phases come from the jobs API;
the Rust setup step includes hosted cache restore. --cache-records reads each
job's log for the sanitized records: CI_CACHE_IDENTITY/RESTORE/REPORT (identity,
restore status, setup and restore seconds, fingerprint inventory),
CI_CARGO_ARTIFACTS (fresh and compiled units from Cargo's own JSON messages
for the commands the job executed) and CI_STAGE_REPORT (compile, execution and
other stage seconds; null when no such stage ran).
"""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
import subprocess
import urllib.error
import urllib.request

DEFINITIONS = {
    'dispatch_to_job_start_seconds': 'current attempt start to job start; includes needs, concurrency and environment waits, not only runner queue',
    'phases.setup_seconds': 'job setup, checkout, comparison fetch, selection and Rust setup including hosted cache restore',
    'phases.work_seconds': 'remaining steps (compilation and execution; see cache_records for the split)',
    'phases.post_seconds': 'post steps, including hosted cache save',
    'phases.report_seconds': 'reporting steps',
    'cache_records.CI_CARGO_ARTIFACTS': 'units in compiler-artifact messages of executed Cargo build/check/clippy/test commands; compiled if any reported fresh=false',
    'cache_records.CI_CACHE_REPORT.fingerprint_inventory': 'fingerprint directories at restore vs job end by content; inventory only, not what Cargo compiled',
    'cache_records.CI_STAGE_REPORT.compile_stage_seconds': 'whole compile/lint Cargo commands, including resolution, downloads, build scripts and linking; null if none ran',
}
SETUP = ('Set up job', 'Run actions/checkout', 'Run ./.github/actions/rust-setup', 'Run dtolnay/',
         'Run Swatinem/', 'Fetch comparison revision', 'Select affected', 'Install ', 'Identify ', 'Restore ',
         'Record restored', 'Run hashicorp/setup-terraform')


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ops/lib'))
from wallet_pir_ops.github_api import api as github_api  # noqa: E402


def api(path):
    # Best-effort reporting must fit the short gate's remaining time budget.
    return github_api(path, timeout=3, attempts=2)


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
        # Colored Cargo output contains escape sequences, which newer gh refuses
        # to print without this flag; older gh lacks the flag.
        for flags in (['--allow-escape-sequences'], []):
            try:
                return subprocess.check_output(['gh', 'api', *flags, path], text=True, stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError:
                if not flags:
                    raise
    except (urllib.error.URLError, subprocess.CalledProcessError, OSError):
        return None


def cache_records(log):
    """Sanitized CI_CACHE_*, CI_CARGO_ARTIFACTS and CI_STAGE_REPORT records from a job log."""
    records = []
    for line in (log or '').splitlines():
        for tag in ('CI_SELECTION', 'CI_CACHE_IDENTITY', 'CI_CACHE_RESTORE', 'CI_CACHE_REPORT', 'CI_CARGO_ARTIFACTS', 'CI_STAGE_REPORT'):
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
    result = {'run': run_id, 'attempt': attempt, 'definitions': DEFINITIONS, 'timing_start': started, 'sha': run['head_sha'], 'status': run['status'], 'event': run.get('event'),
              'branch': run.get('head_branch'), 'conclusion': run.get('conclusion'), 'jobs': []}
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


def feedback_slo(rows, target=180):
    """Qualify representative warm main pushes, never workflow-only or cold runs."""
    eligible, excluded, failures, covered = [], [], [], set()
    required = {'transparent', 'enhance', 'receiver', 'shared', 'docs', 'ops'}
    for row in rows:
        if row.get('event') != 'push' or row.get('branch') != 'main':
            excluded.append({'run': row['run'], 'reason': 'not a main push'})
            continue
        if row.get('conclusion') == 'cancelled':
            excluded.append({'run': row['run'], 'reason': 'cancelled'})
            continue
        if not row.get('complete') or row.get('conclusion') != 'success':
            failures.append(row['run'])
            continue
        jobs = [job for job in row['jobs'] if job['name'] == 'Fast checks']
        records = jobs[0].get('cache_records') or [] if len(jobs) == 1 else []
        selections = [r for r in records if r['type'] == 'CI_SELECTION']
        if len(selections) != 1 or selections[0]['category'] == 'ci':
            excluded.append({'run': row['run'], 'reason': 'missing selection evidence or workflow-only change'})
            continue
        selection = selections[0]
        if selection.get('package_count', len(selection['families'])):
            builds = [r for r in records if r['type'] == 'CI_CARGO_ARTIFACTS']
            if len(builds) != 1 or builds[0].get('third_party', {}).get('compiled') != 0:
                excluded.append({'run': row['run'], 'reason': 'cold or unknown dependency compilation'})
                continue
        covered.update(selection['families'])
        covered.add(selection['category'])
        eligible.append({'run': row['run'], 'sha': row['sha'], 'seconds': row['dispatch_seconds'],
                         'category': selection['category']})
    p95 = percentile95([row['seconds'] for row in eligible])
    return {'target_seconds': target, 'samples': eligible, 'sample_count': len(eligible),
            'p95_seconds': p95, 'missing_categories': sorted(required-covered),
            'failed_or_incomplete_runs': failures, 'excluded': excluded,
            'accepted': p95 is not None and p95 < target and not (required-covered) and not failures}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY', 'valargroup/wallet-pir'))
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--run')
    group.add_argument('--workflow')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--cache-records', action='store_true', help='read job logs for sanitized cache/stage records')
    parser.add_argument('--slo', action='store_true', help='qualify warm main feedback with selection/cache evidence; needs 20 representative runs')
    parser.add_argument('--since-run', help='start the SLO window at this Actions run; excludes older pipeline versions')
    args = parser.parse_args()
    if args.slo and (not args.workflow or not args.since_run):
        parser.error('--slo requires --workflow and --since-run')
    if args.run:
        result = report(args.repo, args.run, args.cache_records)
    else:
        runs = api(f'repos/{args.repo}/actions/workflows/{args.workflow}/runs?status=completed&per_page={args.limit}')['workflow_runs']
        anchor = api(f'repos/{args.repo}/actions/runs/{args.since_run}') if args.slo else None
        if anchor:
            runs = [run for run in runs if timestamp(run['created_at']) >= timestamp(anchor['created_at'])]
        rows = [report(args.repo, r['id'], args.cache_records or args.slo) for r in runs
                if args.slo or r['conclusion'] == 'success']
        result = {'workflow': args.workflow, 'successful_runs': sum(r.get('conclusion') == 'success' for r in rows),
                  'p95_dispatch_seconds': percentile95([r['dispatch_seconds'] for r in rows]), 'runs': rows}
    if args.slo:
        result['slo'] = {**feedback_slo(rows), 'since_run': args.since_run,
                         'since_created_at': anchor['created_at']}
    print(json.dumps(result, indent=2))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('\n## Actions timing evidence\n\nMeasured through the reporting step; final cleanup is excluded.\n\n```json\n' + json.dumps(result, indent=2) + '\n```\n')

    if args.slo and not result['slo']['accepted']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
