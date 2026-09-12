#!/usr/bin/env python3
"""Audit a closed fleet observation against the hardening rollout gate.

The rollout supervisor cannot certify this on its own. Its post-run assertion is
written for the launch duration, so a run held to a later, shorter gate never
reaches a `complete` phase, and a monitor that fails writes a deliberately thin
result -- `{passed, seconds, blocks, error}` with no provenance at all. The
productionization plan nonetheless requires reconciling worker source/binary,
operations, router, monitor and configuration identity across the canary, the
rollout and the observation, and says not to infer acceptance from an exit code.
That reconciliation has had no reusable tool; it has been done by hand, or by a
one-off script living inside each evidence bundle.

Two principles this encodes.

Thresholds are flags, and every default is the documented gate. Relaxing one is
sometimes the right call, but it is a decision that belongs in the record, so a
relaxed run carries it in the command line that produced the verdict and the
report names every threshold that departed from both the default and the value
the run was launched with. A weakened gate should be visible in the evidence
rather than buried in a tool's constants.

A budget that expired is not a measurement. When a monitor abandons a read at
its freshness limit, that bounds the true time from below and says nothing about
how long recovery actually took. This reports such a sample as an abandoned read
and never as a catch-up figure; establishing the real number means reading the
worker logs.

Reads an extracted observation directory. Opens no socket, contacts no host and
changes nothing.
"""
import argparse
import json
from pathlib import Path

# The gate in docs/transparent-pir/deployment.md.
DEFAULT_SECONDS = 43200
DEFAULT_BLOCKS = 300
DEFAULT_FRESHNESS = 30.0
DEFAULT_REPLICA_FRESHNESS = 60.0
MINIMUM_EXACT_QUERIES = 1000
MEMORY_HEADROOM = 0.20

# Identity that must agree across every stage. fleet_config_sha256 is
# deliberately absent: the canary and the full fleet legitimately run different
# configurations (managed_recent_workers covers one worker, then four), so
# requiring agreement across stages would manufacture a finding. It is still
# required to agree among the six observation workers.
CROSS_STAGE_IDENTITY = ('binary_sha256', 'roster_sha256')
WITHIN_RUN_IDENTITY = (
    'binary_sha256', 'roster_sha256', 'fleet_config_sha256', 'fleet_script_sha256',
    'monitor_sha256', 'storage_helper_sha256', 'headless_helper_sha256',
)


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return None


def read_ndjson(path):
    events = []
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        events.append({'event': 'UNPARSEABLE'})
    except FileNotFoundError:
        return None
    return events


def worker_dirs(root):
    return sorted(p for p in root.glob('fleet-*') if p.is_dir())


def thresholds_in_force(args):
    return {
        'seconds': args.seconds,
        'blocks': args.blocks,
        'freshness_seconds': args.freshness_seconds,
        'replica_freshness_seconds': args.replica_freshness_seconds,
    }


def relaxations(applied, launched):
    """Name every threshold weaker than the documented gate or the launch."""
    notes = []
    defaults = {
        'seconds': DEFAULT_SECONDS,
        'blocks': DEFAULT_BLOCKS,
        'freshness_seconds': DEFAULT_FRESHNESS,
        'replica_freshness_seconds': DEFAULT_REPLICA_FRESHNESS,
    }
    weaker_when_smaller = ('seconds', 'blocks')
    for key, default in defaults.items():
        value = applied[key]
        if value == default:
            continue
        weaker = value < default if key in weaker_when_smaller else value > default
        notes.append(
            f'{key} {value} vs documented gate {default}'
            f'{" (weaker)" if weaker else " (stricter)"}')
    for key, value in (launched or {}).items():
        if key in applied and applied[key] != value:
            notes.append(f'{key} audited at {applied[key]} but the run was '
                         f'launched requiring {value}')
    return notes


def audit_worker(path, args):
    """Return (blocking, review, facts) for one worker directory."""
    blocking, review = [], []
    name = path.name.removeprefix('fleet-')
    result = read_json(path / 'result.json')
    samples = read_ndjson(path / 'samples.ndjson')
    facts = {'worker': name, 'result': result}

    if samples is None:
        blocking.append(f'{name}: no samples.ndjson')
        return blocking, review, facts
    if any(e.get('event') == 'UNPARSEABLE' for e in samples):
        blocking.append(f'{name}: samples.ndjson has unparseable lines')

    start = next((e for e in samples if e.get('event') == 'start'), None)
    facts['launched'] = {
        'seconds': start.get('minimum_seconds'),
        'blocks': start.get('minimum_blocks'),
        'freshness_seconds': start.get('public_budget_seconds'),
        'replica_freshness_seconds': start.get('replica_budget_seconds'),
    } if start else None

    if result is None:
        blocking.append(
            f'{name}: no result.json, so this monitor never terminated; a '
            'cancelled monitor earns no acceptance credit and its identity '
            'cannot be read from its own result')
    elif not result.get('passed'):
        blocking.append(
            f'{name}: monitor failed after {result.get("seconds", 0):.0f}s and '
            f'{result.get("blocks")} blocks: {result.get("error")}')
    else:
        seconds, blocks = result.get('seconds', 0), result.get('blocks', 0)
        replica_blocks = result.get('replica_blocks', 0)
        if seconds < args.seconds:
            blocking.append(
                f'{name}: {seconds:.0f}s recorded, gate requires {args.seconds}s')
        if min(blocks, replica_blocks) < args.blocks:
            blocking.append(
                f'{name}: {blocks} public / {replica_blocks} replica blocks, '
                f'gate requires {args.blocks} of each')
        visibility = result.get('maximum_visibility_seconds')
        if visibility is not None and visibility > args.freshness_seconds:
            blocking.append(
                f'{name}: public visibility {visibility:.3f}s exceeds '
                f'{args.freshness_seconds}s')

    # Freshness from the samples, independent of whether the monitor terminated.
    public = [e for e in samples if e.get('event') == 'block_visible']
    replica = [e for e in samples if e.get('event') == 'canary_block_visible']
    for label, events, budget in (
            ('public', public, args.freshness_seconds),
            ('replica', replica, args.replica_freshness_seconds)):
        over = [e for e in events if e.get('seconds', 0) > budget]
        if over:
            worst = max(e['seconds'] for e in over)
            blocking.append(
                f'{name}: {len(over)} {label} sample(s) over {budget}s, worst '
                f'{worst:.3f}s. A sample at or just past the budget is the '
                'monitor abandoning the read, not a measured catch-up; read the '
                'worker log for the actual recovery time')
    facts['public_samples'] = len(public)
    facts['replica_samples'] = len(replica)

    if any(e.get('event') == 'chain_reorganized' for e in samples):
        review.append(
            f'{name}: a reorg reset the counted block samples; confirm the '
            'block gate is met after the reset')

    baseline = next((e for e in samples
                     if e.get('event') == 'routing_availability_baseline'), None)
    if baseline is None:
        blocking.append(f'{name}: no routing availability baseline')
    else:
        facts['routing'] = baseline.get('evidence', {})
        if not facts['routing'].get('available', False):
            blocking.append(f'{name}: routing unavailable at baseline')

    failures = sum(1 for e in samples if e.get('event') == 'http_transport_failure')
    recoveries = sum(1 for e in samples
                     if e.get('event') == 'http_transport_recovered')
    if failures:
        review.append(f'{name}: {failures} transport failure(s), '
                      f'{recoveries} recovered')
        if recoveries < failures:
            blocking.append(
                f'{name}: {failures - recoveries} transport failure(s) never '
                'recovered')

    worker_samples = [e for e in samples if e.get('event') == 'worker']
    if not worker_samples:
        blocking.append(f'{name}: no worker samples')
    else:
        blocking.extend(audit_worker_samples(name, worker_samples))
        facts['identity'], conflicts = sample_identity(worker_samples[0], result)
        blocking.extend(f'{name}: {c}' for c in conflicts)
    facts['worker_samples'] = len(worker_samples)
    return blocking, review, facts


def audit_worker_samples(name, samples):
    """Restart, OOM and memory-headroom findings across every sample."""
    blocking = []
    first = samples[0].get('facts', {})
    base_restarts = first.get('NRestarts')
    base_start = first.get('ExecMainStartTimestampMonotonic')
    for index, sample in enumerate(samples):
        got = sample.get('facts', {})
        if got.get('NRestarts') != base_restarts:
            blocking.append(
                f'{name}: NRestarts changed {base_restarts} -> '
                f'{got.get("NRestarts")} at sample {index}')
            break
    for index, sample in enumerate(samples):
        got = sample.get('facts', {})
        if got.get('ExecMainStartTimestampMonotonic') != base_start:
            blocking.append(f'{name}: the service restarted at sample {index}')
            break
    for index, sample in enumerate(samples):
        got = sample.get('facts', {})
        if got.get('oom') or got.get('oom_kill') or got.get('oom_group_kill'):
            blocking.append(f'{name}: OOM recorded at sample {index}')
            break
    worst, worst_index = None, None
    for index, sample in enumerate(samples):
        got = sample.get('facts', {})
        total, available = got.get('MemTotal'), got.get('MemAvailable')
        if not total or available is None:
            continue
        ratio = available / total
        if worst is None or ratio < worst:
            worst, worst_index = ratio, index
    if worst is not None and worst < MEMORY_HEADROOM:
        blocking.append(
            f'{name}: available host memory fell to {worst:.2%} at sample '
            f'{worst_index}, below the {MEMORY_HEADROOM:.0%} requirement')
    return blocking


def sample_identity(sample, result):
    """Identity for one worker, and where its own evidence contradicts itself.

    The worker attests a binary and helper digests through `/v1/ready` and the
    helper sections; its terminal result reports them again. Those must agree.
    Letting either source silently win would hide exactly the case the plan
    warns about -- a result whose claimed identity is not what the worker was
    actually running.
    """
    attested = {}
    ready = sample.get('ready') or {}
    if ready.get('binary_sha256'):
        attested['binary_sha256'] = ready['binary_sha256']
    for section in ('headless', 'storage'):
        digest = (sample.get(section) or {}).get('helper_sha256')
        if digest:
            attested[f'{section}_helper_sha256'] = digest

    identity, conflicts = dict(attested), []
    if isinstance(result, dict):
        for key in WITHIN_RUN_IDENTITY:
            reported = result.get(key)
            if not reported:
                continue
            if key in attested and attested[key] != reported:
                conflicts.append(
                    f'{key}: attested {attested[key][:12]}… but the result '
                    f'reports {reported[:12]}…')
            identity[key] = reported
    return identity, conflicts


def audit_identity(root, facts, args):
    """Reconcile identity within the run and against the earlier stages."""
    blocking, review = [], []
    provenance = read_json(root / 'provenance.json') or {}
    if not provenance:
        blocking.append('no observation provenance.json')
    initial_ready = read_json(root / 'initial-ready.json')
    if initial_ready is None:
        review.append('no initial-ready.json; pre-start warm state unverifiable')

    for key in WITHIN_RUN_IDENTITY:
        seen = {}
        for entry in facts:
            value = (entry.get('identity') or {}).get(key)
            if value:
                seen.setdefault(value, []).append(entry['worker'])
        if len(seen) > 1:
            blocking.append(
                f'{key} disagrees across the observation: ' +
                '; '.join(f'{v[:12]}… on {", ".join(w)}' for v, w in seen.items()))

    unknown = [e['worker'] for e in facts if not e.get('identity')]
    if unknown:
        blocking.append(
            'identity could not be established from the run for: ' +
            ', '.join(unknown))

    for stage, path in (('canary', args.canary_result),
                        ('fleet upgrade', args.upgrade_result)):
        if path is None:
            review.append(
                f'no {stage} result supplied; cross-stage reconciliation for it '
                'was not performed')
            continue
        other = read_json(path)
        if other is None:
            blocking.append(f'{stage} result {path} is unreadable')
            continue
        if not other.get('passed'):
            blocking.append(f'{stage} result did not pass')
        for key in CROSS_STAGE_IDENTITY:
            mine = provenance.get(key) or next(
                ((e.get('identity') or {}).get(key) for e in facts
                 if (e.get('identity') or {}).get(key)), None)
            theirs = other.get(key)
            if mine and theirs and mine != theirs:
                blocking.append(
                    f'{key} differs between the {stage} ({theirs[:12]}…) and '
                    f'this observation ({mine[:12]}…)')
    return blocking, review


def audit_queries(root, args):
    """Exactness over both sustained private-query clients."""
    blocking, review, totals = [], [], {}
    logs = sorted(root.glob('fleet-*/query-*.ndjson'))
    if not logs:
        blocking.append('no sustained query-client logs found')
        return blocking, review, totals
    for log in logs:
        events = read_ndjson(log) or []
        queries = [e for e in events if e.get('event') == 'query']
        inexact = [e for e in queries if e.get('exact') is not True]
        exact = len(queries) - len(inexact)
        retries = sum(1 for e in events if e.get('event') == 'retry')
        totals[log.name] = {'exact': exact, 'mismatches': len(inexact),
                            'retries': retries}
        if inexact:
            blocking.append(
                f'{log.name}: {len(inexact)} response(s) not exact; any '
                'mismatch fails the gate')
        if exact < args.minimum_queries:
            blocking.append(
                f'{log.name}: {exact} exact responses, below the '
                f'{args.minimum_queries} minimum')
        terminal = next((e for e in reversed(events)
                         if e.get('event') == 'result'), None)
        if terminal is None:
            review.append(f'{log.name}: no terminal result event; the client '
                          'was still running or was cancelled')
        elif not terminal.get('passed'):
            blocking.append(f'{log.name}: query client reported failure')
    return blocking, review, totals


def main():
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--observation', type=Path, required=True,
                   help='extracted observation directory')
    p.add_argument('--canary-result', type=Path)
    p.add_argument('--upgrade-result', type=Path)
    p.add_argument('--workers', type=int, default=6)
    p.add_argument('--seconds', type=float, default=DEFAULT_SECONDS)
    p.add_argument('--blocks', type=int, default=DEFAULT_BLOCKS)
    p.add_argument('--freshness-seconds', type=float, default=DEFAULT_FRESHNESS)
    p.add_argument('--replica-freshness-seconds', type=float,
                   default=DEFAULT_REPLICA_FRESHNESS)
    p.add_argument('--minimum-queries', type=int, default=MINIMUM_EXACT_QUERIES)
    p.add_argument('--out', type=Path)
    a = p.parse_args()

    root = a.observation
    if not root.is_dir():
        raise SystemExit(f'{root} is not a directory')

    blocking, review, facts = [], [], []
    dirs = worker_dirs(root)
    if len(dirs) != a.workers:
        blocking.append(
            f'{len(dirs)} worker director(ies) present, expected {a.workers}')
    for path in dirs:
        worker_blocking, worker_review, worker_facts = audit_worker(path, a)
        blocking.extend(worker_blocking)
        review.extend(worker_review)
        facts.append(worker_facts)

    identity_blocking, identity_review = audit_identity(root, facts, a)
    blocking.extend(identity_blocking)
    review.extend(identity_review)
    query_blocking, query_review, query_totals = audit_queries(root, a)
    blocking.extend(query_blocking)
    review.extend(query_review)

    applied = thresholds_in_force(a)
    launched = next((e['launched'] for e in facts if e.get('launched')), None)
    departures = relaxations(applied, launched)

    print('thresholds applied: ' + ', '.join(f'{k}={v}' for k, v in applied.items()))
    if departures:
        print('THRESHOLD DEPARTURES -- record these with the verdict:')
        for note in departures:
            print(f'  - {note}')
    else:
        print('thresholds are the documented gate, unmodified')
    print()
    print(f'{"worker":<34}{"seconds":>12}{"blocks":>9}{"samples":>9}  result')
    for entry in facts:
        result = entry.get('result')
        if result is None:
            print(f'{entry["worker"]:<34}{"-":>12}{"-":>9}'
                  f'{entry.get("worker_samples", 0):>9}  no result (cancelled)')
        else:
            print(f'{entry["worker"]:<34}{result.get("seconds", 0):>12.0f}'
                  f'{result.get("blocks", 0):>9}{entry.get("worker_samples", 0):>9}'
                  f'  {"passed" if result.get("passed") else "FAILED"}')
    if query_totals:
        print()
        for name, counts in query_totals.items():
            print(f'  {name}: {counts["exact"]} exact, '
                  f'{counts["mismatches"]} mismatches, {counts["retries"]} retries')
    print()
    if blocking:
        print(f'BLOCKING ({len(blocking)}) -- not acceptance:')
        for item in blocking:
            print(f'  - {item}')
    else:
        print('BLOCKING (0): every audited requirement met')
    print()
    if review:
        print(f'REVIEW ({len(review)}):')
        for item in review:
            print(f'  - {item}')
    else:
        print('REVIEW (0)')

    if a.out:
        if a.out.exists():
            raise SystemExit('output exists; audit records are not overwritten')
        a.out.write_text(json.dumps({
            'thresholds_applied': applied,
            'thresholds_launched': launched,
            'threshold_departures': departures,
            'workers': [{k: v for k, v in e.items() if k != 'result'} |
                        {'passed': bool((e.get('result') or {}).get('passed'))}
                        for e in facts],
            'queries': query_totals,
            'blocking': blocking,
            'review': review,
        }, indent=2, default=str) + '\n')
        print(f'\nwrote {a.out}')
    raise SystemExit(1 if blocking else 0)


if __name__ == '__main__':
    main()
