"""Elapsed deployment time, derived from a transaction journal's timestamped events.

Every journal event already carries the runner's wall-clock time, so durations
are computed from the journal rather than stored beside it: retained journals
gain timing without any rewrite. The clocks are:

- total: transaction creation to its final status, including lock waits after
  creation, transfers, retries and readiness polling;
- preparation: creation until the first unit is changed (staging, executable
  checks and product qualification, while the previous processes still run);
- interruption: the first restart issued in the fleet until the last changed
  target is verified ready, plus each target's own gap split into the restart
  itself (stop and start) and readiness polling;
- recovery: the first restoration until the last restored target.

Time before the journal exists (source submission, compilation, queueing for
the production lock) is outside this record; `source_to_accepted` is reported
only when the caller recorded `source.submitted_unix`.
"""

FINAL_STATUSES = ('committed', 'failed', 'rolled-back', 'held')
PHASE_CLOCKS = ('preparation', 'staging', 'qualification', 'activation', 'verification', 'recovery')


def _first(events, predicate):
    return next((event['unix'] for event in events if predicate(event)), None)


def _last(events, predicate):
    return next((event['unix'] for event in reversed(events) if predicate(event)), None)


def _span(start, end):
    return None if start is None or end is None else round(max(0.0, end - start), 3)


def _paired(events, start_message, end_message):
    """Total seconds between each start event and the next matching end."""
    total, started = 0.0, None
    for event in events:
        if event['message'] == start_message:
            started = event['unix']
        elif event['message'] == end_message and started is not None:
            total += event['unix'] - started
            started = None
    return round(total, 3)


def journal_timing(data):
    """Durations in seconds for one journal, `None` where a clock has not run."""
    events = data.get('events', [])

    def status(name):
        return lambda event: event['message'] == 'status ' + name

    created = data.get('created_unix')
    final = _last(events, lambda event: event['message'] in ['status ' + name for name in FINAL_STATUSES])
    activating = _first(events, status('activating'))
    verifying = _first(events, status('verifying'))
    staging = _first(events, lambda event: event['message'] == 'staging')
    first_check = _first(events, lambda event: event['message'] in ('product check', 'product exact check'))
    restored = [event for event in events if event['message'] in ('phase restoring', 'phase restored')]
    targets = {}
    for event in events:
        target = event.get('target')
        if target is None or not event['message'].startswith('phase '):
            continue
        entry = targets.setdefault(target, {})
        phase = event['message'][len('phase '):]
        if phase == 'restarting':
            entry.setdefault('restart_issued', event['unix'])
        elif phase == 'verifying' and 'restart_issued' in entry:
            entry.setdefault('restart_returned', event['unix'])
        elif phase in ('verified', 'restored') and 'restart_issued' in entry:
            entry['ready'] = event['unix']
    gaps = {target: {'interruption': _span(entry.get('restart_issued'), entry.get('ready')),
                     'restart': _span(entry.get('restart_issued'), entry.get('restart_returned')),
                     'readiness': _span(entry.get('restart_returned'), entry.get('ready'))}
            for target, entry in targets.items() if 'restart_issued' in entry}
    issued = [entry['restart_issued'] for entry in targets.values() if 'restart_issued' in entry]
    ready = [entry['ready'] for entry in targets.values() if 'ready' in entry]
    clocks = {
        'total': _span(created, final),
        'preparation': _span(created, activating),
        'staging': _span(staging, first_check or activating),
        'qualification': round(_paired(events, 'product check', 'product check finished')
                               + _paired(events, 'exact check', 'exact check finished'), 3),
        'activation': _span(activating, verifying),
        'verification': _span(verifying, final if data.get('status') == 'committed' else None),
        'interruption': _span(min(issued), max(ready)) if issued and ready else None,
        'recovery': _span(restored[0]['unix'], restored[-1]['unix']) if restored else None,
    }
    source = data.get('source')
    submitted = source.get('submitted_unix') if isinstance(source, dict) else None
    clocks['source_to_accepted'] = (_span(submitted, final)
                                    if isinstance(submitted, (int, float)) and data.get('status') == 'committed'
                                    else None)
    measured = [(name, clocks[name]) for name in PHASE_CLOCKS if clocks[name]]
    largest = [name for name, _ in sorted(measured, key=lambda item: -item[1])[:3]]
    return {'clocks': clocks, 'targets': gaps, 'largest': largest}


def describe(timing):
    """Operator lines for `status`, leaving out clocks that have not run."""
    clocks = timing['clocks']
    lines = ['  timing: ' + ', '.join('%s %.1fs' % (name, value)
                                      for name, value in clocks.items() if value is not None)]
    if timing['largest']:
        lines.append('  largest contributors: ' + ', '.join(timing['largest']))
    for target, gap in sorted(timing['targets'].items()):
        lines.append('  %s gap: %s' % (target, ', '.join('%s %.1fs' % (name, value)
                                                         for name, value in gap.items() if value is not None)))
    return lines
